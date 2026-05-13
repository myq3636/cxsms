package com.king.gmms.processor;

import java.util.Set;
import java.util.UUID;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

/**
 * Core-owned delayed IN_DR dispatcher.
 */
public class DelayedInDRDispatcher extends Thread {
	private static final SystemLogger log = SystemLogger.getSystemLogger(DelayedInDRDispatcher.class);
	private static final DelayedInDRDispatcher instance = new DelayedInDRDispatcher();

	private static final String READY_KEY_PREFIX = "zset:delay:outdr:";
	private static final String PROCESSING_KEY_PREFIX = "zset:delay:outdr:processing:";
	private static final String PAYLOAD_KEY_PREFIX = "delay:outdr:payload:";

	private volatile boolean running = false;
	private GmmsUtility gmmsUtility;
	private RedisClient redis;
	private DBBackupHandler dbHandler;

	private DelayedInDRDispatcher() {
	}

	public static DelayedInDRDispatcher getInstance() {
		return instance;
	}

	public synchronized void startDispatcher() {
		if (running) {
			return;
		}
		gmmsUtility = GmmsUtility.getInstance();
		redis = gmmsUtility.getRedisClient();
		dbHandler = DBBackupHandler.getInstance();
		running = true;
		Thread thread = new Thread(A2PThreadGroup.getInstance(), this, "DelayedInDRDispatcher");
		thread.setDaemon(true);
		thread.start();
		log.info("DelayedInDRDispatcher started. enabled={}", isEnabled());
	}

	public synchronized void stopDispatcher() {
		running = false;
		log.info("DelayedInDRDispatcher stopped.");
	}

	public boolean schedule(GmmsMessage message, int delaySeconds) {
		if (message == null) {
			return true;
		}
		try {
			if (redis == null) {
				redis = GmmsUtility.getInstance().getRedisClient();
			}
			int shardId = calculateShardId(message);
			String payloadKey = PAYLOAD_KEY_PREFIX + shardId + ":" + buildDelayId(message);
			String body = SerializableHandler.convertGmmsMessage2RedisMessage(message);
			if (body == null) {
				return false;
			}
			int ttlSeconds = payloadTtlSeconds(message, delaySeconds);
			long dueAt = System.currentTimeMillis() + delaySeconds * 1000L;
			if (!redis.setString(payloadKey, body, ttlSeconds)) {
				return false;
			}
			Long added = redis.zadd(readyKey(shardId), dueAt, payloadKey);
			boolean success = added != null && added.longValue() >= 0L;
			if (!success) {
				redis.del(payloadKey);
			}
			if (success) {
				log.debug(message, "Scheduled Auto IN_DR direct dispatch. shardId={}, delaySeconds={}, payloadKey={}",
						shardId, delaySeconds, payloadKey);
			}
			return success;
		} catch (Exception e) {
			log.warn(message, "Failed to schedule Auto IN_DR direct dispatch", e);
			return false;
		}
	}

	@Override
	public void run() {
		while (running) {
			try {
				if (!isEnabled()) {
					Thread.sleep(idleSleepMs());
					continue;
				}
				int processed = dispatchDueMessages();
				if (processed <= 0) {
					Thread.sleep(idleSleepMs());
				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				log.error("DelayedInDRDispatcher loop error", e);
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignored) {
					break;
				}
			}
		}
	}

	private int dispatchDueMessages() {
		Set<Integer> shards = gmmsUtility.getMyShards();
		if (shards == null || shards.isEmpty()) {
			return 0;
		}
		int processed = 0;
		long now = System.currentTimeMillis();
		for (Integer shardId : shards) {
			if (shardId == null) {
				continue;
			}
			requeueExpiredProcessing(shardId.intValue(), now);
			for (int i = 0; i < batchSize(); i++) {
				String payloadKey = redis.claimDueZSetMember(readyKey(shardId.intValue()),
						processingKey(shardId.intValue()), now, now + claimTimeoutMs());
				if (payloadKey == null) {
					break;
				}
				handleClaimedPayload(shardId.intValue(), payloadKey);
				processed++;
			}
		}
		return processed;
	}

	private void requeueExpiredProcessing(int shardId, long now) {
		for (int i = 0; i < batchSize(); i++) {
			String payloadKey = redis.claimDueZSetMember(processingKey(shardId), readyKey(shardId), now, now);
			if (payloadKey == null) {
				return;
			}
			log.warn("Requeued expired Auto IN_DR processing payload. shardId={}, payloadKey={}", shardId, payloadKey);
		}
	}

	private void handleClaimedPayload(int shardId, String payloadKey) {
		GmmsMessage message = null;
		try {
			String body = redis.getString(payloadKey);
			if (body == null) {
				redis.zrem(processingKey(shardId), new String[] { payloadKey });
				return;
			}
			message = SerializableHandler.convertRedisMssage2GmmsMessage(body);
			if (message == null) {
				redis.del(payloadKey);
				redis.zrem(processingKey(shardId), new String[] { payloadKey });
				return;
			}
			if (DirectInDRDispatchService.getInstance().dispatchNow(message)) {
				redis.del(payloadKey);
				redis.zrem(processingKey(shardId), new String[] { payloadKey });
				return;
			}
			retryOrFail(shardId, payloadKey, message);
		} catch (Exception e) {
			log.warn(message, "Failed to dispatch delayed Auto IN_DR payload. payloadKey={}", payloadKey, e);
			if (message != null) {
				retryOrFail(shardId, payloadKey, message);
			}
		}
	}

	private void retryOrFail(int shardId, String payloadKey, GmmsMessage message) {
		int retry = message.getRetriedNumber() + 1;
		message.setRetriedNumber(retry);
		if (retry > maxRetry()) {
			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
			dbHandler.putMsg(message);
			redis.del(payloadKey);
			redis.zrem(processingKey(shardId), new String[] { payloadKey });
			log.warn(message, "Auto IN_DR direct dispatch exceeded max retry. payloadKey={}, retry={}", payloadKey, retry);
			return;
		}
		String body = SerializableHandler.convertGmmsMessage2RedisMessage(message);
		if (body != null) {
			redis.setString(payloadKey, body, payloadTtlSeconds(message, 0));
		}
		redis.zrem(processingKey(shardId), new String[] { payloadKey });
		redis.zadd(readyKey(shardId), System.currentTimeMillis() + retryDelayMs(), payloadKey);
	}

	private int calculateShardId(GmmsMessage msg) {
		int totalShards = gmmsUtility.getTotalShards();
		if (totalShards <= 1) {
			return 0;
		}
		String hashKey;
		if (msg.getSarMsgRefNum() != null && msg.getSarMsgRefNum().length() > 0) {
			hashKey = msg.getOSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress() + "_" + msg.getSarMsgRefNum();
		} else {
			hashKey = msg.getOSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		}
		return (hashKey.hashCode() & Integer.MAX_VALUE) % totalShards;
	}

	private String buildDelayId(GmmsMessage message) {
		String msgId = message.getMsgID();
		if (msgId == null || msgId.length() == 0) {
			msgId = message.getInMsgID();
		}
		if (msgId == null || msgId.length() == 0) {
			msgId = "unknown";
		}
		return msgId + ":" + System.currentTimeMillis() + ":" + UUID.randomUUID().toString();
	}

	private String readyKey(int shardId) {
		return READY_KEY_PREFIX + shardId;
	}

	private String processingKey(int shardId) {
		return PROCESSING_KEY_PREFIX + shardId;
	}

	private boolean isEnabled() {
		return Boolean.parseBoolean(gmmsUtility.getCommonProperty("AutoInDR.DelayDispatcherEnable", "true"));
	}

	private int batchSize() {
		return intProperty("AutoInDR.DelayBatchSize", 200);
	}

	private long claimTimeoutMs() {
		return longProperty("AutoInDR.DelayClaimTimeoutMs", 60000L);
	}

	private long retryDelayMs() {
		return longProperty("AutoInDR.DelayRetryDelayMs", 3000L);
	}

	private int maxRetry() {
		return intProperty("AutoInDR.DelayMaxRetry", 20);
	}

	private long idleSleepMs() {
		return longProperty("AutoInDR.DispatcherIdleSleepMs", 200L);
	}

	private int payloadTtlSeconds(GmmsMessage message, int delaySeconds) {
		int configured = intProperty("AutoInDR.DelayPayloadTTLSeconds", 172800);
		int redisExpire = gmmsUtility.getRedisExpireTime(message);
		int minTtl = Math.max(3600, delaySeconds + 3600);
		return Math.max(configured, Math.max(redisExpire, minTtl));
	}

	private int intProperty(String key, int defaultValue) {
		try {
			return Integer.parseInt(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
