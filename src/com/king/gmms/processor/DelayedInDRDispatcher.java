package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * Core-owned delayed IN_DR dispatcher.
 */
public class DelayedInDRDispatcher implements Runnable {
	private static final SystemLogger log = SystemLogger.getSystemLogger(DelayedInDRDispatcher.class);
	private static final DelayedInDRDispatcher instance = new DelayedInDRDispatcher();

	private static final String READY_KEY_PREFIX = "zset:delay:outdr:";
	private static final String PROCESSING_KEY_PREFIX = "zset:delay:outdr:processing:";
	private static final String PAYLOAD_KEY_PREFIX = "delay:outdr:payload:";

	// KEYS[1]=ready KEYS[2]=processing ARGV: 1 now, 2 claim end, 3 count. The claims that ran out (a Core stopped) go back
	// to ready, then up to count due ones are moved to processing: {requeued, {payload keys}}
	private static final String CLAIM_BATCH_LUA =
			"local back = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3])) "
			+ "for i = 1, #back do redis.call('ZREM', KEYS[2], back[i]) redis.call('ZADD', KEYS[1], ARGV[1], back[i]) end "
			+ "local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3])) "
			+ "for i = 1, #m do redis.call('ZREM', KEYS[1], m[i]) redis.call('ZADD', KEYS[2], ARGV[2], m[i]) end "
			+ "return {#back, m}";

	// KEYS[1]=processing ARGV = payload keys: dispatched (or without a readable payload), deleted
	private static final String DONE_LUA =
			"for i = 1, #ARGV do redis.call('DEL', ARGV[i]) redis.call('ZREM', KEYS[1], ARGV[i]) end return #ARGV";

	private volatile boolean running = false;
	// M-3: 持有 worker 线程引用，stop 时 interrupt，避免停在 sleep 里要等 idleSleepMs 才退出。
	private volatile Thread worker;
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
		worker = thread;
		thread.start();
		log.info("DelayedInDRDispatcher started. enabled={}", isEnabled());
	}

	public synchronized void stopDispatcher() {
		running = false;
		Thread t = worker;
		if (t != null) {
			t.interrupt(); // M-3: 唤醒 sleep 中的 worker 立即退出
			worker = null;
		}
		log.info("DelayedInDRDispatcher stopped.");
	}

	public boolean schedule(GmmsMessage message, int delaySeconds) {
		if (message == null) {
			return true;
		}
		if (!com.king.gmms.messagequeue.StreamAttempt.once("drschedule",
				message.getStatusCode() + ":" + message.getSplitStatus())) {
			log.info(message, "MT redelivered, delayed DR already scheduled");
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

	/**
	 * The due delayed DRs of this Core's shards, by batch (AutoInDR.DelayBatchSize per shard and round): one Lua claims
	 * them (and puts back the claims that ran out), one MGET reads them, each is dispatched, one Lua deletes the
	 * dispatched ones. A failed one is retried after AutoInDR.DelayRetryDelayMs. On a Redis error the claimed ones come
	 * back after AutoInDR.DelayClaimTimeoutMs.
	 */
	private int dispatchDueMessages() {
		Set<Integer> shards = gmmsUtility.getMyShards();
		if (shards == null || shards.isEmpty()) {
			return 0;
		}
		RedisConnection state = redis.getStateRedis();
		int processed = 0;
		for (Integer shardId : shards) {
			if (shardId == null) {
				continue;
			}
			processed += dispatchShard(state, shardId.intValue());
		}
		return processed;
	}

	private int dispatchShard(RedisConnection state, int shardId) {
		long now = System.currentTimeMillis();
		List<?> r = (List<?>) state.evalScript(CLAIM_BATCH_LUA, Arrays.asList(readyKey(shardId), processingKey(shardId)),
				Arrays.asList(String.valueOf(now), String.valueOf(now + claimTimeoutMs()), String.valueOf(batchSize())));
		long requeued = r.get(0) instanceof Long ? (Long) r.get(0) : 0L;
		if (requeued > 0) {
			log.warn("Requeued {} expired Auto IN_DR processing payload(s). shardId={}", requeued, shardId);
		}
		List<String> keys = new ArrayList<String>();
		for (Object k : (List<?>) r.get(1)) {
			keys.add(String.valueOf(k));
		}
		if (keys.isEmpty()) {
			return 0;
		}
		List<String> bodies = state.mgetStrict(keys);
		List<String> done = new ArrayList<String>();
		for (int i = 0; i < keys.size(); i++) {
			String payloadKey = keys.get(i);
			String body = bodies == null || i >= bodies.size() ? null : bodies.get(i);
			GmmsMessage message = body == null ? null : SerializableHandler.convertRedisMssage2GmmsMessage(body);
			if (message == null) {
				done.add(payloadKey);   // gone (TTL) or not readable
				continue;
			}
			boolean dispatched;
			try {
				dispatched = DirectInDRDispatchService.getInstance().dispatchNow(message);
			} catch (Exception e) {
				log.warn(message, "Failed to dispatch delayed Auto IN_DR payload. payloadKey={}", payloadKey, e);
				dispatched = false;
			}
			if (dispatched) {
				done.add(payloadKey);
			} else {
				retryOrFail(shardId, payloadKey, message);
			}
		}
		if (!done.isEmpty()) {
			state.evalScript(DONE_LUA, Arrays.asList(processingKey(shardId)), done);
		}
		return keys.size();
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
		return com.king.gmms.messagequeue.ShardHash.of(hashKey, totalShards);
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
		return intProperty("AutoInDR.DelayBatchSize", 500);
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
