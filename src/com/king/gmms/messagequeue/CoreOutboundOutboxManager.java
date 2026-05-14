package com.king.gmms.messagequeue;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.resps.Tuple;

public class CoreOutboundOutboxManager {
    private static final SystemLogger log = SystemLogger.getSystemLogger(CoreOutboundOutboxManager.class);
    private static final CoreOutboundOutboxManager instance = new CoreOutboundOutboxManager();

    private static final String OUTBOX_PREFIX = "core:outbox:submit:";
    private static final String OUTBOX_META_SUFFIX = ":meta";
    private static final String OUTBOX_TIMEOUT_ZSET = "zset:core:outbox:submit:timeout";
    private static final String CLIENT_OUTBOUND_GROUP = "ClientOutboundSubmitGroup";
    private static final long DEFAULT_RECHECK_MS = TimeUnit.MINUTES.toMillis(5);
    private static final long DEFAULT_HARD_TIMEOUT_MS = TimeUnit.HOURS.toMillis(3);
    private static final int DEFAULT_PAYLOAD_TTL_SECONDS = (int) TimeUnit.HOURS.toSeconds(4);
    private final AtomicBoolean scannerStarted = new AtomicBoolean(false);
    private final ScheduledExecutorService scanner = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(A2PThreadGroup.getInstance(), runnable, "CoreOutboundOutboxScanner");
            thread.setDaemon(true);
            return thread;
        }
    });

    private CoreOutboundOutboxManager() {
        startScanner();
    }

    public static CoreOutboundOutboxManager getInstance() {
        return instance;
    }

    public boolean register(GmmsMessage msg, String streamKey) {
        return register(msg, streamKey, msg == null ? null : msg.getRedisStreamID());
    }

    public boolean register(GmmsMessage msg, String streamKey, String redisId) {
        if (!enabled() || !needOutbox(msg, streamKey)) {
            return true;
        }
        try {
            String key = outboxKey(msg);
            if (key == null) {
                return true;
            }
            String value = SerializableHandler.convertGmmsMessage2RedisMessage(msg);
            long now = System.currentTimeMillis();
            long recheckMs = recheckMs();
            long hardTimeoutMs = hardTimeoutMs(recheckMs);
            int payloadTtlSeconds = payloadTtlSeconds(hardTimeoutMs, recheckMs);
            long expireAt = now + recheckMs;
            boolean stored = RedisClient.getInstance().setPendingMessage(key, value, payloadTtlSeconds,
                    OUTBOX_TIMEOUT_ZSET, expireAt, null);
            if (!stored) {
                log.error(msg, "Core outbound outbox register failed. key={}, streamKey={}", key, streamKey);
                return false;
            }
            boolean metaStored = RedisClient.getInstance().setString(metaKey(key),
                    buildMeta(streamKey, redisId, now), payloadTtlSeconds);
            if (!metaStored) {
                RedisClient.getInstance().consumePendingMessage(key, OUTBOX_TIMEOUT_ZSET);
                log.error(msg, "Core outbound outbox meta register failed. key={}, streamKey={}, redisId={}",
                        key, streamKey, redisId);
                return false;
            }
            log.info(msg, "Core outbound outbox registered. key={}, streamKey={}, redisId={}, firstCheckMs={}, hardTimeoutMs={}",
                    key, streamKey, redisId, recheckMs, hardTimeoutMs);
            return true;
        } catch (Exception e) {
            log.error(msg, "Core outbound outbox register exception. streamKey=" + streamKey, e);
            return false;
        }
    }

    public void complete(GmmsMessage msg) {
        if (!enabled()) {
            return;
        }
        String key = outboxKey(msg);
        if (key != null) {
            RedisClient.getInstance().consumePendingMessage(key, OUTBOX_TIMEOUT_ZSET);
            RedisClient.getInstance().del(metaKey(key));
        }
    }

    public void remove(GmmsMessage msg) {
        complete(msg);
    }

    private void startScanner() {
        if (!scannerStarted.compareAndSet(false, true)) {
            return;
        }
        long intervalMs = longProperty("CoreOutboundOutboxScanIntervalMs", 1000L);
        scanner.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                scanTimeouts();
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void scanTimeouts() {
        if (!enabled()) {
            return;
        }
        int batchSize = intProperty("CoreOutboundOutboxScanBatchSize", 100);
        long now = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            List<Tuple> tuples = RedisClient.getInstance().zpopmin(OUTBOX_TIMEOUT_ZSET);
            if (tuples == null || tuples.isEmpty()) {
                return;
            }
            Tuple tuple = tuples.get(0);
            String key = tuple.getElement();
            double score = tuple.getScore();
            if (score > now) {
                RedisClient.getInstance().zadd(OUTBOX_TIMEOUT_ZSET, score, key);
                return;
            }
            String value = RedisClient.getInstance().getString(key);
            if (value == null) {
                RedisClient.getInstance().del(metaKey(key));
                continue;
            }
            try {
                GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(value);
                OutboxMeta meta = loadMeta(key, now);
                if (shouldDefer(key, msg, meta, now)) {
                    continue;
                }
                value = RedisClient.getInstance().consumePendingMessage(key, OUTBOX_TIMEOUT_ZSET);
                if (value == null) {
                    RedisClient.getInstance().del(metaKey(key));
                    continue;
                }
                msg = SerializableHandler.convertRedisMssage2GmmsMessage(value);
                markTimeoutResult(msg);
                if (!StreamQueueManager.getInstance().produceResult(msg)) {
                    requeueResult(key, msg, meta);
                } else {
                    RedisClient.getInstance().del(metaKey(key));
                    log.warn(msg, "Core outbound outbox hard timeout result produced. key={}", key);
                }
            } catch (Exception e) {
                log.warn("Core outbound outbox timeout process failed. key=" + key, e);
            }
        }
    }

    private boolean shouldDefer(String key, GmmsMessage msg, OutboxMeta meta, long now) {
        long recheckMs = recheckMs();
        long hardTimeoutMs = hardTimeoutMs(recheckMs);
        long ageMs = Math.max(0L, now - meta.createdAt);
        if (ageMs >= hardTimeoutMs) {
            return false;
        }
        String reason = "waiting_result";
        if (!isBlank(meta.streamKey) && !isBlank(meta.redisId)) {
            if (RedisClient.getInstance().streamPendingEntryExistsSubmitMq(
                    meta.streamKey, CLIENT_OUTBOUND_GROUP, meta.redisId)) {
                reason = "in_pel";
            } else if (RedisClient.getInstance().streamEntryExistsSubmitMq(meta.streamKey, meta.redisId)) {
                reason = "in_stream";
            }
        } else {
            reason = "missing_meta";
        }
        requeueDelay(key, msg, meta, now + recheckMs, reason, ageMs, recheckMs);
        return true;
    }

    private void requeueDelay(String key, GmmsMessage msg, OutboxMeta meta, long nextCheckAt,
            String reason, long ageMs, long recheckMs) {
        try {
            int payloadTtlSeconds = payloadTtlSeconds(hardTimeoutMs(recheckMs), recheckMs);
            boolean requeued = RedisClient.getInstance().requeuePendingMessageIfExists(key,
                    SerializableHandler.convertGmmsMessage2RedisMessage(msg),
                    payloadTtlSeconds, OUTBOX_TIMEOUT_ZSET, nextCheckAt);
            if (!requeued) {
                RedisClient.getInstance().del(metaKey(key));
                log.info(msg, "Core outbound outbox defer skipped, outbox already completed. key={}, state={}",
                        key, reason);
                return;
            }
            RedisClient.getInstance().setString(metaKey(key),
                    buildMeta(meta.streamKey, meta.redisId, meta.createdAt), payloadTtlSeconds);
            log.info(msg, "Core outbound outbox deferred. key={}, state={}, ageMs={}, nextCheckDelayMs={}, streamKey={}, redisId={}",
                    key, reason, ageMs, recheckMs, meta.streamKey, meta.redisId);
        } catch (Exception e) {
            log.error(msg, "Core outbound outbox defer failed. key=" + key, e);
        }
    }

    private void requeueResult(String key, GmmsMessage msg, OutboxMeta meta) {
        try {
            long recheckMs = recheckMs();
            long hardTimeoutMs = hardTimeoutMs(recheckMs);
            int payloadTtlSeconds = payloadTtlSeconds(hardTimeoutMs, recheckMs);
            long retryAt = System.currentTimeMillis() + longProperty("CoreOutboundOutboxResultRetryMs", 5000L);
            RedisClient.getInstance().setPendingMessage(key,
                    SerializableHandler.convertGmmsMessage2RedisMessage(msg),
                    payloadTtlSeconds,
                    OUTBOX_TIMEOUT_ZSET, retryAt, null);
            RedisClient.getInstance().setString(metaKey(key),
                    buildMeta(meta.streamKey, meta.redisId, meta.createdAt), payloadTtlSeconds);
            log.warn(msg, "Core outbound outbox timeout result produce failed, requeued. key={}", key);
        } catch (Exception e) {
            log.error(msg, "Core outbound outbox requeue failed. key=" + key, e);
        }
    }

	private void markTimeoutResult(GmmsMessage msg) {
		if (msg == null) {
			return;
		}
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
			msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		}
	}

    private String metaKey(String key) {
        return key + OUTBOX_META_SUFFIX;
    }

    private String buildMeta(String streamKey, String redisId, long createdAt) {
        return safe(streamKey) + "\n" + safe(redisId) + "\n" + createdAt;
    }

    private OutboxMeta loadMeta(String key, long now) {
        String value = RedisClient.getInstance().getString(metaKey(key));
        if (value == null || value.length() == 0) {
            return new OutboxMeta(null, null, now);
        }
        String[] parts = value.split("\n", -1);
        String streamKey = parts.length > 0 ? emptyToNull(parts[0]) : null;
        String redisId = parts.length > 1 ? emptyToNull(parts[1]) : null;
        long createdAt = now;
        if (parts.length > 2) {
            try {
                createdAt = Long.parseLong(parts[2]);
            } catch (Exception ignored) {
                createdAt = now;
            }
        }
        return new OutboxMeta(streamKey, redisId, createdAt);
    }

    private String emptyToNull(String value) {
        return value == null || value.length() == 0 ? null : value;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }

    private static final class OutboxMeta {
        private final String streamKey;
        private final String redisId;
        private final long createdAt;

        private OutboxMeta(String streamKey, String redisId, long createdAt) {
            this.streamKey = streamKey;
            this.redisId = redisId;
            this.createdAt = createdAt;
        }
    }

    private boolean needOutbox(GmmsMessage msg, String streamKey) {
        if (msg == null || streamKey == null || !streamKey.startsWith(StreamQueueManager.STR_MT_ROUTED_PREFIX)) {
            return false;
        }
        return GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
                || GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType());
    }

    private String outboxKey(GmmsMessage msg) {
        if (msg == null) {
            return null;
        }
        String id = msg.getMsgID();
        if (id == null || id.length() == 0) {
            id = msg.getInMsgID();
        }
        if (id == null || id.length() == 0) {
            return null;
        }
        return OUTBOX_PREFIX + id;
    }

    private boolean enabled() {
        return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("CoreOutboundOutboxEnable", "true"));
    }

    private static int intProperty(String key, int defaultValue) {
        try {
            return Integer.parseInt(GmmsUtility.getInstance().getCommonProperty(key,
                    String.valueOf(defaultValue)).trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static long longProperty(String key, long defaultValue) {
        try {
            return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key,
                    String.valueOf(defaultValue)).trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static long positiveLongProperty(String key, long defaultValue, long minValue) {
        long value = longProperty(key, defaultValue);
        return value < minValue ? defaultValue : value;
    }

    private static long recheckMs() {
        return positiveLongProperty("CoreOutboundOutboxRecheckMs", DEFAULT_RECHECK_MS, 1000L);
    }

    private static long hardTimeoutMs(long recheckMs) {
        long hardTimeoutMs = longProperty("CoreOutboundOutboxHardTimeoutMs", DEFAULT_HARD_TIMEOUT_MS);
        if (hardTimeoutMs < recheckMs) {
            return Math.max(DEFAULT_HARD_TIMEOUT_MS, recheckMs);
        }
        return hardTimeoutMs;
    }

    private static int payloadTtlSeconds(long hardTimeoutMs, long recheckMs) {
        long configured = positiveLongProperty("CoreOutboundOutboxPayloadTTLSeconds",
                DEFAULT_PAYLOAD_TTL_SECONDS, 60L);
        long minSeconds = TimeUnit.MILLISECONDS.toSeconds(hardTimeoutMs + recheckMs) + 60L;
        long ttlSeconds = Math.max(configured, minSeconds);
        return ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds;
    }
}
