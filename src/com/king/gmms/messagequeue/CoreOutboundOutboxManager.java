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
    private static final String OUTBOX_TIMEOUT_ZSET = "zset:core:outbox:submit:timeout";
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
        if (!enabled() || !needOutbox(msg, streamKey)) {
            return true;
        }
        try {
            String key = outboxKey(msg);
            if (key == null) {
                return true;
            }
            String value = SerializableHandler.convertGmmsMessage2RedisMessage(msg);
            int ttlSeconds = intProperty("CoreOutboundOutboxTTLSeconds", 3600);
            long timeoutMs = longProperty("CoreOutboundResultTimeoutMs", 300000L);
            long expireAt = System.currentTimeMillis() + timeoutMs;
            boolean stored = RedisClient.getInstance().setPendingMessage(key, value, ttlSeconds,
                    OUTBOX_TIMEOUT_ZSET, expireAt, null);
            if (!stored) {
                log.error(msg, "Core outbound outbox register failed. key={}, streamKey={}", key, streamKey);
            }
            return stored;
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
            String value = RedisClient.getInstance().consumePendingMessage(key, OUTBOX_TIMEOUT_ZSET);
            if (value == null) {
                continue;
            }
            try {
                GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(value);
                markTimeoutResult(msg);
                if (!StreamQueueManager.getInstance().produceResult(msg)) {
                    requeue(key, msg);
                } else {
                    log.warn(msg, "Core outbound outbox timeout result produced. key={}", key);
                }
            } catch (Exception e) {
                log.warn("Core outbound outbox timeout process failed. key=" + key, e);
            }
        }
    }

    private void requeue(String key, GmmsMessage msg) {
        try {
            long retryAt = System.currentTimeMillis() + longProperty("CoreOutboundOutboxResultRetryMs", 5000L);
            RedisClient.getInstance().setPendingMessage(key,
                    SerializableHandler.convertGmmsMessage2RedisMessage(msg),
                    intProperty("CoreOutboundOutboxTTLSeconds", 3600),
                    OUTBOX_TIMEOUT_ZSET, retryAt, null);
            log.warn(msg, "Core outbound outbox result produce failed, requeued. key={}", key);
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
            msg.setStatus(GmmsStatus.SUBMIT_RESP_ERROR);
            msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
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
}
