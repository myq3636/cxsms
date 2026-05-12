package com.king.gmms.smpp.pending;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.util.BufferTimeoutInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.resps.Tuple;

public class SmppPendingSubmitManager {
    private static final SystemLogger log = SystemLogger.getSystemLogger(SmppPendingSubmitManager.class);
    private static final AtomicInteger THREAD_ID = new AtomicInteger(0);
    private static final String MODE_MEMORY = "memory";
    private static final String MODE_HYBRID = "hybrid";
    private static final String MODE_REDIS = "redis";
    private static final String REDIS_PENDING_PREFIX = "smpp:pending:submit:";
    private static final String REDIS_TIMEOUT_ZSET_PREFIX = "zset:smpp:pending:submit:timeout:";
    private static final String REDIS_TIMEOUT_ZSET_REGISTRY = "set:smpp:pending:submit:timeout:zsets";

    private static final ExecutorService SCANNER = Executors.newCachedThreadPool(new ThreadFactory() {
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(A2PThreadGroup.getInstance(), runnable,
                    "SmppPendingSubmitScanner-" + THREAD_ID.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    });

    private static final ScheduledExecutorService REDIS_SCANNER = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(A2PThreadGroup.getInstance(), runnable,
                    "RedisSmppPendingSubmitScanner");
            thread.setDaemon(true);
            return thread;
        }
    });
    private static final AtomicBoolean REDIS_SCANNER_STARTED = new AtomicBoolean(false);

    private final String sessionKey;
    private final int sessionSsid;
    private final int windowSize;
    private final long timeoutMs;
    private final long resultRetryDelayMs;
    private final BufferTimeoutInterface timeoutListener;
    private final ConcurrentMap<String, PendingSubmitEntry> pendingMap;
    private final ConcurrentMap<String, String> redisKeyBySequence;
    private final DelayQueue<PendingSubmitEntry> timeoutQueue;
    private final Semaphore permits;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public SmppPendingSubmitManager(String sessionKey, int windowSize, long timeoutMs,
            BufferTimeoutInterface timeoutListener) {
        this.sessionKey = sessionKey == null ? "" : sessionKey;
        this.sessionSsid = parseSessionSsid(this.sessionKey);
        this.windowSize = windowSize;
        this.timeoutMs = timeoutMs > 0 ? timeoutMs : 60000L;
        this.resultRetryDelayMs = Math.max(1000L, Math.min(this.timeoutMs / 3, 10000L));
        this.timeoutListener = timeoutListener;
        this.pendingMap = new ConcurrentHashMap<String, PendingSubmitEntry>();
        this.redisKeyBySequence = new ConcurrentHashMap<String, String>();
        this.timeoutQueue = new DelayQueue<PendingSubmitEntry>();
        this.permits = windowSize > 0 ? new Semaphore(windowSize) : null;
        startRedisScanner();
        SCANNER.execute(new Runnable() {
            public void run() {
                runTimeoutLoop();
            }
        });
        log.info("SMPP pending submit manager started. sessionKey={}, windowSize={}, timeoutMs={}",
                this.sessionKey, this.windowSize, this.timeoutMs);
    }

    public boolean put(String sequence, GmmsMessage message) {
        if (stopped.get() || sequence == null || message == null) {
            return false;
        }
        boolean acquired = false;
        boolean redisStored = false;
        try {
            if (permits != null) {
                acquired = permits.tryAcquire(50L, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    return false;
                }
            }
            long msgTimeoutMs = timeoutMsFor(message);
            PendingSubmitEntry entry = new PendingSubmitEntry(buildPendingKey(sequence), sequence, message, msgTimeoutMs);
            String mode = pendingMode(message);
            if (usesRedis(mode)) {
                redisStored = persistRedisPending(sequence, message, msgTimeoutMs);
                if (!redisStored) {
                    releasePermit();
                    return false;
                }
            }
            PendingSubmitEntry old = pendingMap.putIfAbsent(sequence, entry);
            if (old != null) {
                if (redisStored) {
                    deleteRedisPending(sequence);
                }
                releasePermit();
                log.warn(message, "Duplicate SMPP pending submit sequence. pendingKey={}", entry.getPendingKey());
                return false;
            }
            timeoutQueue.offer(entry);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (redisStored) {
                deleteRedisPending(sequence);
            }
            if (acquired) {
                releasePermit();
            }
            return false;
        } catch (Exception e) {
            if (redisStored) {
                deleteRedisPending(sequence);
            }
            if (acquired) {
                releasePermit();
            }
            log.warn(message, "Failed to put SMPP pending submit. sequence={}", sequence, e);
            return false;
        }
    }

    public PendingSubmitEntry get(String sequence) {
        if (sequence == null) {
            return null;
        }
        PendingSubmitEntry entry = pendingMap.get(sequence);
        if (entry != null) {
            return entry;
        }
        if (!usesRedis(pendingMode(null))) {
            return null;
        }
        GmmsMessage message = loadRedisPending(sequence);
        if (message == null) {
            return null;
        }
        PendingSubmitEntry redisEntry = new PendingSubmitEntry(buildPendingKey(sequence), sequence, message, timeoutMsFor(message));
        PendingSubmitEntry old = pendingMap.putIfAbsent(sequence, redisEntry);
        if (old != null) {
            return old;
        }
        timeoutQueue.offer(redisEntry);
        return redisEntry;
    }

    public PendingSubmitEntry remove(String sequence) {
        if (sequence == null) {
            return null;
        }
        PendingSubmitEntry entry = pendingMap.remove(sequence);
        if (entry != null) {
            timeoutQueue.remove(entry);
            releasePermit();
        }
        deleteRedisPending(sequence);
        return entry;
    }

    public void markResultPending(String sequence, GmmsMessage message) {
        PendingSubmitEntry entry = get(sequence);
        if (entry != null) {
            timeoutQueue.remove(entry);
            entry.markResultPending(message);
            entry.reschedule(resultRetryDelayMs);
            timeoutQueue.offer(entry);
            if (redisKeyBySequence.containsKey(sequence)) {
                persistRedisPending(sequence, entry.getMessage(), resultRetryDelayMs);
            }
            log.warn(message, "SMPP submit response result is pending for retry. pendingKey={}",
                    entry.getPendingKey());
        }
    }

    public int size() {
        return pendingMap.size();
    }

    public void shutdown(boolean failPending, String reason) {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        if (failPending) {
            failAll(reason);
        } else {
            clear();
        }
        log.info("SMPP pending submit manager stopped. sessionKey={}, reason={}, pending={}",
                sessionKey, reason, pendingMap.size());
    }

    private void runTimeoutLoop() {
        while (!stopped.get()) {
            try {
                PendingSubmitEntry entry = timeoutQueue.poll(1L, TimeUnit.SECONDS);
                if (entry == null) {
                    continue;
                }
                String sequence = entry.getSequence();
                if (pendingMap.get(sequence) != entry) {
                    continue;
                }
                if (entry.isResultPending()) {
                    retryPendingResult(sequence, entry);
                } else {
                    timeout(sequence, entry);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.warn("SMPP pending submit timeout loop failed. sessionKey=" + sessionKey, e);
            }
        }
    }

    private void retryPendingResult(String sequence, PendingSubmitEntry entry) {
        GmmsMessage message = entry.getMessage();
        if (message == null) {
            remove(sequence);
            return;
        }
        if (StreamQueueManager.getInstance().produceResult(message)) {
            remove(sequence);
            log.info(message, "Retried pending SMPP submit response result successfully. pendingKey={}",
                    entry.getPendingKey());
        } else {
            entry.reschedule(resultRetryDelayMs);
            timeoutQueue.offer(entry);
            if (redisKeyBySequence.containsKey(sequence)) {
                persistRedisPending(sequence, message, resultRetryDelayMs);
            }
            log.warn(message, "Retry pending SMPP submit response result failed. pendingKey={}",
                    entry.getPendingKey());
        }
    }

    private void timeout(String sequence, PendingSubmitEntry entry) {
        GmmsMessage message = entry.getMessage();
        if (message == null) {
            remove(sequence);
            return;
        }
        if (timeoutListener instanceof PendingSubmitTimeoutHandler) {
            boolean produced = ((PendingSubmitTimeoutHandler) timeoutListener).timeoutPendingSubmit(sequence, message);
            if (produced) {
                remove(sequence);
                log.info(message, "SMPP pending submit timeout result produced. pendingKey={}",
                        entry.getPendingKey());
            } else {
                entry.markResultPending(message);
                entry.reschedule(resultRetryDelayMs);
                timeoutQueue.offer(entry);
                if (redisKeyBySequence.containsKey(sequence)) {
                    persistRedisPending(sequence, message, resultRetryDelayMs);
                }
                log.warn(message, "SMPP pending submit timeout result produce failed, keep pending for retry. pendingKey={}",
                        entry.getPendingKey());
            }
            return;
        }
        if (pendingMap.remove(sequence, entry)) {
            releasePermit();
            deleteRedisPending(sequence);
            if (timeoutListener != null) {
                timeoutListener.timeout(sequence, message);
            }
        }
    }

    private void failAll(String reason) {
        for (Map.Entry<String, PendingSubmitEntry> item : pendingMap.entrySet()) {
            PendingSubmitEntry entry = item.getValue();
            if (!pendingMap.remove(item.getKey(), entry)) {
                continue;
            }
            releasePermit();
            GmmsMessage message = entry.getMessage();
            if (message == null) {
                deleteRedisPending(item.getKey());
                continue;
            }
            if (entry.isResultPending()) {
                if (StreamQueueManager.getInstance().produceResult(message)) {
                    deleteRedisPending(item.getKey());
                } else {
                    log.warn(message, "Failed to produce pending SMPP submit response during shutdown. pendingKey={}, reason={}",
                            entry.getPendingKey(), reason);
                }
            } else if (timeoutListener instanceof PendingSubmitTimeoutHandler) {
                if (((PendingSubmitTimeoutHandler) timeoutListener).timeoutPendingSubmit(item.getKey(), message)) {
                    deleteRedisPending(item.getKey());
                }
            } else if (timeoutListener != null) {
                timeoutListener.timeout(item.getKey(), message);
                deleteRedisPending(item.getKey());
            }
        }
    }

    private void clear() {
        int size = pendingMap.size();
        pendingMap.clear();
        timeoutQueue.clear();
        redisKeyBySequence.clear();
        if (permits != null && size > 0) {
            permits.release(size);
        }
    }

    private void releasePermit() {
        if (permits != null) {
            permits.release();
        }
    }

    private String buildPendingKey(String sequence) {
        return sessionKey + ":" + sequence;
    }

    private String buildRedisPendingKey(String sequence) {
        return REDIS_PENDING_PREFIX + sessionSsid + ":" + sessionKey + ":" + sequence;
    }

    private String buildRedisTimeoutZset() {
        return REDIS_TIMEOUT_ZSET_PREFIX + sessionSsid;
    }

    private boolean persistRedisPending(String sequence, GmmsMessage message, long delayMs) {
        if (sequence == null || message == null) {
            return false;
        }
        try {
            String key = buildRedisPendingKey(sequence);
            String zsetKey = buildRedisTimeoutZset();
            int ttlSeconds = redisTtlSeconds(message);
            long expireAt = System.currentTimeMillis() + Math.max(1000L, delayMs);
            String value = SerializableHandler.convertGmmsMessage2RedisMessage(message);
            boolean stored = RedisClient.getInstance().setPendingMessage(key, value, ttlSeconds,
                    zsetKey, expireAt, REDIS_TIMEOUT_ZSET_REGISTRY);
            if (stored) {
                redisKeyBySequence.put(sequence, key);
            }
            return stored;
        } catch (Exception e) {
            log.warn(message, "Failed to persist SMPP pending submit to Redis. sequence={}", sequence, e);
            return false;
        }
    }

    private GmmsMessage loadRedisPending(String sequence) {
        try {
            String key = buildRedisPendingKey(sequence);
            String value = RedisClient.getInstance().getString(key);
            if (value == null) {
                return null;
            }
            GmmsMessage message = SerializableHandler.convertRedisMssage2GmmsMessage(value);
            redisKeyBySequence.put(sequence, key);
            return message;
        } catch (Exception e) {
            log.warn("Failed to load SMPP pending submit from Redis. sequence=" + sequence, e);
            return null;
        }
    }

    private void deleteRedisPending(String sequence) {
        String key = redisKeyBySequence.remove(sequence);
        if (key == null) {
            key = buildRedisPendingKey(sequence);
        }
        RedisClient.getInstance().consumePendingMessage(key, buildRedisTimeoutZset());
    }

    private String pendingMode(GmmsMessage message) {
        A2PCustomerInfo customer = currentCustomer(message);
        if (customer == null || customer.getSMPPSubmitPendingMode() == null) {
            return MODE_MEMORY;
        }
        String mode = customer.getSMPPSubmitPendingMode().trim().toLowerCase();
        if (MODE_REDIS.equals(mode) || MODE_HYBRID.equals(mode)) {
            return mode;
        }
        return MODE_MEMORY;
    }

    private boolean usesRedis(String mode) {
        return MODE_REDIS.equals(mode) || MODE_HYBRID.equals(mode);
    }

    private long timeoutMsFor(GmmsMessage message) {
        A2PCustomerInfo customer = currentCustomer(message);
        if (customer != null && customer.getSMPPSubmitPendingRedisTimeoutSeconds() > 0) {
            return customer.getSMPPSubmitPendingRedisTimeoutSeconds() * 1000L;
        }
        return timeoutMs;
    }

    private int redisTtlSeconds(GmmsMessage message) {
        A2PCustomerInfo customer = currentCustomer(message);
        if (customer != null && customer.getSMPPSubmitPendingRedisTTLSeconds() > 0) {
            return customer.getSMPPSubmitPendingRedisTTLSeconds();
        }
        return Math.max(600, (int) (timeoutMsFor(message) / 1000L) * 3);
    }

    private A2PCustomerInfo currentCustomer(GmmsMessage message) {
        int ssid = sessionSsid;
        if (message != null && message.getRSsID() > 0) {
            ssid = message.getRSsID();
        }
        try {
            return GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
        } catch (Exception e) {
            return null;
        }
    }

    private static int parseSessionSsid(String sessionKey) {
        try {
            String[] parts = sessionKey.split(":");
            if (parts.length > 1) {
                return Integer.parseInt(parts[1]);
            }
        } catch (Exception e) {
            // ignore and use default
        }
        return 0;
    }

    private static void startRedisScanner() {
        if (!REDIS_SCANNER_STARTED.compareAndSet(false, true)) {
            return;
        }
        final long intervalMs = longProperty("SMPPSubmitPendingRedisScanIntervalMs", 1000L);
        REDIS_SCANNER.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                scanRedisTimeouts();
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private static void scanRedisTimeouts() {
        try {
            Set<String> zsets = RedisClient.getInstance().smembers(REDIS_TIMEOUT_ZSET_REGISTRY);
            if (zsets == null || zsets.isEmpty()) {
                return;
            }
            int batchSize = intProperty("SMPPSubmitPendingRedisScanBatchSize", 100);
            long now = System.currentTimeMillis();
            for (String zsetKey : zsets) {
                scanOneRedisTimeoutZset(zsetKey, now, batchSize);
            }
        } catch (Exception e) {
            log.warn("Redis SMPP pending submit timeout scan failed.", e);
        }
    }

    private static void scanOneRedisTimeoutZset(String zsetKey, long now, int batchSize) {
        for (int i = 0; i < batchSize; i++) {
            List<Tuple> tuples = RedisClient.getInstance().zpopmin(zsetKey);
            if (tuples == null || tuples.isEmpty()) {
                return;
            }
            Tuple tuple = tuples.get(0);
            String pendingKey = tuple.getElement();
            double score = tuple.getScore();
            if (score > now) {
                RedisClient.getInstance().zadd(zsetKey, score, pendingKey);
                return;
            }
            String value = RedisClient.getInstance().consumePendingMessage(pendingKey, zsetKey);
            if (value == null) {
                continue;
            }
            try {
                GmmsMessage message = SerializableHandler.convertRedisMssage2GmmsMessage(value);
                markSubmitTimeout(message);
                if (!StreamQueueManager.getInstance().produceResult(message)) {
                    long retryAt = System.currentTimeMillis() + longProperty("SMPPSubmitPendingRedisResultRetryMs", 5000L);
                    RedisClient.getInstance().setPendingMessage(pendingKey,
                            SerializableHandler.convertGmmsMessage2RedisMessage(message),
                            intProperty("SMPPSubmitPendingRedisRetryTTLSeconds", 600),
                            zsetKey, retryAt, REDIS_TIMEOUT_ZSET_REGISTRY);
                    log.warn(message, "Redis SMPP pending timeout result produce failed, requeued. pendingKey={}",
                            pendingKey);
                } else {
                    log.warn(message, "Redis SMPP pending submit timeout result produced. pendingKey={}", pendingKey);
                }
            } catch (Exception e) {
                log.warn("Failed to process Redis SMPP pending submit timeout. pendingKey=" + pendingKey, e);
            }
        }
    }

    private static void markSubmitTimeout(GmmsMessage message) {
        if (message == null) {
            return;
        }
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType())
                || GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message.getMessageType())) {
            message.setStatus(GmmsStatus.SUBMIT_RESP_ERROR);
            message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
        }
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
