package com.king.gmms.smpp.pending;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
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
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
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
    private static final String REDIS_TIMEOUT_ZSET_REGISTRY_PREFIX = "set:smpp:pending:submit:timeout:zsets:";

    private static final DelayQueue<PendingSubmitTimeoutTask> GLOBAL_TIMEOUT_QUEUE =
            new DelayQueue<PendingSubmitTimeoutTask>();
    private static final AtomicInteger GLOBAL_TIMEOUT_WORKER_COUNT = new AtomicInteger(0);

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
    private final Semaphore permits;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public enum PutResult {
        STORED,
        WINDOW_FULL,
        FAILED
    }

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
        this.permits = windowSize > 0 ? new Semaphore(windowSize) : null;
        startRedisScanner();
        startGlobalTimeoutWorkers();
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
			boolean redisBacked = usesRedis(mode);
			if (redisBacked) {
				startRedisScanner();
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
			offerGlobalTimeout(entry);
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

    public boolean hasCapacity() {
        return permits == null || permits.availablePermits() > 0;
    }

    public PutResult tryPut(String sequence, GmmsMessage message) {
        if (stopped.get() || sequence == null || message == null) {
            return PutResult.FAILED;
        }
        boolean acquired = false;
        boolean redisStored = false;
        try {
            if (permits != null) {
                acquired = permits.tryAcquire();
                if (!acquired) {
                    return PutResult.WINDOW_FULL;
                }
            }
            long msgTimeoutMs = timeoutMsFor(message);
            PendingSubmitEntry entry = new PendingSubmitEntry(buildPendingKey(sequence), sequence, message, msgTimeoutMs);
            String mode = pendingMode(message);
            boolean redisBacked = usesRedis(mode);
            if (redisBacked) {
                startRedisScanner();
                redisStored = persistRedisPending(sequence, message, msgTimeoutMs);
                if (!redisStored) {
                    releasePermit();
                    return PutResult.FAILED;
                }
            }
            PendingSubmitEntry old = pendingMap.putIfAbsent(sequence, entry);
            if (old != null) {
                if (redisStored) {
                    deleteRedisPending(sequence);
                }
                releasePermit();
                log.warn(message, "Duplicate SMPP pending submit sequence. pendingKey={}", entry.getPendingKey());
                return PutResult.FAILED;
            }
            offerGlobalTimeout(entry);
            return PutResult.STORED;
        } catch (Exception e) {
            if (redisStored) {
                deleteRedisPending(sequence);
            }
            if (acquired) {
                releasePermit();
            }
            log.warn(message, "Failed to try put SMPP pending submit. sequence={}", sequence, e);
            return PutResult.FAILED;
        }
    }

    public PendingSubmitEntry get(String sequence) {
        if (sequence == null) {
            return null;
        }
        PendingSubmitEntry entry = pendingMap.get(sequence);
        if (entry != null) {
            if (isRedisBacked(sequence, entry) && !redisPendingExists(sequence)) {
                if (pendingMap.remove(sequence, entry)) {
                    redisKeyBySequence.remove(sequence);
                    releasePermit();
                }
                log.info(entry.getMessage(), "Redis-backed SMPP pending already completed before response. pendingKey={}",
                        entry.getPendingKey());
                return null;
            }
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
		return redisEntry;
	}

    public PendingSubmitEntry remove(String sequence) {
        if (sequence == null) {
            return null;
        }
        PendingSubmitEntry entry = pendingMap.remove(sequence);
        if (entry != null) {
            releasePermit();
        }
        deleteRedisPending(sequence);
        return entry;
    }

    public void markResultPending(String sequence, GmmsMessage message) {
        PendingSubmitEntry entry = get(sequence);
        if (entry != null) {
            entry.markResultPending(message);
            entry.reschedule(resultRetryDelayMs);
            offerGlobalTimeout(entry);
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

    private void processTimeoutTask(PendingSubmitTimeoutTask task) {
        PendingSubmitEntry entry = task.getEntry();
        String sequence = task.getSequence();
        if (stopped.get() || pendingMap.get(sequence) != entry) {
            recordTimeoutSkipped();
            return;
        }
        if (entry.getNextCheckAt() != task.getDueAt()) {
            recordTimeoutSkipped();
            return;
        }
        recordTimeoutProcessed(entry.getMessage());
		if (entry.isResultPending()) {
			retryPendingResult(sequence, entry);
		} else if (isRedisBacked(sequence, entry)) {
			if (!redisPendingExists(sequence)) {
				if (pendingMap.remove(sequence, entry)) {
					redisKeyBySequence.remove(sequence);
					releasePermit();
				}
				log.info(entry.getMessage(), "Redis-backed SMPP pending already handled by Redis timeout scanner. pendingKey={}",
						entry.getPendingKey());
			} else {
				entry.reschedule(resultRetryDelayMs);
				offerGlobalTimeout(entry);
				log.debug(entry.getMessage(), "Redis-backed SMPP pending waits for Redis timeout scanner. pendingKey={}",
						entry.getPendingKey());
			}
		} else {
			timeout(sequence, entry);
		}
    }

    private void retryPendingResult(String sequence, PendingSubmitEntry entry) {
        GmmsMessage message = entry.getMessage();
		if (message == null) {
			remove(sequence);
			return;
		}
		if (isRedisBacked(sequence, entry) && !redisPendingExists(sequence)) {
			if (pendingMap.remove(sequence, entry)) {
				redisKeyBySequence.remove(sequence);
				releasePermit();
			}
			log.info(message, "Redis-backed pending result already handled. pendingKey={}",
					entry.getPendingKey());
			return;
		}
		if (StreamQueueManager.getInstance().produceResult(message)) {
			remove(sequence);
			log.info(message, "Retried pending SMPP submit response result successfully. pendingKey={}",
                    entry.getPendingKey());
        } else {
            entry.reschedule(resultRetryDelayMs);
            offerGlobalTimeout(entry);
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
                offerGlobalTimeout(entry);
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
			boolean redisBacked = isRedisBacked(item.getKey(), entry);
			if (message == null) {
				if (redisBacked) {
					redisKeyBySequence.remove(item.getKey());
				} else {
					deleteRedisPending(item.getKey());
				}
				continue;
			}
			if (entry.isResultPending()) {
				if (StreamQueueManager.getInstance().produceResult(message)) {
					deleteRedisPending(item.getKey());
				} else if (redisBacked) {
					redisKeyBySequence.remove(item.getKey());
					log.warn(message, "Keep Redis-backed pending result for retry during shutdown. pendingKey={}, reason={}",
							entry.getPendingKey(), reason);
				} else {
					log.warn(message, "Failed to produce pending SMPP submit response during shutdown. pendingKey={}, reason={}",
							entry.getPendingKey(), reason);
				}
			} else if (redisBacked) {
				redisKeyBySequence.remove(item.getKey());
				log.info(message, "Keep Redis-backed SMPP pending for timeout scanner during shutdown. pendingKey={}, reason={}",
						entry.getPendingKey(), reason);
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

    private void offerGlobalTimeout(PendingSubmitEntry entry) {
        if (entry != null) {
            GLOBAL_TIMEOUT_QUEUE.offer(new PendingSubmitTimeoutTask(this, entry));
            recordGlobalTimeoutGauges();
        }
    }

    private String buildPendingKey(String sequence) {
        return sessionKey + ":" + sequence;
    }

    private String buildRedisPendingKey(String sequence) {
        return REDIS_PENDING_PREFIX + ownerKey() + ":" + sessionSsid + ":" + sessionKey + ":" + sequence;
    }

    private String buildRedisTimeoutZset() {
        return REDIS_TIMEOUT_ZSET_PREFIX + ownerKey() + ":" + sessionSsid;
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
                    zsetKey, expireAt, redisTimeoutZsetRegistry());
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

	private boolean isRedisBacked(String sequence, PendingSubmitEntry entry) {
		return sequence != null && redisKeyBySequence.containsKey(sequence);
	}

	private boolean redisPendingExists(String sequence) {
		String key = redisKeyBySequence.get(sequence);
		if (key == null) {
			key = buildRedisPendingKey(sequence);
		}
		try {
			return RedisClient.getInstance().getString(key) != null;
		} catch (Exception e) {
			return true;
		}
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

    private static void startGlobalTimeoutWorkers() {
        int desiredWorkerCount = Math.max(1, Math.min(intProperty("SmppPendingSubmitTimeoutWorkerCount", 2), 4));
        while (true) {
            int current = GLOBAL_TIMEOUT_WORKER_COUNT.get();
            if (current >= desiredWorkerCount) {
                return;
            }
            if (!GLOBAL_TIMEOUT_WORKER_COUNT.compareAndSet(current, current + 1)) {
                continue;
            }
            Thread thread = new Thread(A2PThreadGroup.getInstance(), new Runnable() {
                public void run() {
                    runGlobalTimeoutLoop();
                }
            }, "SmppPendingSubmitTimeoutWorker-" + THREAD_ID.incrementAndGet());
            thread.setDaemon(true);
            thread.start();
            log.info("Global SMPP pending submit timeout worker started. count={}, desired={}",
                    GLOBAL_TIMEOUT_WORKER_COUNT.get(), desiredWorkerCount);
            recordGlobalTimeoutGauges();
        }
    }

    private static void runGlobalTimeoutLoop() {
        while (true) {
            try {
                PendingSubmitTimeoutTask task = GLOBAL_TIMEOUT_QUEUE.take();
                recordGlobalTimeoutGauges();
                task.getManager().processTimeoutTask(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Global SMPP pending submit timeout worker failed.", e);
            }
        }
    }

    private static void recordGlobalTimeoutGauges() {
        MetricsCollector.getInstance().setGauge(MetricsNames.gauge(
                "smpp.pending.submit", "timeout_queue_size"), GLOBAL_TIMEOUT_QUEUE.size());
        MetricsCollector.getInstance().setGauge(MetricsNames.gauge(
                "smpp.pending.submit", "timeout_worker_count"), GLOBAL_TIMEOUT_WORKER_COUNT.get());
    }

    private static void recordTimeoutProcessed(GmmsMessage message) {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(
                "smpp.pending.submit", "timeout", message, MetricsNames.ACTION_TIMEOUT_PROCESSED));
    }

    private static void recordTimeoutSkipped() {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.build(
                "smpp.pending.submit", "timeout", "submit", MetricsNames.ACTION_TIMEOUT_SKIPPED));
    }

    private static class PendingSubmitTimeoutTask implements Delayed {
        private final SmppPendingSubmitManager manager;
        private final PendingSubmitEntry entry;
        private final String sequence;
        private final long dueAt;

        PendingSubmitTimeoutTask(SmppPendingSubmitManager manager, PendingSubmitEntry entry) {
            this.manager = manager;
            this.entry = entry;
            this.sequence = entry == null ? null : entry.getSequence();
            this.dueAt = entry == null ? System.currentTimeMillis() : entry.getNextCheckAt();
        }

        public long getDelay(TimeUnit unit) {
            return unit.convert(dueAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        public int compareTo(Delayed other) {
            if (other == this) {
                return 0;
            }
            long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
            return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
        }

        SmppPendingSubmitManager getManager() {
            return manager;
        }

        PendingSubmitEntry getEntry() {
            return entry;
        }

        String getSequence() {
            return sequence;
        }

        long getDueAt() {
            return dueAt;
        }
    }

    private static void startRedisScanner() {
        if (!REDIS_SCANNER_STARTED.compareAndSet(false, true)) {
            return;
        }
        REDIS_SCANNER.execute(new Runnable() {
            public void run() {
                while (true) {
                    scanRedisTimeouts();
                    try {
                        Thread.sleep(Math.max(100L, longProperty("SMPPSubmitPendingRedisScanIntervalMs", 1000L)));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        });
    }

    private static void scanRedisTimeouts() {
        try {
            Set<String> zsets = RedisClient.getInstance().smembers(redisTimeoutZsetRegistry());
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
                            zsetKey, retryAt, redisTimeoutZsetRegistry());
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

    private static String ownerKey() {
        String module = System.getProperty("module", "unknown");
        String nodeId = System.getProperty("NodeID", "0");
        return safePart(module) + ":" + safePart(nodeId);
    }

    private static String redisTimeoutZsetRegistry() {
        return REDIS_TIMEOUT_ZSET_REGISTRY_PREFIX + ownerKey();
    }

    private static String safePart(String value) {
        if (value == null || value.trim().length() == 0) {
            return "unknown";
        }
        return value.trim().replace(':', '_');
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
