package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;

/**
 * V5.2 Server-side MT Consumer (Fully Decoupled Design)
 *
 * 职责：作为 Worker Engine，负责并发调度。
 * 设计改进：
 * 1. 彻底解耦 RedisConnection：不再直接获取 RedisConnection，全部通过 StreamQueueManager 交互。
 * 2. 统一消息处理流程：使用 consumeBatch 和 ack 高级方法。
 */
public class MTStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(MTStreamConsumer.class);
    private static MTStreamConsumer instance = new MTStreamConsumer();
    private static final String MODULE_ROLE = "core";
    private static final String FUNCTION_NAME = "mt-router";

    private final String groupName = "CoreMTGroup";
    private final String consumerName;
    private volatile boolean running = false;
    private volatile long lastPoolConfigRefreshMs = 0L;
    
    private ExecutorService dispatcherThread;
    private ExecutorService recoveryThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<>();

    private final StreamQueueManager queueManager;
    private final RedisStreamConsumerConfig consumerConfig;

    private MTStreamConsumer() {
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + System.getProperty("NodeID", "0");
        this.queueManager = StreamQueueManager.getInstance();
        this.consumerConfig = RedisStreamConsumerConfig.load("Core", "CoreMT", 50, 100, 1000, 100, 50);
    }

    public static MTStreamConsumer getInstance() { return instance; }

    public synchronized void start() {
        if (running) return;
        running = true;
        
        workerPool = new ThreadPoolExecutor(
            consumerConfig.workerCorePoolSize(), consumerConfig.workerMaxPoolSize(), 60L, TimeUnit.SECONDS, 
            new LinkedBlockingQueue<Runnable>(consumerConfig.workerQueueSize()), 
            new ThreadPoolExecutor.AbortPolicy()
        );

        consumerConfig.registerPoolGauges(workerPool);
        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        recoveryThread = Executors.newSingleThreadExecutor();
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Core MT-router",
                StreamQueueManager.STR_MT_PENDING_PREFIX + "*", groupName, consumerName, true, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        processMessage(streamKey, msg, true);
                    }
                }));

        // a backlog whose doorbell is gone (rung before a restart, taken by a Core that died) is found now
        queueManager.reRingExistingStreams(StreamQueueManager.STR_MT_PENDING_PREFIX + "*", true);
        logger.info("Core MT-router consumer started. Consumer: {}, Shards: {}, Config: {}",
            consumerName, GmmsUtility.getInstance().getMyShards(), consumerConfig.summary());
    }

    public boolean isRunning() {
        return running;
    }

    /** Streams a worker is on now. */
    public int inFlight() {
        return processingMap.size();
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) dispatcherThread.shutdownNow();
        if (recoveryThread != null) recoveryThread.shutdownNow();
        if (workerPool != null) workerPool.shutdown();
        logger.info("Core MT-router consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                refreshWorkerPoolConfig();
                String streamKey = queueManager.popResponsibleDoorbell(StreamQueueManager.ZSET_MT_PENDING_ACTIVE, true);
                if (streamKey != null) {
                    if (processingMap.putIfAbsent(streamKey, true) == null) {
                        try {
                            workerPool.submit(new FetchAndProcessTask(streamKey));
                        } catch (java.util.concurrent.RejectedExecutionException full) {
                            // all workers busy: give the stream back instead of running it on the dispatcher
                            processingMap.remove(streamKey);
                            queueManager.triggerDoorbell(streamKey, true);
                            Thread.sleep(consumerConfig.dispatcherIdleSleepMs());
                        }
                    } else {
                        pendingDoorbellMap.put(streamKey, true);
                    }
                } else {
                    Thread.sleep(consumerConfig.dispatcherIdleSleepMs());
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                logger.error("MT Dispatcher loop error", e);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private class FetchAndProcessTask implements Runnable {
        private final String streamKey;

        public FetchAndProcessTask(String streamKey) {
            this.streamKey = streamKey;
        }

        @Override
        public void run() {
            try {
                queueManager.createGroup(streamKey, groupName, true);

                StreamTaskBudget budget = new StreamTaskBudget();
                while (running) {
                    Map<String, StreamEntryID> query = new HashMap<>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
                    
                    // 使用 Manager 封装的批量消费方法
                    long readAt = System.currentTimeMillis();
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, consumerConfig.batchSize(), query, true);

                    if (messages == null || messages.isEmpty()) {
                        break; 
                    }

                    long claimIdleMs = queueManager.getAutoClaimIdleMs();
                    StreamAttempt.Batch batch = claimBatch(streamKey, messages);
                    if (batch == null && StreamAttempt.enabled() && StreamAttempt.batchEnabled()) {
                        break; // the batch claim failed: the entries stay pending for the auto-claim
                    }
                    List<GmmsMessage> later = new java.util.ArrayList<GmmsMessage>();
                    for (GmmsMessage msg : messages) {
                        if (batch != null && !batch.covers(msg)) {
                            later.add(msg);  // its marker came again in the batch: on its own, after the ack
                            continue;
                        }
                        // older than the idle time: auto-claim may have taken this entry too
                        processMessage(streamKey, msg, System.currentTimeMillis() - readAt >= claimIdleMs, batch);
                    }
                    ackBatch(streamKey, batch);
                    for (GmmsMessage msg : later) {
                        processMessage(streamKey, msg, System.currentTimeMillis() - readAt >= claimIdleMs, null);
                    }
                    if (budget.spent()) {
                        break; // give the stream back, the finally re-rings it
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing MT stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                // M-1: entries left that this group has not read (also after the budget) are never put aside;
                // the ones in flight (pending) are the auto-claim's, re-ringing for them only busy-loops
                boolean rung = pendingDoorbellMap.remove(streamKey) != null;
                if (rung || queueManager.unreadEntries(streamKey, groupName, true) > 0) {
                    queueManager.triggerDoorbell(streamKey, true);
                }
            }
        }
    }

    private void refreshWorkerPoolConfig() {
        long now = System.currentTimeMillis();
        if (now - lastPoolConfigRefreshMs < 5000L) {
            return;
        }
        lastPoolConfigRefreshMs = now;
        consumerConfig.applyWorkerPool(workerPool);
    }

    private void processMessage(String streamKey, GmmsMessage msg) {
        processMessage(streamKey, msg, false, null);
    }

    private void processMessage(String streamKey, GmmsMessage msg, boolean mayBeClaimed) {
        processMessage(streamKey, msg, mayBeClaimed, null);
    }

    /**
     * The attempts of a batch claimed in one Lua (CoreStreamAttemptBatch, docs/StreamAttempt按批处理设计.md), null when
     * the entries are claimed one by one. A Redis error is logged and null returned with batches on: the caller
     * leaves the entries pending.
     */
    private StreamAttempt.Batch claimBatch(String streamKey, List<GmmsMessage> messages) {
        if (!StreamAttempt.enabled() || !StreamAttempt.batchEnabled()) {
            return null;
        }
        try {
            return StreamAttempt.Batch.claim(streamKey, groupName, messages,
                    com.king.redis.RedisClient.getInstance().getSubmitMqRedis(), false,
                    consumerName + ":" + Thread.currentThread().getId());
        } catch (Exception e) {
            logger.error("MT batch claim failed, {} entries left pending for the auto-claim. streamKey={}",
                    messages.size(), streamKey, e);
            return null;
        }
    }

    /** XACK+XDEL+DEL marker of the batch's finished entries in one Lua; a failure leaves them pending */
    private void ackBatch(final String streamKey, StreamAttempt.Batch batch) {
        if (batch == null) {
            return;
        }
        try {
            batch.ackAll(new StreamAttempt.AckListener() {
                public void acked(GmmsMessage msg, String redisId, boolean acked) {
                    queueManager.traceAck(streamKey, groupName, redisId, msg, true, acked);
                    if (!acked) {
                        logger.warn(msg, "MT stream ack failed after process success. streamKey={}, sourceRedisId={}",
                                streamKey, redisId);
                    }
                }
            });
        } catch (Exception e) {
            logger.error("MT batch ack failed, the entries stay pending (redelivered, their side effects are flagged). streamKey={}",
                    streamKey, e);
        }
    }

    private void processMessage(String streamKey, GmmsMessage msg, boolean mayBeClaimed, StreamAttempt.Batch batch) {
        long start = System.nanoTime();
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        try {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                    MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                    MetricsNames.STAGE_IN_SUBMIT, msg, false, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            boolean attempt = StreamAttempt.enabled();
            if (attempt && (batch != null ? batch.begin(msg) : StreamAttempt.begin(msg, consumerName + ":" + Thread.currentThread().getId()))
                    == StreamAttempt.Start.BUSY) {
                logger.info(msg, "MT is being processed by another consumer, left pending. streamKey={}, redisId={}",
                        streamKey, sourceRedisId);
                return;
            }
            if (mayBeClaimed && StreamAttempt.entryGoneAfterBegin(streamKey, sourceRedisId,
                    com.king.redis.RedisClient.getInstance().getSubmitMqRedis())) {
                logger.info(msg, "MT already acked by another consumer, this copy dropped. streamKey={}, redisId={}",
                        streamKey, sourceRedisId);
                return;
            }
            boolean processed = false;
            try {
                processed = InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(msg);
            } finally {
                if (attempt && !processed) {
                    StreamAttempt.abandon();
                }
            }
            if (processed && attempt && batch != null) {
                batch.finished(msg);   // flags written now, acked with the batch
            } else if (processed) {
                // with an attempt the ack also removes its marker
                boolean acked = attempt ? StreamAttempt.finish(streamKey, groupName, sourceRedisId)
                        : queueManager.ackById(streamKey, groupName, sourceRedisId, msg, true);
                if (attempt) {
                    queueManager.traceAck(streamKey, groupName, sourceRedisId, msg, true, acked);
                }
                if (!acked) {
                    logger.warn(msg,
                            "MT stream ack failed after process success. streamKey={}, sourceRedisId={}, currentRedisId={}",
                            streamKey, sourceRedisId, msg == null ? null : msg.getRedisStreamID());
                }
            } else {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                        MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
                MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                        MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTING_FAILED));
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_IN_SUBMIT, msg, false, MetricsNames.ACTION_ROUTING_FAILED));
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "process_return_false");
            }
        } catch (Exception e) {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                    MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_ROUTING_FAILED));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                    MetricsNames.STAGE_IN_SUBMIT, msg, false, MetricsNames.ACTION_ROUTING_FAILED));
            logger.error(msg, "Failed to process MT from stream {}", streamKey, e);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.consumer(MetricsNames.CONSUMER_MT),
                    System.nanoTime() - start);
        }
    }
}
