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

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        recoveryThread = Executors.newSingleThreadExecutor();
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Core MT-router",
                StreamQueueManager.STR_MT_PENDING_PREFIX + "*", groupName, consumerName, true, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        processMessage(streamKey, msg);
                    }
                }));

        logger.info("Core MT-router consumer started. Consumer: {}, Shards: {}, Config: {}",
            consumerName, GmmsUtility.getInstance().getMyShards(), consumerConfig.summary());
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
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, consumerConfig.batchSize(), query, true);

                    if (messages == null || messages.isEmpty()) {
                        break; 
                    }

                    for (GmmsMessage msg : messages) {
                        processMessage(streamKey, msg);
                    }
                    if (budget.spent()) {
                        break; // give the stream back, the finally re-rings it
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing MT stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                // M-1: 与 ResultStreamConsumer 安全范式一致 —— pendingDoorbellMap 仅做清理，
                // re-ring 只看 streamLength>0(只处理一批后仍有剩余、或处理期间无新 doorbell 时不再被搁置)。
                pendingDoorbellMap.remove(streamKey);
                if (queueManager.streamLength(streamKey, true) > 0) {
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
        long start = System.nanoTime();
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        try {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                    MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                    MetricsNames.STAGE_IN_SUBMIT, msg, false, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            boolean attempt = StreamAttempt.enabled();
            if (attempt && StreamAttempt.begin(msg, consumerName + ":" + Thread.currentThread().getId())
                    == StreamAttempt.Start.BUSY) {
                logger.info(msg, "MT is being processed by another consumer, left pending. streamKey={}, redisId={}",
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
            if (processed) {
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
            MetricsCollector.getInstance().recordTime(MetricsNames.timer(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_LATENCY),
                    System.nanoTime() - start);
        }
    }
}
