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
    
    private ExecutorService dispatcherThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<>();

    private final StreamQueueManager queueManager;

    private MTStreamConsumer() {
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + System.getProperty("NodeID", "0");
        this.queueManager = StreamQueueManager.getInstance();
    }

    public static MTStreamConsumer getInstance() { return instance; }

    public synchronized void start() {
        if (running) return;
        running = true;
        
        workerPool = new ThreadPoolExecutor(
            50, 100, 60L, TimeUnit.SECONDS, 
            new LinkedBlockingQueue<Runnable>(1000), 
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);

        logger.info("Core MT-router consumer started. Consumer: {}, Shards: {}",
            consumerName, GmmsUtility.getInstance().getMyShards());
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) dispatcherThread.shutdownNow();
        if (workerPool != null) workerPool.shutdown();
        logger.info("Core MT-router consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                String streamKey = queueManager.popResponsibleDoorbell(StreamQueueManager.ZSET_MT_PENDING_ACTIVE, true);
                if (streamKey != null) {
                    if (processingMap.putIfAbsent(streamKey, true) == null) {
                        workerPool.submit(new FetchAndProcessTask(streamKey));
                    } else {
                        pendingDoorbellMap.put(streamKey, true);
                    }
                } else {
                    Thread.sleep(50);
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

                while (running) {
                    processAutoClaim(streamKey);

                    Map<String, StreamEntryID> query = new HashMap<>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
                    
                    // 使用 Manager 封装的批量消费方法
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 100, query, true);

                    if (messages == null || messages.isEmpty()) {
                        break; 
                    }

                    for (GmmsMessage msg : messages) {
                        processMessage(streamKey, msg);
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing MT stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                if (pendingDoorbellMap.remove(streamKey) != null && queueManager.streamLength(streamKey, true) > 0) {
                    queueManager.triggerDoorbell(streamKey, true);
                }
            }
        }
    }

    private void processAutoClaim(String streamKey) {
        List<GmmsMessage> orphans = queueManager.autoClaimBatch(streamKey, groupName, consumerName,
                queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimBatchSize(), true);
        if (orphans == null || orphans.isEmpty()) {
            return;
        }
        for (GmmsMessage msg : orphans) {
            processMessage(streamKey, msg);
        }
    }

    private void processMessage(String streamKey, GmmsMessage msg) {
        long start = System.nanoTime();
        try {
            boolean processed = InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(msg);
            if (processed) {
                queueManager.ack(streamKey, groupName, msg, true);
            } else {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.counter("consumer", "process", msg, "fail"));
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "process_return_false");
            }
        } catch (Exception e) {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter("consumer", "process", msg, "fail"));
            logger.error(msg, "Failed to process MT from stream {}", streamKey, e);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer("consumer", "process", msg, "latency"),
                    System.nanoTime() - start);
        }
    }
}
