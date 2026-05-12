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
import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;

/**
 * Consumes outbound submit/dr results from stream:core:results and closes them in Core.
 */
public class ResultStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(ResultStreamConsumer.class);
    private static ResultStreamConsumer instance = new ResultStreamConsumer();
    private static final String MODULE_ROLE = "core";
    private static final String FUNCTION_NAME = "outbound-result";

    private final String groupName = "CoreResultGroup";
    private final String consumerName;
    private volatile boolean running = false;

    private ExecutorService dispatcherThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<String, Boolean>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<String, Boolean>();

    private final StreamQueueManager queueManager;

    private ResultStreamConsumer() {
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + System.getProperty("NodeID", "0");
        this.queueManager = StreamQueueManager.getInstance();
    }

    public static ResultStreamConsumer getInstance() {
        return instance;
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;

        workerPool = new ThreadPoolExecutor(
            10, 30, 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(1000),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(new Runnable() {
            public void run() {
                dispatcherLoop();
            }
        });

        queueManager.triggerDoorbell(StreamQueueManager.STR_CORE_RESULTS, false);
        logger.info("Core outbound-result consumer started. Group: {}, Consumer: {}", groupName, consumerName);
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) {
            dispatcherThread.shutdownNow();
        }
        if (workerPool != null) {
            workerPool.shutdown();
        }
        logger.info("Core outbound-result consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                String streamKey = queueManager.popDoorbell(StreamQueueManager.ZSET_CORE_RESULTS_ACTIVE, false);
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
                logger.error("Result dispatcher loop error", e);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private class FetchAndProcessTask implements Runnable {
        private final String streamKey;

        public FetchAndProcessTask(String streamKey) {
            this.streamKey = streamKey;
        }

        public void run() {
            try {
                queueManager.createGroup(streamKey, groupName, false);
                processAutoClaim(streamKey);

                while (running) {
                    Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);

                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 100, query, false);
                    if (messages == null || messages.isEmpty()) {
                        break;
                    }

                    for (GmmsMessage msg : messages) {
                        processResult(streamKey, msg);
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing Result stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                if (pendingDoorbellMap.remove(streamKey) != null && queueManager.streamLength(streamKey, false) > 0) {
                    queueManager.triggerDoorbell(streamKey, false);
                }
            }
        }
    }

    private void processAutoClaim(String streamKey) {
        List<GmmsMessage> orphans = queueManager.autoClaimBatch(streamKey, groupName, consumerName,
            queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimBatchSize(), false);
        if (orphans == null || orphans.isEmpty()) {
            return;
        }
        for (GmmsMessage msg : orphans) {
            processResult(streamKey, msg);
        }
    }

    private void processResult(String streamKey, GmmsMessage msg) {
        long start = System.nanoTime();
        try {
            if (!InternalCoreEngineSession.getReactiveInstance().handleOutboundResultInternal(msg)) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.counter("consumer", "process", msg, "fail"));
                queueManager.logNack(streamKey, groupName, consumerName, msg, false, "core_rejected_outbound_result");
                return;
            }
            CoreOutboundOutboxManager.getInstance().complete(msg);
            queueManager.ack(streamKey, groupName, msg, false);
        } catch (Exception e) {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter("consumer", "process", msg, "fail"));
            logger.error(msg, "Failed to process result in Core", e);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer("consumer", "process", msg, "latency"),
                    System.nanoTime() - start);
        }
    }
}
