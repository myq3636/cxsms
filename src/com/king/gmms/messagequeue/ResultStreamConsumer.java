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
    private volatile long lastPoolConfigRefreshMs = 0L;

    private ExecutorService dispatcherThread;
    private ExecutorService recoveryThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<String, Boolean>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<String, Boolean>();

    private final StreamQueueManager queueManager;
    private final RedisStreamConsumerConfig consumerConfig;

    private ResultStreamConsumer() {
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + System.getProperty("NodeID", "0");
        this.queueManager = StreamQueueManager.getInstance();
        this.consumerConfig = RedisStreamConsumerConfig.load("Core", "CoreResult", 10, 30, 1000, 100, 50);
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
            consumerConfig.workerCorePoolSize(), consumerConfig.workerMaxPoolSize(), 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(consumerConfig.workerQueueSize()),
            new ThreadPoolExecutor.AbortPolicy()
        );

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(new Runnable() {
            public void run() {
                dispatcherLoop();
            }
        });
        recoveryThread = Executors.newSingleThreadExecutor();
        // legacy stream:core:results and the sharded stream:core:results:{ssid}:{shard}
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Core outbound-result",
                StreamQueueManager.STR_CORE_RESULTS + "*", groupName, consumerName, false, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        processResult(streamKey, msg);
                    }
                }));

        queueManager.reRingExistingStreams(StreamQueueManager.STR_CORE_RESULTS + "*", false);
        logger.info("Core outbound-result consumer started. Group: {}, Consumer: {}, Config: {}",
                groupName, consumerName, consumerConfig.summary());
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) {
            dispatcherThread.shutdownNow();
        }
        if (recoveryThread != null) {
            recoveryThread.shutdownNow();
        }
        if (workerPool != null) {
            workerPool.shutdown();
        }
        logger.info("Core outbound-result consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                refreshWorkerPoolConfig();
                String streamKey = queueManager.popDoorbell(StreamQueueManager.ZSET_CORE_RESULTS_ACTIVE, false);
                if (streamKey != null) {
                    if (processingMap.putIfAbsent(streamKey, true) == null) {
                        try {
                            workerPool.submit(new FetchAndProcessTask(streamKey));
                        } catch (java.util.concurrent.RejectedExecutionException full) {
                            // all workers busy: give the stream back instead of running it on the dispatcher
                            processingMap.remove(streamKey);
                            queueManager.triggerDoorbell(streamKey, false);
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

                StreamTaskBudget budget = new StreamTaskBudget();
                while (running) {
                    Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);

                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, consumerConfig.batchSize(), query, false);
                    if (messages == null || messages.isEmpty()) {
                        break;
                    }

                    for (GmmsMessage msg : messages) {
                        processResult(streamKey, msg);
                    }
                    if (budget.spent()) {
                        break; // give the stream back, the finally re-rings it
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing Result stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                boolean hadPendingDoorbell = pendingDoorbellMap.remove(streamKey) != null;
                long remaining = queueManager.streamLength(streamKey, false);
                if (remaining > 0) {
                    queueManager.triggerDoorbell(streamKey, false);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Core result stream re-ring after worker exit. streamKey={}, remaining={}, hadPendingDoorbell={}",
                                streamKey, remaining, hadPendingDoorbell);
                    }
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

    private void processResult(String streamKey, GmmsMessage msg) {
        long start = System.nanoTime();
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        try {
            String resultType = msg.getRedisStreamMessageType() != null
                    ? msg.getRedisStreamMessageType() : msg.getMessageType();
            if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(resultType)
                    && CoreOutboundOutboxManager.getInstance().claimResult(msg) == CoreOutboundOutboxManager.Claim.STALE) {
                // this outbound attempt already has its result (e.g. a submit_resp after the timeout)
                logger.warn(msg, "Late or duplicate submit result dropped. streamKey={}, redisId={}, status={}",
                        streamKey, sourceRedisId, msg.getStatusCode());
                queueManager.ackById(streamKey, groupName, sourceRedisId, msg, false);
                return;
            }
            if (!InternalCoreEngineSession.getReactiveInstance().handleOutboundResultInternal(msg)) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                        MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
                queueManager.logNack(streamKey, groupName, consumerName, msg, false, "core_rejected_outbound_result");
                return;
            }
            if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(resultType)) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_PROCESSED));
            } else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(resultType)) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_OUT_DR, msg, false, MetricsNames.ACTION_PROCESSED));
            }
            boolean acked = queueManager.ackById(streamKey, groupName, sourceRedisId, msg, false);
            if (!acked) {
                logger.warn(msg,
                        "Result stream ack failed after process success. streamKey={}, sourceRedisId={}, currentRedisId={}",
                        streamKey, sourceRedisId, msg == null ? null : msg.getRedisStreamID());
            }
        } catch (Exception e) {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
            logger.error(msg, "Failed to process result in Core", e);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_LATENCY),
                    System.nanoTime() - start);
        }
    }
}
