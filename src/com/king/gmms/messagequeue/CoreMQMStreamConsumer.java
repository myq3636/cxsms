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
 * Consumes messages replayed by MessageQueueMonitor through dedicated MQM streams.
 */
public class CoreMQMStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(CoreMQMStreamConsumer.class);
    private static final CoreMQMStreamConsumer instance = new CoreMQMStreamConsumer();
    private static final String MODULE_ROLE = "core";

    private final ConsumerLane submitLane;
    private final ConsumerLane reportLane;

    private CoreMQMStreamConsumer() {
        submitLane = new ConsumerLane(
                "mqm-submit",
                "CoreMQMSubmitGroup",
                StreamQueueManager.ZSET_MQM_SUBMIT_ACTIVE,
                StreamQueueManager.STR_MQM_SUBMIT_PREFIX + "*",
                true,
                RedisStreamConsumerConfig.load("Core", "CoreMQMSubmit", 20, 50, 1000, 100, 50));
        reportLane = new ConsumerLane(
                "mqm-report",
                "CoreMQMReportGroup",
                StreamQueueManager.ZSET_MQM_REPORT_ACTIVE,
                StreamQueueManager.STR_MQM_REPORT_PREFIX + "*",
                false,
                RedisStreamConsumerConfig.load("Core", "CoreMQMReport", 10, 30, 1000, 100, 50));
    }

    public static CoreMQMStreamConsumer getInstance() {
        return instance;
    }

    public synchronized void start() {
        submitLane.start();
        reportLane.start();
    }

    public synchronized void stop() {
        submitLane.stop();
        reportLane.stop();
    }

    private static class ConsumerLane {
        private final String functionName;
        private final String groupName;
        private final String doorbellKey;
        private final String streamPattern;
        private final boolean submitMq;
        private final String consumerName;
        private final StreamQueueManager queueManager;
        private final RedisStreamConsumerConfig consumerConfig;

        private volatile boolean running = false;
        private volatile long lastPoolConfigRefreshMs = 0L;
        private ExecutorService dispatcherThread;
        private ThreadPoolExecutor workerPool;
        private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<String, Boolean>();
        private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<String, Boolean>();

        private ConsumerLane(String functionName, String groupName, String doorbellKey,
                String streamPattern, boolean submitMq, RedisStreamConsumerConfig consumerConfig) {
            this.functionName = functionName;
            this.groupName = groupName;
            this.doorbellKey = doorbellKey;
            this.streamPattern = streamPattern;
            this.submitMq = submitMq;
            this.consumerName = MODULE_ROLE + "-" + functionName + "-" + System.getProperty("NodeID", "0");
            this.queueManager = StreamQueueManager.getInstance();
            this.consumerConfig = consumerConfig;
        }

        private synchronized void start() {
            if (running) {
                return;
            }
            running = true;

            workerPool = new ThreadPoolExecutor(
                    consumerConfig.workerCorePoolSize(), consumerConfig.workerMaxPoolSize(), 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(consumerConfig.workerQueueSize()),
                    new ThreadPoolExecutor.CallerRunsPolicy());

            dispatcherThread = Executors.newSingleThreadExecutor();
            dispatcherThread.execute(new Runnable() {
                public void run() {
                    dispatcherLoop();
                }
            });

            queueManager.reRingExistingStreams(streamPattern, submitMq);
            logger.info("Core MQM stream consumer started. Function: {}, Group: {}, Consumer: {}, Shards: {}, Config: {}",
                    functionName, groupName, consumerName, GmmsUtility.getInstance().getMyShards(), consumerConfig.summary());
        }

        private synchronized void stop() {
            running = false;
            if (dispatcherThread != null) {
                dispatcherThread.shutdownNow();
            }
            if (workerPool != null) {
                workerPool.shutdown();
            }
            logger.info("Core MQM stream consumer stopped. Function: {}, Consumer: {}", functionName, consumerName);
        }

        private void dispatcherLoop() {
            while (running) {
                try {
                    refreshWorkerPoolConfig();
                    String streamKey = queueManager.popResponsibleDoorbell(doorbellKey, submitMq);
                    if (streamKey != null) {
                        if (processingMap.putIfAbsent(streamKey, true) == null) {
                            workerPool.submit(new FetchAndProcessTask(streamKey));
                        } else {
                            pendingDoorbellMap.put(streamKey, true);
                        }
                    } else {
                        Thread.sleep(consumerConfig.dispatcherIdleSleepMs());
                    }
                } catch (InterruptedException ie) {
                    break;
                } catch (Exception e) {
                    logger.error("Core MQM dispatcher loop error. function=" + functionName, e);
                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException ignored) {
                        break;
                    }
                }
            }
        }

        private class FetchAndProcessTask implements Runnable {
            private final String streamKey;

            private FetchAndProcessTask(String streamKey) {
                this.streamKey = streamKey;
            }

            public void run() {
                try {
                    queueManager.createGroup(streamKey, groupName, submitMq);
                    processAutoClaim(streamKey);

                    while (running) {
                        Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
                        query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);

                        List<GmmsMessage> messages = queueManager.consumeBatch(
                                groupName, consumerName, consumerConfig.batchSize(), query, submitMq);
                        if (messages == null || messages.isEmpty()) {
                            break;
                        }

                        for (GmmsMessage msg : messages) {
                            processMessage(streamKey, msg);
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error processing MQM stream: " + streamKey, e);
                } finally {
                    processingMap.remove(streamKey);
                    if (pendingDoorbellMap.remove(streamKey) != null && queueManager.streamLength(streamKey, submitMq) > 0) {
                        queueManager.triggerDoorbell(streamKey, submitMq);
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

        private void processAutoClaim(String streamKey) {
            List<GmmsMessage> orphans = queueManager.autoClaimBatch(streamKey, groupName, consumerName,
                    queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimBatchSize(), submitMq);
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
                MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                        MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
                boolean processed = submitMq
                        ? InternalCoreEngineSession.getReactiveInstance().processMqmSubmitFromStream(msg)
                        : InternalCoreEngineSession.getReactiveInstance().processMqmReportFromStream(msg);
                if (processed) {
                    incrementSsidProcessed(msg);
                    queueManager.ack(streamKey, groupName, msg, submitMq);
                } else {
                    MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                            MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
                    queueManager.logNack(streamKey, groupName, consumerName, msg, submitMq, "core_rejected_mqm_message");
                }
            } catch (Exception e) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                        MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
                logger.error(msg, "Failed to process MQM message from stream {}", streamKey, e);
            } finally {
                MetricsCollector.getInstance().recordTime(MetricsNames.timer(MetricsNames.COMPONENT_CONSUMER,
                        MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_LATENCY),
                        System.nanoTime() - start);
            }
        }

        private void incrementSsidProcessed(GmmsMessage msg) {
            if (msg == null) {
                return;
            }
            if (submitMq) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_PROCESSED));
            } else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_OUT_DR, msg, false, MetricsNames.ACTION_PROCESSED));
            } else {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_IN_DR, msg, false, MetricsNames.ACTION_PROCESSED));
            }
        }
    }
}
