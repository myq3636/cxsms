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
        private ExecutorService recoveryThread;
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
                    new ThreadPoolExecutor.AbortPolicy());

            dispatcherThread = Executors.newSingleThreadExecutor();
            dispatcherThread.execute(new Runnable() {
                public void run() {
                    dispatcherLoop();
                }
            });
            recoveryThread = Executors.newSingleThreadExecutor();
            recoveryThread.execute(new RedisStreamAutoClaimRunner("Core " + functionName,
                    streamPattern, groupName, consumerName, submitMq, queueManager,
                    new RedisStreamAutoClaimRunner.MessageProcessor() {
                        public void process(String streamKey, GmmsMessage msg) {
                            processMessage(streamKey, msg);
                        }
                    }));

            queueManager.reRingExistingStreams(streamPattern, submitMq);
            logger.info("Core MQM stream consumer started. Function: {}, Group: {}, Consumer: {}, Shards: {}, Config: {}",
                    functionName, groupName, consumerName, GmmsUtility.getInstance().getMyShards(), consumerConfig.summary());
        }

        private synchronized void stop() {
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
            logger.info("Core MQM stream consumer stopped. Function: {}, Consumer: {}", functionName, consumerName);
        }

        private void dispatcherLoop() {
            while (running) {
                try {
                    refreshWorkerPoolConfig();
                    String streamKey = queueManager.popResponsibleDoorbell(doorbellKey, submitMq);
                    if (streamKey != null) {
                        if (processingMap.putIfAbsent(streamKey, true) == null) {
                            try {
                                workerPool.submit(new FetchAndProcessTask(streamKey));
                            } catch (java.util.concurrent.RejectedExecutionException full) {
                                // all workers busy: give the stream back instead of running it on the dispatcher
                                processingMap.remove(streamKey);
                                queueManager.triggerDoorbell(streamKey, submitMq);
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

                    StreamTaskBudget budget = new StreamTaskBudget();
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
                        if (budget.spent()) {
                            break; // give the stream back, the finally re-rings it
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error processing MQM stream: " + streamKey, e);
                } finally {
                    processingMap.remove(streamKey);
                    boolean hadPendingDoorbell = pendingDoorbellMap.remove(streamKey) != null;
                    long remaining = queueManager.streamLength(streamKey, submitMq);
                    if (remaining > 0) {
                        queueManager.triggerDoorbell(streamKey, submitMq);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Core MQM stream re-ring after worker exit. function={}, streamKey={}, remaining={}, hadPendingDoorbell={}",
                                    functionName, streamKey, remaining, hadPendingDoorbell);
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

        // C-9: 缓存权威纪元(mqm:active:fence)，每 3s 刷新一次(纪元仅易主时变，无需每条读 Redis)。
        private volatile long cachedAuthorityFence = -1L;
        private volatile long cachedAuthorityFenceMs = 0L;

        /** 消息纪元 < 当前权威纪元 → 旧任期(脑裂旧 active)产出，应拒绝。缺失纪元(-1)按"容忍接受"(灰度)。 */
        private boolean isStaleFence(GmmsMessage msg) {
            long msgFence = msg.getRedisFence();
            if (msgFence < 0L) {
                return false;
            }
            long authority = currentAuthorityFence();
            return authority >= 0L && msgFence < authority;
        }

        private long currentAuthorityFence() {
            long now = System.currentTimeMillis();
            if (now - cachedAuthorityFenceMs >= 3000L) {
                try {
                    com.king.redis.RedisClient rc = com.king.gmms.GmmsUtility.getInstance().getRedisClient();
                    if (rc != null) {
                        cachedAuthorityFence = rc.getFence();
                    }
                } catch (Exception e) {
                    // 读失败保持上次值
                }
                cachedAuthorityFenceMs = now;
            }
            return cachedAuthorityFence;
        }

        private void processMessage(String streamKey, GmmsMessage msg) {
            long start = System.nanoTime();
            String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
            try {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                        MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
                if (isStaleFence(msg)) {
                    // C-9: 旧任期(脑裂的旧 active MQM)产的补偿消息 → 直接丢弃(ACK)，不处理，避免重复下发。
                    logger.warn(msg, "Reject stale-fence MQM message. msgFence={}, streamKey={}",
                            msg.getRedisFence(), streamKey);
                    queueManager.ackById(streamKey, groupName, sourceRedisId, msg, submitMq);
                    return;
                }
                boolean processed = submitMq
                        ? InternalCoreEngineSession.getReactiveInstance().processMqmSubmitFromStream(msg)
                        : InternalCoreEngineSession.getReactiveInstance().processMqmReportFromStream(msg);
                if (processed) {
                    incrementSsidProcessed(msg);
                    boolean acked = queueManager.ackById(streamKey, groupName, sourceRedisId, msg, submitMq);
                    if (!acked) {
                        logger.warn(msg,
                                "MQM stream ack failed after process success. streamKey={}, sourceRedisId={}, currentRedisId={}",
                                streamKey, sourceRedisId, msg == null ? null : msg.getRedisStreamID());
                    }
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
