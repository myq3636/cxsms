package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
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
import com.king.message.gmms.MessageStoreManager;

import redis.clients.jedis.StreamEntryID;

/**
 * Consumes DR messages produced by client-side SMPP sessions and submits them to Core.
 */
public class InboundDRStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(InboundDRStreamConsumer.class);
    private static InboundDRStreamConsumer instance = new InboundDRStreamConsumer();
    private static final String MODULE_ROLE = "core";
    private static final String FUNCTION_NAME = "inbound-dr";

    private final String groupName = "CoreInboundDRGroup";
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

    private InboundDRStreamConsumer() {
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + System.getProperty("NodeID", "0");
        this.queueManager = StreamQueueManager.getInstance();
        this.consumerConfig = RedisStreamConsumerConfig.load("Core", "CoreInboundDR", 20, 50, 1000, 100, 50);
    }

    public static InboundDRStreamConsumer getInstance() {
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
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Core inbound-DR",
                StreamQueueManager.STR_INBOUND_DR_PREFIX + "*", groupName, consumerName, false, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        processMessage(streamKey, msg);
                    }
                }));

        logger.info("Core inbound-DR consumer started. Group: {}, Consumer: {}, Config: {}",
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
        logger.info("Core inbound-DR consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                refreshWorkerPoolConfig();
                String streamKey = queueManager.popResponsibleDoorbell(StreamQueueManager.ZSET_INBOUND_DR_ACTIVE, false);
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
                logger.error("DR dispatcher loop error", e);
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
                        processMessage(streamKey, msg);
                    }
                    if (budget.spent()) {
                        break; // give the stream back, the finally re-rings it
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing inbound DR stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                pendingDoorbellMap.remove(streamKey);
                // re-ring whenever entries remain (the task may have stopped on its budget)
                if (queueManager.streamLength(streamKey, false) > 0) {
                    queueManager.triggerDoorbell(streamKey, false);
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

    private void processMessage(String streamKey, final GmmsMessage msg) {
        long start = System.nanoTime();
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        try {
            boolean attempt = StreamAttempt.drResultEnabled();
            com.king.redis.RedisConnection streamRedis = com.king.redis.RedisClient.getInstance().getReportMqRedis();
            if (attempt) {
                StreamAttempt.Start state = StreamAttempt.beginEntry(streamKey, msg, streamRedis,
                        consumerName + ":" + Thread.currentThread().getId());
                if (state == StreamAttempt.Start.BUSY) {
                    logger.info(msg, "Inbound DR is being processed by another consumer, left pending. streamKey={}, redisId={}",
                            streamKey, sourceRedisId);
                    return;
                }
                if (state == StreamAttempt.Start.DONE) {
                    queueManager.traceAck(streamKey, groupName, sourceRedisId, msg, false,
                            StreamAttempt.ackDone(streamKey, groupName, msg, streamRedis));
                    return;
                }
            }
            boolean processed = false;
            try {
                // The DR to the customer is produced on this thread, before the ack below
                processed = MessageStoreManager.withInlineDrDispatch(new Callable<Boolean>() {
                    public Boolean call() {
                        return InternalCoreEngineSession.getReactiveInstance().handleInboundDRInternal(msg, null);
                    }
                });
            } finally {
                if (attempt && !processed) {
                    StreamAttempt.abandon();
                }
            }
            if (processed) {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                        MetricsNames.FLOW_CORE, msg, MetricsNames.ACTION_PROCESSED));
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_IN_DR, msg, true, MetricsNames.ACTION_PROCESSED));
                // with an attempt: _done first, then XACK+XDEL+DEL marker
                boolean acked = attempt ? StreamAttempt.finish(streamKey, groupName, sourceRedisId)
                        : queueManager.ackById(streamKey, groupName, sourceRedisId, msg, false);
                if (attempt) {
                    queueManager.traceAck(streamKey, groupName, sourceRedisId, msg, false, acked);
                }
                if (!acked) {
                    logger.warn(msg,
                            "Inbound DR stream ack failed after process success. streamKey={}, sourceRedisId={}, currentRedisId={}",
                            streamKey, sourceRedisId, msg == null ? null : msg.getRedisStreamID());
                }
            } else {
                MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                        MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
                queueManager.logNack(streamKey, groupName, consumerName, msg, false, "core_rejected_inbound_dr");
            }
        } catch (Exception e) {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
            logger.error(msg, "Failed to process DR from stream {}", streamKey, e);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_LATENCY),
                    System.nanoTime() - start);
        }
    }
}
