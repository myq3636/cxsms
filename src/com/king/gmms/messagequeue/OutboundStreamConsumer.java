package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.MultiSmppSession;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.session.SmppSubmitResult;
import com.king.gmms.customerconnectionfactory.MultiSmppClientFactory;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.throttle.DistributedThrottlingManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/**
 * V5.2 Client-side outbound consumer.
 *
 * Reads routed MT messages, sends them through SMPP client sessions, and keeps
 * stream entries pending when the selected session submit window is full.
 */
public class OutboundStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(OutboundStreamConsumer.class);
    private static OutboundStreamConsumer instance = new OutboundStreamConsumer();
    private static final String MODULE_ROLE = "client";
    private static final String FUNCTION_NAME = "outbound-submit";

    private final StreamQueueManager queueManager;
    private final String groupName = "ClientOutboundSubmitGroup";
    private final String consumerName;
    
    private volatile boolean running = false;
    private volatile long lastPoolConfigRefreshMs = 0L;
    private ExecutorService dispatcherThread;
    private ExecutorService recoveryThread;
    private ScheduledExecutorService windowRetryThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> windowRetryMap = new ConcurrentHashMap<String, Boolean>();
    private final RedisStreamConsumerConfig consumerConfig;

    private enum ProcessResult {
        DONE,
        WINDOW_FULL
    }

    private OutboundStreamConsumer() {
        this.queueManager = StreamQueueManager.getInstance();
        String nodeID = System.getProperty("NodeID", "0");
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + nodeID;
        this.consumerConfig = RedisStreamConsumerConfig.load("Client", "ClientOutboundSubmit", 50, 200, 2000, 100, 50);
    }

    public static OutboundStreamConsumer getInstance() {
        return instance;
    }

    public synchronized void start() {
        if (running) return;
        running = true;

        workerPool = new ThreadPoolExecutor(
            consumerConfig.workerCorePoolSize(), consumerConfig.workerMaxPoolSize(), 60L, TimeUnit.SECONDS, 
            new LinkedBlockingQueue<Runnable>(consumerConfig.workerQueueSize()), 
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        windowRetryThread = Executors.newSingleThreadScheduledExecutor();
        recoveryThread = Executors.newSingleThreadExecutor();
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Client outbound-submit",
                StreamQueueManager.STR_MT_ROUTED_PREFIX + "*", groupName, consumerName, true, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        if (processMessage(streamKey, msg) == ProcessResult.WINDOW_FULL) {
                            scheduleWindowRetry(streamKey);
                        }
                    }
                }));
        queueManager.reRingExistingStreams(StreamQueueManager.STR_MT_ROUTED_PREFIX + "*", true);

        logger.info("Client outbound-submit consumer started. Consumer: {}, Shards: {}, Config: {}",
            consumerName, GmmsUtility.getInstance().getMyShards(), consumerConfig.summary());
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) dispatcherThread.shutdownNow();
        if (recoveryThread != null) recoveryThread.shutdownNow();
        if (windowRetryThread != null) windowRetryThread.shutdownNow();
        if (workerPool != null) workerPool.shutdown();
        logger.info("Client outbound-submit consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                refreshWorkerPoolConfig();
                // 鐩戝惉 Routed 娲昏穬闃熷垪闂ㄩ搩
                String streamKey = queueManager.popResponsibleDoorbell(StreamQueueManager.ZSET_MT_ROUTED_ACTIVE, true);
                if (streamKey != null) {
                    // 閬垮厤閲嶅璋冨害鍚屼竴涓?streamKey
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
                logger.error("Outbound Dispatcher loop error", e);
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
                    Map<String, StreamEntryID> query = new HashMap<>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
                    
                    // 鎵归噺鎷夊彇
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, consumerConfig.batchSize(), query, true);

                    if (messages == null || messages.isEmpty()) {
                        break; 
                    }

                    for (GmmsMessage msg : messages) {
                        if (processMessage(streamKey, msg) == ProcessResult.WINDOW_FULL) {
                            scheduleWindowRetry(streamKey);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing Outbound stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                if (pendingDoorbellMap.remove(streamKey) != null && queueManager.streamLength(streamKey, true) > 0) {
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

    private ProcessResult processMessage(String streamKey, GmmsMessage msg) {
        long start = System.nanoTime();
        try {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                    MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                    MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            return handleMessageSending(msg, streamKey);
        } catch (Exception e) {
            logger.error(msg, "Error processing outbound stream message", e);
            handleFailure(msg, "Internal error during outbound processing", streamKey);
            return ProcessResult.DONE;
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_LATENCY),
                    System.nanoTime() - start);
        }
    }

    private ProcessResult handleMessageSending(GmmsMessage msg, String streamKey) {
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        try {
            Session session = MultiSmppClientFactory.getInstance().getSession(msg);
            
            if (session != null && isConnected(session)) {
                int ssid = msg.getRSsID();
                SmppSubmitResult submitResult;
                if (session instanceof MultiSmppSession) {
                    MultiSmppSession smppSession = (MultiSmppSession) session;
                    if (!smppSession.hasClientSubmitWindow()) {
                        recordWindowFull(msg);
                        queueManager.logNack(streamKey, groupName, consumerName, msg, true, "client_submit_window_full");
                        return ProcessResult.WINDOW_FULL;
                    }
                    DistributedThrottlingManager.getInstance().acquire(ssid);
                    submitResult = smppSession.submitClientOutbound(msg);
                } else {
                    DistributedThrottlingManager.getInstance().acquire(ssid);
                    submitResult = session.submit(msg) ? SmppSubmitResult.SENT : SmppSubmitResult.FAILED;
                }
                if (submitResult == SmppSubmitResult.WINDOW_FULL) {
                    recordWindowFull(msg);
                    queueManager.logNack(streamKey, groupName, consumerName, msg, true, "client_submit_window_full");
                    return ProcessResult.WINDOW_FULL;
                }
                if (submitResult == SmppSubmitResult.SENT) {
                    MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                            MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_SMPP_SENT));
                    MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                            MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_SMPP_SENT));
                    boolean acked = queueManager.ackById(streamKey, groupName, sourceRedisId, msg, true);
                    if (!acked) {
                        logger.warn(msg,
                                "Outbound submit stream ack failed after send success. streamKey={}, sourceRedisId={}, currentRedisId={}",
                                streamKey, sourceRedisId, msg == null ? null : msg.getRedisStreamID());
                    }
                } else {
                    handleFailure(msg, "Session submit rejected", streamKey);
                }
            } else {
                handleFailure(msg, "Session disconnected", streamKey);
            }
        } catch (Exception e) {
            logger.error(msg, "Error invoking MultiSmppSession submit", e);
            handleFailure(msg, "Internal error during submit", streamKey);
        }
        return ProcessResult.DONE;
    }
    
    private void scheduleWindowRetry(final String streamKey) {
        if (streamKey == null || windowRetryThread == null) {
            return;
        }
        if (windowRetryMap.putIfAbsent(streamKey, Boolean.TRUE) != null) {
            return;
        }
        final long retryMs = getWindowFullRetryMs();
        windowRetryThread.schedule(new Runnable() {
            public void run() {
                boolean retryAgain = false;
                try {
                    List<GmmsMessage> claimed = queueManager.autoClaimBatch(streamKey, groupName, consumerName,
                            retryMs, Math.max(1, consumerConfig.batchSize()), true);
                    if (claimed != null && !claimed.isEmpty()) {
                        MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(
                                MetricsNames.COMPONENT_STREAM, MetricsNames.FLOW_PROCESS,
                                null, MetricsNames.ACTION_RETRY_CLAIM), claimed.size());
                        for (GmmsMessage msg : claimed) {
                            if (processMessage(streamKey, msg) == ProcessResult.WINDOW_FULL) {
                                retryAgain = true;
                                return;
                            }
                        }
                    } else {
                        queueManager.triggerDoorbell(streamKey, true);
                    }
                } catch (Exception e) {
                    logger.warn("Client outbound window retry failed. streamKey={}", streamKey, e);
                    queueManager.triggerDoorbell(streamKey, true);
                } finally {
                    windowRetryMap.remove(streamKey);
                    if (retryAgain) {
                        scheduleWindowRetry(streamKey);
                    }
                }
            }
        }, retryMs, TimeUnit.MILLISECONDS);
    }

    private long getWindowFullRetryMs() {
        try {
            long value = Long.parseLong(GmmsUtility.getInstance()
                    .getCommonProperty("ClientOutboundWindowFullRetryMs", "100").trim());
            return Math.max(10L, Math.min(value, 60000L));
        } catch (Exception e) {
            return 100L;
        }
    }

    private void recordWindowFull(GmmsMessage msg) {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_WINDOW_FULL));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_WINDOW_FULL));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(
                MetricsNames.COMPONENT_CONSUMER, MetricsNames.FLOW_PROCESS, msg,
                MetricsNames.ACTION_WINDOW_FULL));
    }
    
    private boolean isConnected(Session session) {
        ConnectionStatus status = session.getStatus();
        return ConnectionStatus.CONNECT.equals(status) || ConnectionStatus.RECOVER.equals(status);
    }

    private void handleFailure(GmmsMessage msg, String reason, String streamKey) {
        boolean resultProduced = false;
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        try {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                    MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_FAILED_BEFORE_RESPONSE));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                    MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_FAILED_BEFORE_RESPONSE));
            logger.warn(msg, "Outbound submit failed before SMPP response: {}", reason);
            msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
            msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
            resultProduced = queueManager.produceResult(msg);
            if (!resultProduced) {
                logger.error(msg, "Failed to produce outbound failure result to Core");
            }
        } catch (Exception e) {
            logger.error(msg, "Failed to generate outbound failure result", e);
        } finally {
            if (resultProduced) {
                queueManager.ackById(streamKey, groupName, sourceRedisId, msg, true);
            } else {
                if (msg != null) {
                    msg.setRedisStreamID(sourceRedisId);
                }
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "failure_result_not_produced");
            }
        }
    }
}
