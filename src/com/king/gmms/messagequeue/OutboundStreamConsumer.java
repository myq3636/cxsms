package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
 * Reads routed MT messages and sends them through SMPP client sessions. Each entry is sent at most
 * once ({@link ClientSendMarker}). When the session window is full or the channel has no rate permit
 * (OutgoingThrottlingNum, per Client node, local Guava limiter, non-blocking), the rest of the batch
 * is put off for this stream: the worker is released, the entries stay pending for this consumer
 * (idle time reset, not re-delivered) and are processed again after the delay; no other task reads
 * the stream meanwhile.
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
    /** entries put off (window full / no permit), processed again after a delay */
    private StreamDeferrals deferrals;
    private final RedisStreamConsumerConfig consumerConfig;

    private enum ProcessResult {
        DONE,
        WINDOW_FULL,
        THROTTLED
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

        workerPool = consumerConfig.newElasticWorkerPool();

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        windowRetryThread = Executors.newSingleThreadScheduledExecutor();
        deferrals = new StreamDeferrals(windowRetryThread, new StreamDeferrals.Resume() {
            public void resume(String streamKey) {
                resumeDeferred(streamKey);
            }
        });
        recoveryThread = Executors.newSingleThreadExecutor();
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Client outbound-submit",
                StreamQueueManager.STR_MT_ROUTED_PREFIX + "*", groupName, consumerName, true, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        ProcessResult r = processMessage(streamKey, msg);
                        if (r != ProcessResult.DONE) {
                            defer(streamKey, java.util.Collections.singletonList(msg), retryDelayMs(r, msg));
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
                String streamKey = queueManager.popResponsibleDoorbell(StreamQueueManager.ZSET_MT_ROUTED_ACTIVE, true);
                if (streamKey != null) {
                    if (isDeferred(streamKey)) {
                        // its put-off entries go first; the task started for them re-rings afterwards
                        pendingDoorbellMap.put(streamKey, true);
                    } else if (processingMap.putIfAbsent(streamKey, true) == null) {
                        try {
                            workerPool.submit(new FetchAndProcessTask(streamKey, null));
                        } catch (RejectedExecutionException full) {
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
                logger.error("Outbound Dispatcher loop error", e);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private class FetchAndProcessTask implements Runnable {
        private final String streamKey;
        /** entries of this stream put off earlier, processed before reading new ones */
        private final List<GmmsMessage> carried;

        public FetchAndProcessTask(String streamKey, List<GmmsMessage> carried) {
            this.streamKey = streamKey;
            this.carried = carried;
        }

        @Override
        public void run() {
            boolean yielded = false;
            try {
                queueManager.createGroup(streamKey, groupName, true);
                if (carried != null && !processCarried()) {
                    return;
                }
                StreamTaskBudget budget = new StreamTaskBudget("Client");
                while (running) {
                    Map<String, StreamEntryID> query = new HashMap<>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName,
                            batchSize(streamKey), query, true);
                    if (messages == null || messages.isEmpty()) {
                        break; 
                    }
                    if (!processAll(messages)) {
                        return;
                    }
                    if (budget.spent()) {
                        yielded = true;
                        break;
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing Outbound stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                boolean rung = pendingDoorbellMap.remove(streamKey) != null;
                if ((rung || yielded) && !isDeferred(streamKey) && queueManager.streamLength(streamKey, true) > 0) {
                    queueManager.triggerDoorbell(streamKey, true);
                }
            }
        }

        /** Put-off entries: keep the ones still pending (idle reset) and process them. */
        private boolean processCarried() {
            List<String> ids = new ArrayList<String>();
            for (GmmsMessage m : carried) {
                ids.add(m.getRedisStreamID());
            }
            Set<String> still;
            try {
                still = new HashSet<String>(queueManager.keepPending(streamKey, groupName, consumerName, ids, true));
            } catch (Exception e) {
                logger.warn("Client outbound put-off entries not reclaimed, retried later. streamKey={}", streamKey, e);
                defer(streamKey, carried, 1000L);
                return false;
            }
            List<GmmsMessage> live = new ArrayList<GmmsMessage>();
            for (GmmsMessage m : carried) {
                if (still.contains(m.getRedisStreamID())) {
                    live.add(m);
                }
            }
            return processAll(live);
        }

        /** false when the rest was put off (window full / no permit): the task ends. */
        private boolean processAll(List<GmmsMessage> messages) {
            for (int i = 0; i < messages.size(); i++) {
                GmmsMessage msg = messages.get(i);
                ProcessResult r = processMessage(streamKey, msg);
                if (r != ProcessResult.DONE) {
                    defer(streamKey, new ArrayList<GmmsMessage>(messages.subList(i, messages.size())),
                            retryDelayMs(r, msg));
                    return false;
                }
            }
            return true;
        }
    }

    /** Batch read per stream: at most about one second of the channel rate when it is limited. */
    private int batchSize(String streamKey) {
        int batch = consumerConfig.batchSize();
        int tps = DistributedThrottlingManager.getInstance().tps(rssidOf(streamKey));
        return tps > 0 ? Math.max(1, Math.min(batch, tps)) : batch;
    }

    /** stream:mt:routed:{rssid}:{shard} */
    private static int rssidOf(String streamKey) {
        try {
            String rest = streamKey.substring(StreamQueueManager.STR_MT_ROUTED_PREFIX.length());
            return Integer.parseInt(rest.substring(0, rest.indexOf(':')));
        } catch (Exception e) {
            return -1;
        }
    }

    private long retryDelayMs(ProcessResult r, GmmsMessage msg) {
        return r == ProcessResult.THROTTLED
                ? DistributedThrottlingManager.getInstance().retryAfterMs(msg.getRSsID())
                : getWindowFullRetryMs();
    }

    private boolean isDeferred(String streamKey) {
        return deferrals != null && deferrals.isDeferred(streamKey);
    }

    private void defer(String streamKey, List<GmmsMessage> messages, long delayMs) {
        deferrals.defer(streamKey, messages, delayMs);
    }

    /** Delay over: a task processes the put-off entries, then goes on reading the stream. */
    private void resumeDeferred(String streamKey) {
        if (processingMap.putIfAbsent(streamKey, true) != null) {
            deferrals.schedule(streamKey, 20L);    // the task that put them off is still finishing
            return;
        }
        List<GmmsMessage> list = deferrals.take(streamKey);
        if (list == null) {
            processingMap.remove(streamKey);
            return;
        }
        try {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(
                    MetricsNames.COMPONENT_STREAM, MetricsNames.FLOW_PROCESS,
                    null, MetricsNames.ACTION_RETRY_CLAIM), list.size());
            workerPool.submit(new FetchAndProcessTask(streamKey, list));
        } catch (RejectedExecutionException full) {
            processingMap.remove(streamKey);
            defer(streamKey, list, 100L);
        }
    }

    private void refreshWorkerPoolConfig() {
        long now = System.currentTimeMillis();
        if (now - lastPoolConfigRefreshMs < 5000L) {
            return;
        }
        lastPoolConfigRefreshMs = now;
        consumerConfig.applyElasticWorkerPool(workerPool);
    }

    private ProcessResult processMessage(String streamKey, GmmsMessage msg) {
        long start = System.nanoTime();
        try {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                    MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                    MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
            return handleMessageSending(msg, streamKey);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_LATENCY),
                    System.nanoTime() - start);
        }
    }

    private ProcessResult handleMessageSending(GmmsMessage msg, String streamKey) {
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        boolean useMarker = ClientSendMarker.enabled();
        boolean markerHeld = false;
        boolean sent = false;
        try {
            Session session = MultiSmppClientFactory.getInstance().getSession(msg);
            if (session == null || !isConnected(session)) {
                if (useMarker) {
                    ClientSendMarker.Claim c = ClientSendMarker.claim(msg, ClientSendMarker.owner(consumerName));
                    if (c != ClientSendMarker.Claim.SEND) {
                        return settle(c, msg, streamKey, sourceRedisId);
                    }
                    markerHeld = true;
                }
                handleFailure(msg, "Session disconnected", streamKey, markerHeld);
                return ProcessResult.DONE;
            }
            int ssid = msg.getRSsID();
            MultiSmppSession smppSession = session instanceof MultiSmppSession ? (MultiSmppSession) session : null;
            if (smppSession != null && !smppSession.hasClientSubmitWindow()) {
                recordWindowFull(msg);
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "client_submit_window_full");
                return ProcessResult.WINDOW_FULL;
            }
            if (!DistributedThrottlingManager.getInstance().tryAcquire(ssid)) {
                return ProcessResult.THROTTLED;
            }
            if (useMarker) {
                ClientSendMarker.Claim c = ClientSendMarker.claim(msg, ClientSendMarker.owner(consumerName));
                if (c != ClientSendMarker.Claim.SEND) {
                    return settle(c, msg, streamKey, sourceRedisId);
                }
                markerHeld = true;
            }
            SmppSubmitResult submitResult = smppSession != null ? smppSession.submitClientOutbound(msg)
                    : (session.submit(msg) ? SmppSubmitResult.SENT : SmppSubmitResult.FAILED);
            if (submitResult == SmppSubmitResult.WINDOW_FULL) {
                if (markerHeld) {
                    ClientSendMarker.release(msg);
                    markerHeld = false;
                }
                recordWindowFull(msg);
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "client_submit_window_full");
                return ProcessResult.WINDOW_FULL;
            }
            if (submitResult == SmppSubmitResult.SENT) {
                sent = true;
                MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                        MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_SMPP_SENT));
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_SMPP_SENT));
                boolean acked = useMarker ? ClientSendMarker.ackSent(streamKey, groupName, sourceRedisId, msg)
                        : queueManager.ackById(streamKey, groupName, sourceRedisId, msg, true);
                if (!acked) {
                    logger.warn(msg,
                            "Outbound submit stream ack failed after send success. streamKey={}, sourceRedisId={}, currentRedisId={}",
                            streamKey, sourceRedisId, msg == null ? null : msg.getRedisStreamID());
                }
            } else {
                handleFailure(msg, "Session submit rejected", streamKey, markerHeld);
            }
        } catch (Exception e) {
            if (sent) {
                // sent: never report a failure for it; a redelivery finds the marker and only acks
                logger.error(msg, "Outbound MT sent, ack failed; the redelivery will not send it again", e);
            } else if (useMarker && !markerHeld) {
                // the marker could not be checked (Redis error): keep the entry pending, retried by auto-claim
                logger.error(msg, "Outbound send marker not available, left pending", e);
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "send_marker_error");
            } else {
                logger.error(msg, "Error invoking MultiSmppSession submit", e);
                handleFailure(msg, "Internal error during submit", streamKey, markerHeld);
            }
        }
        return ProcessResult.DONE;
    }

    /** Not this consumer's to send: ack an entry sent before, leave one another consumer is sending. */
    private ProcessResult settle(ClientSendMarker.Claim c, GmmsMessage msg, String streamKey, String sourceRedisId) {
        if (c == ClientSendMarker.Claim.ACK_ONLY) {
            ClientSendMarker.ackFailed(streamKey, groupName, sourceRedisId, msg);
        }
        return ProcessResult.DONE;
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

    private void handleFailure(GmmsMessage msg, String reason, String streamKey, boolean markerHeld) {
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
                if (markerHeld) {
                    ClientSendMarker.ackFailed(streamKey, groupName, sourceRedisId, msg);
                } else {
                    queueManager.ackById(streamKey, groupName, sourceRedisId, msg, true);
                }
            } else {
                if (msg != null) {
                    msg.setRedisStreamID(sourceRedisId);
                }
                if (markerHeld) {
                    ClientSendMarker.release(msg);
                }
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "failure_result_not_produced");
            }
        }
    }
}
