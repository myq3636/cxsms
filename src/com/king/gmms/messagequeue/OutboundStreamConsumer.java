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
import com.king.gmms.util.LogRateLimiter;
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
        THROTTLED,
        /** no bind of the connection due (SMSOptionOutgoingThrottlingNumPerBind) */
        BIND_THROTTLED
    }

    /** ms until a bind of the connection is due, set with BIND_THROTTLED on the worker thread */
    private static final ThreadLocal<long[]> BIND_WAIT_MS = ThreadLocal.withInitial(() -> new long[1]);

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
        consumerConfig.registerPoolGauges(workerPool);

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        windowRetryThread = Executors.newSingleThreadScheduledExecutor();
        deferrals = new StreamDeferrals(windowRetryThread, new StreamDeferrals.Resume() {
            public void resume(String streamKey) {
                resumeDeferred(streamKey);
            }
        });
        deferrals.measureWait(MetricsNames.CONSUMER_CLIENT_OUT);
        recoveryThread = Executors.newSingleThreadExecutor();
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Client outbound-submit",
                StreamQueueManager.STR_MT_ROUTED_PREFIX + "*", groupName, consumerName, true, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        ProcessResult r = processMessage(streamKey, msg, true);
                        if (r != ProcessResult.DONE) {
                            defer(streamKey, java.util.Collections.singletonList(msg), retryDelayMs(r, msg), r);
                        }
                    }
                }));
        queueManager.reRingExistingStreams(StreamQueueManager.STR_MT_ROUTED_PREFIX + "*", true);

        logger.info("Client outbound-submit consumer started. Consumer: {}, Shards: {}, Config: {}",
            consumerName, GmmsUtility.getInstance().getMyShards(), consumerConfig.summary());
    }

    /** Streams a worker is on now, and entries read and put off. */
    public int inFlight() {
        return processingMap.size() + (deferrals == null ? 0 : deferrals.size());
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
                    long readAt = System.currentTimeMillis();
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName,
                            batchSize(streamKey), query, true);
                    if (messages == null || messages.isEmpty()) {
                        break; 
                    }
                    if (!processAll(messages, readAt)) {
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
            // keepPending below resets their idle time
            long readAt = System.currentTimeMillis();
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
            return processAll(live, readAt);
        }

        /** false when the rest was put off (window full / no permit): the task ends. */
        private boolean processAll(List<GmmsMessage> messages, long readAt) {
            if (ClientSendMarker.enabled() && ClientSendMarker.batchEnabled()) {
                return processInBatches(streamKey, messages, readAt);
            }
            long claimIdleMs = queueManager.getAutoClaimIdleMs();
            for (int i = 0; i < messages.size(); i++) {
                GmmsMessage msg = messages.get(i);
                // older than the idle time: auto-claim may have taken this entry too
                ProcessResult r = processMessage(streamKey, msg, System.currentTimeMillis() - readAt >= claimIdleMs);
                if (r != ProcessResult.DONE) {
                    defer(streamKey, new ArrayList<GmmsMessage>(messages.subList(i, messages.size())),
                            retryDelayMs(r, msg), r);
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * processAll with ClientSendMarkerBatch: the MTs that have their window and permits now are sent together
     * ({@link #sendReady}); the first that has none puts off itself and the rest, as one by one. Entries that
     * auto-claim may have taken too (read longer ago than its idle time) and MTs without a connected session go
     * one by one.
     */
    private boolean processInBatches(String streamKey, List<GmmsMessage> messages, long readAt) {
        long claimIdleMs = queueManager.getAutoClaimIdleMs();
        ProcessResult[] why = new ProcessResult[1];
        int i = 0;
        while (i < messages.size()) {
            List<Ready> ready = new ArrayList<Ready>();
            ProcessResult stop = null;
            boolean single = false;
            long start = System.nanoTime();
            int j = i;
            while (j < messages.size()) {
                if (System.currentTimeMillis() - readAt >= claimIdleMs) {
                    single = true;
                    break;
                }
                GmmsMessage msg = messages.get(j);
                Ready r;
                try {
                    r = prepare(msg, streamKey, why);
                } catch (Exception e) {
                    single = true;   // the one-by-one path handles it as before
                    break;
                }
                if (r == null && why[0] == null) {
                    single = true;   // no connected session: the one-by-one path reports the failure
                    break;
                }
                countReceived(msg);
                if (r == null) {
                    stop = why[0];
                    break;
                }
                ready.add(r);
                j++;
            }
            int windowFullAt = ready.isEmpty() ? -1 : sendReady(streamKey, ready);
            if (!ready.isEmpty()) {
                long each = (System.nanoTime() - start) / ready.size();
                for (int k = 0; k < ready.size(); k++) {
                    MetricsCollector.getInstance().recordTime(MetricsNames.consumer(MetricsNames.CONSUMER_CLIENT_OUT), each);
                }
            }
            if (windowFullAt >= 0) {
                int from = i + windowFullAt;
                defer(streamKey, new ArrayList<GmmsMessage>(messages.subList(from, messages.size())),
                        getWindowFullRetryMs(), ProcessResult.WINDOW_FULL);
                return false;
            }
            i = j;
            if (stop != null) {
                defer(streamKey, new ArrayList<GmmsMessage>(messages.subList(i, messages.size())),
                        retryDelayMs(stop, messages.get(i)), stop);
                return false;
            }
            if (single) {
                GmmsMessage msg = messages.get(i);
                ProcessResult r = processMessage(streamKey, msg, System.currentTimeMillis() - readAt >= claimIdleMs);
                if (r != ProcessResult.DONE) {
                    defer(streamKey, new ArrayList<GmmsMessage>(messages.subList(i, messages.size())),
                            retryDelayMs(r, msg), r);
                    return false;
                }
                i++;
            }
        }
        return true;
    }

    private void countReceived(GmmsMessage msg) {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
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
        if (r == ProcessResult.BIND_THROTTLED) {
            return Math.max(1L, Math.min(1000L, BIND_WAIT_MS.get()[0]));
        }
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

    private void defer(String streamKey, List<GmmsMessage> messages, long delayMs, ProcessResult why) {
        deferrals.defer(streamKey, messages, delayMs, why.name().toLowerCase());
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
        return processMessage(streamKey, msg, false);
    }

    private ProcessResult processMessage(String streamKey, GmmsMessage msg, boolean mayBeClaimed) {
        long start = System.nanoTime();
        try {
            countReceived(msg);
            return handleMessageSending(msg, streamKey, mayBeClaimed);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.consumer(MetricsNames.CONSUMER_CLIENT_OUT),
                    System.nanoTime() - start);
        }
    }

    /** An MT with its window and permits, to be sent now on this session */
    private static final class Ready {
        final GmmsMessage msg;
        final String sourceRedisId;
        final Session session;
        final MultiSmppSession smppSession;

        Ready(GmmsMessage msg, Session session, MultiSmppSession smppSession) {
            this.msg = msg;
            this.sourceRedisId = msg.getRedisStreamID();
            this.session = session;
            this.smppSession = smppSession;
        }
    }

    /**
     * The session, window and permits (account and bind) for msg. null when it is not sent now: why[0] is the result to
     * put it off with, or null when it has no connected session (the single path reports the failure).
     */
    private Ready prepare(GmmsMessage msg, String streamKey, ProcessResult[] why) {
        why[0] = null;
        Session session = MultiSmppClientFactory.getInstance().getSession(msg);
        if (session == null || !isConnected(session)) {
            return null;
        }
        int ssid = msg.getRSsID();
        MultiSmppSession smppSession = session instanceof MultiSmppSession ? (MultiSmppSession) session : null;
        int bindTps = smppSession != null ? smppSession.bindTps() : 0;
        if (bindTps > 0) {
            // the first bind of the connection, from the one picked, with room in its window and due
            MultiSmppSession due = dueBind(smppSession, bindTps);
            if (due == null) {
                if (BIND_WAIT_MS.get()[0] < 0) {
                    recordWindowFull(msg);
                    queueManager.logNack(streamKey, groupName, consumerName, msg, true, "client_submit_window_full");
                    why[0] = ProcessResult.WINDOW_FULL;
                } else {
                    why[0] = bindThrottled(msg, bindTps);
                }
                return null;
            }
            smppSession = due;
            session = due;
        } else if (smppSession != null && !smppSession.hasClientSubmitWindow()) {
            recordWindowFull(msg);
            queueManager.logNack(streamKey, groupName, consumerName, msg, true, "client_submit_window_full");
            why[0] = ProcessResult.WINDOW_FULL;
            return null;
        }
        if (!DistributedThrottlingManager.getInstance().tryAcquire(ssid)) {
            why[0] = ProcessResult.THROTTLED;
            return null;
        }
        if (bindTps > 0 && !smppSession.bindRate().take(bindTps, System.nanoTime())) {
            // another worker took this bind's permit first: the account permit is lost, at most one
            BIND_WAIT_MS.get()[0] = Math.max(1L, smppSession.bindRate().waitMs(bindTps, System.nanoTime()));
            why[0] = bindThrottled(msg, bindTps);
            return null;
        }
        return new Ready(msg, session, smppSession);
    }

    private SmppSubmitResult submit(Ready r) throws java.io.IOException {
        return r.smppSession != null ? r.smppSession.submitClientOutbound(r.msg)
                : (r.session.submit(r.msg) ? SmppSubmitResult.SENT : SmppSubmitResult.FAILED);
    }

    private void countSent(GmmsMessage msg) {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                MetricsNames.FLOW_CLIENT, msg, MetricsNames.ACTION_SMPP_SENT));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_SMPP_SENT));
    }

    private ProcessResult handleMessageSending(GmmsMessage msg, String streamKey, boolean mayBeClaimed) {
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        boolean useMarker = ClientSendMarker.enabled();
        boolean markerHeld = false;
        boolean sent = false;
        try {
            ProcessResult[] why = new ProcessResult[1];
            Ready ready = prepare(msg, streamKey, why);
            if (ready == null && why[0] != null) {
                return why[0];
            }
            if (ready == null) {
                if (useMarker) {
                    ClientSendMarker.Claim c = ClientSendMarker.claim(msg, ClientSendMarker.owner(consumerName), streamKey, sourceRedisId, mayBeClaimed);
                    if (c != ClientSendMarker.Claim.SEND) {
                        return settle(c, msg, streamKey, sourceRedisId);
                    }
                    markerHeld = true;
                }
                handleFailure(msg, "Session disconnected", streamKey, markerHeld);
                return ProcessResult.DONE;
            }
            if (useMarker) {
                ClientSendMarker.Claim c = ClientSendMarker.claim(msg, ClientSendMarker.owner(consumerName), streamKey, sourceRedisId, mayBeClaimed);
                if (c != ClientSendMarker.Claim.SEND) {
                    return settle(c, msg, streamKey, sourceRedisId);
                }
                markerHeld = true;
            }
            SmppSubmitResult submitResult = submit(ready);
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
                countSent(msg);
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

    /**
     * ClientSendMarkerBatch: MTs of one stream that have their window and permits (prepared in order, up to the first
     * that has not) are claimed in one Lua, sent, and acked in one Lua. Returns the index in ready of the MT that met
     * a full window at the submit (it and the rest are put off, their markers released), or -1. The MTs are claimed
     * only when they are about to be sent, as one by one: a marker still means "being sent now"
     * (docs/5000TPS多主测试慢的环节分析.md).
     */
    private int sendReady(String streamKey, List<Ready> ready) {
        List<GmmsMessage> msgs = new ArrayList<GmmsMessage>(ready.size());
        for (Ready r : ready) {
            msgs.add(r.msg);
        }
        List<ClientSendMarker.Claim> claims;
        try {
            claims = ClientSendMarker.claimBatch(streamKey, msgs, ClientSendMarker.owner(consumerName));
        } catch (Exception e) {
            // the markers could not be checked (Redis error): the entries stay pending, retried by auto-claim
            for (Ready r : ready) {
                logger.error(r.msg, "Outbound send marker not available, left pending", e);
                queueManager.logNack(streamKey, groupName, consumerName, r.msg, true, "send_marker_error");
            }
            return -1;
        }
        List<GmmsMessage> sentMsgs = new ArrayList<GmmsMessage>(ready.size());
        List<String> sentIds = new ArrayList<String>(ready.size());
        int windowFullAt = -1;
        for (int k = 0; k < ready.size(); k++) {
            Ready r = ready.get(k);
            ClientSendMarker.Claim c = claims.get(k);
            if (c != ClientSendMarker.Claim.SEND) {
                settle(c, r.msg, streamKey, r.sourceRedisId);
                continue;
            }
            try {
                SmppSubmitResult result = submit(r);
                if (result == SmppSubmitResult.WINDOW_FULL) {
                    windowFullAt = k;
                    List<GmmsMessage> release = new ArrayList<GmmsMessage>();
                    for (int x = k; x < ready.size(); x++) {
                        if (claims.get(x) == ClientSendMarker.Claim.SEND) {
                            release.add(ready.get(x).msg);
                        }
                    }
                    ClientSendMarker.releaseBatch(streamKey, release);
                    recordWindowFull(r.msg);
                    queueManager.logNack(streamKey, groupName, consumerName, r.msg, true, "client_submit_window_full");
                    break;
                }
                if (result == SmppSubmitResult.SENT) {
                    countSent(r.msg);
                    sentMsgs.add(r.msg);
                    sentIds.add(r.sourceRedisId);
                } else {
                    handleFailure(r.msg, "Session submit rejected", streamKey, true);
                }
            } catch (Exception e) {
                logger.error(r.msg, "Error invoking MultiSmppSession submit", e);
                handleFailure(r.msg, "Internal error during submit", streamKey, true);
            }
        }
        if (!sentMsgs.isEmpty()) {
            try {
                ClientSendMarker.ackSentBatch(streamKey, groupName, sentIds, sentMsgs);
            } catch (Exception e) {
                // sent: never report a failure for them; a redelivery finds the markers and only acks
                for (GmmsMessage m : sentMsgs) {
                    logger.error(m, "Outbound MT sent, ack failed; the redelivery will not send it again", e);
                }
            }
        }
        return windowFullAt;
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

    /**
     * A bound session of the connection of picked, starting with picked, with room in its window and due by
     * its bind rate; null: none, BIND_WAIT_MS then holds the ms until the first one with room is due, or -1
     * when no window has room.
     */
    private MultiSmppSession dueBind(MultiSmppSession picked, int bindTps) {
        long now = System.nanoTime();
        long wait = -1L;
        List<Session> all = picked.connectionSessions();
        if (all.isEmpty()) {
            all = java.util.Collections.<Session>singletonList(picked);
        }
        int start = Math.max(0, all.indexOf(picked));
        for (int i = 0; i < all.size(); i++) {
            Session s = all.get((start + i) % all.size());
            if (!(s instanceof MultiSmppSession) || !isConnected(s)) {
                continue;
            }
            MultiSmppSession m = (MultiSmppSession) s;
            if (!m.hasClientSubmitWindow()) {
                continue;
            }
            long w = m.bindRate().waitMs(bindTps, now);
            if (w == 0L) {
                return m;
            }
            wait = wait < 0 ? w : Math.min(wait, w);
        }
        BIND_WAIT_MS.get()[0] = wait;
        return null;
    }

    private ProcessResult bindThrottled(GmmsMessage msg, int bindTps) {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                MetricsNames.STAGE_OUT_SUBMIT, msg, true, MetricsNames.ACTION_BIND_THROTTLED));
        logger.debug(msg, "No bind due (SMSOptionOutgoingThrottlingNumPerBind {}), put off {} ms", bindTps, BIND_WAIT_MS.get()[0]);
        long n = LogRateLimiter.allow("bind-rate:" + msg.getRSsID(), 600000L);
        if (n > 0) {
            logger.info("[BIND-RATE] rssid={} bind rate {} TPS, put off {} times in the last 10 min", msg.getRSsID(), bindTps, n);
        }
        return ProcessResult.BIND_THROTTLED;
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
