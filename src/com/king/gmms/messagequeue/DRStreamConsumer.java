package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.MultiSmppSession;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.session.SmppDrSubmitResult;
import com.king.gmms.customerconnectionfactory.MultiSmppServerFactory;
import com.king.gmms.ha.SmppServerDrRouteResolver;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/**
 * V5.2 Server-side DR Consumer (Fully Decoupled Design)
 * 
 * 修改说明：
 * 1. 彻底解耦 RedisConnection：通过 StreamQueueManager 进行消费和确认。
 * 2. 统一调度模式：与 MT 消费者共享相同的架构模式，降低维护成本。
 */
public class DRStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(DRStreamConsumer.class);
    private static final DRStreamConsumer instance = new DRStreamConsumer();
    private static final String MODULE_ROLE = "server";
    private static final String FUNCTION_NAME = "dr-to-customer";

    private final String groupName;
    private final String consumerName;
    private volatile boolean running = false;
    private volatile long lastPoolConfigRefreshMs = 0L;
    
    private ExecutorService dispatcherThread;
    private ExecutorService recoveryThread;
    private ScheduledExecutorService windowRetryThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<>();
    /** DRs put off while the customer's window is full, sent again after ServerDrWindowFullRetryMs */
    private StreamDeferrals deferrals;
    private final ConcurrentHashMap<String, AtomicLong> windowFullCountMap = new ConcurrentHashMap<String, AtomicLong>();
    private final ConcurrentHashMap<String, AtomicLong> windowFullLastWarnMsMap = new ConcurrentHashMap<String, AtomicLong>();

    private final StreamQueueManager queueManager;
    private final RedisStreamConsumerConfig consumerConfig;

    private enum ProcessResult {
        DONE,
        WINDOW_FULL
    }

    private DRStreamConsumer() {
        String nodeId = System.getProperty("NodeID", "0");
        this.groupName = "ServerDrToCustomerGroup_" + nodeId;
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + nodeId;
        this.queueManager = StreamQueueManager.getInstance();
        this.consumerConfig = RedisStreamConsumerConfig.load("Server", "ServerDrToCustomer", 20, 50, 5000, 100, 100);
    }

    public static DRStreamConsumer getInstance() { return instance; }

    public synchronized void start() {
        if (running) return;
        running = true;
        
        workerPool = consumerConfig.newElasticWorkerPool();

        windowRetryThread = Executors.newSingleThreadScheduledExecutor();
        deferrals = new StreamDeferrals(windowRetryThread, new StreamDeferrals.Resume() {
            public void resume(String streamKey) {
                resumeDeferred(streamKey);
            }
        });
        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        recoveryThread = Executors.newSingleThreadExecutor();
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Server DR-to-customer",
                StreamQueueManager.STR_DR_PENDING_PREFIX + "*", groupName, consumerName, false, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        if (sendDeliveryReport(streamKey, msg) == ProcessResult.WINDOW_FULL) {
                            deferrals.defer(streamKey, java.util.Collections.singletonList(msg), getWindowFullRetryMs());
                        }
                    }
                }));

        logger.info("Server DR-to-customer consumer started. Group: {}, Consumer: {}, Config: {}",
            groupName, consumerName, consumerConfig.summary());
        logShardCoverage();
        queueManager.reRingExistingStreams(StreamQueueManager.STR_DR_PENDING_PREFIX + "*", false);
    }

    /** DR streams of a shard outside MyShards are never consumed while the shard filter is on. */
    private void logShardCoverage() {
        if (!queueManager.isServerDrShardFilterEnabled()) {
            logger.info("Server DR-to-customer consumes all DR shards of module {}", System.getProperty("module", ""));
            return;
        }
        int totalShards = GmmsUtility.getInstance().getTotalShards();
        Set<Integer> myShards = GmmsUtility.getInstance().getMyShards();
        List<Integer> uncovered = new ArrayList<Integer>();
        for (int i = 0; i < totalShards; i++) {
            if (!myShards.contains(i)) {
                uncovered.add(i);
            }
        }
        if (uncovered.isEmpty()) {
            logger.info("Server DR-to-customer shard filter on. totalShards={}, myShards={}", totalShards, myShards);
        } else {
            logger.warn("Server DR-to-customer shard filter on and shards {} are not consumed by this node. "
                + "Make sure another node with the same module name owns them. totalShards={}, myShards={}",
                uncovered, totalShards, myShards);
        }
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) dispatcherThread.shutdownNow();
        if (recoveryThread != null) recoveryThread.shutdownNow();
        if (windowRetryThread != null) windowRetryThread.shutdownNow();
        if (workerPool != null) workerPool.shutdown();
        logger.info("Server DR-to-customer consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                refreshWorkerPoolConfig();
                String streamKey = queueManager.popResponsibleDoorbell(StreamQueueManager.ZSET_DR_PENDING_ACTIVE, false);
                if (streamKey != null) {
                    logger.debug("Server DR-to-customer popped doorbell. streamKey={}", streamKey);
                    if (deferrals.isDeferred(streamKey)) {
                        // its put-off DRs go first; the task started for them re-rings afterwards
                        pendingDoorbellMap.put(streamKey, true);
                    } else if (processingMap.putIfAbsent(streamKey, true) == null) {
                        logger.debug("Server DR-to-customer submit worker. streamKey={}", streamKey);
                        try {
                            workerPool.submit(new FetchAndProcessTask(streamKey, null));
                        } catch (java.util.concurrent.RejectedExecutionException full) {
                            // all workers busy: give the stream back instead of running it on the dispatcher
                            processingMap.remove(streamKey);
                            queueManager.triggerDoorbell(streamKey, false);
                            Thread.sleep(consumerConfig.dispatcherIdleSleepMs());
                        }
                    } else {
                        pendingDoorbellMap.put(streamKey, true);
                        logger.debug("Server DR-to-customer stream already processing, mark pending doorbell. streamKey={}", streamKey);
                    }
                } else {
                    Thread.sleep(consumerConfig.dispatcherIdleSleepMs());
                }
            } catch (InterruptedException ie) {
                break;
            } catch (Exception e) {
                logger.error("DR Dispatcher loop error", e);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private class FetchAndProcessTask implements Runnable {
        private final String streamKey;
        /** DRs of this stream put off earlier (window full), sent before reading new ones */
        private final List<GmmsMessage> carried;

        public FetchAndProcessTask(String streamKey, List<GmmsMessage> carried) {
            this.streamKey = streamKey;
            this.carried = carried;
        }

        @Override
        public void run() {
            boolean yielded = false;
            try {
                queueManager.createGroup(streamKey, groupName, false);
                logger.debug("Server DR-to-customer ensured consumer group. streamKey={}, group={}, consumer={}",
                    streamKey, groupName, consumerName);
                if (carried != null && !processCarried()) {
                    return;
                }
                StreamTaskBudget budget = new StreamTaskBudget("Server");
                while (running) {
                    Map<String, StreamEntryID> query = new HashMap<>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
                    
                    // 使用 Manager 封装的批量消费方法 (Report-MQ 通道)
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, consumerConfig.batchSize(), query, false);

                    if (messages == null || messages.isEmpty()) {
                        logger.debug("Server DR-to-customer no messages read. streamKey={}, group={}, consumer={}",
                            streamKey, groupName, consumerName);
                        break; 
                    }
                    logger.debug("Server DR-to-customer read messages. streamKey={}, count={}", streamKey, messages.size());
                    if (!sendAll(messages)) {
                        return;
                    }
                    if (budget.spent()) {
                        yielded = true;
                        break;
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing DR stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                boolean rung = pendingDoorbellMap.remove(streamKey) != null;
                if ((rung || yielded) && !deferrals.isDeferred(streamKey)
                        && queueManager.streamLength(streamKey, false) > 0) {
                    queueManager.triggerDoorbell(streamKey, false);
                }
            }
        }

        /** Put-off DRs: keep the ones still pending (idle reset, no delivery counted) and send them. */
        private boolean processCarried() {
            List<String> ids = new ArrayList<String>();
            for (GmmsMessage m : carried) {
                ids.add(m.getRedisStreamID());
            }
            Set<String> still;
            try {
                still = new java.util.HashSet<String>(queueManager.keepPending(streamKey, groupName, consumerName, ids, false));
            } catch (Exception e) {
                logger.warn("Server DR put-off entries not reclaimed, retried later. streamKey={}", streamKey, e);
                deferrals.defer(streamKey, carried, 1000L);
                return false;
            }
            List<GmmsMessage> live = new ArrayList<GmmsMessage>();
            for (GmmsMessage m : carried) {
                if (still.contains(m.getRedisStreamID())) {
                    live.add(m);
                }
            }
            return sendAll(live);
        }

        /**
         * false when the customer's window filled up: the DRs not sent yet are put off for this
         * stream (only these: no other task's DRs are taken, so none is sent twice) and the task ends.
         */
        private boolean sendAll(List<GmmsMessage> messages) {
            for (int i = 0; i < messages.size(); i++) {
                if (sendDeliveryReport(streamKey, messages.get(i)) == ProcessResult.WINDOW_FULL) {
                    deferrals.defer(streamKey, new ArrayList<GmmsMessage>(messages.subList(i, messages.size())),
                            getWindowFullRetryMs());
                    return false;
                }
            }
            return true;
        }
    }

    /** Delay over: a task sends the put-off DRs, then goes on reading the stream. */
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
        } catch (java.util.concurrent.RejectedExecutionException full) {
            processingMap.remove(streamKey);
            deferrals.defer(streamKey, list, 100L);
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

    private ProcessResult sendDeliveryReport(String streamKey, GmmsMessage msg) {
        long start = System.nanoTime();
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        try {
            Session session = resolveServerSession(msg);
            if (session == null) {
                logger.warn(msg, "Server DR-to-customer session not found. streamKey={}", streamKey);
                failToCoreAndAck(streamKey, msg, "session not found");
                return ProcessResult.DONE;
            }
            logger.info(msg, "Server DR-to-customer resolved session. streamKey={}, session={}, status={}",
                streamKey, session.getSessionName(), session.getStatus());
            SmppDrSubmitResult submitResult = SmppDrSubmitResult.FAILED;
            if (session != null && isConnected(session)) {
                if (session instanceof MultiSmppSession) {
                    MultiSmppSession smppSession = (MultiSmppSession) session;
                    if (!smppSession.hasServerDrWindow()) {
                        recordWindowFull(streamKey, msg, session.getSessionName(), "precheck");
                        queueManager.logNack(streamKey, groupName, consumerName, msg, false, "server_dr_window_full");
                        return ProcessResult.WINDOW_FULL;
                    }
                    submitResult = smppSession.submitServerDr(msg);
                } else {
                    submitResult = session.submit(msg) ? SmppDrSubmitResult.SENT : SmppDrSubmitResult.FAILED;
                }
            }
            if (submitResult == SmppDrSubmitResult.WINDOW_FULL) {
                recordWindowFull(streamKey, msg, session.getSessionName(), "submit");
                queueManager.logNack(streamKey, groupName, consumerName, msg, false, "server_dr_window_full");
                return ProcessResult.WINDOW_FULL;
            }
            if (submitResult == SmppDrSubmitResult.SENT) {
                clearWindowFullState(streamKey);
                MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                        MetricsNames.FLOW_SERVER, msg, MetricsNames.ACTION_SENT_TO_CUSTOMER));
                MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                        MetricsNames.STAGE_OUT_DR, msg, false, MetricsNames.ACTION_SENT_TO_CUSTOMER));
                boolean acked = queueManager.ackById(streamKey, groupName, sourceRedisId, msg, false);
                if (acked) {
                    logger.debug(msg, "Server DR-to-customer sent and acked. streamKey={}, session={}, sourceRedisId={}",
                        streamKey, session.getSessionName(), sourceRedisId);
                } else {
                    logger.warn(msg, "Server DR-to-customer sent but ack failed. streamKey={}, session={}, sourceRedisId={}, currentRedisId={}",
                        streamKey, session.getSessionName(), sourceRedisId,
                        msg == null ? null : msg.getRedisStreamID());
                }
                return ProcessResult.DONE;
            }
            logger.warn(msg, "Server DR-to-customer submit failed or session unavailable. streamKey={}, session={}, status={}",
                streamKey, session.getSessionName(), session.getStatus());
            failToCoreAndAck(streamKey, msg, "submit failed, session=" + session.getSessionName()
                + ", status=" + session.getStatus());
        } catch (Exception e) {
            logger.error(msg, "Failed to send DR through MultiSmppSession", e);
            failToCoreAndAck(streamKey, msg, "exception: " + e.getClass().getSimpleName());
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer(MetricsNames.COMPONENT_CONSUMER,
                    MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_LATENCY),
                    System.nanoTime() - start);
        }
        return ProcessResult.DONE;
    }

    private long getWindowFullRetryMs() {
        try {
            long value = Long.parseLong(GmmsUtility.getInstance()
                    .getCommonProperty("ServerDrWindowFullRetryMs", "100").trim());
            return Math.max(10L, Math.min(value, 60000L));
        } catch (Exception e) {
            return 100L;
        }
    }

    private void recordWindowFull(String streamKey, GmmsMessage msg, String sessionName, String phase) {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                MetricsNames.FLOW_SERVER, msg, MetricsNames.ACTION_WINDOW_FULL));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                MetricsNames.STAGE_OUT_DR, msg, false, MetricsNames.ACTION_WINDOW_FULL));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(
                MetricsNames.COMPONENT_CONSUMER, MetricsNames.FLOW_PROCESS, msg,
                MetricsNames.ACTION_WINDOW_FULL));
        AtomicLong count = getAtomic(windowFullCountMap, streamKey);
        long currentCount = count.incrementAndGet();
        MetricsCollector.getInstance().setGauge(MetricsNames.gauge(
                "stream.server.dr.window_full", "consecutive"), currentCount);
        maybeLogWindowFull(streamKey, msg, sessionName, phase, currentCount);
    }

    private void maybeLogWindowFull(String streamKey, GmmsMessage msg, String sessionName, String phase, long count) {
        long now = System.currentTimeMillis();
        AtomicLong lastWarnMs = getAtomic(windowFullLastWarnMsMap, streamKey);
        long last = lastWarnMs.get();
        boolean shouldLog = count == 1 || count % 1000 == 0 || now - last >= 30000L;
        if (!shouldLog || !lastWarnMs.compareAndSet(last, now)) {
            return;
        }
        logger.warn(msg, "Server DR-to-customer window full. streamKey={}, session={}, phase={}, consecutive={}, retryMs={}",
                streamKey, sessionName, phase, count, getWindowFullRetryMs());
    }

    private void clearWindowFullState(String streamKey) {
        windowFullCountMap.remove(streamKey);
        windowFullLastWarnMsMap.remove(streamKey);
    }

    private AtomicLong getAtomic(ConcurrentHashMap<String, AtomicLong> map, String key) {
        String safeKey = key == null ? "unknown" : key;
        AtomicLong value = map.get(safeKey);
        if (value == null) {
            map.putIfAbsent(safeKey, new AtomicLong(0));
            value = map.get(safeKey);
        }
        return value;
    }

    private Session resolveServerSession(GmmsMessage msg) {
        String currentModule = System.getProperty("module", "");
        SmppServerDrRouteResolver resolver = SmppServerDrRouteResolver.getInstance();
        boolean originalModule = resolver.isOriginalModule(msg, currentModule);
        if (originalModule && msg.getTransaction() == null && msg.getInnerTransaction() != null) {
            msg.setTransaction(msg.getInnerTransaction());
        }
        Session session = MultiSmppServerFactory.getInstance().getSession(msg);
        if (session != null) {
            return session;
        }
        if (!originalModule) {
            logger.warn(msg, "SMPP DR old strategy did not resolve session on fallback module. currentModule={}, originalModule={}",
                    currentModule, resolver.originalModule(msg));
            return MultiSmppServerFactory.getInstance().getFallbackSession(msg);
        }
        return null;
    }
    
    private boolean isConnected(Session session) {
        ConnectionStatus status = session.getStatus();
        return ConnectionStatus.CONNECT.equals(status) || ConnectionStatus.RECOVER.equals(status);
    }
    
    private void failToCoreAndAck(String streamKey, GmmsMessage msg, String reason) {
        String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
        MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(MetricsNames.COMPONENT_CONSUMER,
                MetricsNames.FLOW_PROCESS, msg, MetricsNames.ACTION_FAIL));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
                MetricsNames.FLOW_SERVER, msg, MetricsNames.ACTION_FAILED_TO_CUSTOMER));
        MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(
                MetricsNames.STAGE_OUT_DR, msg, false, MetricsNames.ACTION_FAILED_TO_CUSTOMER));
        if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
            msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
            msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
        } else {
            msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
            msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
        }

        if (queueManager.produceResult(msg)) {
            queueManager.ackById(streamKey, groupName, sourceRedisId, msg, false);
            logger.warn(msg, "Server DR-to-customer failed, reported 11005 to core and acked. streamKey={}, reason={}",
                streamKey, reason);
            return;
        }
        if (msg != null) {
            msg.setRedisStreamID(sourceRedisId);
        }

        logger.error(msg, "Server DR-to-customer failed and could not report 11005 to core. "
            + "Message remains pending for PEL auto-claim. streamKey={}, reason={}", streamKey, reason);
        queueManager.logNack(streamKey, groupName, consumerName, msg, false, "failure_result_not_produced:" + reason);
    }
}
