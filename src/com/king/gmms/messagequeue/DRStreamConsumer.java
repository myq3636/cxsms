package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    private final ConcurrentHashMap<String, Boolean> windowRetryMap = new ConcurrentHashMap<String, Boolean>();
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
        
        workerPool = new ThreadPoolExecutor(
            consumerConfig.workerCorePoolSize(), consumerConfig.workerMaxPoolSize(), 60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(consumerConfig.workerQueueSize()),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        windowRetryThread = Executors.newSingleThreadScheduledExecutor();
        recoveryThread = Executors.newSingleThreadExecutor();
        recoveryThread.execute(new RedisStreamAutoClaimRunner("Server DR-to-customer",
                StreamQueueManager.STR_DR_PENDING_PREFIX + "*", groupName, consumerName, false, queueManager,
                new RedisStreamAutoClaimRunner.MessageProcessor() {
                    public void process(String streamKey, GmmsMessage msg) {
                        if (sendDeliveryReport(streamKey, msg) == ProcessResult.WINDOW_FULL) {
                            scheduleWindowRetry(streamKey);
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
                    if (processingMap.putIfAbsent(streamKey, true) == null) {
                        logger.debug("Server DR-to-customer submit worker. streamKey={}", streamKey);
                        workerPool.submit(new FetchAndProcessTask(streamKey));
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

        public FetchAndProcessTask(String streamKey) {
            this.streamKey = streamKey;
        }

        @Override
        public void run() {
            try {
                queueManager.createGroup(streamKey, groupName, false);
                logger.debug("Server DR-to-customer ensured consumer group. streamKey={}, group={}, consumer={}",
                    streamKey, groupName, consumerName);

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

                    for (GmmsMessage msg : messages) {
                        // 【业务集成】：将 DR 发送回 SmppServer 的客户端
                        if (sendDeliveryReport(streamKey, msg) == ProcessResult.WINDOW_FULL) {
                            scheduleWindowRetry(streamKey);
                            break;
                        }
                        // 确认消费
                    }
                }
            } catch (Exception e) {
                logger.error("Error processing DR stream: " + streamKey, e);
            } finally {
                processingMap.remove(streamKey);
                if (pendingDoorbellMap.remove(streamKey) != null && queueManager.streamLength(streamKey, false) > 0) {
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
                            retryMs, getWindowFullRetryBatchSize(), false);
                    if (claimed != null && !claimed.isEmpty()) {
                        MetricsCollector.getInstance().incrementCounter(MetricsNames.counter(
                                MetricsNames.COMPONENT_STREAM, MetricsNames.FLOW_PROCESS,
                                null, MetricsNames.ACTION_RETRY_CLAIM), claimed.size());
                        MetricsCollector.getInstance().setGauge(MetricsNames.gauge(
                                "stream.server.dr.window_retry", "last_claim_count"), claimed.size());
                        for (GmmsMessage msg : claimed) {
                            if (sendDeliveryReport(streamKey, msg) == ProcessResult.WINDOW_FULL) {
                                retryAgain = true;
                                return;
                            }
                        }
                    } else {
                        queueManager.triggerDoorbell(streamKey, false);
                    }
                } catch (Exception e) {
                    logger.warn("Server DR-to-customer window retry failed. streamKey={}", streamKey, e);
                    queueManager.triggerDoorbell(streamKey, false);
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
        logger.warn(msg, "Server DR-to-customer window full. streamKey={}, session={}, phase={}, consecutive={}, retryMs={}, retryBatch={}",
                streamKey, sessionName, phase, count, getWindowFullRetryMs(), getWindowFullRetryBatchSize());
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

    private int getWindowFullRetryBatchSize() {
        try {
            int value = Integer.parseInt(GmmsUtility.getInstance()
                    .getCommonProperty("ServerDrWindowFullRetryBatchSize", "100").trim());
            return Math.max(1, Math.min(value, 10000));
        } catch (Exception e) {
            return 100;
        }
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
