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
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.customerconnectionfactory.MultiSmppServerFactory;
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
    
    private ExecutorService dispatcherThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<>();

    private final StreamQueueManager queueManager;

    private DRStreamConsumer() {
        String nodeId = System.getProperty("NodeID", "0");
        this.groupName = "ServerDrToCustomerGroup_" + nodeId;
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + nodeId;
        this.queueManager = StreamQueueManager.getInstance();
    }

    public static DRStreamConsumer getInstance() { return instance; }

    public synchronized void start() {
        if (running) return;
        running = true;
        
        workerPool = new ThreadPoolExecutor(
            20, 50, 60L, TimeUnit.SECONDS, 
            new LinkedBlockingQueue<Runnable>(5000), 
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);

        logger.info("Server DR-to-customer consumer started. Group: {}, Consumer: {}", groupName, consumerName);
        queueManager.reRingExistingStreams(StreamQueueManager.STR_DR_PENDING_PREFIX + "*", false);
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) dispatcherThread.shutdownNow();
        if (workerPool != null) workerPool.shutdown();
        logger.info("Server DR-to-customer consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
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
                    Thread.sleep(100);
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
                    processAutoClaim(streamKey);

                    Map<String, StreamEntryID> query = new HashMap<>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
                    
                    // 使用 Manager 封装的批量消费方法 (Report-MQ 通道)
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 100, query, false);

                    if (messages == null || messages.isEmpty()) {
                        logger.debug("Server DR-to-customer no messages read. streamKey={}, group={}, consumer={}",
                            streamKey, groupName, consumerName);
                        break; 
                    }
                    logger.debug("Server DR-to-customer read messages. streamKey={}, count={}", streamKey, messages.size());

                    for (GmmsMessage msg : messages) {
                        // 【业务集成】：将 DR 发送回 SmppServer 的客户端
                        sendDeliveryReport(streamKey, msg);
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
    
    private void processAutoClaim(String streamKey) {
        List<GmmsMessage> orphans = queueManager.autoClaimBatch(streamKey, groupName, consumerName,
                queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimBatchSize(), false);
        if (orphans == null || orphans.isEmpty()) {
            return;
        }
        for (GmmsMessage msg : orphans) {
            sendDeliveryReport(streamKey, msg);
        }
    }

    private void sendDeliveryReport(String streamKey, GmmsMessage msg) {
        long start = System.nanoTime();
        try {
            Session session = MultiSmppServerFactory.getInstance().getSession(msg);
            if (session == null) {
                logger.warn(msg, "Server DR-to-customer session not found. streamKey={}", streamKey);
                failToCoreAndAck(streamKey, msg, "session not found");
                return;
            }
            logger.info(msg, "Server DR-to-customer resolved session. streamKey={}, session={}, status={}",
                streamKey, session.getSessionName(), session.getStatus());
            if (session != null && isConnected(session) && session.submit(msg)) {
                queueManager.ack(streamKey, groupName, msg, false);
                logger.debug(msg, "Server DR-to-customer sent and acked. streamKey={}, session={}",
                    streamKey, session.getSessionName());
                return;
            }
            logger.warn(msg, "Server DR-to-customer submit failed or session unavailable. streamKey={}, session={}, status={}",
                streamKey, session.getSessionName(), session.getStatus());
            failToCoreAndAck(streamKey, msg, "submit failed, session=" + session.getSessionName()
                + ", status=" + session.getStatus());
        } catch (Exception e) {
            logger.error(msg, "Failed to send DR through MultiSmppSession", e);
            failToCoreAndAck(streamKey, msg, "exception: " + e.getClass().getSimpleName());
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer("consumer", "process", msg, "latency"),
                    System.nanoTime() - start);
        }
    }
    
    private boolean isConnected(Session session) {
        ConnectionStatus status = session.getStatus();
        return ConnectionStatus.CONNECT.equals(status) || ConnectionStatus.RECOVER.equals(status);
    }
    
    private void failToCoreAndAck(String streamKey, GmmsMessage msg, String reason) {
        MetricsCollector.getInstance().incrementCounter(MetricsNames.counter("consumer", "process", msg, "fail"));
        if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
            msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
            msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
        } else {
            msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
            msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
        }

        if (queueManager.produceResult(msg)) {
            queueManager.ack(streamKey, groupName, msg, false);
            logger.warn(msg, "Server DR-to-customer failed, reported 11005 to core and acked. streamKey={}, reason={}",
                streamKey, reason);
            return;
        }

        logger.error(msg, "Server DR-to-customer failed and could not report 11005 to core. "
            + "Message remains pending for PEL auto-claim. streamKey={}, reason={}", streamKey, reason);
        queueManager.logNack(streamKey, groupName, consumerName, msg, false, "failure_result_not_produced:" + reason);
    }
}
