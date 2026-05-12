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
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.customerconnectionfactory.MultiSmppClientFactory;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.throttle.DistributedThrottlingManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/**
 * V5.2 Client-side Outbound Consumer (Fully Decoupled Design)
 *
 * 职责：负责从 MT Routed 队列处理下行消息外派。
 * 设计改进：
 * 1. 多个 client 节点能并发消费，长短信通过 ShardId 分配到同一 client 保证顺序和状态机本地性。
 * 2. 多 SSID 隔离为不同 Stream，不会发生头部阻塞问题。
 * 3. 门铃模式 (Doorbell Pattern)，动态调配，解决空转问题。
 * 4. 集成分布式通道 TPS 节流。
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
    private ExecutorService dispatcherThread;
    private ThreadPoolExecutor workerPool;
    private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<>();

    private OutboundStreamConsumer() {
        this.queueManager = StreamQueueManager.getInstance();
        String nodeID = System.getProperty("NodeID", "0");
        this.consumerName = MODULE_ROLE + "-" + FUNCTION_NAME + "-" + nodeID;
    }

    public static OutboundStreamConsumer getInstance() {
        return instance;
    }

    public synchronized void start() {
        if (running) return;
        running = true;

        workerPool = new ThreadPoolExecutor(
            50, 200, 60L, TimeUnit.SECONDS, 
            new LinkedBlockingQueue<Runnable>(2000), 
            new ThreadPoolExecutor.CallerRunsPolicy()
        );

        dispatcherThread = Executors.newSingleThreadExecutor();
        dispatcherThread.execute(this::dispatcherLoop);
        queueManager.reRingExistingStreams(StreamQueueManager.STR_MT_ROUTED_PREFIX + "*", true);

        logger.info("Client outbound-submit consumer started. Consumer: {}, Shards: {}",
            consumerName, GmmsUtility.getInstance().getMyShards());
    }

    public synchronized void stop() {
        running = false;
        if (dispatcherThread != null) dispatcherThread.shutdownNow();
        if (workerPool != null) workerPool.shutdown();
        logger.info("Client outbound-submit consumer stopped. Consumer: {}", consumerName);
    }

    private void dispatcherLoop() {
        while (running) {
            try {
                // 监听 Routed 活跃队列门铃
                String streamKey = queueManager.popResponsibleDoorbell(StreamQueueManager.ZSET_MT_ROUTED_ACTIVE, true);
                if (streamKey != null) {
                    // 避免重复调度同一个 streamKey
                    if (processingMap.putIfAbsent(streamKey, true) == null) {
                        workerPool.submit(new FetchAndProcessTask(streamKey));
                    } else {
                        pendingDoorbellMap.put(streamKey, true);
                    }
                } else {
                    Thread.sleep(50);
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
                    processAutoClaim(streamKey);

                    Map<String, StreamEntryID> query = new HashMap<>();
                    query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
                    
                    // 批量拉取
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 100, query, true);

                    if (messages == null || messages.isEmpty()) {
                        break; 
                    }

                    for (GmmsMessage msg : messages) {
                        processMessage(streamKey, msg);
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

    private void processAutoClaim(String streamKey) {
        List<GmmsMessage> orphans = queueManager.autoClaimBatch(streamKey, groupName, consumerName,
                queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimBatchSize(), true);
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
            int ssid = msg.getOSsID();
            // 1. TPS 节流控制 (基于 SSID 通道配置)
            DistributedThrottlingManager.getInstance().acquire(ssid);

            // 2. 发送处理
            handleMessageSending(msg, streamKey);
        } catch (Exception e) {
            logger.error(msg, "Error processing outbound stream message", e);
            handleFailure(msg, "Internal error during outbound processing", streamKey);
        } finally {
            MetricsCollector.getInstance().recordTime(MetricsNames.timer("consumer", "process", msg, "latency"),
                    System.nanoTime() - start);
        }
    }

    private void handleMessageSending(GmmsMessage msg, String streamKey) {
        try {
            Session session = MultiSmppClientFactory.getInstance().getSession(msg);
            
            if (session != null && isConnected(session)) {
                boolean success = session.submit(msg);
                if (success) {
                    queueManager.ack(streamKey, groupName, msg, true);
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
    }
    
    private boolean isConnected(Session session) {
        ConnectionStatus status = session.getStatus();
        return ConnectionStatus.CONNECT.equals(status) || ConnectionStatus.RECOVER.equals(status);
    }

    private void handleFailure(GmmsMessage msg, String reason, String streamKey) {
        boolean resultProduced = false;
        try {
            MetricsCollector.getInstance().incrementCounter(MetricsNames.counter("consumer", "process", msg, "fail"));
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
                queueManager.ack(streamKey, groupName, msg, true);
            } else {
                queueManager.logNack(streamKey, groupName, consumerName, msg, true, "failure_result_not_produced");
            }
        }
    }
}
