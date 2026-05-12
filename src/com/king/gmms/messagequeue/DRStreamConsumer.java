package com.king.gmms.messagequeue;

import java.util.*;
import java.util.concurrent.*;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.customerconnectionfactory.CustomerConnectionFactory;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageStoreManager;
import redis.clients.jedis.StreamEntryID;

/**
 * V4.0 Server-side DR Consumer
 * Responsible for pulling DRs from Redis and dispatching to local sessions.
 */
public class DRStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(DRStreamConsumer.class);
    private static DRStreamConsumer instance = new DRStreamConsumer();

    private final StreamQueueManager queueManager;
    private final GmmsUtility gmmsUtility;
    private final String nodeId;
    private final String groupName;
    private final String consumerName;

    private final Set<Integer> activeSsids = ConcurrentHashMap.newKeySet();
    private final Map<Integer, Future<?>> workerThreads = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private volatile boolean running = false;

    private DRStreamConsumer() {
        this.gmmsUtility = GmmsUtility.getInstance();
        this.queueManager = StreamQueueManager.getInstance();
        this.nodeId = gmmsUtility.getNodeId();
        this.groupName = "cg_dr_server_" + nodeId;
        this.consumerName = "c_" + nodeId + "_" + UUID.randomUUID().toString().substring(0, 8);
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "DRStreamConsumerWorker");
            t.setDaemon(true);
            return t;
        });
    }

    public static DRStreamConsumer getInstance() {
        return instance;
    }

    public void start() {
        if (running) return;
        running = true;
        logger.info("DRStreamConsumer started for node: {}", nodeId);
    }

    public void stop() {
        running = false;
        executor.shutdownNow();
        logger.info("DRStreamConsumer stopped.");
    }

    /**
     * Called when a new customer session is established (e.g., SMPP Bind)
     */
    public synchronized void registerSSID(int ssid) {
        if (!activeSsids.contains(ssid)) {
            activeSsids.add(ssid);
            startPollingForSsid(ssid);
            logger.info("Started DR polling for SSID: {}", ssid);
        }
    }

    /**
     * Called when all sessions for an SSID are closed
     */
    public synchronized void unregisterSSID(int ssid) {
        // In a real implementation, you'd check if this was the last session
        // For simplicity, we just keep polling once started, or implement a counter
    }

    private void startPollingForSsid(int ssid) {
        if (workerThreads.containsKey(ssid)) return;

        Future<?> future = executor.submit(() -> {
            String streamKey = queueManager.getDrPendingQueue(ssid) + ":" + nodeId;

            // Ensure consumer group exists (only once, before the loop)
            queueManager.createGroup(streamKey, groupName, false);

            Map<String, StreamEntryID> streamsMap = new HashMap<>();
            streamsMap.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);

            // V4.0 Fix: 补全 while(running) 循环，保证持续轮询
            while (running) {
                try {
                    // 1. 获取分配给本节点的待处理死信 (由 PELMonitor 重新分发)
                    Map<String, StreamEntryID> pendingMap = new HashMap<>();
                    pendingMap.put(streamKey, new StreamEntryID("0-0"));
                    List<GmmsMessage> pendingMessages = queueManager.consumeBatch(groupName, consumerName, 10, pendingMap, false);
                    if (pendingMessages != null && !pendingMessages.isEmpty()) {
                        for (GmmsMessage msg : pendingMessages) {
                            processDR(streamKey, msg);
                        }
                    }

                    // 2. 尝试获取常规新消息
                    List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 10, streamsMap, false);
                    for (GmmsMessage msg : messages) {
                        processDR(streamKey, msg);
                    }

                    if (messages.isEmpty() && (pendingMessages == null || pendingMessages.isEmpty())) {
                        Thread.sleep(500); // Backoff when idle
                    }
                } catch (InterruptedException ie) {
                    break;
                } catch (Exception e) {
                    logger.error("Error polling DR for SSID " + ssid, e);
                    try { Thread.sleep(2000); } catch (Exception ignored) {}
                }
            }
        });
        workerThreads.put(ssid, future);
    }

    private void processDR(String streamKey, GmmsMessage msg) {
        try {
            // V4.0 Sticky Routing: Restore transaction context from innerTransaction
            // This allows CustomerMessageSender to use SameSessionStrategy
            if (msg.getInnerTransaction() != null) {
                msg.setTransaction(msg.getInnerTransaction());
            }

            InternalAgentConnectionFactory agentFactory = InternalAgentConnectionFactory.getInstance();
            CustomerConnectionFactory customerFactory = agentFactory.getCustomerFactory();
            
            if (customerFactory == null) {
                 logger.error(msg, "No CustomerConnectionFactory found for DR delivery");
                 return;
            }

            // Route to OperatorMessageQueue
            // The factory will handle finding the correct session based on SSID (and optionally ConnectionName)
            OperatorMessageQueue msgQueue = customerFactory.getOperatorMessageQueue(msg.getOSsID());
            
            if (msgQueue != null) {
                boolean success = msgQueue.putMsg(msg);
                if (success) {
                    // ACK ONLY after successfully putting to local queue / next stage
                    queueManager.ack(streamKey, groupName, msg, false);
                }
            } else {
                logger.warn(msg, "No active session queue found locally for SSID: {}", msg.getOSsID());
                // Do not ACK. Leave it pending so PELMonitorThread can re-assign it to another node where the session might be active. 
            }
        } catch (Exception e) {
            logger.error(msg, "Failed to process DR from Redis", e);
        }
    }
}
