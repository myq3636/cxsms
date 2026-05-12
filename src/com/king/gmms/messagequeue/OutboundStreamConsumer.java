package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.throttle.DistributedThrottlingManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/**
 * V4.1 Client 模块核心消费者
 * 负责从 MT Routed 队列处理下行消息外派
 */
public class OutboundStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(OutboundStreamConsumer.class);
    private static OutboundStreamConsumer instance = new OutboundStreamConsumer();

    private final StreamQueueManager queueManager;
    private final GmmsUtility gmmsUtility;
    private final String groupName = "OutboundGroup";
    private final String consumerName;
    private final String streamKey;
    
    private volatile boolean running = false;
    private ExecutorService dispatchExecutor;
    private ExecutorService senderThreadPool;

    private OutboundStreamConsumer() {
        this.queueManager = StreamQueueManager.getInstance();
        this.gmmsUtility = GmmsUtility.getInstance();
        String nodeID = System.getProperty("NodeID", "0");
        this.consumerName = "ClientNode_" + nodeID;
        // 节点专属外派流
        this.streamKey = StreamQueueManager.getInstance().getSubmitRoutedQueue(nodeID);
    }

    public static OutboundStreamConsumer getInstance() {
        return instance;
    }

    public synchronized void start() {
        if (running) return;
        running = true;

        logger.info("Starting OutboundStreamConsumer for stream: {}", streamKey);
        
        // 分发线程：负责读取 Stream
        dispatchExecutor = Executors.newSingleThreadExecutor();
        // 发送线程池：负责执行限速与物理提交
        senderThreadPool = Executors.newFixedThreadPool(50); 
        
        // 初始化消费组
        queueManager.createGroup(streamKey, groupName);

        dispatchExecutor.execute(new Runnable() {
            @Override
            public void run() {
                consumerLoop();
            }
        });
    }

    public synchronized void stop() {
        running = false;
        if (dispatchExecutor != null) dispatchExecutor.shutdownNow();
        if (senderThreadPool != null) senderThreadPool.shutdownNow();
    }

    private void consumerLoop() {
        Map<String, StreamEntryID> streamsMap = new HashMap<String, StreamEntryID>();
        streamsMap.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);

        while (running) {
            try {
                // 1. 处理 PEL (超时未确认的消息)
                processAutoClaim();

                // 2. 读取新消息
                List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 20, streamsMap, true);
                if (messages == null || messages.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }

                for (final GmmsMessage msg : messages) {
                    senderThreadPool.execute(new Runnable() {
                        @Override
                        public void run() {
                            handleMessageSending(msg);
                        }
                    });
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in OutboundStreamConsumer loop for {}", streamKey, e);
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private void processAutoClaim() {
        List<GmmsMessage> claimed = queueManager.autoClaimBatch(streamKey, groupName, consumerName, 30000, 10, true);
        for (final GmmsMessage msg : claimed) {
            logger.info(msg, "Recovered orphaned outbound message via XAUTOCLAIM");
            senderThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    handleMessageSending(msg);
                }
            });
        }
    }

    private void handleMessageSending(GmmsMessage msg) {
        try {
            int ssid = msg.getOSsID();
            A2PCustomerInfo cst = gmmsUtility.getCustomerManager().getCustomerBySSID(ssid);
            
            if (cst == null) {
                logger.error(msg, "Customer info not found for Outbound SSID: {}", ssid);
                queueManager.ack(streamKey, groupName, msg, true);
                return;
            }

            // 1. 获取双重流控许可 (阻塞)
            DistributedThrottlingManager.getInstance().acquire(ssid, cst.getOutgoingThrottlingNum());

            // 2. 查找物理连接
            // 注意：SSID 级别的发送通常需要通过 ConnectionManager 获取对应的 Session
            // 这里逻辑对齐 PrioritrySender
            ConnectionManager connMgr = gmmsUtility.getConnectionManager();
            Session session = connMgr.getSession(ssid);
            
            if (session != null && session.getStatus().isConnected()) {
                // 3. 执行真实发送
                boolean success = session.submit(msg);
                if (success) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(msg, "Successfully submitted message to channel for SSID {}", ssid);
                    }
                    // 确认消息已完全进入通道发送流程
                    queueManager.ack(streamKey, groupName, msg, true);
                } else {
                    logger.warn(msg, "Failed to submit message to session for SSID {}", ssid);
                    // 发送失败处理，由业务层决定是否重回队列或产生失败回执
                    // 这里简单处理：确认消费并发送失败 DR (对齐旧版逻辑)
                    handleFailure(msg, "Physical submit failed");
                }
            } else {
                logger.error(msg, "No active session for SSID {}, status: {}", ssid, 
                             (session != null ? session.getStatus() : "NULL"));
                handleFailure(msg, "No active session");
            }
            
        } catch (Exception e) {
            logger.error(msg, "Failed to process handleMessageSending", e);
        }
    }

    private void handleFailure(GmmsMessage msg, String reason) {
        // 进行失败处理 (如产生 0xFF 响应或内部错误 DR)
        msg.setStatusCode(GmmsStatus.COMMUNICATION_ERROR);
        gmmsUtility.getMessageStoreManager().sendDRMessageForServerReject(msg);
        queueManager.ack(streamKey, groupName, msg, true);
    }
}
