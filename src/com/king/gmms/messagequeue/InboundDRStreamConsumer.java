package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;

/**
 * V4.5 Core 节点 Redis Stream 消费者 (DR 链路)
 * 负责人从 stream:dr:pending 队列拉取状态报告并执行还原逻辑
 * 对齐 InternalCoreEngineSession 的 Pdu.COMMAND_DELIVERY_REPORT 分支
 */
public class InboundDRStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(InboundDRStreamConsumer.class);
    private static InboundDRStreamConsumer instance = new InboundDRStreamConsumer();

    private final StreamQueueManager queueManager;
    private final GmmsUtility gmmsUtility;
    private final String groupName = "CoreDRGroup";
    private final String consumerName;
    private volatile boolean running = false;
    private ExecutorService consumerExecutor;

    private InboundDRStreamConsumer() {
        this.queueManager = StreamQueueManager.getInstance();
        this.gmmsUtility = GmmsUtility.getInstance();
        this.consumerName = "CoreDR_" + System.getProperty("NodeID", "0");
    }

    public static InboundDRStreamConsumer getInstance() {
        return instance;
    }

    public synchronized void start() {
        if (running) return;
        running = true;

        // DR 消费通常不需要复杂分片，但为了集群横向扩展，我们依然支持多 SSID 轮询
        logger.info("Starting InboundDRStreamConsumer...");
        consumerExecutor = Executors.newFixedThreadPool(2); // DR 处理吞吐量通常低于 MT，2个线程即可

        consumerExecutor.execute(new ConsumerWorker());
    }

    public synchronized void stop() {
        running = false;
        if (consumerExecutor != null) {
            consumerExecutor.shutdownNow();
        }
    }

    private class ConsumerWorker implements Runnable {
        @Override
        public void run() {
            logger.info("DR ConsumerWorker started.");
            
            while (running) {
                try {
                    List<A2PCustomerInfo> customers = gmmsUtility.getCustomerManager().getAllCustomers();
                    if (customers == null || customers.isEmpty()) {
                        Thread.sleep(5000);
                        continue;
                    }

                    for (A2PCustomerInfo customer : customers) {
                        String streamKey = queueManager.getDrPendingQueue(customer.getSSID());
                        
                        // 注意：如果 Client 端生产时带了 NodeID 后缀（Sticky Routing），
                        // 这里也需要扫描带后缀的队列，或者 Core 端只管主 SSID 队列。
                        // 由于生产端使用了 getDrPendingQueue(msg)，可能会包含 ":NodeID"。
                        // 目前简化处理，由 Core 统一扫描主队列和本地 Node 相关的队列。
                        
                        consumeStream(streamKey);
                        
                        // 额外扫描属于本节点的 Sticky 队列（如果有）
                        String myNode = System.getProperty("module", "Core") + "_" + System.getProperty("NodeID", "0");
                        consumeStream(streamKey + ":" + myNode);
                    }
                    
                    Thread.sleep(100); 
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in DR ConsumerWorker", e);
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        }

        private void consumeStream(String streamKey) {
            Map<String, StreamEntryID> streamsMap = new HashMap<>();
            streamsMap.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);

            // 1. 获取新消息
            List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 20, streamsMap, false);
            for (GmmsMessage msg : messages) {
                processMessage(streamKey, msg);
            }

            // 2. 自动认领超时消息 (可靠性)
            List<GmmsMessage> claimedMessages = queueManager.autoClaimBatch(streamKey, groupName, consumerName, 60000, 20, false);
            for (GmmsMessage msg : claimedMessages) {
                processMessage(streamKey, msg);
            }
        }

        private void processMessage(String streamKey, GmmsMessage msg) {
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug(msg, "Processing reactive DR from stream {}", streamKey);
                }

                // 精准进入 InternalCoreEngineSession 的业务分支
                InternalCoreEngineSession.getReactiveInstance().handleInboundDRInternal(msg, null);

                // 处理完成后 ACK
                queueManager.ack(streamKey, groupName, msg, false);
                
            } catch (Exception e) {
                logger.error(msg, "Failed to process DR from stream {}", streamKey, e);
            }
        }
    }
}
