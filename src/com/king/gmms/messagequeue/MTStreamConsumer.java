package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;

/**
 * V4.0 Core 模块 Redis Stream 消费者
 * 负责从 MT Pending 队列（Submit-MQ）拉取消息并送入处理器
 */
public class MTStreamConsumer {
    private static final SystemLogger logger = SystemLogger.getSystemLogger(MTStreamConsumer.class);
    private static MTStreamConsumer instance = new MTStreamConsumer();

    private final StreamQueueManager queueManager;
    private final GmmsUtility gmmsUtility;
    private final String groupName = "CoreGroup";
    private final String consumerName;
    private volatile boolean running = false;
    private ExecutorService consumerExecutor;

    private MTStreamConsumer() {
        this.queueManager = StreamQueueManager.getInstance();
        this.gmmsUtility = GmmsUtility.getInstance();
        // 消费者名称使用 ModuleName + NodeID
        this.consumerName = System.getProperty("module", "Core") + "_" + System.getProperty("NodeID", "0");
    }

    public static MTStreamConsumer getInstance() {
        return instance;
    }

    public synchronized void start() {
        if (running) return;
        running = true;

        Set<Integer> myShards = gmmsUtility.getMyShards();
        if (myShards == null || myShards.isEmpty()) {
            logger.warn("No shards assigned to this node. MTStreamConsumer idle.");
            return;
        }

        logger.info("Starting MTStreamConsumer for shards: {}", myShards);
        consumerExecutor = Executors.newFixedThreadPool(myShards.size());

        for (Integer shardId : myShards) {
            consumerExecutor.execute(new ConsumerWorker(shardId));
        }
    }

    public synchronized void stop() {
        running = false;
        if (consumerExecutor != null) {
            consumerExecutor.shutdownNow();
        }
    }

    private class ConsumerWorker implements Runnable {
        private final int shardId;

        public ConsumerWorker(int shardId) {
            this.shardId = shardId;
        }

        @Override
        public void run() {
            logger.info("ConsumerWorker for shard {} started.", shardId);
            
            // 为所有客户 SSID 初始化消费
            // 注意：生产环境中 SSID 列表可能动态变化，这里简单处理
            while (running) {
                try {
                    List<A2PCustomerInfo> customers = gmmsUtility.getCustomerManager().getAllCustomers();
                    if (customers == null || customers.isEmpty()) {
                        Thread.sleep(5000);
                        continue;
                    }

                    for (A2PCustomerInfo customer : customers) {
                        String streamKey = queueManager.getSubmitPendingQueue(customer.getSSID(), shardId);
                        consumeStream(streamKey);
                    }
                    
                    Thread.sleep(100); // 避免空转过快
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error in ConsumerWorker for shard {}", shardId, e);
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        }

        private void consumeStream(String streamKey) {
            // 1. 确保组已创建
            queueManager.createGroup(streamKey, groupName, true);

            // 2. 获取分配给本 Consumer 的待处理死信 (由 PELMonitorThread XCLAIM 或前次挂信)
            Map<String, StreamEntryID> pendingMap = new HashMap<>();
            pendingMap.put(streamKey, new StreamEntryID("0-0"));
            List<GmmsMessage> pendingMessages = queueManager.consumeBatch(groupName, consumerName, 10, pendingMap, true);
            if (pendingMessages != null && !pendingMessages.isEmpty()) {
                for (GmmsMessage msg : pendingMessages) {
                    processMessage(streamKey, msg);
                }
            }

            // 3. 尝试获取新消息
            Map<String, StreamEntryID> streamsMap = new HashMap<>();
            streamsMap.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
            List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName, 10, streamsMap, true);
            
            for (GmmsMessage msg : messages) {
                processMessage(streamKey, msg);
            }
            
            // XAUTOCLAIM 移除，已交给后台 PELMonitor 线程全局死信调配处理
        }

        private void processMessage(String streamKey, GmmsMessage msg) {
            try {
                A2PCustomerInfo cst = gmmsUtility.getCustomerManager().getCustomerBySSID(msg.getOSsID());
                if (cst == null) {
                    logger.error(msg, "Customer info not found for SSID: {}, dropping message.", msg.getOSsID());
                    queueManager.ack(streamKey, groupName, msg, true);
                    return;
                }

                // V4.0 委托给 InternalCoreEngineSession 执行完整的旧业务逻辑：
                // 黑名单/白名单检查、内容过滤、CSM重组、prepareInSubmit、
                // 收件人地址规则、消息大小检查、24h频控、deliveryRouterHandler 路由等。
                // 与 TCP 路径的 processSuccessSubmit/processErrorSubmit 功能完全一致，
                // 但跳过 inBuffer + sendPdu(ack) TCP 反馈（所有消息均走 Stream）。
                InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(msg);

                // 业务处理完成后 ACK，确保消息不被重复消费
                queueManager.ack(streamKey, groupName, msg, true);

            } catch (Exception e) {
                logger.error(msg, "Failed to process message from stream {}", streamKey, e);
                // 不 ACK，让 PELMonitor 超时后重新分发
            }
        }
    }
}
