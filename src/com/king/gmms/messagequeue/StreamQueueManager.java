package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import com.king.framework.SystemLogger;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntry;
import redis.clients.jedis.StreamEntryID;

/**
 * V4.0 Redis Stream 队列管理器 (单例)
 * 负责分层 MQ 的生产与消费封装
 */
public class StreamQueueManager {
	private static final SystemLogger logger = SystemLogger.getSystemLogger(StreamQueueManager.class);
	private static final StreamQueueManager instance = new StreamQueueManager();

	private final RedisClient redisClient;
	private final long MAX_LEN = 1000000; // 默认队列阈值

	// 队列名称模板
	private static final String STR_MT_PENDING_PREFIX = "stream:mt:pending:"; // Server -> Core
	private static final String STR_MT_ROUTED_PREFIX = "stream:mt:routed:";  // Core -> Client
	private static final String STR_DR_PENDING_PREFIX = "stream:dr:pending:"; // Client -> Server
	
	// V4.1 New Streams for Decentralized Feedback Loop
	public static final String STR_OUTBOUND_HTTP = "stream:outbound:http:tasks"; // Core -> Any HTTP Client
	public static final String STR_CORE_RESULTS = "stream:core:results";         // Any Client -> Core

	private StreamQueueManager() {
		this.redisClient = RedisClient.getInstance();
		startPELMonitor();
	}

	public static StreamQueueManager getInstance() {
		return instance;
	}

	/**
	 * 【生产】Server 收到请求后，根据 Hash 路由到对应分片队列
	 * V4.0 保证长短信路由到同一个 Core 分片
	 */
	public boolean produceSubmitMessage(GmmsMessage msg) {
		int shardId = calculateShardId(msg);
		String key = STR_MT_PENDING_PREFIX + msg.getOSsID() + ":" + shardId;
		return produce(key, msg, true);
	}

	/**
	 * 计算分片 ID，确保长短信路由一致性
	 */
	public int calculateShardId(GmmsMessage msg) {
		int totalShards = GmmsUtility.getInstance().getTotalShards();
		if (totalShards <= 1) return 0;
		
		String hashKey;
		// 路由键：SSID + 发送方 + 接收方 + 参考号（如果有）
		if (msg.getSarMsgRefNum() != null && !msg.getSarMsgRefNum().isEmpty()) {
			hashKey = msg.getOSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress() + "_" + msg.getSarMsgRefNum();
		} else {
			hashKey = msg.getOSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		}
		
		return Math.abs(hashKey.hashCode()) % totalShards;
	}

	/**
	 * 【生产】Core 处理路由后，推送到特定渠道输出队列
	 */
	public boolean produceSubmitRouted(GmmsMessage msg, String channelId) {
		String key = STR_MT_ROUTED_PREFIX + channelId;
		return produce(key, msg, true);
	}

	/**
	 * 【生产】Client 收到回执后，推送到状态报告处理队列
	 */
	public boolean produceDeliveryReport(GmmsMessage msg) {
		String key = getDrPendingQueue(msg);
		return produce(key, msg, false); // DR 走 Report-MQ
	}

	private boolean produce(String key, GmmsMessage msg, boolean isSubmitMq) {
		try {
			Map<String, String> body = new HashMap<>();
			body.put("body", SerializableHandler.convertGmmsMessage2RedisMessage(msg));
			
			String msgId = isSubmitMq 
				? redisClient.xaddToSubmitMq(key, body, MAX_LEN)
				: redisClient.xaddToReportMq(key, body, MAX_LEN);
				
			return msgId != null;
		} catch (Exception e) {
			logger.error("Produce to stream {} failed", key, e);
			return false;
		}
	}

	/**
	 * 【获取队列名称】包括分片后缀
	 */
	public String getSubmitPendingQueue(int ssid, int shardId) {
		return STR_MT_PENDING_PREFIX + ssid + ":" + shardId;
	}

	public String getSubmitRoutedQueue(String channelId) {
		return STR_MT_ROUTED_PREFIX + channelId;
	}

	public String getDrPendingQueue(GmmsMessage msg) {
		String key = STR_DR_PENDING_PREFIX + msg.getOSsID();
		// V4.0 Sticky Routing: 优先定向回流到原始 Server 节点
		if (msg.getInnerTransaction() != null && msg.getInnerTransaction().getModule() != null) {
			String sourceNode = msg.getInnerTransaction().getModule().getModule();
			if (sourceNode != null && !sourceNode.isEmpty()) {
				key += ":" + sourceNode;
			}
		}
		return key;
	}

	public String getDrPendingQueue(int ssid) {
		return STR_DR_PENDING_PREFIX + ssid;
	}

	/**
	 * 【通用消费封装】
	 * @param streamsMap Map<StreamKey, LastEntryID>
	 */
	public List<GmmsMessage> consumeBatch(String groupName, String consumerName, int count, 
			Map<String, StreamEntryID> streamsMap, boolean isSubmitMq) {
		
		List<Map.Entry<String, List<StreamEntry>>> results = isSubmitMq
			? redisClient.xreadGroupSubmitMq(groupName, consumerName, count, 2000, streamsMap)
			: redisClient.xreadGroupReportMq(groupName, consumerName, count, 2000, streamsMap);

		List<GmmsMessage> messages = new ArrayList<>();
		if (results == null) return messages;

		for (Map.Entry<String, List<StreamEntry>> entry : results) {
			for (StreamEntry se : entry.getValue()) {
				String body = se.getFields().get("body");
				GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(body);
				if (msg != null) {
					// V4.0 Persistent StreamID for Late ACK
					msg.setRedisStreamID(se.getID().toString());
					messages.add(msg);
				}
			}
		}
		return messages;
	}

	/**
	 * 【通用确认】
	 */
	public void ack(String key, String groupName, GmmsMessage msg, boolean isSubmitMq) {
		String rid = msg.getRedisStreamID();
		if (rid != null) {
			StreamEntryID id = new StreamEntryID(rid);
			if (isSubmitMq) {
				redisClient.xackSubmitMq(key, groupName, id);
			} else {
				redisClient.xackReportMq(key, groupName, id);
			}
		}
	}

	/**
	 * 【生产】发往客户端的异步任务
	 */
	public boolean produceOutboundMessage(GmmsMessage msg) {
		String key;
		if ("HTTP".equalsIgnoreCase(msg.getProperty("Protocol") != null ? msg.getProperty("Protocol").toString() : "")) {
			key = STR_OUTBOUND_HTTP;
		} else {
			// SMPP 或其他协议，寻找特定 Node 流
			key = getSubmitRoutedQueue(msg.getDeliveryChannel());
		}
		return produce(key, msg, true);
	}

	/**
	 * 【生产】客户端上报给核心的结果 (SUBMIT_RESP/DR_RESP)
	 */
	public boolean produceResult(GmmsMessage msg) {
		return produce(STR_CORE_RESULTS, msg, false); // 结果上报走 Report-MQ 优先级
	}

	/**
	 * 【创建消费组】支持自动创建流
	 */
	public void createGroup(String key, String groupName, boolean isSubmitMq) {
		try {
			if (isSubmitMq) {
				if (redisClient.getSubmitMqRedis() != null) {
					redisClient.getSubmitMqRedis().xgroupCreate(key, groupName, true);
				}
			} else {
				if (redisClient.getReportMqRedis() != null) {
					redisClient.getReportMqRedis().xgroupCreate(key, groupName, true);
				}
			}
		} catch (Exception e) {
			// Group already exists or other error, usually safe to ignore if already exists
		}
	}

	private Thread pelMonitorThread;
	private volatile boolean pelMonitorRunning = false;

	public synchronized void startPELMonitor() {
		if (pelMonitorRunning) return;
		pelMonitorRunning = true;
		pelMonitorThread = new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info("PELMonitorThread started");
				while (pelMonitorRunning) {
					try {
						Thread.sleep(30000); // 30s interval
						// Scan nodes
						Set<String> allNodes = redisClient.getStateRedis().smembers("system:server:nodes");
						if (allNodes == null || allNodes.isEmpty()) continue;
						
						String myNodeId = com.king.gmms.GmmsUtility.getInstance().getNodeId();
						if (myNodeId == null || myNodeId.isEmpty()) myNodeId = System.getProperty("NodeID", "0");
						
						// Use xautoclaim for dead-letter allocation (it fundamentally uses pending+claim)
						for (String otherNodeId : allNodes) {
							if (otherNodeId.equals(myNodeId)) continue;
							
							// Scan report streams (DR)
							scanAndClaimStreams("stream:dr:pending:*", "cg_dr_server_" + otherNodeId, "c_" + myNodeId, false);
							// Scan submit streams (MT)
							scanAndClaimStreams("stream:mt:pending:*", "CoreGroup", "c_" + myNodeId, true);
						}
					} catch (InterruptedException e) {
						break;
					} catch (Exception e) {
						logger.error("PELMonitorThread Error", e);
					}
				}
			}
		}, "PELMonitorThread");
		pelMonitorThread.setDaemon(true);
		pelMonitorThread.start();
	}
	
	private void scanAndClaimStreams(String pattern, String groupName, String consumerName, boolean isSubmitMq) {
		try (redis.clients.jedis.Jedis jedis = isSubmitMq ? redisClient.getSubmitMqRedis().getJedis() : redisClient.getReportMqRedis().getJedis()) {
			if (jedis == null) return;
			Set<String> streams = jedis.keys(pattern);
			if (streams != null) {
				for (String streamKey : streams) {
					// autoClaimBatch will read and we don't hold them in memory unless needed
					List<GmmsMessage> claimed = autoClaimBatch(streamKey, groupName, consumerName, 60000, 100, isSubmitMq);
					if (claimed != null && !claimed.isEmpty()) {
						// XCLAIM automatically takes ownership to consumerName. 
						// They will be picked up in the consumer's next 0-0 fetch, 
						// OR we can leave them in Redis, but we just called autoClaimBatch which fetches them!
						// Warning: autoClaimBatch has side effect of pulling messages to JAVA memory, 
						// but since PELMonitor doesn't process them, we should just let them stay unacked for now.
						// The main consumer thread will fetch 0-0 and get them.
					}
				}
			}
		} catch (Exception e) {
			logger.warn("scanAndClaimStreams error for pattern " + pattern, e);
		}
	}

	/**
	 * 【自动认领】处理超时的 Pending 消息
	 */
	public List<GmmsMessage> autoClaimBatch(String streamKey, String groupName, String consumerName, 
			long minIdleMs, int count, boolean isSubmitMq) {
		
		Map.Entry<redis.clients.jedis.StreamEntryID, List<redis.clients.jedis.StreamEntry>> result = isSubmitMq
			? redisClient.xautoclaimSubmitMq(streamKey, groupName, consumerName, minIdleMs, 
					redis.clients.jedis.StreamEntryID.UNRECEIVED_ENTRY, count)
			: redisClient.xautoclaimReportMq(streamKey, groupName, consumerName, minIdleMs, 
					redis.clients.jedis.StreamEntryID.UNRECEIVED_ENTRY, count);

		List<GmmsMessage> messages = new ArrayList<>();
		if (result == null || result.getValue() == null) return messages;

		for (redis.clients.jedis.StreamEntry se : result.getValue()) {
			String body = se.getFields().get("body");
			GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(body);
			if (msg != null) {
				msg.setRedisStreamID(se.getID().toString());
				messages.add(msg);
				
				if (logger.isDebugEnabled()) {
					logger.debug("AutoClaimed message from stream {}: {}", streamKey, se.getID());
				}
			}
		}
		return messages;
	}
}
