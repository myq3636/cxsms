package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.Tuple;

/**
 * V5.1 Redis Stream 队列管理器 (单例)
 * 负责分层 MQ 的生产与消费封装，已集成 Doorbell Pattern (信令门铃模式)
 * 
 * 修改说明：
 * 1. 彻底移除 raw jedis 调用，改为使用 RedisClient 的封装方法。
 * 2. 提供高层次的 Doorbell 交互接口 (popDoorbell, reRingDoorbell, isResponsibleForStream)。
 */
public class StreamQueueManager {
	private static final SystemLogger logger = SystemLogger.getSystemLogger(StreamQueueManager.class);
	private static final StreamQueueManager instance = new StreamQueueManager();

	private final RedisClient redisClient;
	private final long MAX_LEN = 1000000;

	// 队列名称模板
	public static final String STR_MT_PENDING_PREFIX = "stream:mt:pending:";
	public static final String STR_MT_ROUTED_PREFIX = "stream:mt:routed:";
	public static final String STR_DR_PENDING_PREFIX = "stream:dr:pending:";
	public static final String STR_INBOUND_DR_PREFIX = "stream:core:inbound-dr:";
	
	public static final String STR_OUTBOUND_HTTP = "stream:outbound:http:tasks";
	public static final String STR_CORE_RESULTS = "stream:core:results";

	// 全局活跃门铃 ZSet 键名
	public static final String ZSET_MT_PENDING_ACTIVE = "zset:mt:pending:active";
	public static final String ZSET_MT_ROUTED_ACTIVE = "zset:mt:routed:active";
	public static final String ZSET_DR_PENDING_ACTIVE = "zset:dr:pending:active";
	public static final String ZSET_INBOUND_DR_ACTIVE = "zset:core:inbound-dr:active";
	public static final String ZSET_CORE_RESULTS_ACTIVE = "zset:core:results:active";
	private static final StreamEntryID AUTOCLAIM_START_ID = new StreamEntryID("0-0");

	private StreamQueueManager() {
		this.redisClient = RedisClient.getInstance();
		if (isGlobalPELMonitorEnabled()) {
			startPELMonitor();
		}
	}

	public static StreamQueueManager getInstance() {
		return instance;
	}

	/**
	 * 【生产】Server 收到请求后路由到分片队列
	 */
	public boolean produceSubmitMessage(GmmsMessage msg) {
		int shardId = calculateShardId(msg);
		String key = STR_MT_PENDING_PREFIX + msg.getOSsID() + ":" + shardId;
		boolean produced = produce(key, msg, true);
		if (!produced) {
			logger.error(msg, "Failed to produce submit message. streamKey={}, shardId={}, osSid={}",
				key, shardId, msg == null ? -1 : msg.getOSsID());
		}
		return produced;
	}

	public int calculateShardId(GmmsMessage msg) {
		int totalShards = GmmsUtility.getInstance().getTotalShards();
		if (totalShards <= 1) return 0;
		String hashKey;
		if (msg.getSarMsgRefNum() != null && !msg.getSarMsgRefNum().isEmpty()) {
			hashKey = msg.getOSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress() + "_" + msg.getSarMsgRefNum();
		} else {
			hashKey = msg.getOSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		}
		return Math.abs(hashKey.hashCode()) % totalShards;
	}

	public int calculateOutboundShardId(GmmsMessage msg) {
		int totalShards = GmmsUtility.getInstance().getTotalShards();
		if (totalShards <= 1) return 0;
		String longMessageKey = msg.getSarMsgRefNum();
		if (isBlank(longMessageKey)) {
			longMessageKey = msg.getInMsgID();
		}
		String hashKey;
		if (!isBlank(longMessageKey)) {
			hashKey = msg.getRSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress() + "_" + longMessageKey;
		} else {
			hashKey = msg.getRSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		}
		return (hashKey.hashCode() & Integer.MAX_VALUE) % totalShards;
	}

	public boolean produceSubmitRouted(GmmsMessage msg, String channelId) {
		String key = STR_MT_ROUTED_PREFIX + channelId;
		if (!CoreOutboundOutboxManager.getInstance().register(msg, key)) {
			return false;
		}
		boolean produced = produce(key, msg, true);
		if (!produced) {
			CoreOutboundOutboxManager.getInstance().remove(msg);
		}
		return produced;
	}

	public boolean produceDeliveryReport(GmmsMessage msg) {
		String key = getDrPendingQueue(msg);
		return produce(key, msg, false);
	}

	public boolean produceServerDeliveryMessage(GmmsMessage msg) {
		String key = getServerDeliveryQueue(msg);
		return produce(key, msg, false);
	}

	public boolean produceInboundDeliveryReport(GmmsMessage msg) {
		String key = getInboundDrQueue(msg);
		return produce(key, msg, false);
	}

	/**
	 * 底层统一生产方法
	 */
	private boolean produce(String key, GmmsMessage msg, boolean isSubmitMq) {
		try {
			Map<String, String> body = new HashMap<>();
			body.put("body", SerializableHandler.convertGmmsMessage2RedisMessage(msg));
			
			String msgId = isSubmitMq 
				? redisClient.xaddToSubmitMq(key, body, MAX_LEN)
				: redisClient.xaddToReportMq(key, body, MAX_LEN);
				
			if (msgId != null) {
				String zsetKey = getDoorbellZSet(key);
				Long doorbellResult = zsetKey == null ? null : zaddDoorbell(zsetKey, key, isSubmitMq);
				if (zsetKey == null || doorbellResult == null || doorbellResult.longValue() < 0L) {
					logStreamTrace("STREAM-PUT", key, null, null, msgId, msg, isSubmitMq,
							"zset=" + zsetKey + " doorbell=" + doorbellResult + " result=fail reason=doorbell_failed");
					incrementStreamCounter("out", key, msg, "write_fail");
					return false;
				}
				incrementStreamCounter("out", key, msg, "write_ok");
				logStreamTrace("STREAM-PUT", key, null, null, msgId, msg, isSubmitMq,
						"zset=" + zsetKey + " doorbell=" + doorbellResult + " result=ok");
			} else {
				logStreamTrace("STREAM-PUT", key, null, null, null, msg, isSubmitMq,
						"result=fail reason=xadd_returned_null");
				incrementStreamCounter("out", key, msg, "write_fail");
			}
			return msgId != null;
		} catch (Exception e) {
			logStreamTrace("STREAM-PUT", key, null, null, null, msg, isSubmitMq,
					"result=fail reason=exception:" + e.getClass().getSimpleName());
			logger.error("Produce to stream " + key + " failed", e);
			incrementStreamCounter("out", key, msg, "write_fail");
			return false;
		}
	}

	/**
	 * 触发门铃逻辑 (使用 RedisClient 封装方法)
	 */
	public boolean triggerDoorbell(String streamKey, boolean isSubmitMq) {
		try {
			String zsetKey = getDoorbellZSet(streamKey);

			if (zsetKey != null) {
				Long result = zaddDoorbell(zsetKey, streamKey, isSubmitMq);
				if (result == null || result < 0) {
					logger.error("Failed to ring doorbell. zsetKey={}, streamKey={}, isSubmitMq={}, result={}",
							zsetKey, streamKey, isSubmitMq, result);
					return false;
				}
				return true;
			}
			logger.warn("No doorbell zset mapped for streamKey={}, isSubmitMq={}", streamKey, isSubmitMq);
			return false;
		} catch (Exception e) {
			logger.warn("Failed to ring doorbell for stream: " + streamKey, e);
			return false;
		}
	}

	private String getDoorbellZSet(String streamKey) {
		if (streamKey == null) {
			return null;
		}
		if (streamKey.startsWith(STR_MT_PENDING_PREFIX)) {
			return ZSET_MT_PENDING_ACTIVE;
		} else if (streamKey.startsWith(STR_MT_ROUTED_PREFIX)) {
			return ZSET_MT_ROUTED_ACTIVE;
		} else if (streamKey.startsWith(STR_DR_PENDING_PREFIX)) {
			return ZSET_DR_PENDING_ACTIVE;
		} else if (streamKey.startsWith(STR_INBOUND_DR_PREFIX)) {
			return ZSET_INBOUND_DR_ACTIVE;
		} else if (STR_CORE_RESULTS.equals(streamKey)) {
			return ZSET_CORE_RESULTS_ACTIVE;
		}
		return null;
	}

	private Long zaddDoorbell(String zsetKey, String streamKey, boolean isSubmitMq) {
		return isSubmitMq
			? redisClient.zaddSubmitMq(zsetKey, System.currentTimeMillis(), streamKey)
			: redisClient.zaddReportMq(zsetKey, System.currentTimeMillis(), streamKey);
	}

	/**
	 * 【门铃弹出】从指定的 ZSet 弹出最早的一个活跃 StreamKey
	 */
	public String popDoorbell(String doorbellKey, boolean isSubmitMq) {
		List<Tuple> tuples = isSubmitMq 
			? redisClient.zpopminSubmitMq(doorbellKey)
			: redisClient.zpopminReportMq(doorbellKey);
		
		if (tuples != null && !tuples.isEmpty()) {
			return tuples.get(0).getElement();
		}
		return null;
	}

	public String popResponsibleDoorbell(String doorbellKey, boolean isSubmitMq) {
		int scanLimit = getDoorbellScanLimit();
		Set<String> candidates = isSubmitMq
			? redisClient.zrangeSubmitMq(doorbellKey, 0, scanLimit - 1)
			: redisClient.zrangeReportMq(doorbellKey, 0, scanLimit - 1);
		if (candidates == null || candidates.isEmpty()) {
			return null;
		}
		for (String streamKey : candidates) {
			if (!isResponsibleForStream(streamKey)) {
				continue;
			}
			long len = streamLength(streamKey, isSubmitMq);
			if (len <= 0) {
				removeDoorbell(doorbellKey, streamKey, isSubmitMq);
				if (logger.isDebugEnabled()) {
					logger.debug("Removed stale doorbell. zsetKey={}, streamKey={}, isSubmitMq={}",
							doorbellKey, streamKey, isSubmitMq);
				}
				continue;
			}
			if (removeDoorbell(doorbellKey, streamKey, isSubmitMq)) {
				return streamKey;
			}
		}
		return null;
	}

	public long streamLength(String streamKey, boolean isSubmitMq) {
		return isSubmitMq ? redisClient.xlenSubmitMq(streamKey) : redisClient.xlenReportMq(streamKey);
	}

	private boolean removeDoorbell(String doorbellKey, String streamKey, boolean isSubmitMq) {
		Long removed = isSubmitMq
			? redisClient.zremSubmitMq(doorbellKey, streamKey)
			: redisClient.zremReportMq(doorbellKey, streamKey);
		return removed != null && removed.longValue() > 0L;
	}

	private int getDoorbellScanLimit() {
		return intProperty("RedisStreamDoorbellScanLimit", 100);
	}

	/**
	 * 【分片校验】判断当前节点是否负责该 Stream 的消费
	 */
	public boolean isResponsibleForStream(String streamKey) {
		if (streamKey.startsWith(STR_MT_PENDING_PREFIX)) {
			String[] parts = streamKey.split(":");
			if (parts.length >= 5) {
				try {
					int shardId = Integer.parseInt(parts[4]);
					return GmmsUtility.getInstance().getMyShards().contains(shardId);
				} catch (Exception e) {
					logger.warn("Failed to parse shardId from streamKey: " + streamKey);
				}
			}
		} else if (streamKey.startsWith(STR_MT_ROUTED_PREFIX)) {
			// stream:mt:routed:{rssid}:{shardId}
			String[] parts = streamKey.split(":");
			if (parts.length >= 5) {
				try {
					int shardId = Integer.parseInt(parts[4]);
					return GmmsUtility.getInstance().getMyShards().contains(shardId);
				} catch (Exception e) {
					logger.warn("Failed to parse shardId from streamKey: " + streamKey);
				}
			}
		} else if (streamKey.startsWith(STR_DR_PENDING_PREFIX)) {
			String[] parts = streamKey.split(":");
			if (parts.length >= 5) {
				String moduleName = System.getProperty("module", "");
				return parts[4] == null || parts[4].length() == 0
						|| parts[4].equalsIgnoreCase(moduleName);
			}
		}
		return true;
	}

	/**
	 * 【重按门铃】Worker 退出前检查 Stream 是否还有数据
	 */
	public void reRingDoorbell(String streamKey, boolean isSubmitMq) {
		long len = streamLength(streamKey, isSubmitMq);
			
		if (len > 0) {
			triggerDoorbell(streamKey, isSubmitMq);
		}
	}

	public void reRingExistingStreams(String pattern, boolean isSubmitMq) {
		Set<String> streams = isSubmitMq ? redisClient.keysSubmitMq(pattern) : redisClient.keysReportMq(pattern);
		if (streams == null || streams.isEmpty()) {
			logger.info("No redis stream found for re-ring. pattern={}, isSubmitMq={}", pattern, isSubmitMq);
			return;
		}
		for (String streamKey : streams) {
			long len = isSubmitMq ? redisClient.xlenSubmitMq(streamKey) : redisClient.xlenReportMq(streamKey);
			if (len > 0) {
				logger.info("Re-ring existing redis stream. streamKey={}, len={}, isSubmitMq={}", streamKey, len, isSubmitMq);
				triggerDoorbell(streamKey, isSubmitMq);
			}
		}
	}

	public String getDrPendingQueue(GmmsMessage msg) {
		return buildServerPendingQueue(msg, msg.getOSsID(), false);
	}

	public String getServerDeliveryQueue(GmmsMessage msg) {
		return buildServerPendingQueue(msg, msg.getRSsID(), true);
	}

	private String buildServerPendingQueue(GmmsMessage msg, int targetSsid, boolean allowDeliveryChannel) {
		String key = STR_DR_PENDING_PREFIX + targetSsid;
		String moduleName = null;
		if (msg.getInnerTransaction() != null && msg.getInnerTransaction().getModule() != null) {
			moduleName = msg.getInnerTransaction().getModule().getModule();
		}
		if (isBlank(moduleName) && allowDeliveryChannel) {
			moduleName = msg.getDeliveryChannel();
			if (!isBlank(moduleName)) {
				int idx = moduleName.lastIndexOf(':');
				if (idx >= 0 && idx + 1 < moduleName.length()) {
					moduleName = moduleName.substring(idx + 1);
				}
			}
		}
		if (!isBlank(moduleName)) {
			key += ":" + moduleName;
		}
		return key;
	}

	public String getInboundDrQueue(GmmsMessage msg) {
		return STR_INBOUND_DR_PREFIX + msg.getOSsID();
	}

	public void createGroup(String key, String groupName, boolean isSubmitMq) {
		if (isSubmitMq) {
			redisClient.xgroupCreateSubmitMq(key, groupName, true);
		} else {
			redisClient.xgroupCreateReportMq(key, groupName, true);
		}
	}

	public void ack(String key, String groupName, GmmsMessage msg, boolean isSubmitMq) {
		String rid = msg.getRedisStreamID();
		if (rid != null) {
			StreamEntryID id = new StreamEntryID(rid);
			Long acked;
			if (isSubmitMq) {
				acked = redisClient.xackSubmitMq(key, groupName, id);
			} else {
				acked = redisClient.xackReportMq(key, groupName, id);
			}
			if (acked != null && acked.longValue() > 0) {
				Long deleted = isSubmitMq ? redisClient.xdelSubmitMq(key, id) : redisClient.xdelReportMq(key, id);
				incrementStreamCounter("in", key, msg, "ack");
				logStreamTrace("STREAM-ACK", key, groupName, null, rid, msg, isSubmitMq,
						"acked=" + acked + " deleted=" + deleted + " result=ok");
			} else {
				incrementStreamCounter("in", key, msg, "ack_fail");
				logStreamTrace("STREAM-ACK", key, groupName, null, rid, msg, isSubmitMq,
						"acked=" + acked + " result=fail reason=ack_returned_zero");
			}
		}
	}

	/**
	 * 【批量消费封装】从指定流中提取并反序列化消息
	 */
	public List<GmmsMessage> consumeBatch(String groupName, String consumerName, int count, 
			Map<String, StreamEntryID> streamsMap, boolean isSubmitMq) {
		
		List<Map.Entry<String, List<StreamEntry>>> results = readGroup(groupName, consumerName, count, streamsMap, isSubmitMq);
		if (results == null && recoverGroups(streamsMap, groupName, isSubmitMq)) {
			results = readGroup(groupName, consumerName, count, streamsMap, isSubmitMq);
		}

		List<GmmsMessage> messages = new ArrayList<>();
		if (results == null) return messages;

		for (Map.Entry<String, List<StreamEntry>> entry : results) {
			for (StreamEntry se : entry.getValue()) {
				String body = se.getFields().get("body");
				GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(body);
				if (msg != null) {
					msg.setRedisStreamID(se.getID().toString());
					messages.add(msg);
					incrementStreamCounter("in", entry.getKey(), msg, "read");
					logStreamTrace("STREAM-GET", entry.getKey(), groupName, consumerName,
							se.getID().toString(), msg, isSubmitMq, "result=ok");
				} else {
					logStreamTraceWithoutMessage("STREAM-GET", entry.getKey(), groupName, consumerName,
							se.getID().toString(), isSubmitMq, "result=fail reason=deserialize_failed body=" + body);
				}
			}
		}
		return messages;
	}

	public void logNack(String streamKey, String groupName, String consumerName, GmmsMessage msg,
			boolean isSubmitMq, String reason) {
		String redisId = msg == null ? null : msg.getRedisStreamID();
		logStreamTrace("STREAM-NACK", streamKey, groupName, consumerName, redisId, msg, isSubmitMq,
				"result=pending reason=" + reason);
	}

	private void logStreamTrace(String event, String streamKey, String groupName, String consumerName,
			String redisId, GmmsMessage msg, boolean isSubmitMq, String detail) {
		if (!isStreamTraceLogEnabled()) {
			return;
		}
		String line = buildStreamTraceLine(event, streamKey, groupName, consumerName, redisId, isSubmitMq, detail);
		if (msg != null) {
			logger.info(msg, line + " msg={}", msg);
		} else {
			logger.info("{} msg=null", line);
		}
	}

	private void logStreamTraceWithoutMessage(String event, String streamKey, String groupName, String consumerName,
			String redisId, boolean isSubmitMq, String detail) {
		if (!isStreamTraceLogEnabled()) {
			return;
		}
		logger.info("UNKNOWN {}", buildStreamTraceLine(event, streamKey, groupName, consumerName,
				redisId, isSubmitMq, detail));
	}

	private String buildStreamTraceLine(String event, String streamKey, String groupName, String consumerName,
			String redisId, boolean isSubmitMq, String detail) {
		StringBuilder sb = new StringBuilder(256);
		sb.append("[").append(event).append("]");
		sb.append(" flow=").append(streamFlow(streamKey));
		sb.append(" module=").append(System.getProperty("module", ""));
		sb.append(" nodeId=").append(System.getProperty("NodeID", "0"));
		sb.append(" stream=").append(streamKey);
		if (groupName != null) {
			sb.append(" group=").append(groupName);
		}
		if (consumerName != null) {
			sb.append(" consumer=").append(consumerName);
		}
		if (redisId != null) {
			sb.append(" redisId=").append(redisId);
		}
		sb.append(" isSubmitMq=").append(isSubmitMq);
		if (detail != null && detail.length() > 0) {
			sb.append(" ").append(detail);
		}
		return sb.toString();
	}

	private String streamFlow(String streamKey) {
		if (streamKey == null) {
			return "unknown";
		}
		if (streamKey.startsWith(STR_MT_PENDING_PREFIX)) {
			return "server->core";
		}
		if (streamKey.startsWith(STR_MT_ROUTED_PREFIX)) {
			return "core->client";
		}
		if (streamKey.startsWith(STR_DR_PENDING_PREFIX)) {
			return "core->server";
		}
		if (streamKey.startsWith(STR_INBOUND_DR_PREFIX)) {
			return "client->core";
		}
		if (STR_OUTBOUND_HTTP.equals(streamKey)) {
			return "core->http-client";
		}
		if (STR_CORE_RESULTS.equals(streamKey)) {
			return "result->core";
		}
		return "unknown";
	}

	private boolean isStreamTraceLogEnabled() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("RedisStreamTraceLogEnable", "true"));
	}

	private void incrementStreamCounter(String flow, String streamKey, GmmsMessage msg, String action) {
		MetricsCollector.getInstance().incrementCounter(
				MetricsNames.build("stream", flow, metricMessageType(streamKey, msg), action));
	}

	private String metricMessageType(String streamKey, GmmsMessage msg) {
		if (streamKey != null) {
			if (streamKey.startsWith(STR_MT_PENDING_PREFIX) || streamKey.startsWith(STR_MT_ROUTED_PREFIX)
					|| STR_OUTBOUND_HTTP.equals(streamKey)) {
				return "submit";
			}
			if (streamKey.startsWith(STR_DR_PENDING_PREFIX) || streamKey.startsWith(STR_INBOUND_DR_PREFIX)) {
				return MetricsNames.msgType(msg);
			}
		}
		return MetricsNames.msgType(msg);
	}

	private List<Map.Entry<String, List<StreamEntry>>> readGroup(String groupName, String consumerName, int count,
			Map<String, StreamEntryID> streamsMap, boolean isSubmitMq) {
		return isSubmitMq
			? redisClient.xreadGroupSubmitMq(groupName, consumerName, count, 0, streamsMap)
			: redisClient.xreadGroupReportMq(groupName, consumerName, count, 0, streamsMap);
	}

	private boolean recoverGroups(Map<String, StreamEntryID> streamsMap, String groupName, boolean isSubmitMq) {
		if (streamsMap == null || streamsMap.isEmpty()) {
			return false;
		}
		boolean recovered = false;
		for (String streamKey : streamsMap.keySet()) {
			logger.warn("Try to recover redis stream consumer group. streamKey={}, group={}, isSubmitMq={}",
					streamKey, groupName, isSubmitMq);
			createGroup(streamKey, groupName, isSubmitMq);
			triggerDoorbell(streamKey, isSubmitMq);
			recovered = true;
		}
		return recovered;
	}

	public boolean produceOutboundMessage(GmmsMessage msg) {
		String key;
		if ("HTTP".equalsIgnoreCase(msg.getProperty("Protocol") != null ? msg.getProperty("Protocol").toString() : "")) {
			key = STR_OUTBOUND_HTTP;
		} else {
			int shardId = calculateOutboundShardId(msg);
			key = STR_MT_ROUTED_PREFIX + msg.getRSsID() + ":" + shardId;
		}
		boolean needOutbox = key.startsWith(STR_MT_ROUTED_PREFIX);
		if (needOutbox && !CoreOutboundOutboxManager.getInstance().register(msg, key)) {
			return false;
		}
		boolean produced = produce(key, msg, true);
		if (!produced && needOutbox) {
			CoreOutboundOutboxManager.getInstance().remove(msg);
		}
		return produced;
	}

	private boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}


	public boolean produceResult(GmmsMessage msg) {
		return produce(STR_CORE_RESULTS, msg, false);
	}

	public long getAutoClaimIdleMs() {
		return longProperty("RedisStreamAutoClaimIdleMs", 60000L);
	}

	public int getAutoClaimBatchSize() {
		return intProperty("RedisStreamAutoClaimBatchSize", 100);
	}

	private boolean isGlobalPELMonitorEnabled() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("RedisStreamGlobalPELMonitorEnable", "false"));
	}

	private long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private int intProperty(String key, int defaultValue) {
		try {
			return Integer.parseInt(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
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
						Thread.sleep(30000);
						Set<String> allNodes = redisClient.getStateRedis().smembers("system:server:nodes");
						if (allNodes == null || allNodes.isEmpty()) continue;
						
						String myNodeId = GmmsUtility.getInstance().getNodeId();
						if (myNodeId == null || myNodeId.isEmpty()) myNodeId = System.getProperty("NodeID", "0");
						
						for (String otherNodeId : allNodes) {
							if (otherNodeId.equals(myNodeId)) continue;
							
							// Scan DR streams
							scanAndClaimStreams(STR_DR_PENDING_PREFIX + "*", "ServerDrToCustomerGroup_" + otherNodeId, "c_" + myNodeId, false);
							// Scan MT streams
							scanAndClaimStreams(STR_MT_PENDING_PREFIX + "*", "CoreMTGroup", "c_" + myNodeId, true);
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
		try {
			Set<String> streams = isSubmitMq 
				? redisClient.keysSubmitMq(pattern)
				: redisClient.keysReportMq(pattern);
				
			if (streams != null) {
				for (String streamKey : streams) {
					List<GmmsMessage> claimed = autoClaimBatch(streamKey, groupName, consumerName,
							getAutoClaimIdleMs(), getAutoClaimBatchSize(), isSubmitMq);
					if (claimed != null && !claimed.isEmpty()) {
						triggerDoorbell(streamKey, isSubmitMq);
					}
				}
			}
		} catch (Exception e) {
			logger.warn("scanAndClaimStreams error for pattern " + pattern, e);
		}
	}

	public List<GmmsMessage> autoClaimBatch(String streamKey, String groupName, String consumerName, 
			long minIdleMs, int count, boolean isSubmitMq) {
		
		Map.Entry<StreamEntryID, List<StreamEntry>> result = isSubmitMq
			? redisClient.xautoclaimSubmitMq(streamKey, groupName, consumerName, minIdleMs, 
					AUTOCLAIM_START_ID, count)
			: redisClient.xautoclaimReportMq(streamKey, groupName, consumerName, minIdleMs, 
					AUTOCLAIM_START_ID, count);

		List<GmmsMessage> messages = new ArrayList<>();
		if (result == null || result.getValue() == null) return messages;

		for (StreamEntry se : result.getValue()) {
			String body = se.getFields().get("body");
			GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(body);
			if (msg != null) {
				msg.setRedisStreamID(se.getID().toString());
				messages.add(msg);
				logStreamTrace("STREAM-CLAIM", streamKey, groupName, consumerName,
						se.getID().toString(), msg, isSubmitMq,
						"minIdleMs=" + minIdleMs + " result=claimed");
			} else {
				logStreamTraceWithoutMessage("STREAM-CLAIM", streamKey, groupName, consumerName,
						se.getID().toString(), isSubmitMq,
						"minIdleMs=" + minIdleMs + " result=fail reason=deserialize_failed body=" + body);
			}
		}
		return messages;
	}
}
