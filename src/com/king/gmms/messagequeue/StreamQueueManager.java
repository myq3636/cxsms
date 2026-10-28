package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.SmppServerDrRouteResolver;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingSummary;
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

	// 队列名称模板
	public static final String STR_MT_PENDING_PREFIX = "stream:mt:pending:";
	public static final String STR_MT_ROUTED_PREFIX = "stream:mt:routed:";
	public static final String STR_HTTP_SUBMIT_PREFIX = "stream:http:submit:";
	public static final String STR_HTTP_DR_PREFIX = "stream:http:dr:";
	public static final String STR_MQM_SUBMIT_PREFIX = "stream:mqm:submit:";
	public static final String STR_MQM_REPORT_PREFIX = "stream:mqm:report:";
	public static final String STR_DR_PENDING_PREFIX = "stream:dr:pending:";
	public static final String STR_INBOUND_DR_PREFIX = "stream:core:inbound-dr:";
	
	public static final String STR_OUTBOUND_HTTP = "stream:outbound:http:tasks";
	public static final String STR_CORE_RESULTS = "stream:core:results";
	public static final String STR_DLQ_SUBMIT = "stream:dlq:submit";
	public static final String STR_DLQ_REPORT = "stream:dlq:report";

	// 全局活跃门铃 ZSet 键名
	public static final String ZSET_MT_PENDING_ACTIVE = "zset:mt:pending:active";
	public static final String ZSET_MT_ROUTED_ACTIVE = "zset:mt:routed:active";
	public static final String ZSET_HTTP_SUBMIT_ACTIVE = "zset:http:submit:active";
	public static final String ZSET_HTTP_DR_ACTIVE = "zset:http:dr:active";
	public static final String ZSET_MQM_SUBMIT_ACTIVE = "zset:mqm:submit:active";
	public static final String ZSET_MQM_REPORT_ACTIVE = "zset:mqm:report:active";
	public static final String ZSET_DR_PENDING_ACTIVE = "zset:dr:pending:active";
	public static final String ZSET_INBOUND_DR_ACTIVE = "zset:core:inbound-dr:active";
	public static final String ZSET_CORE_RESULTS_ACTIVE = "zset:core:results:active";
	private static final StreamEntryID AUTOCLAIM_START_ID = new StreamEntryID("0-0");

	private StreamQueueManager() {
		this.redisClient = RedisClient.getInstance();
		if (isGlobalPELMonitorEnabled()) {
			startPELMonitor();
		}
		startBacklogMonitor();
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
		// Same shard as Math.abs(hash) % n for every hash, but never negative for Integer.MIN_VALUE
		return Math.abs(hashKey.hashCode() % totalShards);
	}

	public int calculateOutboundShardId(GmmsMessage msg) {
		int totalShards = GmmsUtility.getInstance().getTotalShards();
		if (totalShards <= 1) return 0;
		String hashKey = msg.getRSsID() + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		return (hashKey.hashCode() & Integer.MAX_VALUE) % totalShards;
	}

	public boolean produceSubmitRouted(GmmsMessage msg, String channelId) {
		String key = STR_MT_ROUTED_PREFIX + channelId;
		boolean produced = produce(key, msg, true);
		if (produced && !CoreOutboundOutboxManager.getInstance().register(msg, key, msg.getRedisStreamID())) {
			deleteStreamEntry(key, msg.getRedisStreamID(), true);
			return false;
		}
		return produced;
	}

	public boolean produceHttpSubmitMessage(GmmsMessage msg) {
		int targetSsid = msg.getRSsID();
		int shardId = calculateHttpShardId(msg, targetSsid);
		String key = STR_HTTP_SUBMIT_PREFIX + targetSsid + ":" + shardId;
		return produce(key, msg, true);
	}

	public boolean produceHttpDeliveryReport(GmmsMessage msg) {
		int targetSsid = msg.getOSsID() > 0 ? msg.getOSsID() : msg.getRSsID();
		int shardId = calculateHttpShardId(msg, targetSsid);
		String key = STR_HTTP_DR_PREFIX + targetSsid + ":" + shardId;
		return produce(key, msg, false);
	}

	private int calculateHttpShardId(GmmsMessage msg, int targetSsid) {
		int totalShards = GmmsUtility.getInstance().getTotalShards();
		if (totalShards <= 1) return 0;
		String hashKey = targetSsid + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		return (hashKey.hashCode() & Integer.MAX_VALUE) % totalShards;
	}

	public boolean produceMqmSubmitMessage(GmmsMessage msg) {
		int targetSsid = msg.getOSsID();
		int shardId = calculateMqmShardId(msg, targetSsid);
		String key = STR_MQM_SUBMIT_PREFIX + targetSsid + ":" + shardId;
		return produce(key, msg, true, mqmFenceFields());
	}

	public boolean produceMqmReportMessage(GmmsMessage msg) {
		int targetSsid = msg.getOSsID() > 0 ? msg.getOSsID() : msg.getRSsID();
		int shardId = calculateMqmShardId(msg, targetSsid);
		String key = STR_MQM_REPORT_PREFIX + targetSsid + ":" + shardId;
		return produce(key, msg, false, mqmFenceFields());
	}

	/** C-9: 解析 stream 条目的 fence 字段；缺失/非法返回 -1(消费方按"缺失=接受"容忍)。 */
	private static long parseStreamFence(String s) {
		if (s == null || s.length() == 0) {
			return -1L;
		}
		try {
			return Long.parseLong(s.trim());
		} catch (Exception e) {
			return -1L;
		}
	}

	/** C-9: 给 MQM 补偿消息盖上当前任期纪元(fence)；无有效任期则不盖(消费方按"缺失=接受"容忍)。 */
	private Map<String, String> mqmFenceFields() {
		long f = com.king.gmms.mqm.MqmActiveState.getFence();
		if (f < 0L) {
			return null;
		}
		Map<String, String> m = new HashMap<>(2);
		m.put("fence", String.valueOf(f));
		return m;
	}

	private int calculateMqmShardId(GmmsMessage msg, int targetSsid) {
		int totalShards = GmmsUtility.getInstance().getTotalShards();
		if (totalShards <= 1) return 0;
		String hashKey;
		if (msg.getSarMsgRefNum() != null && msg.getSarMsgRefNum().length() > 0) {
			hashKey = targetSsid + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress() + "_" + msg.getSarMsgRefNum();
		} else {
			hashKey = targetSsid + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		}
		return (hashKey.hashCode() & Integer.MAX_VALUE) % totalShards;
	}

	private int calculateDrShardId(GmmsMessage msg, int targetSsid) {
		int totalShards = GmmsUtility.getInstance().getTotalShards();
		if (totalShards <= 1 || msg == null) return 0;
		String hashKey;
		if (!isBlank(msg.getMsgID())) {
			hashKey = targetSsid + "_" + msg.getMsgID();
		} else if (!isBlank(msg.getInMsgID()) || !isBlank(msg.getOutMsgID())) {
			hashKey = targetSsid + "_" + msg.getInMsgID() + "_" + msg.getOutMsgID();
		} else {
			hashKey = targetSsid + "_" + msg.getSenderAddress() + "_" + msg.getRecipientAddress();
		}
		return (hashKey.hashCode() & Integer.MAX_VALUE) % totalShards;
	}

	public boolean produceDeliveryReport(GmmsMessage msg) {
		String key = getDrPendingQueue(msg);
		if (isBlank(key)) {
			logger.warn(msg, "Can not resolve SMPP server DR stream key.");
			return false;
		}
		return produce(key, msg, false);
	}

	public boolean produceDeliveryReportToQueue(GmmsMessage msg, String key) {
		if (isBlank(key)) {
			logger.warn(msg, "Can not resolve SMPP server DR stream key.");
			return false;
		}
		return produce(key, msg, false);
	}

	public boolean produceDeliveryReportToModule(GmmsMessage msg, String moduleName) {
		int targetSsid = msg == null ? -1 : msg.getOSsID();
		String key = STR_DR_PENDING_PREFIX + targetSsid;
		if (!isBlank(moduleName)) {
			key += ":" + moduleName;
			key += ":" + calculateDrShardId(msg, targetSsid);
		}
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
		return produce(key, msg, isSubmitMq, null);
	}

	/** 带附加 stream 字段的生产(如 C-9 fencing 的 fence 字段)；extraFields 可为 null。 */
	private boolean produce(String key, GmmsMessage msg, boolean isSubmitMq, Map<String, String> extraFields) {
		try {
			if (isBlank(key)) {
				logStreamTrace("STREAM-PUT", key, null, null, null, msg, isSubmitMq,
						"result=fail reason=blank_stream_key");
				incrementStreamCounter(MetricsNames.FLOW_OUT, key, msg, MetricsNames.ACTION_WRITE_FAIL);
				return false;
			}
			if (isBackPressureBlocked(key, isSubmitMq)) {
				logStreamTrace("STREAM-PUT", key, null, null, null, msg, isSubmitMq,
						"result=fail reason=backpressure_blocked");
				incrementStreamCounter(MetricsNames.FLOW_OUT, key, msg, MetricsNames.ACTION_WRITE_FAIL);
				return false;
			}
			Map<String, String> body = new HashMap<>();
			body.put("body", SerializableHandler.convertGmmsMessage2RedisMessage(msg));
			if (extraFields != null && !extraFields.isEmpty()) {
				body.putAll(extraFields); // C-9: 附加 fence 等 stream 字段
			}

			String zsetKey = getDoorbellZSet(key);
			String msgId;
			if (zsetKey != null) {
				// 阶段1: XADD+ZADD(doorbell) 合并为单段 Lua 原子写 —— 降一次 RTT，
				// 且根本不会产生"已写流未响铃"的孤儿(强化 C-4，无需写后回滚)。
				java.util.List<String> fv = new java.util.ArrayList<String>(body.size() * 2);
				for (Map.Entry<String, String> e : body.entrySet()) {
					fv.add(e.getKey());
					fv.add(e.getValue());
				}
				msgId = isSubmitMq
					? redisClient.xaddWithDoorbellSubmitMq(key, zsetKey, System.currentTimeMillis(), getStreamMaxLen(), isApproximateTrimming(), fv)
					: redisClient.xaddWithDoorbellReportMq(key, zsetKey, System.currentTimeMillis(), getStreamMaxLen(), isApproximateTrimming(), fv);
			} else {
				// 无对应 doorbell zset 的流(理论上不到此)：退回普通 XADD。
				msgId = isSubmitMq
					? redisClient.xaddToSubmitMq(key, body, getStreamMaxLen(), isApproximateTrimming())
					: redisClient.xaddToReportMq(key, body, getStreamMaxLen(), isApproximateTrimming());
			}

			if (msgId != null) {
				msg.setRedisStreamID(msgId);
				incrementStreamCounter(MetricsNames.FLOW_OUT, key, msg, MetricsNames.ACTION_WRITE_OK);
				logStreamTrace("STREAM-PUT", key, null, null, msgId, msg, isSubmitMq,
						"zset=" + zsetKey + " result=ok");
			} else {
				// Lua 原子写：失败即未写流也未响铃，无孤儿、无需回滚。
				logStreamTrace("STREAM-PUT", key, null, null, null, msg, isSubmitMq,
						"zset=" + zsetKey + " result=fail reason=atomic_write_failed");
				incrementStreamCounter(MetricsNames.FLOW_OUT, key, msg, MetricsNames.ACTION_WRITE_FAIL);
			}
			return msgId != null;
		} catch (Exception e) {
			logStreamTrace("STREAM-PUT", key, null, null, null, msg, isSubmitMq,
					"result=fail reason=exception:" + e.getClass().getSimpleName());
			logger.error("Produce to stream " + key + " failed", e);
			incrementStreamCounter(MetricsNames.FLOW_OUT, key, msg, MetricsNames.ACTION_WRITE_FAIL);
			return false;
		}
	}

	private void deleteStreamEntry(String key, String redisId, boolean isSubmitMq) {
		if (isBlank(key) || isBlank(redisId)) {
			return;
		}
		try {
			StreamEntryID id = new StreamEntryID(redisId);
			Long deleted = isSubmitMq ? redisClient.xdelSubmitMq(key, id) : redisClient.xdelReportMq(key, id);
			logger.warn("Deleted stream entry after outbox register failure. streamKey={}, redisId={}, deleted={}",
					key, redisId, deleted);
		} catch (Exception e) {
			logger.warn("Failed to delete stream entry after outbox register failure. streamKey={}, redisId={}",
					key, redisId, e);
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
		} else if (streamKey.startsWith(STR_HTTP_SUBMIT_PREFIX)) {
			return ZSET_HTTP_SUBMIT_ACTIVE;
		} else if (streamKey.startsWith(STR_HTTP_DR_PREFIX)) {
			return ZSET_HTTP_DR_ACTIVE;
		} else if (streamKey.startsWith(STR_MQM_SUBMIT_PREFIX)) {
			return ZSET_MQM_SUBMIT_ACTIVE;
		} else if (streamKey.startsWith(STR_MQM_REPORT_PREFIX)) {
			return ZSET_MQM_REPORT_ACTIVE;
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
		} else if (streamKey.startsWith(STR_HTTP_SUBMIT_PREFIX)
				|| streamKey.startsWith(STR_HTTP_DR_PREFIX)
				|| streamKey.startsWith(STR_MQM_SUBMIT_PREFIX)
				|| streamKey.startsWith(STR_MQM_REPORT_PREFIX)) {
			String[] parts = streamKey.split(":");
			if (parts.length >= 5) {
				try {
					int shardId = Integer.parseInt(parts[4]);
					return GmmsUtility.getInstance().getMyShards().contains(shardId);
				} catch (Exception e) {
					logger.warn("Failed to parse stream shardId from streamKey: " + streamKey);
				}
			}
		} else if (streamKey.startsWith(STR_DR_PENDING_PREFIX)) {
			String[] parts = streamKey.split(":");
			if (parts.length >= 5) {
				String moduleName = System.getProperty("module", "");
				boolean moduleMatched = parts[4] == null || parts[4].length() == 0
						|| parts[4].equalsIgnoreCase(moduleName);
				if (!moduleMatched) {
					return false;
				}
				// The stream already names the owning server module, so the shard suffix only
				// spreads one customer's DRs over parallel workers. Filter by MyShards only when
				// several nodes share one module name and split the shards between them.
				if (parts.length >= 6 && isServerDrShardFilterEnabled()) {
					try {
						int shardId = Integer.parseInt(parts[5]);
						return GmmsUtility.getInstance().getMyShards().contains(shardId);
					} catch (Exception e) {
						logger.warn("Failed to parse DR shardId from streamKey: " + streamKey);
						return false;
					}
				}
				return true;
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
		if (msg == null) {
			return null;
		}
		String key = STR_DR_PENDING_PREFIX + msg.getOSsID();
		String moduleName = SmppServerDrRouteResolver.getInstance().resolveTargetModule(msg);
		if (isBlank(moduleName)) {
			return null;
		}
		return key + ":" + moduleName + ":" + calculateDrShardId(msg, msg.getOSsID());
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
			key += ":" + calculateDrShardId(msg, targetSsid);
		}
		return key;
	}

	public String getInboundDrQueue(GmmsMessage msg) {
		int targetSsid = msg.getRSsID() >= 0 ? msg.getRSsID() : msg.getOSsID();
		return STR_INBOUND_DR_PREFIX + targetSsid;
	}

	public void createGroup(String key, String groupName, boolean isSubmitMq) {
		if (isSubmitMq) {
			redisClient.xgroupCreateSubmitMq(key, groupName, true);
		} else {
			redisClient.xgroupCreateReportMq(key, groupName, true);
		}
	}

	public void ack(String key, String groupName, GmmsMessage msg, boolean isSubmitMq) {
		ackById(key, groupName, msg == null ? null : msg.getRedisStreamID(), msg, isSubmitMq);
	}

	public boolean ackById(String key, String groupName, String redisStreamId, GmmsMessage msg, boolean isSubmitMq) {
		String rid = redisStreamId;
		if (!isBlank(rid)) {
			StreamEntryID id = new StreamEntryID(rid);
			// 原子 XACK+XDEL：确认成功后在同一 Lua 中立即删除消息体，
			// 避免两步之间崩溃导致 PEL 已确认但消息体残留，或与 auto-claim 竞争重复处理。
			Long acked;
			if (isSubmitMq) {
				acked = redisClient.xackDelSubmitMq(key, groupName, id);
			} else {
				acked = redisClient.xackDelReportMq(key, groupName, id);
			}
			if (acked != null && acked.longValue() > 0) {
				incrementStreamCounter(MetricsNames.FLOW_IN, key, msg, MetricsNames.ACTION_ACK);
				logStreamTrace("STREAM-ACK", key, groupName, null, rid, msg, isSubmitMq,
						"acked=" + acked + " deleted=" + acked + " result=ok");
			} else {
				incrementStreamCounter(MetricsNames.FLOW_IN, key, msg, MetricsNames.ACTION_ACK_FAIL);
				logStreamTrace("STREAM-ACK", key, groupName, null, rid, msg, isSubmitMq,
						"acked=" + acked + " result=fail reason=ack_returned_zero");
			}
			return acked != null && acked.longValue() > 0;
		}
		incrementStreamCounter(MetricsNames.FLOW_IN, key, msg, MetricsNames.ACTION_ACK_FAIL);
		logStreamTrace("STREAM-ACK", key, groupName, null, rid, msg, isSubmitMq,
				"acked=0 result=fail reason=blank_redis_id");
		return false;
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
					msg.setRedisFence(parseStreamFence(se.getFields().get("fence"))); // C-9
					msg.setRedisStreamMessageType(msg.getMessageType());
					messages.add(msg);
					incrementStreamCounter(MetricsNames.FLOW_IN, entry.getKey(), msg, MetricsNames.ACTION_READ);
					logStreamTrace("STREAM-GET", entry.getKey(), groupName, consumerName,
							se.getID().toString(), msg, isSubmitMq, "result=ok");
				} else {
					deadLetterDeserializeFailure("STREAM-GET", entry.getKey(), groupName, consumerName,
							se.getID().toString(), se.getFields(), isSubmitMq, null);
				}
			}
		}
		return messages;
	}

	private void deadLetterDeserializeFailure(String event, String sourceStreamKey, String groupName,
			String consumerName, String redisId, Map<String, String> fields, boolean isSubmitMq, String detail) {
		String body = fields == null ? null : fields.get("body");
		String dlqKey = isSubmitMq ? STR_DLQ_SUBMIT : STR_DLQ_REPORT;
		Map<String, String> dlq = new HashMap<String, String>();
		dlq.put("reason", "deserialize_failed");
		dlq.put("event", safeText(event));
		dlq.put("sourceStream", safeText(sourceStreamKey));
		dlq.put("sourceGroup", safeText(groupName));
		dlq.put("sourceConsumer", safeText(consumerName));
		dlq.put("sourceRedisId", safeText(redisId));
		dlq.put("body", safeText(body));
		dlq.put("createdAt", String.valueOf(System.currentTimeMillis()));
		if (fields != null) {
			copyIfPresent(dlq, fields, "fence");
			copyIfPresent(dlq, fields, "type");
		}
		if (!isBlank(detail)) {
			dlq.put("detail", detail);
		}

		try {
			String dlqId = isSubmitMq
					? redisClient.xaddToSubmitMq(dlqKey, dlq, getStreamMaxLen(), isApproximateTrimming())
					: redisClient.xaddToReportMq(dlqKey, dlq, getStreamMaxLen(), isApproximateTrimming());
			if (isBlank(dlqId)) {
				logStreamTraceWithoutMessage(event, sourceStreamKey, groupName, consumerName, redisId, isSubmitMq,
						deadLetterLogDetail(detail, "result=fail reason=deserialize_failed dlq_write=null bodyLength="
								+ bodyLength(body)));
				return;
			}
			boolean acked = ackById(sourceStreamKey, groupName, redisId, null, isSubmitMq);
			logStreamTraceWithoutMessage(event, sourceStreamKey, groupName, consumerName, redisId, isSubmitMq,
					deadLetterLogDetail(detail, "result=dead_lettered reason=deserialize_failed dlq="
							+ dlqKey + " dlqId=" + dlqId + " acked=" + acked + " bodyLength=" + bodyLength(body)));
		} catch (Exception e) {
			logger.error("Failed to dead-letter deserialize failure. event={}, stream={}, group={}, consumer={}, redisId={}, dlq={}, isSubmitMq={}",
					event, sourceStreamKey, groupName, consumerName, redisId, dlqKey, isSubmitMq, e);
			logStreamTraceWithoutMessage(event, sourceStreamKey, groupName, consumerName, redisId, isSubmitMq,
					deadLetterLogDetail(detail, "result=fail reason=deserialize_failed dlq_write=exception bodyLength="
							+ bodyLength(body)));
		}
	}

	private void copyIfPresent(Map<String, String> target, Map<String, String> source, String key) {
		String value = source.get(key);
		if (value != null) {
			target.put(key, value);
		}
	}

	private String deadLetterLogDetail(String prefix, String suffix) {
		return isBlank(prefix) ? suffix : prefix + " " + suffix;
	}

	private String safeText(String value) {
		return value == null ? "" : value;
	}

	private int bodyLength(String body) {
		return body == null ? 0 : body.length();
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
			logger.info(msg, line + " msg={}", shouldPrintFullMessage(event) ? msg : shortMessage(msg));
		} else {
			logger.info("{} msg=null", line);
		}
	}

	private boolean shouldPrintFullMessage(String event) {
		return "STREAM-GET".equals(event);
	}

	private String shortMessage(GmmsMessage msg) {
		if (msg == null) {
			return "null";
		}
		StringBuilder sb = new StringBuilder(256);
		sb.append("{msgId=").append(msg.getMsgID());
		sb.append(",in=").append(msg.getInMsgID());
		sb.append(",out=").append(msg.getOutMsgID());
		sb.append(",type=").append(msg.getMessageType());
		sb.append(",status=").append(msg.getStatusCode()).append("/").append(msg.getStatusText());
		sb.append(",os=").append(msg.getOSsID());
		sb.append(",rs=").append(msg.getRSsID());
		sb.append(",oa=").append(msg.getSenderAddress());
		sb.append(",ra=").append(msg.getRecipientAddress());
		sb.append(",split=").append(msg.getSplitStatus());
		sb.append(",udh=").append(formatUdh(msg.getUdh()));
		sb.append(",sar=").append(msg.getSarMsgRefNum());
		sb.append(",outTrans=").append(msg.getOutTransID());
		sb.append("}");
		return sb.toString();
	}

	private String formatUdh(byte[] udh) {
		if (udh == null || udh.length == 0) {
			return "";
		}
		StringBuilder sb = new StringBuilder(udh.length * 2);
		for (byte value : udh) {
			String hex = Integer.toHexString(value & 0xff).toUpperCase();
			if (hex.length() < 2) {
				sb.append('0');
			}
			sb.append(hex);
		}
		return sb.toString();
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
		if (streamKey.startsWith(STR_HTTP_SUBMIT_PREFIX)) {
			return "core->http-client-submit";
		}
		if (streamKey.startsWith(STR_HTTP_DR_PREFIX)) {
			return "core->http-client-dr";
		}
		if (streamKey.startsWith(STR_MQM_SUBMIT_PREFIX)) {
			return "mqm->core-submit";
		}
		if (streamKey.startsWith(STR_MQM_REPORT_PREFIX)) {
			return "mqm->core-report";
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
				MetricsNames.build(MetricsNames.COMPONENT_STREAM, flow, metricMessageType(flow, streamKey, msg), action));
	}

	private String metricMessageType(String flow, String streamKey, GmmsMessage msg) {
		if (MetricsNames.FLOW_OUT.equals(flow)) {
			return MetricsNames.msgType(msg);
		}
		if (streamKey != null) {
			if (streamKey.startsWith(STR_MT_PENDING_PREFIX) || streamKey.startsWith(STR_MT_ROUTED_PREFIX)
					|| streamKey.startsWith(STR_HTTP_SUBMIT_PREFIX) || streamKey.startsWith(STR_MQM_SUBMIT_PREFIX)
					|| STR_OUTBOUND_HTTP.equals(streamKey)) {
				return "submit";
			}
			if (STR_CORE_RESULTS.equals(streamKey) && msg != null && msg.getRedisStreamMessageType() != null) {
				return MetricsNames.msgType(msg.getRedisStreamMessageType());
			}
			if (streamKey.startsWith(STR_DR_PENDING_PREFIX) || streamKey.startsWith(STR_INBOUND_DR_PREFIX)
					|| streamKey.startsWith(STR_HTTP_DR_PREFIX) || streamKey.startsWith(STR_MQM_REPORT_PREFIX)) {
				if (msg != null && msg.getRedisStreamMessageType() != null) {
					return MetricsNames.msgType(msg.getRedisStreamMessageType());
				}
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
			if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
				return produceHttpDeliveryReport(msg);
			}
			return produceHttpSubmitMessage(msg);
		} else {
			int shardId = calculateOutboundShardId(msg);
			key = STR_MT_ROUTED_PREFIX + msg.getRSsID() + ":" + shardId;
		}
		boolean needOutbox = key.startsWith(STR_MT_ROUTED_PREFIX);
		boolean produced = produce(key, msg, true);
		if (produced && needOutbox
				&& !CoreOutboundOutboxManager.getInstance().register(msg, key, msg.getRedisStreamID())) {
			deleteStreamEntry(key, msg.getRedisStreamID(), true);
			return false;
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

	public boolean isAutoClaimEnabled() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("RedisStreamAutoClaimEnable", "true"));
	}

	public long getAutoClaimIntervalMs() {
		long interval = longProperty("RedisStreamAutoClaimIntervalMs", 300000L);
		return interval <= 0 ? 300000L : interval;
	}

	public int getAutoClaimMaxMessagesPerRun() {
		int max = intProperty("RedisStreamAutoClaimMaxMessagesPerRun", 1000);
		return max <= 0 ? 1000 : max;
	}

	public List<String> listResponsibleStreams(String pattern, boolean isSubmitMq) {
		List<String> result = new ArrayList<String>();
		Set<String> streams = isSubmitMq ? redisClient.keysSubmitMq(pattern) : redisClient.keysReportMq(pattern);
		if (streams == null || streams.isEmpty()) {
			return result;
		}
		for (String streamKey : streams) {
			if (isResponsibleForStream(streamKey)) {
				result.add(streamKey);
			}
		}
		return result;
	}

	private long getStreamMaxLen() {
		long maxLen = longProperty("RedisStreamMaxLen", 1000000L);
		return maxLen <= 0 ? 1000000L : maxLen;
	}

	private boolean isApproximateTrimming() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("RedisStreamTrimApproximate", "true"));
	}

	public boolean isServerDrShardFilterEnabled() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("ServerDrStreamShardFilter", "false").trim());
	}

	private boolean isBackPressureEnabled() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("RedisStreamBackPressureEnable", "false"));
	}

	private long getBackPressureBlockThreshold() {
		return longProperty("RedisStreamMonitor.BlockXLEN", 500000L);
	}

	private boolean isBackPressureBlocked(String streamKey, boolean isSubmitMq) {
		if (!isBackPressureEnabled()) {
			return false;
		}
		long blockThreshold = getBackPressureBlockThreshold();
		if (blockThreshold <= 0) {
			return false;
		}
		long len = streamLength(streamKey, isSubmitMq);
		if (len < blockThreshold) {
			return false;
		}
		logger.warn("[STREAM-BACKPRESSURE] module={} stream={} xlen={} blockThreshold={} isSubmitMq={} action=block",
				System.getProperty("module", ""), streamKey, len, blockThreshold, isSubmitMq);
		return true;
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
	private Thread backlogMonitorThread;
	private volatile boolean backlogMonitorRunning = false;

	private synchronized void startBacklogMonitor() {
		if (backlogMonitorRunning) {
			return;
		}
		backlogMonitorRunning = true;
		backlogMonitorThread = new Thread(new Runnable() {
			@Override
			public void run() {
				logger.info("RedisStreamBacklogMonitor started");
				while (backlogMonitorRunning) {
					try {
						if (isBacklogMonitorEnabled()) {
							scanBacklogByModuleRole();
						}
						Thread.sleep(getBacklogMonitorIntervalSeconds() * 1000L);
					} catch (InterruptedException e) {
						break;
					} catch (Exception e) {
						logger.error("RedisStreamBacklogMonitor error", e);
						try {
							Thread.sleep(30000L);
						} catch (InterruptedException ignored) {
							break;
						}
					}
				}
			}
		}, "RedisStreamBacklogMonitor");
		backlogMonitorThread.setDaemon(true);
		backlogMonitorThread.start();
	}

	private boolean isBacklogMonitorEnabled() {
		return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("RedisStreamMonitor.Enable", "false"));
	}

	private int getBacklogMonitorIntervalSeconds() {
		int interval = intProperty("RedisStreamMonitor.IntervalSeconds", 30);
		return interval <= 0 ? 30 : interval;
	}

	private void scanBacklogByModuleRole() {
		String module = System.getProperty("module", "");
		String lowerModule = module == null ? "" : module.toLowerCase();
		String nodeId = System.getProperty("NodeID", "0");
		if (lowerModule.contains("core")) {
			scanBacklogPattern(STR_MT_PENDING_PREFIX + "*", "CoreMTGroup", ZSET_MT_PENDING_ACTIVE, true, "server_to_core");
			scanBacklogPattern(STR_INBOUND_DR_PREFIX + "*", "CoreInboundDRGroup", ZSET_INBOUND_DR_ACTIVE, false, "client_to_core");
			scanBacklogPattern(STR_CORE_RESULTS, "CoreResultGroup", ZSET_CORE_RESULTS_ACTIVE, false, "result_to_core");
			scanBacklogPattern(STR_MQM_SUBMIT_PREFIX + "*", "CoreMQMSubmitGroup", ZSET_MQM_SUBMIT_ACTIVE, true, "mqm_to_core_submit");
			scanBacklogPattern(STR_MQM_REPORT_PREFIX + "*", "CoreMQMReportGroup", ZSET_MQM_REPORT_ACTIVE, false, "mqm_to_core_report");
		} else if (lowerModule.contains("commonhttpclient")) {
			scanBacklogPattern(STR_HTTP_SUBMIT_PREFIX + "*", "HttpClientSubmitGroup", ZSET_HTTP_SUBMIT_ACTIVE, true, "core_to_http_submit");
			scanBacklogPattern(STR_HTTP_DR_PREFIX + "*", "HttpClientDRGroup", ZSET_HTTP_DR_ACTIVE, false, "core_to_http_dr");
		} else if (lowerModule.contains("client")) {
			scanBacklogPattern(STR_MT_ROUTED_PREFIX + "*", "ClientOutboundSubmitGroup", ZSET_MT_ROUTED_ACTIVE, true, "core_to_client");
		} else if (lowerModule.contains("server")) {
			scanBacklogPattern(STR_DR_PENDING_PREFIX + "*", "ServerDrToCustomerGroup_" + nodeId,
					ZSET_DR_PENDING_ACTIVE, false, "core_to_server");
		}
	}

	private void scanBacklogPattern(String pattern, String groupName, String doorbellKey,
			boolean isSubmitMq, String flow) {
		Set<String> streams = isSubmitMq ? redisClient.keysSubmitMq(pattern) : redisClient.keysReportMq(pattern);
		long totalXlen = 0L;
		long totalPending = 0L;
		long doorbellMissing = 0L;
		if (streams != null) {
			for (String streamKey : streams) {
				if (!isResponsibleForStream(streamKey)) {
					continue;
				}
				long xlen = streamLength(streamKey, isSubmitMq);
				if (xlen <= 0) {
					continue;
				}
				totalXlen += xlen;
				createGroup(streamKey, groupName, isSubmitMq);
				long pending = pendingCount(streamKey, groupName, isSubmitMq);
				if (pending > 0) {
					totalPending += pending;
				}
				boolean missingDoorbell = !hasDoorbell(doorbellKey, streamKey, isSubmitMq);
				if (missingDoorbell) {
					doorbellMissing++;
				}
				logBacklogIfNeeded(flow, streamKey, groupName, xlen, pending, missingDoorbell, isSubmitMq);
			}
		}
		MetricsCollector.getInstance().setGauge(MetricsNames.gauge("redis.stream." + flow, "xlen"), totalXlen);
		MetricsCollector.getInstance().setGauge(MetricsNames.gauge("redis.stream." + flow, "pending"), totalPending);
		MetricsCollector.getInstance().setGauge(MetricsNames.gauge("redis.stream." + flow, "doorbell_missing"), doorbellMissing);
	}

	private long pendingCount(String streamKey, String groupName, boolean isSubmitMq) {
		List<StreamPendingSummary> summaries = isSubmitMq
				? redisClient.xpendingSubmitMq(streamKey, groupName)
				: redisClient.xpendingReportMq(streamKey, groupName);
		if (summaries == null || summaries.isEmpty() || summaries.get(0) == null) {
			return 0L;
		}
		return summaries.get(0).getTotal();
	}

	private boolean hasDoorbell(String doorbellKey, String streamKey, boolean isSubmitMq) {
		Double score = isSubmitMq
				? redisClient.zscoreSubmitMq(doorbellKey, streamKey)
				: redisClient.zscoreReportMq(doorbellKey, streamKey);
		return score != null;
	}

	private void logBacklogIfNeeded(String flow, String streamKey, String groupName, long xlen,
			long pending, boolean missingDoorbell, boolean isSubmitMq) {
		long warnXlen = longProperty("RedisStreamMonitor.WarnXLEN", 100000L);
		long warnPending = longProperty("RedisStreamMonitor.WarnPending", 10000L);
		boolean warn = xlen >= warnXlen || pending >= warnPending || missingDoorbell;
		if (!warn) {
			return;
		}
		logger.warn("[STREAM-BACKLOG] module={} flow={} stream={} group={} xlen={} pending={} doorbellMissing={} "
				+ "warnXlen={} warnPending={} isSubmitMq={}",
				System.getProperty("module", ""), flow, streamKey, groupName, xlen, pending, missingDoorbell,
				warnXlen, warnPending, isSubmitMq);
	}

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
					// H-8: PEL 接管也必须遵守分片责任，只接管本节点负责 shard 内其他(死亡)消费者的 pending，
					// 否则越权拉取非属主分片消息会破坏 SAR 多段/CSM 的分片有序性。与 scanBacklogPattern 一致。
					if (!isResponsibleForStream(streamKey)) {
						continue;
					}
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
		if (count <= 0) {
			return new ArrayList<GmmsMessage>();
		}
		
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
				msg.setRedisFence(parseStreamFence(se.getFields().get("fence"))); // C-9
				msg.setRedisStreamMessageType(msg.getMessageType());
				messages.add(msg);
				logStreamTrace("STREAM-CLAIM", streamKey, groupName, consumerName,
						se.getID().toString(), msg, isSubmitMq,
						"minIdleMs=" + minIdleMs + " result=claimed");
			} else {
				deadLetterDeserializeFailure("STREAM-CLAIM", streamKey, groupName, consumerName,
						se.getID().toString(), se.getFields(), isSubmitMq, "minIdleMs=" + minIdleMs);
			}
		}
		return messages;
	}
}
