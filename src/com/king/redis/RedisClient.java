package com.king.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.JedisPoolConfig;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

/**
 * Redis operate class.
 * V4.0 微服务化重构版：支持 State、Submit-MQ、Report-MQ 三通道物理隔离
 */
public class RedisClient {
	private static SystemLogger log = SystemLogger.getSystemLogger(RedisClient.class);

	// 【核心改造1：三通道连接对象拆分】
	private static RedisConnection stateRedis = null; // 原 redis 对象 (中枢、计费、频控)
	private static RedisConnection submitMqRedis = null; // 下行消息总线 MQ (Server -> Core -> Client)
	private static RedisConnection reportMqRedis = null; // 上行及状态报告 MQ (Client -> Server)

	private static RedisClient init = new RedisClient();
	private boolean redisHaFlag = true;
	protected JedisPoolConfig config = new JedisPoolConfig();

	// State-Redis 配置 (原主备)
	protected String host = "192.168.23.112";
	protected String host1 = "192.168.23.112";
	protected int port = 6379;
	protected int port1 = 6378;

	// Submit-MQ 配置 (下行)
	protected String submitHost = "";
	protected String submitHost1 = "";
	protected int submitPort = 6379;
	protected int submitPort1 = 6378;

	// Report-MQ 配置 (上行及DR)
	protected String reportHost = "";
	protected String reportHost1 = "";
	protected int reportPort = 6379;
	protected int reportPort1 = 6378;

	protected int maxActive = 50;
	protected int maxIdle = 50;
	protected int minIdle = 30;
	protected int maxWait = 20000;
	protected int minEvictableIdleTime = 180000;
	protected int timeBetweenEvictionRuns = -1;
	protected boolean isAuth = false;
	protected String password = "";

	private RedisClient() {
		try {
			GmmsUtility gmmsUtility = GmmsUtility.getInstance();

			// 1. 读取原有的 State Redis 配置
			host = gmmsUtility.getCommonProperty("Redis_master_host");
			host1 = gmmsUtility.getCommonProperty("Redis_slave_host");
			port = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_master_port", "6379"));
			port1 = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_slave_port", "6379"));

			// 【核心改造2：读取 MQ Redis 配置，若未配置则默认复用 State Redis，实现平滑兼容】
			submitHost = gmmsUtility.getCommonProperty("Redis_submit_master_host", host);
			submitHost1 = gmmsUtility.getCommonProperty("Redis_submit_slave_host", host1);
			submitPort = Integer
					.parseInt(gmmsUtility.getCommonProperty("Redis_submit_master_port", String.valueOf(port)));
			submitPort1 = Integer
					.parseInt(gmmsUtility.getCommonProperty("Redis_submit_slave_port", String.valueOf(port1)));

			reportHost = gmmsUtility.getCommonProperty("Redis_report_master_host", host);
			reportHost1 = gmmsUtility.getCommonProperty("Redis_report_slave_host", host1);
			reportPort = Integer
					.parseInt(gmmsUtility.getCommonProperty("Redis_report_master_port", String.valueOf(port)));
			reportPort1 = Integer
					.parseInt(gmmsUtility.getCommonProperty("Redis_report_slave_port", String.valueOf(port1)));

			// 读取连接池参数
			try {
				maxActive = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_maxActive", "50"));
			} catch (Exception e) {
				maxActive = 50;
			}
			try {
				maxIdle = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_maxIdle", "50"));
			} catch (Exception e) {
				maxIdle = 50;
			}
			try {
				minIdle = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_minIdle", "30"));
			} catch (Exception e) {
				minIdle = 30;
			}
			try {
				maxWait = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_maxWait", "20000"));
			} catch (Exception e) {
				maxWait = 20000;
			}
			try {
				minEvictableIdleTime = Integer
						.parseInt(gmmsUtility.getCommonProperty("Redis_pool_minEvictableIdleTime", "180000"));
			} catch (Exception e) {
				minEvictableIdleTime = 180000;
			}
			try {
				timeBetweenEvictionRuns = Integer
						.parseInt(gmmsUtility.getCommonProperty("Redis_pool_timeBetweenEvictionRuns", "-1"));
			} catch (Exception e) {
				timeBetweenEvictionRuns = -1;
			}

			if (config == null) {
				config = new JedisPoolConfig();
			}
			isAuth = Boolean.parseBoolean(gmmsUtility.getCommonProperty("Redis_isAuth", "false"));
			password = gmmsUtility.getCommonProperty("Redis_password", null);

			config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
			config.setJmxEnabled(true);
			config.setMaxTotal(maxActive);
			config.setMaxIdle(maxIdle);
			config.setMinIdle(minIdle);
			config.setMinEvictableIdleTimeMillis(minEvictableIdleTime);
			config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRuns);
			config.setTestOnReturn(false);
			config.setTestOnBorrow(false);
			config.setTestWhileIdle(true);

		} catch (Exception e) {
			log.error("Redis config init error!", e);
		}
	}

	public static RedisClient getInstance() {
		return init;
	}

	// =========================================================================
	// V4.0 新增获取 MQ Connection 的方法，供 StreamQueueManager 调用
	// =========================================================================
	public RedisConnection getStateRedis() {
		return stateRedis;
	}

	public RedisConnection getSubmitMqRedis() {
		return submitMqRedis;
	}

	public RedisConnection getReportMqRedis() {
		return reportMqRedis;
	}

	public redis.clients.jedis.Jedis getJedis(ClusterType type) {
		switch (type) {
			case SUBMIT_MQ:
				return submitMqRedis != null ? submitMqRedis.getJedis() : null;
			case REPORT_MQ:
				return reportMqRedis != null ? reportMqRedis.getJedis() : null;
			case STATE:
			default:
				return stateRedis != null ? stateRedis.getJedis() : null;
		}
	}

	// =========================================================================
	// 原有业务方法映射区 (100% 向下兼容，全部默认走 stateRedis)
	// =========================================================================

	public boolean setString(String key, String obj) {
		return stateRedis.setString(key, obj);
	}

	public Long incBlackhole(String key) {
		return stateRedis.incblackhole(key);
	}

	public Long incrString(String key) {
		return stateRedis.incrString(key);
	}

	public void setExpire(String key, int time) {
		stateRedis.setExpired(key, time);
	}

	public boolean setString(String key, String obj, int time) {
		return stateRedis.setString(key, obj, time);
	}

	public String setnxString(String key, String value, int expireSeconds) {
		return stateRedis.setnxString(key, value, expireSeconds);
	}

	public boolean lpush(String key, String obj) {
		return stateRedis.lpush(key, obj);
	}

	private static final java.util.concurrent.BlockingQueue<RedisConnection.RedisTask> asyncRedisQueue = new java.util.concurrent.LinkedBlockingQueue<RedisConnection.RedisTask>(
			100000);

	static {
		Thread asyncThread = new Thread(new Runnable() {
			public void run() {
				java.util.List<RedisConnection.RedisTask> batch = new java.util.ArrayList<RedisConnection.RedisTask>();
				long lastExecuteTime = System.currentTimeMillis();
				while (true) {
					try {
						RedisConnection.RedisTask task = asyncRedisQueue.poll(10,
								java.util.concurrent.TimeUnit.MILLISECONDS);
						if (task != null) {
							batch.add(task);
						}
						long now = System.currentTimeMillis();
						if (batch.size() >= 500 || (batch.size() > 0 && (now - lastExecuteTime) >= 200)) {
							// 【核心改造3：异步线程默认使用 stateRedis 进行高频缓存更新】
							RedisConnection rc = stateRedis;
							if (rc != null) {
								rc.executeAsyncPipeline(batch);
							}
							batch.clear();
							lastExecuteTime = now;
						}
					} catch (Exception e) {
						log.error("AsyncRedisWriterThread error", e);
					}
				}
			}
		}, "AsyncRedisWriterThread");
		asyncThread.setDaemon(true);
		asyncThread.start();
	}

	public void asyncLpush(String key, String value) {
		if (!asyncRedisQueue.offer(new RedisConnection.RedisTask(RedisConnection.RedisTask.Type.LPUSH, key, value))) {
			lpush(key, value);
		}
	}

	public void asyncHashInc(String key, String field) {
		if (!asyncRedisQueue.offer(new RedisConnection.RedisTask(RedisConnection.RedisTask.Type.HINCRBY, key, field))) {
			hashINC(key, field);
		}
	}

	public boolean setPipeline(String key, String value, int expireTime, String hashKey) {
		return stateRedis.setPipeline(key, value, expireTime, hashKey);
	}

	public boolean setDelayDR(String key, String value, int expireTime, double score, int ossid) {
		return stateRedis.setDelayDR(key, value, expireTime, score, ossid);
	}

	public Set<String> zrange(String key, double start, double end) {
		return stateRedis.zrange(key, start, end);
	}

	public Set<String> zrangeByIndex(String key, long start, long end) {
		return stateRedis.zrangeByIndex(key, start, end);
	}

	public boolean zrem(String key, String[] members) {
		return stateRedis.zrem(key, members);
	}

	public boolean setRoutingInfo(String keyword, Map<String, Map<String, String>> infos) {
		return stateRedis.setHashMapWithPipline(keyword, infos);
	}

	public boolean setContentTemplate(String keyword, Map<String, Map<String, String>> infos) {
		return stateRedis.setHashMapWithPipline(keyword, infos);
	}

	public void delPipeline(String key, String hashKey) {
		stateRedis.delPipeline(key, hashKey);
	}

	public String getString(String key) {
		return stateRedis.getString(key);
	}

	public boolean hashINC(String key, String field) {
		return stateRedis.hashINC(key, field);
	}

	public String rpop(String key) {
		return stateRedis.rpop(key);
	}

	public List<String> rpopBatch(String key, int count) {
		return stateRedis.rpopBatch(key, count);
	}

	public String brpop(String key) {
		return stateRedis.brpop(key);
	}

	public boolean lpush(String setKey, String key, String[] values) {
		return stateRedis.lpushArr(setKey, key, values);
	}

	public boolean setHash(String key, Map<String, String> map) {
		return stateRedis.setHash(key, map);
	}

	public boolean setHashAndSet(String setKey, String key, Map<String, String> map) {
		return stateRedis.setHashAndSet(setKey, key, map);
	}

	public Set<String> getHash(String key) {
		Set<String> set = stateRedis.getHashValue(key);
		stateRedis.delObject(key);
		return set;
	}

	public Map<String, String> getHashAll(String key) {
		return stateRedis.getHashAll(key);
	}

	public List<String> getHashMap(String key, String[] s) throws Exception {
		return stateRedis.getHashMap(key, s);
	}

	public void del(String key) {
		stateRedis.delObject(key);
	}

	public void delHash(String key, String field) {
		stateRedis.delHash(key, field);
	}

	public boolean isRedisHaFlag() {
		return redisHaFlag;
	}

	// 【核心改造4：主备切换时，同时管理三个连接对象的生命周期】
	public void setRedisHaFlag(boolean redisHaFlag) {
		this.redisHaFlag = redisHaFlag;
		RedisConnection oldStateRedis = stateRedis;
		RedisConnection oldSubmitMqRedis = submitMqRedis;
		RedisConnection oldReportMqRedis = reportMqRedis;

		if (redisHaFlag) {
			stateRedis = new RedisConnection(config, host, port, isAuth, password);
			submitMqRedis = new RedisConnection(config, submitHost, submitPort, isAuth, password);
			reportMqRedis = new RedisConnection(config, reportHost, reportPort, isAuth, password);
		} else {
			stateRedis = new RedisConnection(config, host1, port1, isAuth, password);
			submitMqRedis = new RedisConnection(config, submitHost1, submitPort1, isAuth, password);
			reportMqRedis = new RedisConnection(config, reportHost1, reportPort1, isAuth, password);
		}

		// 销毁旧连接
		if (oldStateRedis != null) {
			oldStateRedis.destroy();
		}
		if (oldSubmitMqRedis != null) {
			oldSubmitMqRedis.destroy();
		}
		if (oldReportMqRedis != null) {
			oldReportMqRedis.destroy();
		}
	}

	public boolean addStock(String key, long timemark, long num) {
		return stateRedis.addStock(key, timemark, num);
	}

	public Long stock(String key, int num) {
		return stateRedis.stock(key, num);
	}

	public Set<String> smembers(String key) {
		return stateRedis.smembers(key);
	}

	public boolean sadd(String key, List<String> values) {
		return stateRedis.sadd(key, values);
	}

	// =========================================================================
	// V4.0 新增: Redis Streams 业务包装方法 (Submit-MQ & Report-MQ)
	// =========================================================================

	/**
	 * 向下行消息队列添加数据 (Submit-MQ)
	 */
	public String xaddToSubmitMq(String key, Map<String, String> hash, long maxLen) {
		if (submitMqRedis == null)
			return null;
		return submitMqRedis.xadd(key, hash, maxLen);
	}

	/**
	 * 向状态报告/上行队列添加数据 (Report-MQ)
	 */
	public String xaddToReportMq(String key, Map<String, String> hash, long maxLen) {
		if (reportMqRedis == null)
			return null;
		return reportMqRedis.xadd(key, hash, maxLen);
	}

	/**
	 * 在下行队列创建消费组
	 */
	public boolean xgroupCreateSubmitMq(String key, String groupName, boolean mkStream) {
		if (submitMqRedis == null)
			return false;
		return submitMqRedis.xgroupCreate(key, groupName, mkStream);
	}

	/**
	 * 从下行队列消费数据
	 */
	public java.util.List<java.util.Map.Entry<String, java.util.List<redis.clients.jedis.resps.StreamEntry>>> xreadGroupSubmitMq(
			String groupName, String consumerName, int count, int blockMillis,
			Map<String, redis.clients.jedis.StreamEntryID> streams) {
		if (submitMqRedis == null)
			return null;
		return submitMqRedis.xreadGroup(groupName, consumerName, count, blockMillis, streams);
	}

	/**
	 * 确认下行队列消息
	 */
	public Long xackSubmitMq(String key, String groupName, redis.clients.jedis.StreamEntryID... ids) {
		if (submitMqRedis == null)
			return 0L;
		return submitMqRedis.xack(key, groupName, ids);
	}

	/**
	 * 从状态报告队列消费数据
	 */
	public java.util.List<java.util.Map.Entry<String, java.util.List<redis.clients.jedis.resps.StreamEntry>>> xreadGroupReportMq(
			String groupName, String consumerName, int count, int blockMillis,
			Map<String, redis.clients.jedis.StreamEntryID> streams) {
		if (reportMqRedis == null)
			return null;
		return reportMqRedis.xreadGroup(groupName, consumerName, count, blockMillis, streams);
	}

	/**
	 * 确认上行队列消息
	 */
	public Long xackReportMq(String key, String groupName, redis.clients.jedis.StreamEntryID... ids) {
		if (reportMqRedis == null)
			return 0L;
		return reportMqRedis.xack(key, groupName, ids);
	}

	/**
	 * 自动认领下行队列超时消息 (Submit-MQ)
	 */
	public java.util.Map.Entry<redis.clients.jedis.StreamEntryID, java.util.List<redis.clients.jedis.resps.StreamEntry>> xautoclaimSubmitMq(
			String key, String group, String consumer, long minIdleMs,
			redis.clients.jedis.StreamEntryID start, int count) {
		if (submitMqRedis == null)
			return null;
		return submitMqRedis.xautoclaim(key, group, consumer, minIdleMs, start, count);
	}

	/**
	 * 自动认领上行/回执队列超时消息 (Report-MQ)
	 */
	public java.util.Map.Entry<redis.clients.jedis.StreamEntryID, java.util.List<redis.clients.jedis.resps.StreamEntry>> xautoclaimReportMq(
			String key, String group, String consumer, long minIdleMs,
			redis.clients.jedis.StreamEntryID start, int count) {
		if (reportMqRedis == null)
			return null;
		return reportMqRedis.xautoclaim(key, group, consumer, minIdleMs, start, count);
	}

	public java.util.List<redis.clients.jedis.resps.StreamPendingSummary> xpendingSubmitMq(String key, String group) {
		if (submitMqRedis == null)
			return null;
		return submitMqRedis.xpending(key, group);
	}

	public java.util.List<redis.clients.jedis.resps.StreamPendingSummary> xpendingReportMq(String key, String group) {
		if (reportMqRedis == null)
			return null;
		return reportMqRedis.xpending(key, group);
	}

	public java.util.List<redis.clients.jedis.resps.StreamEntry> transferOwnershipSubmitMq(String key, String group, String consumer, long minIdleMs, redis.clients.jedis.StreamEntryID... ids) {
		if (submitMqRedis == null)
			return null;
		return submitMqRedis.transferOwnership(key, group, consumer, minIdleMs, ids);
	}

	public java.util.List<redis.clients.jedis.resps.StreamEntry> transferOwnershipReportMq(String key, String group, String consumer, long minIdleMs, redis.clients.jedis.StreamEntryID... ids) {
		if (reportMqRedis == null)
			return null;
		return reportMqRedis.transferOwnership(key, group, consumer, minIdleMs, ids);
	}
}