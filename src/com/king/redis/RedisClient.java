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

	private static RedisConnection stateRedis = null; 
	private static RedisConnection submitMqRedis = null; 
	private static RedisConnection reportMqRedis = null; 

	private static RedisClient init = new RedisClient();
	private boolean redisHaFlag = true;
	protected JedisPoolConfig config = new JedisPoolConfig();

	protected String host = "192.168.23.112";
	protected String host1 = "192.168.23.112";
	protected int port = 6379;
	protected int port1 = 6378;

	protected String submitHost = "";
	protected String submitHost1 = "";
	protected int submitPort = 6379;
	protected int submitPort1 = 6378;

	protected String reportHost = "";
	protected String reportHost1 = "";
	protected int reportPort = 6379;
	protected int reportPort1 = 6378;

	protected int maxActive = 50;
	protected int maxIdle = 50;
	protected int minIdle = 0;
	protected int maxWait = 20000;
	protected int connectTimeout = 2000;
	protected int minEvictableIdleTime = 180000;
	protected int timeBetweenEvictionRuns = -1;
	protected boolean jmxEnabled = false;
	protected boolean isAuth = false;
	protected String password = "";

	private RedisClient() {
		try {
			GmmsUtility gmmsUtility = GmmsUtility.getInstance();
			host = gmmsUtility.getCommonProperty("Redis_master_host");
			host1 = gmmsUtility.getCommonProperty("Redis_slave_host");
			port = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_master_port", "6379"));
			port1 = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_slave_port", "6379"));

			submitHost = gmmsUtility.getCommonProperty("Redis_submit_master_host", host);
			submitHost1 = gmmsUtility.getCommonProperty("Redis_submit_slave_host", host1);
			submitPort = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_submit_master_port", String.valueOf(port)));
			submitPort1 = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_submit_slave_port", String.valueOf(port1)));

			reportHost = gmmsUtility.getCommonProperty("Redis_report_master_host", host);
			reportHost1 = gmmsUtility.getCommonProperty("Redis_report_slave_host", host1);
			reportPort = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_report_master_port", String.valueOf(port)));
			reportPort1 = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_report_slave_port", String.valueOf(port1)));

			try { maxActive = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_maxActive", "50")); } catch (Exception e) { maxActive = 50; }
			try { maxIdle = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_maxIdle", "50")); } catch (Exception e) { maxIdle = 50; }
			try { minIdle = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_minIdle", "0")); } catch (Exception e) { minIdle = 0; }
			try { maxWait = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_maxWait", "20000")); } catch (Exception e) { maxWait = 20000; }
			try { connectTimeout = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_connect_timeout_ms", "2000")); } catch (Exception e) { connectTimeout = 2000; }
			try { minEvictableIdleTime = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_minEvictableIdleTime", "180000")); } catch (Exception e) { minEvictableIdleTime = 180000; }
			try { timeBetweenEvictionRuns = Integer.parseInt(gmmsUtility.getCommonProperty("Redis_pool_timeBetweenEvictionRuns", "-1")); } catch (Exception e) { timeBetweenEvictionRuns = -1; }
			jmxEnabled = Boolean.parseBoolean(gmmsUtility.getCommonProperty("Redis_pool_jmxEnabled", "false"));

			if (config == null) config = new JedisPoolConfig();
			isAuth = Boolean.parseBoolean(gmmsUtility.getCommonProperty("Redis_isAuth", "false"));
			password = gmmsUtility.getCommonProperty("Redis_password", null);

			config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");
			config.setJmxEnabled(jmxEnabled);
			config.setMaxTotal(maxActive);
			config.setMaxIdle(maxIdle);
			config.setMinIdle(minIdle);
			config.setMaxWaitMillis(maxWait);
			config.setMinEvictableIdleTimeMillis(minEvictableIdleTime);
			config.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRuns);
			config.setTestOnReturn(false);
			config.setTestOnBorrow(false);
			config.setTestWhileIdle(true);
			log.info("Redis pool config initialized. maxActive={}, maxIdle={}, minIdle={}, maxWait={}ms, connectTimeout={}ms, evictionRun={}ms, jmxEnabled={}, jedisJar={}, commonsPoolJar={}",
					maxActive, maxIdle, minIdle, maxWait, connectTimeout, timeBetweenEvictionRuns, jmxEnabled,
					getCodeLocation(redis.clients.jedis.JedisPool.class),
					getCodeLocation(org.apache.commons.pool2.impl.GenericObjectPool.class));
		} catch (Exception e) {
			log.error("Redis config init error!", e);
		}
	}

	private static String getCodeLocation(Class<?> clazz) {
		try {
			java.security.CodeSource codeSource = clazz.getProtectionDomain().getCodeSource();
			return codeSource == null || codeSource.getLocation() == null ? "unknown" : codeSource.getLocation().toString();
		} catch (Exception e) {
			return "unknown:" + e.getMessage();
		}
	}

	public static RedisClient getInstance() { return init; }

	public RedisConnection getStateRedis() { return stateRedis; }
	public RedisConnection getSubmitMqRedis() { return submitMqRedis; }
	public RedisConnection getReportMqRedis() { return reportMqRedis; }

	public redis.clients.jedis.Jedis getJedis(ClusterType type) {
		switch (type) {
			case SUBMIT_MQ: return submitMqRedis != null ? submitMqRedis.getJedis() : null;
			case REPORT_MQ: return reportMqRedis != null ? reportMqRedis.getJedis() : null;
			case STATE:
			default: return stateRedis != null ? stateRedis.getJedis() : null;
		}
	}

	public boolean setString(String key, String obj) { return stateRedis.setString(key, obj); }
	public Long incBlackhole(String key) { return stateRedis.incblackhole(key); }
	public Long incrString(String key) { return stateRedis.incrString(key); }
	public void setExpire(String key, int time) { stateRedis.setExpired(key, time); }
	public boolean setString(String key, String obj, int time) { return stateRedis.setString(key, obj, time); }
	public String setnxString(String key, String value, int expireSeconds) { return stateRedis.setnxString(key, value, expireSeconds); }
	public boolean lpush(String key, String obj) { return stateRedis.lpush(key, obj); }

	private static final java.util.concurrent.BlockingQueue<RedisConnection.RedisTask> asyncRedisQueue = new java.util.concurrent.LinkedBlockingQueue<RedisConnection.RedisTask>(100000);

	static {
		Thread asyncThread = new Thread(() -> {
			java.util.List<RedisConnection.RedisTask> batch = new java.util.ArrayList<>();
			long lastExecuteTime = System.currentTimeMillis();
			while (true) {
				try {
					RedisConnection.RedisTask task = asyncRedisQueue.poll(10, java.util.concurrent.TimeUnit.MILLISECONDS);
					if (task != null) batch.add(task);
					long now = System.currentTimeMillis();
					if (batch.size() >= 500 || (batch.size() > 0 && (now - lastExecuteTime) >= 200)) {
						if (stateRedis != null) stateRedis.executeAsyncPipeline(batch);
						batch.clear();
						lastExecuteTime = now;
					}
				} catch (Exception e) {
					log.error("AsyncRedisWriterThread error", e);
				}
			}
		}, "AsyncRedisWriterThread");
		asyncThread.setDaemon(true);
		asyncThread.start();
	}

	public void asyncLpush(String key, String value) {
		if (!asyncRedisQueue.offer(new RedisConnection.RedisTask(RedisConnection.RedisTask.Type.LPUSH, key, value))) lpush(key, value);
	}
	public void asyncHashInc(String key, String field) {
		if (!asyncRedisQueue.offer(new RedisConnection.RedisTask(RedisConnection.RedisTask.Type.HINCRBY, key, field))) hashINC(key, field);
	}
	public boolean setPipeline(String key, String value, int expireTime, String hashKey) { return stateRedis.setPipeline(key, value, expireTime, hashKey); }
	public boolean setPendingMessage(String key, String value, int expireTime, String zsetKey, double score, String registryKey) { return stateRedis.setPendingMessage(key, value, expireTime, zsetKey, score, registryKey); }
	public String consumePendingMessage(String key, String zsetKey) { return stateRedis.consumePendingMessage(key, zsetKey); }
	public boolean setDelayDR(String key, String value, int expireTime, double score, int ossid) { return stateRedis.setDelayDR(key, value, expireTime, score, ossid); }
	public Set<String> zrange(String key, long start, long end) { return stateRedis.zrange(key, start, end); }
	public Set<String> zrangeByIndex(String key, long start, long end) { return stateRedis.zrangeByIndex(key, start, end); }
	public boolean zrem(String key, String[] members) { return stateRedis.zrem(key, members); }
	public List<redis.clients.jedis.resps.Tuple> zpopmin(String key) { return stateRedis.zpopmin(key); }
	public Long zadd(String key, double score, String member) { return stateRedis.zadd(key, score, member); }
	public boolean setRoutingInfo(String keyword, Map<String, Map<String, String>> infos) { return stateRedis.setHashMapWithPipline(keyword, infos); }
	public boolean setContentTemplate(String keyword, Map<String, Map<String, String>> infos) { return stateRedis.setHashMapWithPipline(keyword, infos); }
	public void delPipeline(String key, String hashKey) { stateRedis.delPipeline(key, hashKey); }
	public String getString(String key) { return stateRedis.getString(key); }
	public boolean hashINC(String key, String field) { return stateRedis.hashINC(key, field); }
	public String rpop(String key) { return stateRedis.rpop(key); }
	public List<String> rpopBatch(String key, int count) { return stateRedis.rpopBatch(key, count); }
	public String brpop(String key) { return stateRedis.brpop(key); }
	public boolean lpush(String setKey, String key, String[] values) { return stateRedis.lpushArr(setKey, key, values); }
	public boolean setHash(String key, Map<String, String> map) { return stateRedis.setHash(key, map); }
	public boolean setHashAndSet(String setKey, String key, Map<String, String> map) { return stateRedis.setHashAndSet(setKey, key, map); }
	public Set<String> getHash(String key) { Set<String> set = stateRedis.getHashValue(key); stateRedis.delObject(key); return set; }
	public Map<String, String> getHashAll(String key) { return stateRedis.getHashAll(key); }
	public List<String> getHashMap(String key, String[] s) throws Exception { return stateRedis.getHashMap(key, s); }
	public void del(String key) { stateRedis.delObject(key); }
	public void delHash(String key, String field) { stateRedis.delHash(key, field); }
	public boolean isRedisHaFlag() { return redisHaFlag; }

	public void setRedisHaFlag(boolean redisHaFlag) {
		log.info("setRedisHaFlag start. redisHaFlag={}, stateHost={}:{}, submitHost={}:{}, reportHost={}:{}",
				redisHaFlag, redisHaFlag ? host : host1, redisHaFlag ? port : port1,
				redisHaFlag ? submitHost : submitHost1, redisHaFlag ? submitPort : submitPort1,
				redisHaFlag ? reportHost : reportHost1, redisHaFlag ? reportPort : reportPort1);
		this.redisHaFlag = redisHaFlag;
		RedisConnection oldStateRedis = stateRedis;
		RedisConnection oldSubmitMqRedis = submitMqRedis;
		RedisConnection oldReportMqRedis = reportMqRedis;

		if (redisHaFlag) {
			log.info("Creating state RedisConnection. host={}, port={}", host, port);
			stateRedis = new RedisConnection(config, host, port, isAuth, password, connectTimeout);
			log.info("Creating submit MQ RedisConnection. host={}, port={}", submitHost, submitPort);
			submitMqRedis = new RedisConnection(config, submitHost, submitPort, isAuth, password, connectTimeout);
			log.info("Creating report MQ RedisConnection. host={}, port={}", reportHost, reportPort);
			reportMqRedis = new RedisConnection(config, reportHost, reportPort, isAuth, password, connectTimeout);
		} else {
			log.info("Creating state RedisConnection. host={}, port={}", host1, port1);
			stateRedis = new RedisConnection(config, host1, port1, isAuth, password, connectTimeout);
			log.info("Creating submit MQ RedisConnection. host={}, port={}", submitHost1, submitPort1);
			submitMqRedis = new RedisConnection(config, submitHost1, submitPort1, isAuth, password, connectTimeout);
			log.info("Creating report MQ RedisConnection. host={}, port={}", reportHost1, reportPort1);
			reportMqRedis = new RedisConnection(config, reportHost1, reportPort1, isAuth, password, connectTimeout);
		}
		log.info("RedisConnection creation finished. redisHaFlag={}", redisHaFlag);

		if (oldStateRedis != null) oldStateRedis.destroy();
		if (oldSubmitMqRedis != null) oldSubmitMqRedis.destroy();
		if (oldReportMqRedis != null) oldReportMqRedis.destroy();
		log.info("setRedisHaFlag finished. redisHaFlag={}", redisHaFlag);
	}

	public boolean addStock(String key, long timemark, long num) { return stateRedis.addStock(key, timemark, num); }
	public Long stock(String key, int num) { return stateRedis.stock(key, num); }
	public Set<String> smembers(String key) { return stateRedis.smembers(key); }
	public boolean sadd(String key, List<String> values) { return stateRedis.sadd(key, values); }
	public boolean sadd(String key, String value) { return stateRedis.sadd(key, value); }
	public boolean srem(String key, String value) { return stateRedis.srem(key, value); }

	// =========================================================================
	// V5.0 Redis Stream & ZSet Wrapper Methods
	// =========================================================================

	// --- Submit-MQ ---
	public String xaddToSubmitMq(String key, Map<String, String> hash, long maxLen) {
		if (submitMqRedis == null) {
			log.error("xaddToSubmitMq failed. submitMqRedis is null, key={}", key);
			return null;
		}
		return submitMqRedis.xadd(key, hash, maxLen);
	}
	public boolean xgroupCreateSubmitMq(String key, String groupName, boolean mkStream) {
		return submitMqRedis != null ? submitMqRedis.xgroupCreate(key, groupName, mkStream) : false;
	}
	public List<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>> xreadGroupSubmitMq(String groupName, String consumerName, int count, int blockMillis, Map<String, redis.clients.jedis.StreamEntryID> streams) {
		return submitMqRedis != null ? submitMqRedis.xreadGroup(groupName, consumerName, count, blockMillis, streams) : null;
	}
	public Long xackSubmitMq(String key, String groupName, redis.clients.jedis.StreamEntryID... ids) {
		return submitMqRedis != null ? submitMqRedis.xack(key, groupName, ids) : 0L;
	}
	public Long xdelSubmitMq(String key, redis.clients.jedis.StreamEntryID... ids) {
		return submitMqRedis != null ? submitMqRedis.xdel(key, ids) : 0L;
	}
	public Map.Entry<redis.clients.jedis.StreamEntryID, List<redis.clients.jedis.resps.StreamEntry>> xautoclaimSubmitMq(String key, String group, String consumer, long minIdleMs, redis.clients.jedis.StreamEntryID start, int count) {
		return submitMqRedis != null ? submitMqRedis.xautoclaim(key, group, consumer, minIdleMs, start, count) : null;
	}
	public List<redis.clients.jedis.resps.Tuple> zpopminSubmitMq(String key) {
		return submitMqRedis != null ? submitMqRedis.zpopmin(key) : null;
	}
	public Long zaddSubmitMq(String key, double score, String member) {
		return submitMqRedis != null ? submitMqRedis.zadd(key, score, member) : 0L;
	}
	public Set<String> zrangeSubmitMq(String key, long start, long end) {
		return submitMqRedis != null ? submitMqRedis.zrangeDoorbell(key, start, end) : null;
	}
	public Long zremSubmitMq(String key, String member) {
		return submitMqRedis != null ? submitMqRedis.zremDoorbell(key, member) : 0L;
	}
	public long xlenSubmitMq(String key) {
		return submitMqRedis != null ? submitMqRedis.xlen(key) : 0L;
	}
	public Set<String> keysSubmitMq(String pattern) {
		return submitMqRedis != null ? submitMqRedis.keys(pattern) : null;
	}

	// --- Report-MQ ---
	public String xaddToReportMq(String key, Map<String, String> hash, long maxLen) {
		return reportMqRedis != null ? reportMqRedis.xadd(key, hash, maxLen) : null;
	}
	public boolean xgroupCreateReportMq(String key, String groupName, boolean mkStream) {
		return reportMqRedis != null ? reportMqRedis.xgroupCreate(key, groupName, mkStream) : false;
	}
	public List<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>> xreadGroupReportMq(String groupName, String consumerName, int count, int blockMillis, Map<String, redis.clients.jedis.StreamEntryID> streams) {
		return reportMqRedis != null ? reportMqRedis.xreadGroup(groupName, consumerName, count, blockMillis, streams) : null;
	}
	public Long xackReportMq(String key, String groupName, redis.clients.jedis.StreamEntryID... ids) {
		return reportMqRedis != null ? reportMqRedis.xack(key, groupName, ids) : 0L;
	}
	public Long xdelReportMq(String key, redis.clients.jedis.StreamEntryID... ids) {
		return reportMqRedis != null ? reportMqRedis.xdel(key, ids) : 0L;
	}
	public Map.Entry<redis.clients.jedis.StreamEntryID, List<redis.clients.jedis.resps.StreamEntry>> xautoclaimReportMq(String key, String group, String consumer, long minIdleMs, redis.clients.jedis.StreamEntryID start, int count) {
		return reportMqRedis != null ? reportMqRedis.xautoclaim(key, group, consumer, minIdleMs, start, count) : null;
	}
	public List<redis.clients.jedis.resps.Tuple> zpopminReportMq(String key) {
		return reportMqRedis != null ? reportMqRedis.zpopmin(key) : null;
	}
	public Long zaddReportMq(String key, double score, String member) {
		return reportMqRedis != null ? reportMqRedis.zadd(key, score, member) : 0L;
	}
	public Set<String> zrangeReportMq(String key, long start, long end) {
		return reportMqRedis != null ? reportMqRedis.zrangeDoorbell(key, start, end) : null;
	}
	public Long zremReportMq(String key, String member) {
		return reportMqRedis != null ? reportMqRedis.zremDoorbell(key, member) : 0L;
	}
	public long xlenReportMq(String key) {
		return reportMqRedis != null ? reportMqRedis.xlen(key) : 0L;
	}

	public Set<String> keysReportMq(String pattern) {
		return reportMqRedis != null ? reportMqRedis.keys(pattern) : null;
	}

	// --- Pub/Sub (STATE channel) ---
	public void subscribeState(redis.clients.jedis.JedisPubSub pubSub, String... channels) {
		if (stateRedis != null) {
			stateRedis.subscribe(pubSub, channels);
		}
	}

	public Long publishState(String channel, String message) {
		if (stateRedis == null) {
			log.error("publishState failed. stateRedis is null, channel={}, message={}", channel, message);
			return -1L;
		}
		Long subscribers = stateRedis.publish(channel, message);
		log.info("publishState finished. channel={}, message={}, subscribers={}", channel, message, subscribers);
		return subscribers;
	}
}
