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

	// H-7: volatile —— 切换线程重新赋值后，所有 worker/consumer 热路径读必须及时看到新池引用，
	// 否则 JMM 下可能长期走旧(已 destroy)池。配合 setRedisHaFlag 的 synchronized + 延迟销毁。
	private static volatile RedisConnection stateRedis = null;
	private static volatile RedisConnection submitMqRedis = null;
	private static volatile RedisConnection reportMqRedis = null;
	/**
	 * The submit and report MQ masters (Redis_submit_masters / Redis_report_masters, docs/Redis多主分片设计.md); [0] is
	 * submitMqRedis / reportMqRedis. One master unless configured: every key on it, as before.
	 */
	private static volatile RedisConnection[] submitMqs = new RedisConnection[0];
	private static volatile RedisConnection[] reportMqs = new RedisConnection[0];

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
	/** host:port of each MQ master and its slave, same order */
	protected String[] submitMasters, submitSlaves, reportMasters, reportSlaves;

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
			submitMasters = hostPorts(gmmsUtility.getCommonProperty("Redis_submit_masters", ""), submitHost + ":" + submitPort);
			submitSlaves = slaves(gmmsUtility.getCommonProperty("Redis_submit_slaves", ""), submitHost1 + ":" + submitPort1, submitMasters, "submit");
			reportMasters = hostPorts(gmmsUtility.getCommonProperty("Redis_report_masters", ""), reportHost + ":" + reportPort);
			reportSlaves = slaves(gmmsUtility.getCommonProperty("Redis_report_slaves", ""), reportHost1 + ":" + reportPort1, reportMasters, "report");
			log.info("Redis MQ masters: submit {} (slaves {}), report {} (slaves {})", java.util.Arrays.toString(submitMasters),
					java.util.Arrays.toString(submitSlaves), java.util.Arrays.toString(reportMasters), java.util.Arrays.toString(reportSlaves));

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

	/** "host:port,host:port" -> its entries; blank: the single one */
	private static String[] hostPorts(String list, String single) {
		List<String> out = new java.util.ArrayList<String>();
		for (String hp : list == null ? new String[0] : list.split(",")) {
			if (hp.trim().length() > 0) {
				out.add(hp.trim());
			}
		}
		return out.isEmpty() ? new String[] { single } : out.toArray(new String[0]);
	}

	/** The slaves of the masters, one each; with one master and none listed, the single slave as before. */
	private static String[] slaves(String list, String single, String[] masters, String name) {
		String[] s = hostPorts(list, single);
		if (s.length != masters.length) {
			log.error("Redis_{}_slaves has {} entries for {} masters: each master is its own slave", name, s.length, masters.length);
			return masters.clone();
		}
		return s;
	}

	private RedisConnection[] connect(String[] hostPorts, String name) {
		RedisConnection[] out = new RedisConnection[hostPorts.length];
		for (int i = 0; i < hostPorts.length; i++) {
			int c = hostPorts[i].lastIndexOf(':');
			String h = hostPorts[i].substring(0, c);
			int p = Integer.parseInt(hostPorts[i].substring(c + 1).trim());
			log.info("Creating {} MQ RedisConnection {}. host={}, port={}", name, i, h, p);
			out[i] = new RedisConnection(config, h, p, isAuth, password, connectTimeout);
		}
		return out;
	}

	// ---------------------------------------------------------------- MQ instance of a key

	/**
	 * Streams whose name ends with their shard (":{shard}"): on master shard % masters. Their markers, outbox and
	 * send markers go where their stream is; the callers pass that connection. Every other key (hold streams,
	 * stream:outbound:http:tasks, dead letters, the stream index) is on master 0; doorbells are on every master.
	 */
	private static final String[] SHARDED_STREAMS = { "stream:mt:pending:", "stream:mt:routed:", "stream:http:submit:",
			"stream:http:dr:", "stream:mqm:submit:", "stream:mqm:report:", "stream:dr:pending:", "stream:core:inbound-dr:",
			"stream:core:results:" };

	public static int instanceOf(String key, int masters) {
		if (masters <= 1 || key == null) {
			return 0;
		}
		for (String prefix : SHARDED_STREAMS) {
			if (key.startsWith(prefix)) {
				int c = key.lastIndexOf(':');
				try {
					return (int) (Long.parseLong(key.substring(c + 1)) % masters);
				} catch (NumberFormatException e) {
					return 0;
				}
			}
		}
		return 0;
	}

	public int submitMqCount() { return submitMqs.length; }
	public int reportMqCount() { return reportMqs.length; }
	public int mqCount(boolean isSubmitMq) { return isSubmitMq ? submitMqs.length : reportMqs.length; }
	public boolean isMultiMaster() { return submitMqs.length > 1 || reportMqs.length > 1; }

	/** The submit MQ master of this key (see instanceOf) */
	public RedisConnection submitMq(String key) {
		RedisConnection[] a = submitMqs;
		return a.length == 0 ? submitMqRedis : a[instanceOf(key, a.length)];
	}

	public RedisConnection reportMq(String key) {
		RedisConnection[] a = reportMqs;
		return a.length == 0 ? reportMqRedis : a[instanceOf(key, a.length)];
	}

	public RedisConnection mq(String key, boolean isSubmitMq) {
		return isSubmitMq ? submitMq(key) : reportMq(key);
	}

	/** Every master of the MQ, [0] first */
	public RedisConnection[] mqs(boolean isSubmitMq) {
		RedisConnection[] a = isSubmitMq ? submitMqs : reportMqs;
		if (a.length == 0) {
			RedisConnection one = isSubmitMq ? submitMqRedis : reportMqRedis;
			return one == null ? new RedisConnection[0] : new RedisConnection[] { one };
		}
		return a.clone();
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
	public Long incrWithExpire(String key, int expireSeconds) { return stateRedis.incrWithExpire(key, expireSeconds); }
	public void setExpire(String key, int time) { stateRedis.setExpired(key, time); }
	public boolean setString(String key, String obj, int time) { return stateRedis.setString(key, obj, time); }
	public String setnxString(String key, String value, int expireSeconds) { return stateRedis.setnxString(key, value, expireSeconds); }
	public boolean setStringIfAbsent(String key, String value, int expireSeconds) { return stateRedis.setStringIfAbsent(key, value, expireSeconds); }
	public boolean compareAndSetString(String key, String expectedValue, String newValue, int expireSeconds) { return stateRedis.compareAndSetString(key, expectedValue, newValue, expireSeconds); }
	public boolean deleteIfStringEquals(String key, String expectedValue) { return stateRedis.deleteIfStringEquals(key, expectedValue); }
	public java.util.Map<String,String> hgetAll(String key) { return stateRedis.hgetAll(key); }
	public Long delKey(String key) { return stateRedis.delKey(key); }
	public boolean lpush(String key, String obj) { return stateRedis.lpush(key, obj); }

	private static final java.util.concurrent.BlockingQueue<RedisConnection.RedisTask> asyncRedisQueue = new java.util.concurrent.LinkedBlockingQueue<RedisConnection.RedisTask>(100000);
	// M-6: 关停排空控制 —— 标志置位后 writer 继续处理直到队列空再退出；shutdown hook 通知并 join。
	private static volatile boolean asyncWriterRunning = true;
	private static Thread asyncWriterThread;

	static {
		asyncWriterThread = new Thread(() -> {
			java.util.List<RedisConnection.RedisTask> batch = new java.util.ArrayList<>();
			long lastExecuteTime = System.currentTimeMillis();
			// M-6: 关停时排空 —— 停止后继续处理直到队列为空才退出，避免缓冲的 CDR/计数丢失。
			while (asyncWriterRunning || !asyncRedisQueue.isEmpty()) {
				try {
					RedisConnection.RedisTask task = asyncRedisQueue.poll(10, java.util.concurrent.TimeUnit.MILLISECONDS);
					if (task != null) batch.add(task);
					long now = System.currentTimeMillis();
					boolean flushDueToStop = !asyncWriterRunning && task == null && !batch.isEmpty();
					if (batch.size() >= 500 || (batch.size() > 0 && (now - lastExecuteTime) >= 200) || flushDueToStop) {
						if (stateRedis != null) stateRedis.executeAsyncPipeline(batch);
						batch.clear();
						lastExecuteTime = now;
					}
				} catch (Exception e) {
					log.error("AsyncRedisWriterThread error", e);
				}
			}
			// M-6: 退出前最后冲刷残留 batch。
			try {
				if (!batch.isEmpty() && stateRedis != null) stateRedis.executeAsyncPipeline(batch);
			} catch (Exception e) {
				log.error("AsyncRedisWriterThread final flush error", e);
			}
		}, "AsyncRedisWriterThread");
		asyncWriterThread.setDaemon(true);
		asyncWriterThread.start();

		// M-6: 关停钩子 —— daemon 线程在 JVM 退出时会被强杀，用 hook 通知停止并 join，尽量把缓冲落库。
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			asyncWriterRunning = false;
			try {
				if (asyncWriterThread != null) asyncWriterThread.join(5000);
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		}, "AsyncRedisWriterShutdown"));
	}

	/**
	 * The webapp (CommonHttpServer) stops without the JVM going down (GmmsUtility.shutdown): the async writer
	 * writes its queue and ends, then every pool (state, submit and report MQ masters) is closed.
	 */
	public void close() {
		asyncWriterRunning = false;
		try {
			if (asyncWriterThread != null) asyncWriterThread.join(5000);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
		java.util.Set<RedisConnection> all = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<RedisConnection, Boolean>());
		for (RedisConnection c : new RedisConnection[] { stateRedis, submitMqRedis, reportMqRedis }) {
			if (c != null) all.add(c);
		}
		for (RedisConnection c : submitMqs) if (c != null) all.add(c);
		for (RedisConnection c : reportMqs) if (c != null) all.add(c);
		for (RedisConnection c : all) {
			c.destroy();
		}
		log.info("Redis client closed: {} pool(s)", all.size());
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
	public boolean requeuePendingMessageIfExists(String key, String value, int expireTime, String zsetKey, double score) { return stateRedis.requeuePendingMessageIfExists(key, value, expireTime, zsetKey, score); }
	public Set<String> zrange(String key, long start, long end) { return stateRedis.zrange(key, start, end); }
	public Set<String> zrangeByScore(String key, long minScore, long maxScore, int limit) { return stateRedis.zrangeByScore(key, minScore, maxScore, limit); }
	public Set<String> zrangeByIndex(String key, long start, long end) { return stateRedis.zrangeByIndex(key, start, end); }
	public boolean zrem(String key, String[] members) { return stateRedis.zrem(key, members); }
	public List<redis.clients.jedis.resps.Tuple> zpopmin(String key) { return stateRedis.zpopmin(key); }
	public Long zadd(String key, double score, String member) { return stateRedis.zadd(key, score, member); }
	public String claimDueZSetMember(String fromKey, String toKey, double dueScore, double processingScore) { return stateRedis.claimDueZSetMember(fromKey, toKey, dueScore, processingScore); }
	public boolean setRoutingInfo(String keyword, Map<String, Map<String, String>> infos) { return stateRedis.setHashMapWithPipline(keyword, infos); }
	public boolean setContentTemplate(String keyword, Map<String, Map<String, String>> infos) { return stateRedis.setHashMapWithPipline(keyword, infos); }
	public void delPipeline(String key, String hashKey) { stateRedis.delPipeline(key, hashKey); }
	public String getString(String key) { return stateRedis.getString(key); }
	public boolean hashINC(String key, String field) { return stateRedis.hashINC(key, field); }
	// C-9 MQM fencing：单调递增任期纪元号。
	public static final String MQM_FENCE_KEY = "mqm:active:fence";
	public Long incrFence() { return stateRedis != null ? stateRedis.incrString(MQM_FENCE_KEY) : null; }
	public long getFence() {
		try {
			String s = (stateRedis != null) ? stateRedis.getString(MQM_FENCE_KEY) : null;
			return (s != null) ? Long.parseLong(s.trim()) : -1L;
		} catch (Exception e) { return -1L; }
	}
	// DB HA：DB 主备状态真源(全集群共享, 值 "master"|"slave")。
	public static final String DB_STATUS_KEY = "system:db:status";
	public String getDbStatus() { return stateRedis != null ? stateRedis.getString(DB_STATUS_KEY) : null; }
	public boolean setDbStatus(String status) { return stateRedis != null && stateRedis.setString(DB_STATUS_KEY, status); }
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

	/**
	 * The side whose state Redis answers role:master now: "M" (Redis_master_*), "S" (Redis_slave_*), null when neither
	 * does (down, or both replicas). The master side is asked first: the slave only when the master is not a master.
	 */
	public String detectMasterSide() {
		if ("master".equals(role(host, port))) {
			return "M";
		}
		if ("master".equals(role(host1, port1))) {
			return "S";
		}
		return null;
	}

	/** every Redis of a side (the state Redis and each submit / report MQ instance) answers role:master */
	public boolean sideIsMaster(boolean masterSide) {
		List<String> all = new java.util.ArrayList<String>();
		all.add((masterSide ? host : host1) + ":" + (masterSide ? port : port1));
		all.addAll(java.util.Arrays.asList(masterSide ? submitMasters : submitSlaves));
		all.addAll(java.util.Arrays.asList(masterSide ? reportMasters : reportSlaves));
		for (String hp : new java.util.LinkedHashSet<String>(all)) {
			int c = hp.lastIndexOf(':');
			String r = role(hp.substring(0, c), Integer.parseInt(hp.substring(c + 1).trim()));
			if (!"master".equals(r)) {
				log.warn("Redis {} answers role {}, not master", hp, r);
				return false;
			}
		}
		return true;
	}

	/** the replication role of a Redis (INFO replication): master, slave; null when it does not answer */
	private String role(String h, int p) {
		if (h == null || h.trim().isEmpty()) {
			return null;
		}
		try (redis.clients.jedis.Jedis j = new redis.clients.jedis.Jedis(h.trim(), p, connectTimeout)) {
			if (isAuth && password != null) {
				j.auth(password);
			}
			for (String line : j.info("replication").split("\r?\n")) {
				if (line.startsWith("role:")) {
					return line.substring(5).trim();
				}
			}
		} catch (Exception e) {
			log.warn("Redis {}:{} role not read: {}", h, p, e.getMessage());
		}
		return null;
	}

	public synchronized void setRedisHaFlag(boolean redisHaFlag) {
		log.info("setRedisHaFlag start. redisHaFlag={}, stateHost={}:{}, submitHost={}:{}, reportHost={}:{}",
				redisHaFlag, redisHaFlag ? host : host1, redisHaFlag ? port : port1,
				redisHaFlag ? submitHost : submitHost1, redisHaFlag ? submitPort : submitPort1,
				redisHaFlag ? reportHost : reportHost1, redisHaFlag ? reportPort : reportPort1);
		this.redisHaFlag = redisHaFlag;
		RedisConnection oldStateRedis = stateRedis;
		RedisConnection oldSubmitMqRedis = submitMqRedis;
		RedisConnection oldReportMqRedis = reportMqRedis;

		RedisConnection[] oldSubmitMqs = submitMqs;
		RedisConnection[] oldReportMqs = reportMqs;

		// all MQ masters switch to their slaves (and back) together, as the single ones did
		if (redisHaFlag) {
			log.info("Creating state RedisConnection. host={}, port={}", host, port);
			stateRedis = new RedisConnection(config, host, port, isAuth, password, connectTimeout);
			submitMqs = connect(submitMasters, "submit");
			reportMqs = connect(reportMasters, "report");
		} else {
			log.info("Creating state RedisConnection. host={}, port={}", host1, port1);
			stateRedis = new RedisConnection(config, host1, port1, isAuth, password, connectTimeout);
			submitMqs = connect(submitSlaves, "submit");
			reportMqs = connect(reportSlaves, "report");
		}
		submitMqRedis = submitMqs[0];
		reportMqRedis = reportMqs[0];
		log.info("RedisConnection creation finished. redisHaFlag={}", redisHaFlag);

		// H-7: 延迟销毁旧池 —— 新引用已发布(volatile)，但在重新赋值前已读到旧引用的在途请求仍在执行；
		// 立即 pool.destroy() 会打挂这些在途操作。等一个 grace 期让其排空后再销毁。
		List<RedisConnection> old = new java.util.ArrayList<RedisConnection>();
		old.add(oldStateRedis);
		old.addAll(java.util.Arrays.asList(oldSubmitMqs.length > 0 ? oldSubmitMqs : new RedisConnection[] { oldSubmitMqRedis }));
		old.addAll(java.util.Arrays.asList(oldReportMqs.length > 0 ? oldReportMqs : new RedisConnection[] { oldReportMqRedis }));
		scheduleDelayedDestroy(old.toArray(new RedisConnection[0]));
		log.info("setRedisHaFlag finished. redisHaFlag={}", redisHaFlag);
	}

	private void scheduleDelayedDestroy(final RedisConnection... oldPools) {
		if (oldPools == null || oldPools.length == 0) {
			return;
		}
		long graceMs;
		try {
			graceMs = Long.parseLong(GmmsUtility.getInstance()
					.getCommonProperty("Redis_ha_old_pool_destroy_delay_ms", "30000").trim());
		} catch (Exception e) {
			graceMs = 30000L;
		}
		final long delayMs = graceMs;
		Thread t = new Thread(new Runnable() {
			public void run() {
				try {
					Thread.sleep(delayMs);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
				}
				for (RedisConnection old : oldPools) {
					if (old != null) {
						try {
							old.destroy();
						} catch (Exception e) {
							log.warn("delayed destroy old RedisConnection failed: {}", e.getMessage());
						}
					}
				}
				log.info("Delayed destroy of old RedisConnection pools finished. delayMs={}", delayMs);
			}
		}, "RedisOldPoolDestroyer");
		t.setDaemon(true);
		t.start();
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

	// --- Submit-MQ and Report-MQ: each call on the master of its stream (the doorbell member for doorbell writes) ---
	public String xaddToSubmitMq(String key, Map<String, String> hash, long maxLen) {
		return xaddToSubmitMq(key, hash, maxLen, true);
	}
	public String xaddToSubmitMq(String key, Map<String, String> hash, long maxLen, boolean approximateTrimming) {
		RedisConnection c = submitMq(key);
		if (c == null) {
			log.error("xaddToSubmitMq failed. submitMqRedis is null, key={}", key);
			return null;
		}
		return c.xadd(key, hash, maxLen, approximateTrimming);
	}
	// 阶段1: 原子写(XADD+ZADD doorbell)合并，降 RTT 并强化 C-4。
	public String xaddWithDoorbellSubmitMq(String streamKey, String doorbellZset, long doorbellScore,
			long maxLen, boolean approximateTrimming, java.util.List<String> fieldValues) {
		RedisConnection c = submitMq(streamKey);
		if (c == null) {
			log.error("xaddWithDoorbellSubmitMq failed. submitMqRedis is null, key={}", streamKey);
			return null;
		}
		return c.xaddWithDoorbell(streamKey, doorbellZset, doorbellScore, maxLen, approximateTrimming, fieldValues);
	}
	public boolean xgroupCreateSubmitMq(String key, String groupName, boolean mkStream) {
		RedisConnection c = submitMq(key);
		return c != null ? c.xgroupCreate(key, groupName, mkStream) : false;
	}
	public List<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>> xreadGroupSubmitMq(String groupName, String consumerName, int count, int blockMillis, Map<String, redis.clients.jedis.StreamEntryID> streams) {
		return xreadGroup(true, groupName, consumerName, count, blockMillis, streams);
	}
	public Long xackSubmitMq(String key, String groupName, redis.clients.jedis.StreamEntryID... ids) {
		RedisConnection c = submitMq(key);
		return c != null ? c.xack(key, groupName, ids) : 0L;
	}
	public Long xdelSubmitMq(String key, redis.clients.jedis.StreamEntryID... ids) {
		RedisConnection c = submitMq(key);
		return c != null ? c.xdel(key, ids) : 0L;
	}
	public Long xackDelSubmitMq(String key, String groupName, redis.clients.jedis.StreamEntryID id) {
		RedisConnection c = submitMq(key);
		return c != null ? c.xackAndDel(key, groupName, id) : 0L;
	}
	public Map.Entry<redis.clients.jedis.StreamEntryID, List<redis.clients.jedis.resps.StreamEntry>> xautoclaimSubmitMq(String key, String group, String consumer, long minIdleMs, redis.clients.jedis.StreamEntryID start, int count) {
		RedisConnection c = submitMq(key);
		return c != null ? c.xautoclaim(key, group, consumer, minIdleMs, start, count) : null;
	}
	public List<redis.clients.jedis.resps.StreamPendingSummary> xpendingSubmitMq(String key, String group) {
		RedisConnection c = submitMq(key);
		return c != null ? c.xpending(key, group) : null;
	}
	/** The lowest member of the doorbell on each master in turn: the first master that has one */
	public List<redis.clients.jedis.resps.Tuple> zpopminSubmitMq(String key) {
		return zpopmin(true, key);
	}
	public Long zaddSubmitMq(String key, double score, String member) {
		// C-4: doorbell 专用，异常/池缺失返回 null 以区分"失败"与"已存在(0)"。
		RedisConnection c = submitMq(member);
		return c != null ? c.zaddOrNull(key, score, member) : null;
	}
	/** The doorbell's members on every master (each master: start..end of its own) */
	public Set<String> zrangeSubmitMq(String key, long start, long end) {
		return zrange(true, key, start, end);
	}
	public Long zremSubmitMq(String key, String member) {
		RedisConnection c = submitMq(member);
		return c != null ? c.zremDoorbell(key, member) : 0L;
	}
	public Double zscoreSubmitMq(String key, String member) {
		RedisConnection c = submitMq(member);
		return c != null ? c.zscoreDoorbell(key, member) : null;
	}
	public long xlenSubmitMq(String key) {
		RedisConnection c = submitMq(key);
		return c != null ? c.xlen(key) : 0L;
	}
	public boolean streamEntryExistsSubmitMq(String key, String id) {
		RedisConnection c = submitMq(key);
		return c != null && c.streamEntryExists(key, id);
	}
	public boolean streamPendingEntryExistsSubmitMq(String key, String group, String id) {
		RedisConnection c = submitMq(key);
		return c != null && c.streamPendingEntryExists(key, group, id);
	}
	/** Keys matching pattern on every submit MQ master, listed with SCAN (KEYS would block Redis). */
	public Set<String> keysSubmitMq(String pattern) {
		return keys(true, pattern);
	}

	public String xaddToReportMq(String key, Map<String, String> hash, long maxLen) {
		return xaddToReportMq(key, hash, maxLen, true);
	}
	public String xaddToReportMq(String key, Map<String, String> hash, long maxLen, boolean approximateTrimming) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xadd(key, hash, maxLen, approximateTrimming) : null;
	}
	// 阶段1: 原子写(XADD+ZADD doorbell)合并。
	public String xaddWithDoorbellReportMq(String streamKey, String doorbellZset, long doorbellScore,
			long maxLen, boolean approximateTrimming, java.util.List<String> fieldValues) {
		RedisConnection c = reportMq(streamKey);
		return c != null
			? c.xaddWithDoorbell(streamKey, doorbellZset, doorbellScore, maxLen, approximateTrimming, fieldValues)
			: null;
	}
	public boolean xgroupCreateReportMq(String key, String groupName, boolean mkStream) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xgroupCreate(key, groupName, mkStream) : false;
	}
	public List<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>> xreadGroupReportMq(String groupName, String consumerName, int count, int blockMillis, Map<String, redis.clients.jedis.StreamEntryID> streams) {
		return xreadGroup(false, groupName, consumerName, count, blockMillis, streams);
	}
	public Long xackReportMq(String key, String groupName, redis.clients.jedis.StreamEntryID... ids) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xack(key, groupName, ids) : 0L;
	}
	public Long xdelReportMq(String key, redis.clients.jedis.StreamEntryID... ids) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xdel(key, ids) : 0L;
	}
	public Long xackDelReportMq(String key, String groupName, redis.clients.jedis.StreamEntryID id) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xackAndDel(key, groupName, id) : 0L;
	}
	public Map.Entry<redis.clients.jedis.StreamEntryID, List<redis.clients.jedis.resps.StreamEntry>> xautoclaimReportMq(String key, String group, String consumer, long minIdleMs, redis.clients.jedis.StreamEntryID start, int count) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xautoclaim(key, group, consumer, minIdleMs, start, count) : null;
	}
	public List<redis.clients.jedis.resps.StreamPendingSummary> xpendingReportMq(String key, String group) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xpending(key, group) : null;
	}
	public List<redis.clients.jedis.resps.Tuple> zpopminReportMq(String key) {
		return zpopmin(false, key);
	}
	public Long zaddReportMq(String key, double score, String member) {
		// C-4: doorbell 专用，异常/池缺失返回 null 以区分"失败"与"已存在(0)"。
		RedisConnection c = reportMq(member);
		return c != null ? c.zaddOrNull(key, score, member) : null;
	}
	public Set<String> zrangeReportMq(String key, long start, long end) {
		return zrange(false, key, start, end);
	}
	public Long zremReportMq(String key, String member) {
		RedisConnection c = reportMq(member);
		return c != null ? c.zremDoorbell(key, member) : 0L;
	}
	public Double zscoreReportMq(String key, String member) {
		RedisConnection c = reportMq(member);
		return c != null ? c.zscoreDoorbell(key, member) : null;
	}
	public long xlenReportMq(String key) {
		RedisConnection c = reportMq(key);
		return c != null ? c.xlen(key) : 0L;
	}

	/** Keys matching pattern on every report MQ master, listed with SCAN (KEYS would block Redis). */
	public Set<String> keysReportMq(String pattern) {
		return keys(false, pattern);
	}

	// --- the same call on every master ---
	private final java.util.concurrent.atomic.AtomicInteger popTurn = new java.util.concurrent.atomic.AtomicInteger();

	private List<redis.clients.jedis.resps.Tuple> zpopmin(boolean isSubmitMq, String key) {
		RedisConnection[] a = mqs(isSubmitMq);
		if (a.length == 0) {
			return null;
		}
		int first = a.length == 1 ? 0 : (popTurn.getAndIncrement() & Integer.MAX_VALUE) % a.length;
		List<redis.clients.jedis.resps.Tuple> last = null;
		for (int i = 0; i < a.length; i++) {
			last = a[(first + i) % a.length].zpopmin(key);
			if (last != null && !last.isEmpty()) {
				return last;
			}
		}
		return last;
	}

	private Set<String> zrange(boolean isSubmitMq, String key, long start, long end) {
		RedisConnection[] a = mqs(isSubmitMq);
		if (a.length <= 1) {
			return a.length == 0 ? null : a[0].zrangeDoorbell(key, start, end);
		}
		Set<String> out = null;
		for (RedisConnection c : a) {
			Set<String> one = c.zrangeDoorbell(key, start, end);
			if (one != null) {
				if (out == null) out = new java.util.LinkedHashSet<String>();
				out.addAll(one);
			}
		}
		return out;
	}

	private Set<String> keys(boolean isSubmitMq, String pattern) {
		RedisConnection[] a = mqs(isSubmitMq);
		if (a.length <= 1) {
			return a.length == 0 ? null : a[0].scanAll(pattern);
		}
		Set<String> out = null;
		for (RedisConnection c : a) {
			Set<String> one = c.scanAll(pattern);
			if (one != null) {
				if (out == null) out = new java.util.LinkedHashSet<String>();
				out.addAll(one);
			}
		}
		return out;
	}

	/** One XREADGROUP per master that holds some of the streams */
	private List<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>> xreadGroup(boolean isSubmitMq, String groupName,
			String consumerName, int count, int blockMillis, Map<String, redis.clients.jedis.StreamEntryID> streams) {
		RedisConnection[] a = mqs(isSubmitMq);
		if (a.length == 0) {
			return null;
		}
		if (a.length == 1 || streams == null || streams.size() <= 1) {
			String first = streams == null || streams.isEmpty() ? null : streams.keySet().iterator().next();
			return mq(first, isSubmitMq).xreadGroup(groupName, consumerName, count, blockMillis, streams);
		}
		Map<Integer, Map<String, redis.clients.jedis.StreamEntryID>> byMaster = new java.util.TreeMap<Integer, Map<String, redis.clients.jedis.StreamEntryID>>();
		for (Map.Entry<String, redis.clients.jedis.StreamEntryID> e : streams.entrySet()) {
			byMaster.computeIfAbsent(instanceOf(e.getKey(), a.length), k -> new java.util.LinkedHashMap<String, redis.clients.jedis.StreamEntryID>())
					.put(e.getKey(), e.getValue());
		}
		List<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>> out = null;
		for (Map.Entry<Integer, Map<String, redis.clients.jedis.StreamEntryID>> e : byMaster.entrySet()) {
			List<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>> one = a[e.getKey()].xreadGroup(groupName, consumerName, count, blockMillis, e.getValue());
			if (one != null) {
				if (out == null) out = new java.util.ArrayList<Map.Entry<String, List<redis.clients.jedis.resps.StreamEntry>>>();
				out.addAll(one);
			}
		}
		return out;
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
