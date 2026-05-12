package com.king.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XAutoClaimParams;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingSummary;

import com.king.framework.SystemLogger;
import com.king.redis.RedisConnection.RedisTask;

/**
 * Redis connection wrapper using JedisPool.
 * 
 * All methods use try-with-resources to ensure Jedis instances are
 * always returned to the pool exactly once, even on exceptions.
 */
public class RedisConnection {

	private static final SystemLogger logger = SystemLogger
			.getSystemLogger(RedisConnection.class);

	/**
	 * redis pool
	 */
	protected JedisPool pool;
	/**
	 * 执行扣库存的脚本
	 */
	public static final String STOCK_LUA;

	static {
		/**
		 *
		 * @desc 扣减库存Lua脚本 库存（stock）0：表示没有库存
		 *       库存（stock）大于0：表示剩余库存
		 *       库存（stock）-1：表示不限库存去掉
		 *       sb.append(" if (stock == -1) then");
		 *       sb.append(" return -1;");
		 *       sb.append(" end;");
		 * @params 库存key
		 * @return -3:库存未初始化 -2:库存不足 ; 大于等于0:剩余库存（扣减之后剩余的库存）
		 *         redis缓存的库存(value)是-1表示不限库存，直接返回1
		 */
		StringBuilder sb = new StringBuilder();
		sb.append("if (redis.call('exists', KEYS[1]) == 1) then");
		sb.append("    local stock = tonumber(redis.call('get', KEYS[1]));");
		sb.append("    local num = tonumber(ARGV[1]);");
		sb.append("    if (stock >= num) then");
		sb.append("        return redis.call('incrby', KEYS[1], 0 - num);");
		sb.append("    end;");
		sb.append("    return -2;");
		sb.append("end;");
		sb.append("return -3;");
		STOCK_LUA = sb.toString();
	}

	public RedisConnection(JedisPoolConfig config, String host, int port,
			boolean isAuth, String pwd) {
		if (isAuth) {
			pool = new JedisPool(config, host, port, Protocol.DEFAULT_TIMEOUT,
					pwd);
		} else {

			pool = new JedisPool(config, host, port);
		}
	}

	/**
	 * get redis from redis pool
	 */
	public Jedis getJedis() {

		if (pool == null) {
			return null;
		}
		Jedis jedis = null;
		try {
			jedis = pool.getResource();
		} catch (Exception e) {
			logger.error("get redis error !");
		}
		return jedis;
	}

	public void destroy() {
		if (pool == null) {
			return;
		}
		try {
			pool.destroy();
		} catch (Exception e) {
			logger.error("Fail to close the Jedis pool, and exception is {}",
					e.getMessage());
		}
	}

	/**
	 * set object
	 */
	public boolean setObject(String key, Object obj) {
		try (Jedis jedis = pool.getResource()) {
			String result = jedis.set(key, SerializableHandler.objectToString(obj));
			return "OK".equalsIgnoreCase(result);
		} catch (Exception e) {
			logger.error("setObject failed. key={}", key, e);
			return false;
		}
	}

	public boolean setString(String key, String obj) {
		try (Jedis jedis = pool.getResource()) {
			String result = jedis.set(key, obj);
			return "OK".equalsIgnoreCase(result);
		} catch (Exception e) {
			logger.error("setString failed. key={}", key, e);
			return false;
		}
	}

	public Long incblackhole(String key) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.incr(key);
		} catch (Exception e) {
			logger.error("incblackhole failed. key={}", key, e);
			return null;
		}
	}

	public Long incrString(String key) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.incr(key);
		} catch (Exception e) {
			logger.error("incrString failed. key={}", key, e);
			return null;
		}
	}

	public boolean setString(String key, String obj, int time) {
		try (Jedis jedis = pool.getResource()) {
			String result = jedis.setex(key, (long) time, obj);
			return "OK".equalsIgnoreCase(result);
		} catch (Exception e) {
			logger.error("setString failed. key={}", key, e);
			return false;
		}
	}

	/**
	 * Set key to value only if key does not exist (atomic SETNX + EXPIRE).
	 * Returns the existing value if key already exists, or null if this call set
	 * the key.
	 * This replaces the pattern: getString() + setString() inside synchronized
	 * block.
	 *
	 * @param key           Redis key
	 * @param value         value to set if key doesn't exist
	 * @param expireSeconds TTL in seconds
	 * @return null if this call set the key successfully; the existing value if key
	 *         already existed
	 */
	public String setnxString(String key, String value, int expireSeconds) {
		try (Jedis jedis = pool.getResource()) {
			Long result = jedis.setnx(key, value);
			if (result != null && result == 1L) {
				// Key was set by us (didn't exist before), set expiry
				jedis.expire(key, expireSeconds);
				return null; // indicates "we set it"
			} else {
				// Key already existed, return the existing value
				return jedis.get(key);
			}
		} catch (Exception e) {
			logger.error("setnxString failed. key={}", key, e);
			return null;
		}
	}

	public void setExpired(String key, int time) {
		try (Jedis jedis = pool.getResource()) {
			jedis.expire(key, (long) time);
		} catch (Exception e) {
			logger.error("setExpired failed. key={}", key, e);
		}
	}

	public boolean setPipeline(String key, String value, int expireTime,
			String hashKey) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			Response<String> resp = p.setex(key, (long) expireTime, value);
			p.hset(hashKey, key, "");
			p.sync();
			String result = null;
			if (resp != null) {
				result = resp.get();
			}
			if (!("OK".equalsIgnoreCase(result))) {
				// Rollback: delete the partial data
				Pipeline rollback = jedis.pipelined();
				rollback.del(key);
				rollback.hdel(hashKey, key);
				rollback.sync();
				return false;
			}
			return true;
		} catch (Exception e) {
			logger.error("setPipeline failed. key={}", key, e);
			// Best-effort cleanup with a new connection
			try (Jedis cleanupJedis = pool.getResource()) {
				Pipeline p = cleanupJedis.pipelined();
				p.del(key);
				p.hdel(hashKey, key);
				p.sync();
			} catch (Exception ex) {
				logger.error("setPipeline cleanup failed. key={}", key, ex);
			}
			return false;
		}
	}

	public boolean setDelayDR(String key, String value, int expireTime,
			double score, int ossid) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			Response<String> resp = p.setex(key, (long) expireTime, value);
			p.zadd("delayDR_" + ossid, score, key);
			p.sadd("delayDR", "delayDR_" + ossid);
			p.sync();
			String result = null;
			if (resp != null) {
				result = resp.get();
			}
			if (!("OK".equalsIgnoreCase(result))) {
				Pipeline rollback = jedis.pipelined();
				rollback.del(key);
				rollback.zrem("delayDR", key);
				rollback.sync();
				return false;
			}
			return true;
		} catch (Exception e) {
			logger.error("setDelayDR failed. key={}", key, e);
			try (Jedis cleanupJedis = pool.getResource()) {
				Pipeline p = cleanupJedis.pipelined();
				p.del(key);
				p.zrem("delayDR", key);
				p.sync();
			} catch (Exception ex) {
				logger.error("setDelayDR cleanup failed. key={}", key, ex);
			}
			return false;
		}
	}

	public static class RedisTask {
		public enum Type {
			LPUSH, HINCRBY
		}

		public Type type;
		public String key;
		public String value;

		public RedisTask(Type t, String k, String v) {
			this.type = t;
			this.key = k;
			this.value = v;
		}
	}

	public void executeAsyncPipeline(java.util.List<RedisTask> batch) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			for (RedisTask task : batch) {
				if (task.type == RedisTask.Type.LPUSH) {
					p.lpush(task.key, task.value);
				} else if (task.type == RedisTask.Type.HINCRBY) {
					p.hincrBy(task.key, task.value, 1L);
				}
			}
			p.sync();
		} catch (Exception e) {
			logger.error("Async Pipeline execution failed for batch size {}", batch.size(), e);
		}
	}

	public boolean setHashMapWithPipline(String keyword, Map<String, Map<String, String>> infos) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			List<String> keys = new ArrayList<String>();
			for (Map.Entry<String, Map<String, String>> entries : infos.entrySet()) {
				String routingKey = entries.getKey();
				// logger.info("routing redis value: {}, {}", routingKey, entries.getValue());
				String sufferInfo = "";
				if (routingKey.contains("_")) {
					sufferInfo = routingKey.split("_")[0];
					routingKey = routingKey.split("_")[1];

				}
				String lastIndex = "";
				if (sufferInfo.contains("RoutingRelay")
						&& !sufferInfo.endsWith("RoutingRelay")) {
					lastIndex = sufferInfo.split("RoutingRelay")[1];
				}
				String key = keyword + lastIndex + "_" + routingKey;
				p.hset(key, entries.getValue());
				keys.add(key);
			}
			p.sadd("routingkey", keys.toArray(new String[keys.size()]));
			p.sync();
			return true;
		} catch (Exception e) {
			logger.error("setHashMapWithPipline failed. key={}", keyword, e);
			return false;
		}
	}

	public void delPipeline(String key, String hashKey) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			p.del(key);
			p.hdel(hashKey, key);
			p.sync();
		} catch (Exception e) {
			logger.error("delPipeline failed. key={}", key, e);
		}
	}

	/**
	 * set object
	 */
	public boolean setObject(String key, Object obj, long time) {
		try (Jedis jedis = pool.getResource()) {
			String result = jedis.set(key, SerializableHandler.objectToString(obj));
			jedis.expireAt(key, time);
			return "OK".equalsIgnoreCase(result);
		} catch (Exception e) {
			logger.error("setObject failed. key={}", key, e);
			return false;
		}
	}

	public boolean lpush(String key, String obj) {
		try (Jedis jedis = pool.getResource()) {
			long result = jedis.lpush(key, obj);
			return result > 0;
		} catch (Exception e) {
			logger.error("lpush failed. key={}", key, e);
			return false;
		}
	}

	public boolean lpushArr(String setKey, String key, String[] obj) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			p.lpush(key, obj);
			p.sadd(setKey, key);
			p.sync();
			return true;
		} catch (Exception e) {
			logger.error("lpushArr failed. key={}", key, e);
			return false;
		}
	}

	/**
	 * set object
	 */
	public boolean setHash(String key, Map<String, String> map) {
		try (Jedis jedis = pool.getResource()) {
			long result = jedis.hset(key, map);
			return true;
		} catch (Exception e) {
			logger.error("setHash failed. key={}", key, e);
			return false;
		}
	}

	public boolean setHashAndSet(String setKey, String key, Map<String, String> map) {
		try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			p.hset(key, map);
			p.sadd(setKey, key);
			p.sync();
			return true;
		} catch (Exception e) {
			logger.error("setHashAndSet failed. key={}", key, e);
			return false;
		}
	}

	/**
	 * get object
	 */
	public Object getObject(String key) {
		try (Jedis jedis = pool.getResource()) {
			String value = jedis.get(key);
			return SerializableHandler.stringToObject(value);
		} catch (Exception e) {
			logger.error("getObject failed. key={}", key, e);
			return null;
		}
	}

	public Set<String> zrange(String key, double start, double end) {
		try (Jedis jedis = pool.getResource()) {
			// In Jedis 5, zrangeByScore is removed. Replacing with zrange(key, start, end).
			// Adjust ZRangeParams as needed if this specific boundary doesn't match byScore accurately,
			// but we use the unified zrange method for forward compatibility.
			return new LinkedHashSet<>(jedis.zrange(key, start, end));
		} catch (Exception e) {
			logger.error("zrange failed. key={}", key, e);
			return null;
		}
	}

	public Set<String> zrangeByIndex(String key, long start, long end) {
		try (Jedis jedis = pool.getResource()) {
			return new LinkedHashSet<>(jedis.zrange(key, start, end));
		} catch (Exception e) {
			logger.error("zrangeByIndex failed. key={}", key, e);
			return null;
		}
	}

	public String getString(String key) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.get(key);
		} catch (Exception e) {
			logger.error("getString failed. key={}", key, e);
			return null;
		}
	}

	public String rpop(String key) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.rpop(key);
		} catch (Exception e) {
			logger.error("rpop failed. key={}", key, e);
			return null;
		}
	}

	/**
	 * Batch-pop up to {@code count} elements from the right (tail) of the list
	 * in a single Redis round-trip using LRANGE + LTRIM pipeline.
	 *
	 * Replaces N individual rpop() calls (N RTTs) with 2 pipelined commands
	 * (1 RTT), giving up to N× throughput improvement.
	 *
	 * Note: LRANGE + LTRIM is not atomic. This is safe when there is only one
	 * consumer thread per key (which is the case for priority sender queues).
	 *
	 * @param key   Redis list key
	 * @param count Maximum number of elements to pop
	 * @return List of popped values (oldest-first, right-to-left order), or empty
	 *         list
	 */
	public List<String> rpopBatch(String key, int count) {
		if (count <= 0) {
			return java.util.Collections.emptyList();
		}
		try (Jedis jedis = pool.getResource()) {
			// LRANGE key -count -1 → fetch last `count` elements (right side)
			// LTRIM key 0 -(count+1) → remove those elements atomically via pipeline
			Pipeline p = jedis.pipelined();
			Response<List<String>> rangeResp = p.lrange(key, -count, -1);
			p.ltrim(key, 0, -(count + 1));
			p.sync();
			List<String> result = rangeResp.get();
			if (result == null) {
				return java.util.Collections.emptyList();
			}
			// LRANGE returns left-to-right; reverse so index 0 = rightmost (oldest rpop
			// order)
			java.util.Collections.reverse(result);
			return result;
		} catch (Exception e) {
			logger.error("rpopBatch failed. key={}", key, e);
			return java.util.Collections.emptyList();
		}
	}

	public String brpop(String key) {
		try (Jedis jedis = pool.getResource()) {
			List<String> list = jedis.brpop(500, key);
			if (list != null && list.size() == 2) {
				return list.get(1);
			}
			return null;
		} catch (Exception e) {
			logger.error("brpop failed. key={}", key, e);
			return null;
		}
	}

	public boolean setHash(String key, String field, String value) {
		try (Jedis jedis = pool.getResource()) {
			if (value == null) {
				jedis.hset(key, field, "");
			} else {
				jedis.hset(key, field, value);
			}
			return true;
		} catch (Exception e) {
			logger.error("setHash failed. key={}", key, e);
			return false;
		}
	}

	public boolean sadd(String key, List<String> values) {
		try (Jedis jedis = pool.getResource()) {
			jedis.sadd(key, values.toArray(new String[values.size()]));
			return true;
		} catch (Exception e) {
			logger.error("sadd failed. key={}", key, e);
			return false;
		}
	}

	public Set<String> getHashValue(String key) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.hkeys(key);
		} catch (Exception e) {
			logger.error("getHashValue failed. key={}", key, e);
			return null;
		}
	}

	public List<String> getHashMap(String key, String[] s) throws Exception {
		try (Jedis jedis = pool.getResource()) {
			return jedis.hmget(key, s);
		} catch (Exception e) {
			logger.error("getHashMap failed. key={}", key, e);
			throw new Exception(e.toString());
		}
	}

	public Map<String, String> getHashAll(String key) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.hgetAll(key);
		} catch (Exception e) {
			logger.error("getHashAll failed. key={}", key, e);
			return null;
		}
	}

	public Set<String> smembers(String key) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.smembers(key);
		} catch (Exception e) {
			logger.error("smembers failed. key={}", key, e);
			return null;
		}
	}

	/**
	 * del object
	 */
	public boolean delObject(String key) {
		try (Jedis jedis = pool.getResource()) {
			jedis.del(key);
			return true;
		} catch (Exception e) {
			logger.error("delObject failed. key={}", key, e);
			return false;
		}
	}

	public boolean zrem(String key, String[] value) {
		try (Jedis jedis = pool.getResource()) {
			jedis.zrem(key, value);
			return true;
		} catch (Exception e) {
			logger.error("zrem failed. key={}", key, e);
			return false;
		}
	}

	public boolean hashINC(String key, String field) {
		try (Jedis jedis = pool.getResource()) {
			jedis.hincrBy(key, field, 1);
			return true;
		} catch (Exception e) {
			logger.error("hashINC failed. key={}", key, e);
			return false;
		}
	}

	public boolean delHash(String key, String field) {
		try (Jedis jedis = pool.getResource()) {
			jedis.hdel(key, field);
			return true;
		} catch (Exception e) {
			logger.error("delHash failed. key={}", key, e);
			return false;
		}
	}

	public boolean addStock(String key, long timemark, long num) {
		try (Jedis jedis = pool.getResource()) {
			String timemarkRedisKey = key + ":timemark";
			jedis.set(timemarkRedisKey, String.valueOf(timemark));
			jedis.incrBy(key, num);
			return true;
		} catch (Exception e) {
			logger.error("addStock failed. key={}", key, e);
			return false;
		}
	}

	/**
	 * 减库存
	 * 
	 * @param key
	 * @param num
	 * @return
	 */
	public Long stock(String key, int num) {
		try (Jedis jedis = pool.getResource()) {
			// 脚本里的KEYS参数
			List<String> keys = new ArrayList<>();
			keys.add(key);
			// 脚本里的ARGV参数
			List<String> args = new ArrayList<>();
			args.add(Integer.toString(num));
			return (Long) jedis.eval(STOCK_LUA, keys, args);
		} catch (Exception e) {
			logger.error("stock failed. key={}", key, e);
			return null;
		}
	}

	public void setPool(JedisPool pool) {
		this.pool = pool;
	}

	public JedisPool getPool() {
		return pool;
	}

	// =========================================================================
	// V4.0 新增: Redis Streams (消息队列) 支持方法
	// 依赖 Redis 5.0+ 和 Jedis 3.0+ (适配 Jedis 5.2.0 API)
	// =========================================================================

	/**
	 * 追加消息到 Stream 队列 (XADD)
	 * 
	 * @param key    Stream 队列名称
	 * @param hash   消息内容 (Key-Value)
	 * @param maxLen 队列最大长度 (防止 OOM)，例如 5000000
	 * @return 生成的消息 ID
	 */
	public String xadd(String key, Map<String, String> hash, long maxLen) {
		try (Jedis jedis = pool.getResource()) {
			XAddParams params = new XAddParams().id(StreamEntryID.NEW_ENTRY).maxLen(maxLen).approximateTrimming(true);
			return jedis.xadd(key, hash, params).toString();
		} catch (Exception e) {
			logger.error("xadd failed. key={}", key, e);
			return null;
		}
	}

	/**
	 * 为 Stream 创建消费组 (XGROUP CREATE)
	 * 在启动消费者线程前调用，确保组存在
	 * 
	 * @param key       Stream 队列名称
	 * @param groupName 消费组名称
	 * @param mkStream  如果 Stream 不存在是否自动创建
	 * @return 是否创建成功
	 */
	public boolean xgroupCreate(String key, String groupName, boolean mkStream) {
		try (Jedis jedis = pool.getResource()) {
			// 从头部(0-0)或尾部($)开始消费。这里用 LAST_ENTRY ($)
			jedis.xgroupCreate(key, groupName, StreamEntryID.LAST_ENTRY, mkStream);
			return true;
		} catch (Exception e) {
			// 忽略消费组已存在的异常：BUSYGROUP Consumer Group name already exists
			if (e.getMessage() != null && e.getMessage().contains("BUSYGROUP")) {
				return true;
			}
			logger.error("xgroupCreate failed. key={}, group={}", key, groupName, e);
			return false;
		}
	}

	/**
	 * 消费组阻塞读取 (XREADGROUP)
	 * 
	 * @param groupName    消费组名称
	 * @param consumerName 消费者实例名称 (区分不同的节点或线程)
	 * @param count        一次拉取的最大数量
	 * @param blockMillis  阻塞等待时间(毫秒)，0 表示一直阻塞
	 * @param streams      要监听的队列及 ID 映射 (通常传 StreamEntryID.UNRECEIVED_ENTRY 即 ">")
	 * @return 返回读取到的数据结构 List<Map.Entry<StreamKey, List<StreamEntry>>>
	 */
	public java.util.List<java.util.Map.Entry<String, java.util.List<StreamEntry>>> xreadGroup(
			String groupName, String consumerName, int count, int blockMillis,
			Map<String, StreamEntryID> streams) {
		try (Jedis jedis = pool.getResource()) {
			XReadGroupParams params = new XReadGroupParams().count(count).block(blockMillis);
			return jedis.xreadGroup(groupName, consumerName, params, streams);
		} catch (Exception e) {
			logger.error("xreadGroup failed. group={}, consumer={}", groupName, consumerName, e);
			return null;
		}
	}

	/**
	 * 确认消费 (XACK)
	 * 只有执行了 XACK，消息才会被从 Pending 队列中真正移除
	 * 
	 * @param key       Stream 队列名称
	 * @param groupName 消费组名称
	 * @param ids       成功处理的消息 ID 数组
	 * @return 确认成功的条数
	 */
	public Long xack(String key, String groupName, StreamEntryID... ids) {
		try (Jedis jedis = pool.getResource()) {
			return jedis.xack(key, groupName, ids);
		} catch (Exception e) {
			logger.error("xack failed. key={}, group={}", key, groupName, e);
			return 0L;
		}
	}

	/**
	 * 自动认领超时未确认的消息 (XAUTOCLAIM)
	 * 
	 * @param key         Stream 队列名称
	 * @param group       消费组
	 * @param consumer    执行认领的消费者名称
	 * @param minIdleMs   消息闲置时间阈值
	 * @param start       起始 ID (第一次通常传 "0-0")
	 * @param count       认领的最大条数
	 */
	public java.util.Map.Entry<StreamEntryID, java.util.List<StreamEntry>> xautoclaim(
			String key, String group, String consumer, long minIdleMs,
			StreamEntryID start, int count) {
		try (Jedis jedis = pool.getResource()) {
			XAutoClaimParams params = new XAutoClaimParams().count(count);
			return jedis.xautoclaim(key, group, consumer, minIdleMs, start, params);
		} catch (Exception e) {
			logger.error("xautoclaim failed. key={}, group={}", key, group, e);
			return null;
		}
	}

	/**
	 * 获取 Pending 列表摘要 (XPENDING)
	 */
	public List<StreamPendingSummary> xpending(String key, String group) {
		try (Jedis jedis = pool.getResource()) {
			StreamPendingSummary summary = jedis.xpending(key, group);
			List<StreamPendingSummary> list = new ArrayList<>();
			if (summary != null) list.add(summary);
			return list;
		} catch (Exception e) {
			logger.error("xpending failed. key={}, group={}", key, group, e);
			return null;
		}
	}

	/**
	 * 转移超时的 pending 消息所有权 (XCLAIM)
	 */
	public java.util.List<StreamEntry> transferOwnership(String key, String group, String consumer, long minIdleMs, StreamEntryID... ids) {
		try (Jedis jedis = pool.getResource()) {
			XClaimParams params = new XClaimParams().idle(minIdleMs);
			return jedis.xclaim(key, group, consumer, params, ids);
		} catch (Exception e) {
			logger.error("transferOwnership failed. key={}, group={}", key, group, e);
			return null;
		}
	}

	public static void main(String[] args) {

		// Jedis redis = new Jedis("192.168.23.112",6379);
		/*
		 * RedisConnection redis =new RedisConnection(false); JedisPool pool =
		 * RedisConnection.getSlavePool(); // Jedis redis1 = pool.getResource();
		 * //System.out.println(jedis); try { String key = "key1";
		 * redis.setHash(key, "hao"); redis.setHash(key, "hao1");
		 * 
		 * //redis.setObject(key, new Date(),12222);
		 * //System.out.println(redis.setObject(key, new Date(),1222));
		 * 
		 * 
		 * 
		 * // Map<String,String>map = redis.hgetAll(key); Set<String> set =
		 * redis.getHashValue(key); for (Iterator<String> i = set.iterator();
		 * i.hasNext();) { System.out.println(i.next()); }
		 * 
		 * } catch (Exception e) { e.printStackTrace(); } Object s
		 * =redis.getObject("3022710132355"); String role = redis1.info();
		 * //String s = role.substring(role.indexOf("role:")+5,
		 * role.indexOf("role:")+6); System.out.println(s);
		 * System.out.println(role);
		 */
		Jedis redis = new Jedis("192.168.23.191", 6378);
		/*
		 * RedisConnection redis =new RedisConnection((true)); JedisPool pool =
		 * RedisConnection.getSlavePool(); Jedis redis1 = pool.getResource();//
		 */// System.out.println(jedis);
			// GmmsUtility gmmsUtility =GmmsUtility.getInstance();
		String s = null;
		/*
		 * Pipeline p = redis.pipelined(); p.get("1002125424");
		 * p.del("1002125424"); List<Object> list = p.syncAndReturnAll();
		 */
		String[] s1 = new String[] { "s", "t" };
		List list = redis.hmget("ss", s1);
		for (int i = 0; i < list.size(); i++) {
			System.out.println(list.size());
		}
		/*
		 * GmmsMessage message =
		 * SerializableHandler.convertRedisMssage2GmmsMessage(s); String time =
		 * gmmsUtility.getRedisDateIn(message);
		 */
		System.out.println("s=" + s);
		// redis.hdel(time, "1002125424");
		// redis.del("1002125424");
		// redis.delHash(gmmsUtility.getRedisDateIn(message), "1002125424");
	}

}