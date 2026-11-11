package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * Way 4 of {@link FakeDr}: an MT the vendor accepted waits here for its final DR, member = outMsgId, score = when
 * its timeout is up. Sharded like the Core streams; each Core runs the shards it owns, so a moved shard goes on
 * on its new Core. A real DR and the timeout can meet: the member is in the ready or the processing ZSET, or
 * gone, and whoever removes it decides; a timeout that sent its DR leaves indr:once:{outMsgId} for the real DR.
 * Tuning shares the AutoInDR.* properties of {@link DelayedInDRDispatcher}.
 */
public class FakeDrTimeoutScheduler implements Runnable {
	private static final SystemLogger log = SystemLogger.getSystemLogger(FakeDrTimeoutScheduler.class);
	private static final FakeDrTimeoutScheduler instance = new FakeDrTimeoutScheduler();

	static final String READY_PREFIX = "zset:fakedr:timeout:";
	static final String PROCESSING_PREFIX = "zset:fakedr:timeout:processing:";
	static final String ONCE_PREFIX = "indr:once:";

	// KEYS: 1 from, 2 to; ARGV: 1 due (ms), 2 new score, 3 count. Moves up to count due members, returns them.
	private static final String CLAIM_LUA =
			"local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3])) "
			+ "for _, v in ipairs(m) do redis.call('ZREM', KEYS[1], v) redis.call('ZADD', KEYS[2], ARGV[2], v) end "
			+ "return m";
	// KEYS: 1 processing, 2 once, 3 the MT stored for its DR (key = outMsgId); ARGV: 1 member.
	// 1: send the timeout DR; 0: the real DR came first; -1: the stored MT is gone.
	private static final String FIRE_LUA =
			"if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end "
			+ "local ttl = redis.call('TTL', KEYS[3]) "
			+ "if ttl < 0 then return -1 end "
			+ "redis.call('SET', KEYS[2], 'TIMEOUT', 'EX', ttl) "
			+ "return 1";
	// KEYS: 1 ready, 2 processing, 3 once; ARGV: 1 member. 1: the real DR goes to the customer; 0: the timeout DR was sent.
	private static final String CANCEL_LUA =
			"if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then return 1 end "
			+ "if redis.call('ZREM', KEYS[2], ARGV[1]) == 1 then return 1 end "
			+ "if redis.call('GET', KEYS[3]) == 'TIMEOUT' then redis.call('DEL', KEYS[3]) return 0 end "
			+ "return 1";

	private volatile boolean running = false;
	private volatile Thread worker;

	public static FakeDrTimeoutScheduler getInstance() {
		return instance;
	}

	private static RedisConnection redis() {
		return GmmsUtility.getInstance().getRedisClient().getStateRedis();
	}

	static int shardOf(String outMsgId) {
		int total = GmmsUtility.getInstance().getTotalShards();
		return total <= 1 ? 0 : (outMsgId.hashCode() & Integer.MAX_VALUE) % total;
	}

	/** starts timing; false when it could not be stored (no timeout for this MT then) */
	public boolean schedule(String outMsgId, int seconds) {
		try {
			return redis().zaddOrNull(READY_PREFIX + shardOf(outMsgId), System.currentTimeMillis() + seconds * 1000L, outMsgId) != null;
		} catch (Exception e) {
			log.warn("fake DR timeout not started, outMsgId={}: {}", outMsgId, e.getMessage());
			return false;
		}
	}

	/** a final vendor DR came: true = it goes to the customer (timing ended), false = the timeout DR was sent */
	public boolean cancel(String outMsgId) {
		if (outMsgId == null) {
			return true;
		}
		int shard = shardOf(outMsgId);
		Object r = redis().evalScript(CANCEL_LUA,
				Arrays.asList(READY_PREFIX + shard, PROCESSING_PREFIX + shard, ONCE_PREFIX + outMsgId),
				Collections.singletonList(outMsgId));
		return !(r instanceof Number) || ((Number) r).longValue() != 0L;
	}

	public synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		worker = new Thread(this, "FakeDrTimeoutScheduler");
		worker.setDaemon(true);
		worker.start();
		// MTs being timed on the shards of this Core: one ZCARD per shard each report
		com.king.gmms.metrics.MetricsCollector.getInstance().registerGauge(
				com.king.gmms.metrics.MetricsNames.gauge("fakedr.timeout", "timing"), () -> timing());
		log.info("FakeDrTimeoutScheduler started");
	}

	private long timing() {
		long n = 0;
		try (redis.clients.jedis.Jedis j = redis().getJedis()) {
			for (Integer shard : GmmsUtility.getInstance().getMyShards()) {
				n += j.zcard(READY_PREFIX + shard);
			}
		}
		return n;
	}

	public synchronized void stop() {
		running = false;
		Thread t = worker;
		if (t != null) {
			t.interrupt();
		}
	}

	public void run() {
		while (running) {
			try {
				if (dueOnce() <= 0) {
					Thread.sleep(longProperty("AutoInDR.DispatcherIdleSleepMs", 200L));
				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				log.error("FakeDrTimeoutScheduler loop error", e);
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignored) {
					break;
				}
			}
		}
	}

	/** one round over this Core's shards; returns the members handled */
	int dueOnce() {
		Set<Integer> shards = GmmsUtility.getInstance().getMyShards();
		if (shards == null) {
			return 0;
		}
		int n = 0;
		long now = System.currentTimeMillis();
		String batch = String.valueOf(longProperty("AutoInDR.DelayBatchSize", 200L));
		for (Integer shard : shards) {
			if (shard == null) {
				continue;
			}
			String ready = READY_PREFIX + shard, processing = PROCESSING_PREFIX + shard;
			// a Core that died while handling its claim: back to ready
			redis().evalScript(CLAIM_LUA, Arrays.asList(processing, ready), Arrays.asList(String.valueOf(now), String.valueOf(now), batch));
			Object claimed = redis().evalScript(CLAIM_LUA, Arrays.asList(ready, processing),
					Arrays.asList(String.valueOf(now), String.valueOf(now + longProperty("AutoInDR.DelayClaimTimeoutMs", 60000L)), batch));
			for (String outMsgId : members(claimed)) {
				fire(processing, outMsgId);
				n++;
			}
		}
		return n;
	}

	private void fire(String processing, String outMsgId) {
		try {
			Object r = redis().evalScript(FIRE_LUA, Arrays.asList(processing, ONCE_PREFIX + outMsgId, outMsgId),
					Collections.singletonList(outMsgId));
			long code = r instanceof Number ? ((Number) r).longValue() : -1L;
			if (code == 0L) {
				return;
			}
			GmmsMessage mt = code == 1L ? SerializableHandler.convertRedisMssage2GmmsMessage(redis().getString(outMsgId)) : null;
			if (mt == null) {
				log.warn("fake DR timeout of outMsgId={}: the MT stored for its DR is gone, no timeout DR", outMsgId);
				return;
			}
			FakeDr.onTimeout(mt);
		} catch (Exception e) {
			log.warn("fake DR timeout of outMsgId={} failed: {}", outMsgId, e.getMessage());
		}
	}

	private static List<String> members(Object claimed) {
		List<String> l = new ArrayList<String>();
		if (claimed instanceof List) {
			for (Object o : (List<?>) claimed) {
				if (o instanceof byte[]) {
					l.add(new String((byte[]) o, java.nio.charset.StandardCharsets.UTF_8));
				} else if (o != null) {
					l.add(o.toString());
				}
			}
		}
		return l;
	}

	private static long longProperty(String key, long def) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(def)).trim());
		} catch (Exception e) {
			return def;
		}
	}
}
