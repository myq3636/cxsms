package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * Vendor DRs that came before their submit result was processed (no DR lookup entry yet), on the state Redis:
 * Core puts one here ({@link #offer}) instead of storing it in SDQ; the MQM job (EarlyDrDispatcher) takes the due
 * ones ({@link #dispatchDue}) and writes them back to stream:core:inbound-dr, where Core matches them again. After
 * EarlyDr.MaxTries tries a DR goes the old way (SDQ). docs/5000TPS多主测试慢的环节分析.md 7.3
 *
 * <p>dr:early:{id} (hash: body, tries) with its member in zset:dr:early (score: when it is due). The MQM job moves
 * the due ones to zset:dr:early:processing (score: the end of its claim) and removes them once written back; a
 * claim that ran out (MQM stopped) is moved back.
 */
public final class EarlyDrQueue {
	private static final SystemLogger log = SystemLogger.getSystemLogger(EarlyDrQueue.class);
	public static final String READY_ZSET = "zset:dr:early";
	public static final String PROCESSING_ZSET = "zset:dr:early:processing";
	public static final String PREFIX = "dr:early:";

	// KEYS[1]=entry KEYS[2]=ready ARGV: 1 max tries, 2 body, 3 due, 4 ttl. The try number, negative when over the max
	private static final String OFFER_LUA =
			"local t = redis.call('HINCRBY', KEYS[1], 'tries', 1) "
			+ "if t > tonumber(ARGV[1]) then redis.call('DEL', KEYS[1]) return -t end "
			+ "redis.call('HSET', KEYS[1], 'body', ARGV[2]) "
			+ "redis.call('EXPIRE', KEYS[1], tonumber(ARGV[4])) "
			+ "redis.call('ZADD', KEYS[2], ARGV[3], KEYS[1]) "
			+ "return t";

	// KEYS[1]=ready KEYS[2]=processing ARGV: 1 now, 2 claim end, 3 count. {entry, body, ...} of the due ones
	private static final String CLAIM_LUA =
			"local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3])) "
			+ "local out = {} "
			+ "for i = 1, #m do "
			+ "  redis.call('ZREM', KEYS[1], m[i]) "
			+ "  redis.call('ZADD', KEYS[2], ARGV[2], m[i]) "
			+ "  out[#out + 1] = m[i] "
			+ "  out[#out + 1] = redis.call('HGET', m[i], 'body') or '' "
			+ "end "
			+ "return out";

	// KEYS[1]=processing ARGV = entries: written back, the body dropped (tries kept for the next time it comes)
	private static final String DONE_LUA =
			"for i = 1, #ARGV do redis.call('ZREM', KEYS[1], ARGV[i]) redis.call('HDEL', ARGV[i], 'body') end return #ARGV";

	// KEYS[1]=processing KEYS[2]=ready ARGV: 1 now, 2 count: claims that ran out go back to ready
	private static final String REQUEUE_LUA =
			"local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2])) "
			+ "for i = 1, #m do redis.call('ZREM', KEYS[1], m[i]) redis.call('ZADD', KEYS[2], ARGV[1], m[i]) end "
			+ "return #m";

	private EarlyDrQueue() {
	}

	/** EarlyDr.Enable (default true) */
	public static boolean enabled() {
		return !"false".equalsIgnoreCase(property("EarlyDr.Enable", "true"));
	}

	private static RedisConnection redis() {
		RedisClient rc = RedisClient.getInstance();
		return rc == null ? null : rc.getStateRedis();
	}

	static String entryKey(GmmsMessage dr) {
		String id = dr.getMsgID();
		return PREFIX + dr.getOutMsgID() + ":" + dr.getStatusCode() + ":" + (id == null ? "" : id);
	}

	/**
	 * Core: keep the DR for another try in EarlyDr.DelayMs. false when it was tried EarlyDr.MaxTries times already,
	 * or on a Redis error: the caller stores it as before.
	 */
	public static boolean offer(GmmsMessage dr) {
		RedisConnection r = redis();
		String body = SerializableHandler.convertGmmsMessage2RedisMessage(dr);
		if (r == null || body == null || dr.getOutMsgID() == null) {
			return false;
		}
		try {
			long due = System.currentTimeMillis() + longProperty("EarlyDr.DelayMs", 2000L);
			Object t = r.evalScript(OFFER_LUA, Arrays.asList(entryKey(dr), READY_ZSET), Arrays.asList(
					String.valueOf(intProperty("EarlyDr.MaxTries", 15)), body, String.valueOf(due),
					String.valueOf(intProperty("EarlyDr.TtlSeconds", 3600))));
			long tries = t instanceof Long ? (Long) t : -1L;
			if (tries < 0) {
				log.info(dr, "[VDR-EARLY] outMsgId={} still no original after {} tries, stored as before", dr.getOutMsgID(), -tries - 1);
				return false;
			}
			if (log.isInfoEnabled()) {
				log.info(dr, "[VDR-EARLY] outMsgId={} status={} has no original yet, try {} again in {} ms", dr.getOutMsgID(),
						dr.getStatusCode(), tries, longProperty("EarlyDr.DelayMs", 2000L));
			}
			return true;
		} catch (Exception e) {
			log.warn(dr, "[VDR-EARLY] not queued, stored as before. outMsgId={}", dr.getOutMsgID(), e);
			return false;
		}
	}

	/**
	 * MQM job: the due DRs (at most EarlyDr.BatchSize), written back to stream:core:inbound-dr. Returns how many were
	 * claimed. A DR that is not written back stays claimed and comes back after EarlyDr.ClaimTimeoutMs.
	 */
	public static int dispatchDue() {
		RedisConnection r = redis();
		if (r == null) {
			return 0;
		}
		long now = System.currentTimeMillis();
		int batch = intProperty("EarlyDr.BatchSize", 500);
		r.evalScript(REQUEUE_LUA, Arrays.asList(PROCESSING_ZSET, READY_ZSET), Arrays.asList(String.valueOf(now), String.valueOf(batch)));
		List<?> claimed = (List<?>) r.evalScript(CLAIM_LUA, Arrays.asList(READY_ZSET, PROCESSING_ZSET), Arrays.asList(
				String.valueOf(now), String.valueOf(now + longProperty("EarlyDr.ClaimTimeoutMs", 60000L)), String.valueOf(batch)));
		List<String> done = new ArrayList<String>();
		StreamQueueManager q = StreamQueueManager.getInstance();
		for (int i = 0; i + 1 < claimed.size(); i += 2) {
			String key = String.valueOf(claimed.get(i));
			String body = String.valueOf(claimed.get(i + 1));
			GmmsMessage dr = body.isEmpty() ? null : SerializableHandler.convertRedisMssage2GmmsMessage(body);
			if (dr == null) {
				log.warn("[VDR-EARLY] entry without a readable DR dropped. key={}", key);
				done.add(key);
				continue;
			}
			try {
				if (q.produceInboundDeliveryReport(dr)) {
					done.add(key);
				} else {
					log.warn(dr, "[VDR-EARLY] not written back, retried after the claim. key={}", key);
				}
			} catch (Exception e) {
				log.warn(dr, "[VDR-EARLY] not written back, retried after the claim. key={}", key, e);
			}
		}
		if (!done.isEmpty()) {
			r.evalScript(DONE_LUA, Arrays.asList(PROCESSING_ZSET), done);
		}
		return claimed.size() / 2;
	}

	private static String property(String key, String def) {
		return GmmsUtility.getInstance().getCommonProperty(key, def).trim();
	}

	private static int intProperty(String key, int def) {
		try {
			return Integer.parseInt(property(key, String.valueOf(def)));
		} catch (Exception e) {
			return def;
		}
	}

	private static long longProperty(String key, long def) {
		try {
			return Long.parseLong(property(key, String.valueOf(def)));
		} catch (Exception e) {
			return def;
		}
	}
}
