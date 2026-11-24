package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * The DR lookup: the original of an MT by its outMsgId (written when its submit result is processed, read when the
 * channel's DR comes, deleted after the final DR), docs/DR匹配数据迁移设计.md.
 *
 * <p>DrLookupStore (hot): state (default, as before: {outMsgId} and the hash DR{seconds of dateIn} on the state Redis)
 * or report: drl:{outMsgId} on the report MQ master of the outMsgId (ShardHash), with its member in the ZSET drl:due of
 * that master (score: dateIn ms); the MQM job moves the ones older than Redis_RetrieveRedisInterval to WDQ through
 * drl:due:processing ({@link #moveDue}). DrLookupLegacyRead (hot, default true): a lookup not found where DrLookupStore
 * says is looked for in the other place too (the entries written before a switch, either way).
 */
public final class DrLookup {
	private static final SystemLogger log = SystemLogger.getSystemLogger(DrLookup.class);
	public static final String PREFIX = "drl:";
	public static final String DUE_ZSET = "drl:due";
	public static final String PROCESSING_ZSET = "drl:due:processing";

	// KEYS[1]=entry KEYS[2]=due ARGV: 1 value, 2 ttl seconds, 3 score (dateIn ms)
	private static final String PUT_LUA =
			"redis.call('SETEX', KEYS[1], tonumber(ARGV[2]), ARGV[1]) "
			+ "redis.call('ZADD', KEYS[2], ARGV[3], KEYS[1]) "
			+ "return 1";

	// KEYS[1]=entry KEYS[2]=due. Not the member of processing: one claimed by the MQM job meanwhile is moved with the
	// value it already read either way, and its RESOLVE removes the member
	private static final String DELETE_LUA =
			"redis.call('DEL', KEYS[1]) redis.call('ZREM', KEYS[2], KEYS[1]) return 1";

	// KEYS[1]=due KEYS[2]=processing ARGV: 1 cutoff, 2 claim end, 3 count. {entry, value, ...} of the claimed ones
	private static final String CLAIM_LUA =
			"local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3])) "
			+ "local out = {} "
			+ "for i = 1, #m do "
			+ "  redis.call('ZREM', KEYS[1], m[i]) "
			+ "  redis.call('ZADD', KEYS[2], ARGV[2], m[i]) "
			+ "  out[#out + 1] = m[i] "
			+ "  out[#out + 1] = redis.call('GET', m[i]) or '' "
			+ "end "
			+ "return out";

	// KEYS[1]=processing ARGV = entries moved to WDQ (or gone): deleted
	private static final String RESOLVE_LUA =
			"for i = 1, #ARGV do redis.call('ZREM', KEYS[1], ARGV[i]) redis.call('DEL', ARGV[i]) end return #ARGV";

	// KEYS[1]=processing KEYS[2]=due ARGV: 1 now, 2 count: claims that ran out (MQM stopped) go back, due at once
	private static final String REQUEUE_LUA =
			"local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2])) "
			+ "for i = 1, #m do redis.call('ZREM', KEYS[1], m[i]) redis.call('ZADD', KEYS[2], 0, m[i]) end "
			+ "return #m";

	private DrLookup() {
	}

	/** DrLookupStore=report: the lookup is written on the report MQ masters */
	public static boolean onReport() {
		return "report".equalsIgnoreCase(property("DrLookupStore", "state"));
	}

	/** DrLookupLegacyRead (default true): a lookup not found is looked for in the other place too */
	public static boolean legacyRead() {
		return !"false".equalsIgnoreCase(property("DrLookupLegacyRead", "true"));
	}

	/** the report MQ master of an outMsgId */
	public static RedisConnection master(String outMsgId) {
		RedisConnection[] m = RedisClient.getInstance().mqs(false);
		return m.length == 1 ? m[0] : m[ShardHash.of(outMsgId, m.length)];
	}

	public static String key(String outMsgId) {
		return PREFIX + outMsgId;
	}

	/** where a lookup was found */
	public static final class Hit {
		public final String value;
		final boolean report;

		Hit(String value, boolean report) {
			this.value = value;
			this.report = report;
		}
	}

	/**
	 * The original of the MT sent with this outMsgId (serialized), written where DrLookupStore says, with ttlSeconds;
	 * false: not written (the caller stores it in WDQ)
	 */
	public static boolean put(GmmsMessage msg, String outMsgId, String value, int ttlSeconds) {
		if (!onReport()) {
			return RedisClient.getInstance().setPipeline(outMsgId, value, ttlSeconds, GmmsUtility.getInstance().getRedisDateIn(msg));
		}
		try {
			long score = msg.getDateIn() != null ? msg.getDateIn().getTime() : System.currentTimeMillis();
			master(outMsgId).evalScript(PUT_LUA, Arrays.asList(key(outMsgId), DUE_ZSET),
					Arrays.asList(value, String.valueOf(ttlSeconds), String.valueOf(score)));
			return true;
		} catch (Exception e) {
			log.warn(msg, "DR lookup not written to the report Redis. out={}", outMsgId, e);
			return false;
		}
	}

	/** the lookup of this outMsgId: where DrLookupStore says, then (DrLookupLegacyRead) the other place; null: none */
	public static Hit find(String outMsgId) {
		if (outMsgId == null) {
			return null;
		}
		boolean report = onReport();
		String v = get(outMsgId, report);
		if (v != null) {
			return new Hit(v, report);
		}
		if (legacyRead()) {
			v = get(outMsgId, !report);
			if (v != null) {
				return new Hit(v, !report);
			}
		}
		return null;
	}

	private static String get(String outMsgId, boolean report) {
		return report ? master(outMsgId).getString(key(outMsgId)) : RedisClient.getInstance().getString(outMsgId);
	}

	/** delete the lookup where it was found (after the final DR); the original gives the DR{seconds} hash of the state Redis */
	public static void delete(String outMsgId, Hit hit, GmmsMessage original) {
		if (hit == null || outMsgId == null) {
			return;
		}
		if (hit.report) {
			master(outMsgId).evalScript(DELETE_LUA, Arrays.asList(key(outMsgId), DUE_ZSET), new ArrayList<String>());
		} else {
			String hashKey = original == null ? null : GmmsUtility.getInstance().getRedisDateIn(original);
			if (hashKey != null) {
				RedisClient.getInstance().delPipeline(outMsgId, hashKey);
			} else {
				RedisClient.getInstance().del(outMsgId);
			}
		}
	}

	/** the outMsgIds that have a lookup (both places as find): one MGET per Redis */
	public static Set<String> existing(List<String> outMsgIds) {
		Set<String> found = new HashSet<String>();
		if (outMsgIds == null || outMsgIds.isEmpty()) {
			return found;
		}
		boolean report = onReport();
		List<String> missing = existingIn(outMsgIds, report, found);
		if (!missing.isEmpty() && legacyRead()) {
			existingIn(missing, !report, found);
		}
		return found;
	}

	/** adds the ones found to found, returns the others */
	private static List<String> existingIn(List<String> outMsgIds, boolean report, Set<String> found) {
		List<String> missing = new ArrayList<String>();
		Map<RedisConnection, List<String>> byRedis = new LinkedHashMap<RedisConnection, List<String>>();
		for (String id : outMsgIds) {
			RedisConnection r = report ? master(id) : RedisClient.getInstance().getStateRedis();
			byRedis.computeIfAbsent(r, k -> new ArrayList<String>()).add(id);
		}
		for (Map.Entry<RedisConnection, List<String>> e : byRedis.entrySet()) {
			List<String> keys = new ArrayList<String>();
			for (String id : e.getValue()) {
				keys.add(report ? key(id) : id);
			}
			List<String> values = e.getKey().mgetStrict(keys);
			for (int i = 0; i < e.getValue().size(); i++) {
				if (values != null && i < values.size() && values.get(i) != null) {
					found.add(e.getValue().get(i));
				} else {
					missing.add(e.getValue().get(i));
				}
			}
		}
		return missing;
	}

	/** what the MQM job does with an original still without a final DR: stores it in WDQ */
	public interface Mover {
		boolean move(GmmsMessage original);
	}

	/**
	 * MQM job (DrLookupStore=report, or entries left there): on every report master the lookups whose dateIn is older
	 * than olderThanMs, at most DrLookup.MoveBatchSize a master and round, go to WDQ and are deleted from Redis; one not
	 * stored stays claimed and is due again after DrLookup.ClaimTimeoutMs. Returns how many were moved.
	 */
	public static int moveDue(long olderThanMs, Mover mover) {
		RedisConnection[] masters = RedisClient.getInstance().mqs(false);
		int moved = 0;
		long now = System.currentTimeMillis();
		int batch = intProperty("DrLookup.MoveBatchSize", 500);
		long claimEnd = now + longProperty("DrLookup.ClaimTimeoutMs", 60000L);
		for (RedisConnection r : masters) {
			try {
				r.evalScript(REQUEUE_LUA, Arrays.asList(PROCESSING_ZSET, DUE_ZSET), Arrays.asList(String.valueOf(now), String.valueOf(batch)));
				List<?> claimed = (List<?>) r.evalScript(CLAIM_LUA, Arrays.asList(DUE_ZSET, PROCESSING_ZSET),
						Arrays.asList(String.valueOf(now - olderThanMs), String.valueOf(claimEnd), String.valueOf(batch)));
				List<String> done = new ArrayList<String>();
				for (int i = 0; i + 1 < claimed.size(); i += 2) {
					String entry = String.valueOf(claimed.get(i));
					String value = String.valueOf(claimed.get(i + 1));
					if (value.isEmpty()) {
						done.add(entry);   // its final DR came meanwhile, or it expired
						continue;
					}
					GmmsMessage original = SerializableHandler.convertRedisMssage2GmmsMessage(value);
					if (original == null) {
						log.warn("DR lookup not readable, dropped. key={}", entry);
						done.add(entry);
					} else if (mover.move(original)) {
						done.add(entry);
						moved++;
					} else {
						log.warn(original, "DR lookup not moved to WDQ, tried again after the claim. key={}", entry);
					}
				}
				if (!done.isEmpty()) {
					r.evalScript(RESOLVE_LUA, Arrays.asList(PROCESSING_ZSET), done);
				}
			} catch (Exception e) {
				log.warn("DR lookup move to WDQ failed on a report master, tried again later", e);
			}
		}
		return moved;
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
