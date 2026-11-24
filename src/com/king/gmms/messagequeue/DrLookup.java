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
 * channel's DR comes, deleted after the final DR), on the report Redis (docs/DR匹配数据迁移设计.md): drl:{outMsgId} on the
 * report MQ master of the outMsgId (ShardHash), with its member in the ZSET drl:due of that master (score: dateIn ms);
 * the MQM job moves the ones older than Redis_RetrieveRedisInterval to WDQ through drl:due:processing ({@link #moveDue}).
 * The DR scheduling data of a Core shard (early DR queue with the lookup, delayed DRs, fake DR timeouts) is on the report
 * master of the shard ({@link #placeOf}).
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

	/** the report MQ master of an outMsgId */
	public static RedisConnection master(String outMsgId) {
		RedisConnection[] m = RedisClient.getInstance().mqs(false);
		return m.length == 1 ? m[0] : m[ShardHash.of(outMsgId, m.length)];
	}

	public static String key(String outMsgId) {
		return PREFIX + outMsgId;
	}

	/** the report MQ master of a Core shard (shard % masters, as the sharded streams): its DR scheduling data */
	public static RedisConnection placeOf(int shard) {
		RedisConnection[] m = RedisClient.getInstance().mqs(false);
		return m[Math.floorMod(shard, m.length)];
	}

	/** the report masters (each holds lookups and the scheduling data of its shards) */
	public static List<RedisConnection> masters() {
		return Arrays.asList(RedisClient.getInstance().mqs(false));
	}

	/**
	 * The original of the MT sent with this outMsgId (serialized), with ttlSeconds; false: not written (the caller
	 * stores it in WDQ)
	 */
	public static boolean put(GmmsMessage msg, String outMsgId, String value, int ttlSeconds) {
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

	/** the original (serialized) of this outMsgId; null: none */
	public static String get(String outMsgId) {
		return outMsgId == null ? null : master(outMsgId).getString(key(outMsgId));
	}

	/** seconds the lookup of this outMsgId still lives; -2: none */
	public static long ttl(String outMsgId) {
		if (outMsgId == null) {
			return -2L;
		}
		try (redis.clients.jedis.Jedis j = master(outMsgId).getJedis()) {
			return j.ttl(key(outMsgId));
		}
	}

	/** after the final DR */
	public static void delete(String outMsgId) {
		if (outMsgId != null) {
			master(outMsgId).evalScript(DELETE_LUA, Arrays.asList(key(outMsgId), DUE_ZSET), new ArrayList<String>());
		}
	}

	/** the outMsgIds that have a lookup: one MGET per master */
	public static Set<String> existing(List<String> outMsgIds) {
		Set<String> found = new HashSet<String>();
		if (outMsgIds == null || outMsgIds.isEmpty()) {
			return found;
		}
		Map<RedisConnection, List<String>> byMaster = new LinkedHashMap<RedisConnection, List<String>>();
		for (String id : outMsgIds) {
			byMaster.computeIfAbsent(master(id), k -> new ArrayList<String>()).add(id);
		}
		for (Map.Entry<RedisConnection, List<String>> e : byMaster.entrySet()) {
			List<String> keys = new ArrayList<String>();
			for (String id : e.getValue()) {
				keys.add(key(id));
			}
			List<String> values = e.getKey().mgetStrict(keys);
			for (int i = 0; i < e.getValue().size(); i++) {
				if (values != null && i < values.size() && values.get(i) != null) {
					found.add(e.getValue().get(i));
				}
			}
		}
		return found;
	}

	/** what the MQM job does with an original still without a final DR: stores it in WDQ */
	public interface Mover {
		boolean move(GmmsMessage original);
	}

	/**
	 * MQM job: on every report master the lookups whose dateIn is older
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
