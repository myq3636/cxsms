package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;

/**
 * One processing attempt of an inbound MT taken from stream:mt:pending.
 *
 * <p>The stream is at-least-once: an entry that was processed but not acked (ack failed,
 * crash between produce and ack, or claimed by auto-claim while still being processed)
 * is processed again. The attempt keeps a marker hash core:mt:{msgId} on the submit MQ
 * Redis that records which side effects are already done, so a redelivery repeats none
 * of them:
 * <ul>
 * <li>{@code _lease}: owner|startMs, a second consumer backs off while it is fresh;</li>
 * <li>{@code p:{split}}: outbound part produced, written in the produce Lua;</li>
 * <li>{@code {category}#{n}:{detail}}: the n-th CDR / DR / DB backup / counter of the
 * attempt, recorded by {@link #once} and flushed with the next produce or at the end.</li>
 * </ul>
 * The marker is deleted in the same Lua as XACK+XDEL; once the entry is gone it cannot
 * come back, so markers only live for messages in flight.
 *
 * <p>Code anywhere on the processing thread calls {@link #once} before a side effect; with
 * no attempt on the thread (other consumers, RMQ retries, CoreMtIdempotency=false) it always
 * returns true and nothing changes.
 */
public final class MtAttempt {
	private static final SystemLogger log = SystemLogger.getSystemLogger(MtAttempt.class);
	private static final ThreadLocal<MtAttempt> current = new ThreadLocal<MtAttempt>();

	private static final String MARKER_PREFIX = "core:mt:";
	private static final String LEASE = "_lease";
	private static final String PART_PREFIX = "p:";

	public enum Start {
		/** first attempt, or a retry whose previous owner gave up or died */
		RUN,
		/** another consumer holds a fresh lease: leave the entry pending */
		BUSY
	}

	// KEYS[1]=marker ARGV[1]=owner ARGV[2]=nowMs ARGV[3]=leaseMs ARGV[4]=ttlSeconds
	private static final String CLAIM_LUA =
			"local lease = redis.call('HGET', KEYS[1], '" + LEASE + "') "
			+ "if lease then "
			+ "  local sep = string.find(lease, '|', 1, true) "
			+ "  if sep then "
			+ "    local owner = string.sub(lease, 1, sep - 1) "
			+ "    local at = tonumber(string.sub(lease, sep + 1)) "
			+ "    if owner ~= ARGV[1] and at and tonumber(ARGV[2]) - at < tonumber(ARGV[3]) then "
			+ "      return {'BUSY'} "
			+ "    end "
			+ "  end "
			+ "end "
			+ "redis.call('HSET', KEYS[1], '" + LEASE + "', ARGV[1] .. '|' .. ARGV[2]) "
			+ "redis.call('EXPIRE', KEYS[1], ARGV[4]) "
			+ "local all = redis.call('HGETALL', KEYS[1]) "
			+ "table.insert(all, 1, 'RUN') "
			+ "return all";

	// KEYS[1]=marker ARGV[1]=releaseLease(1/0) ARGV[2..]=field,value...
	private static final String FLUSH_LUA =
			"if #ARGV > 1 then redis.call('HSET', KEYS[1], unpack(ARGV, 2)) end "
			+ "if ARGV[1] == '1' then redis.call('HDEL', KEYS[1], '" + LEASE + "') end "
			+ "return 1";

	// KEYS[1]=stream KEYS[2]=marker ARGV[1]=group ARGV[2]=id
	private static final String ACK_LUA =
			"local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) "
			+ "if acked > 0 then redis.call('XDEL', KEYS[1], ARGV[2]) end "
			+ "redis.call('DEL', KEYS[2]) "
			+ "return acked";

	private final String markerKey;
	private final Map<String, String> recorded;
	private final Map<String, String> pending = new LinkedHashMap<String, String>();
	private final Map<String, Integer> sequence = new HashMap<String, Integer>();

	private MtAttempt(String markerKey, Map<String, String> recorded) {
		this.markerKey = markerKey;
		this.recorded = recorded;
	}

	public static boolean enabled() {
		return !"false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("CoreMtIdempotency", "true"));
	}

	public static MtAttempt current() {
		return current.get();
	}

	/** Marker key of a message, by msgId (stable across redelivery) with the stream id as fallback. */
	public static String markerKey(GmmsMessage msg) {
		String id = msg.getMsgID();
		if (id == null || id.length() == 0) {
			id = msg.getRedisStreamID();
		}
		return MARKER_PREFIX + id;
	}

	/**
	 * Claim the message for this thread. On RUN the attempt is bound to the thread until
	 * {@link #finish} or {@link #abandon}. Redis errors are thrown: the caller must not process.
	 */
	public static Start begin(GmmsMessage msg, String owner) {
		String key = markerKey(msg);
		List<String> args = Arrays.asList(owner, String.valueOf(System.currentTimeMillis()),
				String.valueOf(longProperty("CoreMtLeaseMs", 60000L)),
				String.valueOf(longProperty("CoreMtMarkerTtlSeconds", 7L * 24 * 3600)));
		Object r = redis().evalScript(CLAIM_LUA, Arrays.asList(key), args);
		List<?> list = (List<?>) r;
		if ("BUSY".equals(String.valueOf(list.get(0)))) {
			return Start.BUSY;
		}
		Map<String, String> recorded = new HashMap<String, String>();
		for (int i = 1; i + 1 < list.size(); i += 2) {
			recorded.put(String.valueOf(list.get(i)), String.valueOf(list.get(i + 1)));
		}
		MtAttempt attempt = new MtAttempt(key, recorded);
		current.set(attempt);
		if (recorded.size() > 1) {
			log.info(msg, "MT redelivered, skipping side effects already done: {}", recorded.keySet());
		}
		return Start.RUN;
	}

	/**
	 * Gate a side effect of the current attempt: false when an earlier attempt already did
	 * the n-th effect of this category with the same detail. Always true outside an attempt.
	 */
	public static boolean once(String category, String detail) {
		MtAttempt a = current.get();
		if (a == null) {
			return true;
		}
		Integer n = a.sequence.get(category);
		n = n == null ? 1 : n + 1;
		a.sequence.put(category, n);
		String field = category + "#" + n + ":" + detail;
		if (a.recorded.containsKey(field) || a.pending.containsKey(field)) {
			return false;
		}
		a.pending.put(field, "1");
		return true;
	}

	/**
	 * Counter that must move once per message (24h recipient limit, blackhole timer):
	 * incremented on the first attempt, only read back on a redelivery.
	 */
	public static long countOnce(String key, int expireSeconds) {
		RedisClient rc = GmmsUtility.getInstance().getRedisClient();
		if (once("cnt", key)) {
			Long v = rc.incrWithExpire(key, expireSeconds);
			return v == null ? 0 : v;
		}
		String v = rc.getString(key);
		try {
			return v == null ? 0 : Long.parseLong(v.trim());
		} catch (NumberFormatException e) {
			return 0;
		}
	}

	/** Marker key of the current attempt, null outside an attempt. */
	public static String currentMarker() {
		MtAttempt a = current.get();
		return a == null ? null : a.markerKey;
	}

	/** Stream id recorded for an outbound part by an earlier attempt, null if not produced yet. */
	public static String producedPart(int split) {
		MtAttempt a = current.get();
		return a == null ? null : a.recorded.get(PART_PREFIX + split);
	}

	public static String partField(int split) {
		return PART_PREFIX + split;
	}

	/** Pending side-effect flags as field,value pairs, for the produce Lua to write with the part. */
	public static List<String> pendingFlags() {
		MtAttempt a = current.get();
		List<String> fv = new ArrayList<String>();
		if (a != null) {
			for (Map.Entry<String, String> e : a.pending.entrySet()) {
				fv.add(e.getKey());
				fv.add(e.getValue());
			}
		}
		return fv;
	}

	/** The produce Lua wrote the part and the flags returned by {@link #pendingFlags}. */
	public static void partProduced(int split, String id, List<String> writtenFlags) {
		MtAttempt a = current.get();
		if (a == null) {
			return;
		}
		a.recorded.put(PART_PREFIX + split, id);
		for (int i = 0; i + 1 < writtenFlags.size(); i += 2) {
			a.recorded.put(writtenFlags.get(i), writtenFlags.get(i + 1));
			a.pending.remove(writtenFlags.get(i));
		}
	}

	/**
	 * Processing succeeded: persist the remaining flags (so a failed ack is harmless), then
	 * XACK+XDEL+DEL marker in one Lua. Returns whether the entry was acked.
	 */
	public static boolean finish(String streamKey, String group, String redisId) {
		MtAttempt a = current.get();
		current.remove();
		if (a == null) {
			return false;
		}
		a.flush(false);
		Object r = redis().evalScript(ACK_LUA, Arrays.asList(streamKey, a.markerKey), Arrays.asList(group, redisId));
		return r instanceof Long && (Long) r > 0;
	}

	/** Processing failed: keep what was done, release the lease so the retry can start at once. */
	public static void abandon() {
		MtAttempt a = current.get();
		current.remove();
		if (a == null) {
			return;
		}
		try {
			a.flush(true);
		} catch (Exception e) {
			log.warn("MT attempt flush failed, the retry may repeat some side effects. marker={}", a.markerKey, e);
		}
	}

	private void flush(boolean releaseLease) {
		if (pending.isEmpty() && !releaseLease) {
			return;
		}
		List<String> args = new ArrayList<String>();
		args.add(releaseLease ? "1" : "0");
		for (Map.Entry<String, String> e : pending.entrySet()) {
			args.add(e.getKey());
			args.add(e.getValue());
		}
		redis().evalScript(FLUSH_LUA, Arrays.asList(markerKey), args);
		recorded.putAll(pending);
		pending.clear();
	}

	static RedisConnection redis() {
		return RedisClient.getInstance().getSubmitMqRedis();
	}

	private static long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
