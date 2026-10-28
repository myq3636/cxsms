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
 * One processing attempt of a stream entry: an inbound MT (stream:mt:pending), a vendor DR
 * (stream:core:inbound-dr:*) or an outbound result (stream:core:results*).
 *
 * <p>The streams are at-least-once: an entry that was processed but not acked (ack failed,
 * crash between produce and ack, or claimed by auto-claim while still being processed)
 * is processed again. The attempt keeps a marker hash next to the stream that records
 * which side effects are already done, so a redelivery repeats none of them:
 * core:mt:{msgId} on the submit MQ for MTs, core:att:{stream}:{id} on the report MQ for
 * vendor DRs and results (their msgId is not unique, the entry id is).
 * <ul>
 * <li>{@code _lease}: owner|startMs, a second consumer backs off while it is fresh;</li>
 * <li>{@code _done}: entry attempts only, written before the ack; a redelivery after a failed
 * ack is then only acked (a DR whose lookup entry is already deleted would take another path);</li>
 * <li>{@code p:{split}} / {@code dr:...}: produced to an outbound stream, written in the produce Lua;</li>
 * <li>{@code {category}#{n}:{detail}}: the n-th CDR / DR / DB queue insert / counter of the
 * attempt, recorded by {@link #once} and flushed with the next produce or at the end;</li>
 * <li>{@code x:{name}}: actions written at once ({@link #markDoneNow}), e.g. a reroute.</li>
 * </ul>
 * The marker is deleted in the same Lua as XACK+XDEL; once the entry is gone it cannot
 * come back, so markers only live for messages in flight. Each entry has its own marker:
 * regular retries (RMQ/RDQ resends) produce new entries and are processed normally.
 *
 * <p>Code anywhere on the processing thread calls {@link #once} before a side effect; with
 * no attempt on the thread (other consumers, retries from the DB queues, the switches
 * CoreMtIdempotency / CoreDrResultIdempotency off) it always returns true and nothing changes.
 */
public final class StreamAttempt {
	private static final SystemLogger log = SystemLogger.getSystemLogger(StreamAttempt.class);
	private static final ThreadLocal<StreamAttempt> current = new ThreadLocal<StreamAttempt>();

	private static final String MARKER_PREFIX = "core:mt:";
	private static final String ENTRY_MARKER_PREFIX = "core:att:";
	private static final String LEASE = "_lease";
	private static final String DONE = "_done";
	private static final String PART_PREFIX = "p:";
	private static final String MARK_PREFIX = "x:";

	public enum Start {
		/** first attempt, or a retry whose previous owner gave up or died */
		RUN,
		/** another consumer holds a fresh lease: leave the entry pending */
		BUSY,
		/** an earlier attempt finished it completely, only its ack failed: just ack */
		DONE
	}

	// KEYS[1]=marker ARGV[1]=owner ARGV[2]=nowMs ARGV[3]=leaseMs ARGV[4]=ttlSeconds
	private static final String CLAIM_LUA =
			"if redis.call('HEXISTS', KEYS[1], '" + DONE + "') == 1 then return {'DONE'} end "
			+ "local lease = redis.call('HGET', KEYS[1], '" + LEASE + "') "
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

	// KEYS[1]=marker ARGV[1]=field
	private static final String FORGET_LUA = "return redis.call('HDEL', KEYS[1], ARGV[1])";

	// KEYS[1]=stream KEYS[2]=marker ARGV[1]=group ARGV[2]=id
	private static final String ACK_LUA =
			"local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) "
			+ "if acked > 0 then redis.call('XDEL', KEYS[1], ARGV[2]) end "
			+ "redis.call('DEL', KEYS[2]) "
			+ "return acked";

	private final String markerKey;
	/** Redis holding the marker: the instance of the stream the entry comes from */
	private final RedisConnection redis;
	/** write _done before the ack, so a redelivery after a failed ack is not processed again */
	private final boolean trackDone;
	private final Map<String, String> recorded;
	private final Map<String, String> pending = new LinkedHashMap<String, String>();
	private final Map<String, Integer> sequence = new HashMap<String, Integer>();
	/** last field handed out by {@link #once} per category, for {@link #forget} */
	private final Map<String, String> lastField = new HashMap<String, String>();
	private final List<Runnable> afterDone = new ArrayList<Runnable>();

	private StreamAttempt(String markerKey, RedisConnection redis, boolean trackDone, Map<String, String> recorded) {
		this.markerKey = markerKey;
		this.redis = redis;
		this.trackDone = trackDone;
		this.recorded = recorded;
	}

	/** MT processing (MTStreamConsumer), CoreMtIdempotency */
	public static boolean enabled() {
		return !"false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("CoreMtIdempotency", "true"));
	}

	/** Vendor DR and outbound result processing, CoreDrResultIdempotency */
	public static boolean drResultEnabled() {
		return !"false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("CoreDrResultIdempotency", "true"));
	}

	public static StreamAttempt current() {
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

	/** Marker key of a stream entry: vendor DRs and results have no unique msgId, the entry id is. */
	public static String entryMarkerKey(String streamKey, String redisId) {
		return ENTRY_MARKER_PREFIX + streamKey + ":" + redisId;
	}

	/**
	 * Claim an MT (marker core:mt:{msgId} on the submit MQ). On RUN the attempt is bound to the
	 * thread until {@link #finish} or {@link #abandon}. Redis errors are thrown: the caller must not process.
	 */
	public static Start begin(GmmsMessage msg, String owner) {
		return claim(markerKey(msg), RedisClient.getInstance().getSubmitMqRedis(), false, owner, msg);
	}

	/**
	 * Claim a stream entry of the report MQ (vendor DR, outbound result): marker
	 * core:att:{stream}:{id} next to the stream, with _done tracking.
	 */
	public static Start beginEntry(String streamKey, GmmsMessage msg, RedisConnection streamRedis, String owner) {
		return claim(entryMarkerKey(streamKey, msg.getRedisStreamID()), streamRedis, true, owner, msg);
	}

	private static Start claim(String key, RedisConnection redis, boolean trackDone, String owner, GmmsMessage msg) {
		List<String> args = Arrays.asList(owner, String.valueOf(System.currentTimeMillis()),
				String.valueOf(longProperty("CoreMtLeaseMs", 60000L)),
				String.valueOf(longProperty("CoreMtMarkerTtlSeconds", 7L * 24 * 3600)));
		List<?> list = (List<?>) redis.evalScript(CLAIM_LUA, Arrays.asList(key), args);
		String state = String.valueOf(list.get(0));
		if ("BUSY".equals(state)) {
			return Start.BUSY;
		}
		if ("DONE".equals(state)) {
			log.info(msg, "Redelivered entry already processed completely, only acked. marker={}", key);
			return Start.DONE;
		}
		Map<String, String> recorded = new HashMap<String, String>();
		for (int i = 1; i + 1 < list.size(); i += 2) {
			recorded.put(String.valueOf(list.get(i)), String.valueOf(list.get(i + 1)));
		}
		current.set(new StreamAttempt(key, redis, trackDone, recorded));
		if (recorded.size() > 1) {
			log.info(msg, "Redelivered, skipping side effects already done: {}", recorded.keySet());
		}
		return Start.RUN;
	}

	/**
	 * Gate a side effect of the current attempt: false when an earlier attempt already did
	 * the n-th effect of this category with the same detail. Always true outside an attempt.
	 */
	public static boolean once(String category, String detail) {
		StreamAttempt a = current.get();
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
		a.lastField.put(category, field);
		return true;
	}

	/**
	 * The side effect just let through by {@link #once} for this category failed: drop its
	 * flag so the retry does it again.
	 */
	public static void forget(String category) {
		StreamAttempt a = current.get();
		if (a == null) {
			return;
		}
		String field = a.lastField.remove(category);
		if (field == null) {
			return;
		}
		if (a.pending.remove(field) == null && a.recorded.remove(field) != null) {
			try {
				a.redis.evalScript(FORGET_LUA, Arrays.asList(a.markerKey), Arrays.asList(field));
			} catch (Exception e) {
				log.warn("Attempt flag {} not removed, the retry will skip it. marker={}", field, a.markerKey, e);
			}
		}
	}

	/** Whether an earlier attempt recorded {@link #markDoneNow} for this name. */
	public static boolean isDone(String name) {
		StreamAttempt a = current.get();
		return a != null && (a.recorded.containsKey(MARK_PREFIX + name) || a.pending.containsKey(MARK_PREFIX + name));
	}

	/**
	 * Record a side effect that must not be repeated and cannot be undone (reroute, resend),
	 * written to Redis at once instead of with the next flush.
	 */
	public static void markDoneNow(String name) {
		StreamAttempt a = current.get();
		if (a == null) {
			return;
		}
		a.pending.put(MARK_PREFIX + name, "1");
		try {
			a.flush(false, false);
		} catch (Exception e) {
			log.warn("Attempt mark {} not written now, kept for the final flush. marker={}", name, a.markerKey, e);
		}
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

	/** Marker of the current attempt when it lives on redis (so a Lua there can write it), else null. */
	public static String currentMarkerOn(RedisConnection redis) {
		StreamAttempt a = current.get();
		return a == null || a.redis != redis ? null : a.markerKey;
	}

	/** Stream id recorded for a produce by an earlier attempt, null if not produced yet. */
	public static String produced(String field) {
		StreamAttempt a = current.get();
		if (a == null) {
			return null;
		}
		String id = a.recorded.get(field);
		return id != null ? id : a.pending.get(field);
	}

	public static String partField(int split) {
		return PART_PREFIX + split;
	}

	/** Pending side-effect flags as field,value pairs, for the produce Lua to write with the part. */
	public static List<String> pendingFlags() {
		StreamAttempt a = current.get();
		List<String> fv = new ArrayList<String>();
		if (a != null) {
			for (Map.Entry<String, String> e : a.pending.entrySet()) {
				fv.add(e.getKey());
				fv.add(e.getValue());
			}
		}
		return fv;
	}

	/** The produce Lua wrote field=id and the flags returned by {@link #pendingFlags}. */
	public static void produced(String field, String id, List<String> writtenFlags) {
		StreamAttempt a = current.get();
		if (a == null) {
			return;
		}
		a.recorded.put(field, id);
		for (int i = 0; i + 1 < writtenFlags.size(); i += 2) {
			a.recorded.put(writtenFlags.get(i), writtenFlags.get(i + 1));
			a.pending.remove(writtenFlags.get(i));
		}
	}

	/**
	 * Run action once the attempt is recorded as done (entry attempts: after _done is written),
	 * at once when there is no attempt. For cleanups a redelivery still needs, e.g. the DR lookup
	 * entry: deleted too early, a redelivery after a crash would take another path.
	 */
	public static void afterDone(Runnable action) {
		StreamAttempt a = current.get();
		if (a == null || !a.trackDone) {
			action.run();
		} else {
			a.afterDone.add(action);
		}
	}

	/** A produce to a stream on another Redis than the marker: recorded with the next flush. */
	public static void producedElsewhere(String field, String id) {
		StreamAttempt a = current.get();
		if (a != null) {
			a.pending.put(field, id);
		}
	}

	/**
	 * Processing succeeded: persist the remaining flags (and _done for entry attempts) so a
	 * failed ack is harmless, then XACK+XDEL+DEL marker in one Lua. Returns whether the entry was acked.
	 */
	public static boolean finish(String streamKey, String group, String redisId) {
		StreamAttempt a = current.get();
		current.remove();
		if (a == null) {
			return false;
		}
		a.flush(false, a.trackDone);
		for (Runnable action : a.afterDone) {
			try {
				action.run();
			} catch (Exception e) {
				log.warn("Attempt after-done action failed. marker={}", a.markerKey, e);
			}
		}
		Object r = a.redis.evalScript(ACK_LUA, Arrays.asList(streamKey, a.markerKey), Arrays.asList(group, redisId));
		return r instanceof Long && (Long) r > 0;
	}

	/** Ack an entry whose earlier attempt finished (Start.DONE), removing its marker. */
	public static boolean ackDone(String streamKey, String group, GmmsMessage msg, RedisConnection streamRedis) {
		Object r = streamRedis.evalScript(ACK_LUA,
				Arrays.asList(streamKey, entryMarkerKey(streamKey, msg.getRedisStreamID())),
				Arrays.asList(group, msg.getRedisStreamID()));
		return r instanceof Long && (Long) r > 0;
	}

	/** Processing failed: keep what was done, release the lease so the retry can start at once. */
	public static void abandon() {
		StreamAttempt a = current.get();
		current.remove();
		if (a == null) {
			return;
		}
		try {
			a.flush(true, false);
		} catch (Exception e) {
			log.warn("Attempt flush failed, the retry may repeat some side effects. marker={}", a.markerKey, e);
		}
	}

	private void flush(boolean releaseLease, boolean done) {
		if (pending.isEmpty() && !releaseLease && !done) {
			return;
		}
		List<String> args = new ArrayList<String>();
		args.add(releaseLease ? "1" : "0");
		for (Map.Entry<String, String> e : pending.entrySet()) {
			args.add(e.getKey());
			args.add(e.getValue());
		}
		if (done) {
			args.add(DONE);
			args.add("1");
		}
		redis.evalScript(FLUSH_LUA, Arrays.asList(markerKey), args);
		recorded.putAll(pending);
		pending.clear();
	}

	private static long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
