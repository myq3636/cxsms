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
		// on the master of the stream it was read from (stream:mt:pending), as the batch claim
		return claim(markerKey(msg), RedisClient.getInstance().submitMq(msg.getRedisStreamKey()), false, owner, msg);
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

	/** CoreMtMarkerTtlSeconds: the fallback expiry of a marker */
	public static long markerTtlSeconds() {
		return longProperty("CoreMtMarkerTtlSeconds", 7L * 24 * 3600);
	}

	/** Marker key of the attempt on this thread, null without one. */
	public static String currentMarker() {
		StreamAttempt a = current.get();
		return a == null ? null : a.markerKey;
	}

	/** The Redis of the attempt's marker on this thread, null without one. */
	public static RedisConnection currentRedis() {
		StreamAttempt a = current.get();
		return a == null ? null : a.redis;
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

	/** Write the pending flags now (e.g. after a produce to another Redis); on an error they stay for the final flush. */
	public static void flushNow() {
		StreamAttempt a = current.get();
		if (a == null) {
			return;
		}
		try {
			a.flush(false, false);
		} catch (Exception e) {
			log.warn("Attempt flags not written now, kept for the final flush. marker={}", a.markerKey, e);
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

	/**
	 * After begin, for a copy that another consumer may also hold (taken by auto-claim, or read by a worker
	 * longer ago than the auto-claim idle time): false when the entry is still in the stream. true when the
	 * other consumer acked it after this copy was read: the ack also removed the marker, so begin saw a new
	 * message. The attempt is dropped with the marker it just created; the copy must not be processed.
	 */
	public static boolean entryGoneAfterBegin(String streamKey, String redisId, RedisConnection streamRedis) {
		StreamAttempt a = current.get();
		boolean gone;
		try {
			// through evalScript: a Redis error must not look like a missing entry
			gone = ((List<?>) streamRedis.evalScript("return redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1], 'COUNT', 1)",
					Arrays.asList(streamKey), Arrays.asList(redisId))).isEmpty();
		} catch (RuntimeException e) {
			abandon();
			throw e;
		}
		if (gone && a != null) {
			current.remove();
			a.redis.evalScript("return redis.call('DEL', KEYS[1])", Arrays.asList(a.markerKey), java.util.Collections.<String>emptyList());
		}
		return gone;
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
		// HSET of the fields, HDEL of the lease when it is given up: plain commands on one pipeline, in this order
		// (the earlier FLUSH Lua did the same two steps; docs/StreamAttempt按批处理设计.md 2.4). An error is thrown as before.
		final Map<String, String> fields = new LinkedHashMap<String, String>();
		for (int i = 1; i + 1 < args.size(); i += 2) {
			fields.put(args.get(i), args.get(i + 1));
		}
		final List<redis.clients.jedis.Response<?>> responses = new ArrayList<redis.clients.jedis.Response<?>>(2);
		redis.runPipeline(pl -> {
			if (!fields.isEmpty()) {
				responses.add(pl.hset(markerKey, fields));
			}
			if (releaseLease) {
				responses.add(pl.hdel(markerKey, LEASE));
			}
		});
		for (redis.clients.jedis.Response<?> r : responses) {
			r.get();
		}
		recorded.putAll(pending);
		pending.clear();
	}

	// ---------------------------------------------------------------- one batch of entries (docs/StreamAttempt按批处理设计.md)

	/** CoreStreamAttemptBatch: claim and ack a batch of stream entries in one Lua each */
	public static boolean batchEnabled() {
		return !"false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("CoreStreamAttemptBatch", "true").trim());
	}

	// KEYS = markers; ARGV as CLAIM_LUA. For each marker the CLAIM_LUA decision: {'DONE'}, {'BUSY'} or {'RUN', fields...}
	private static final String CLAIM_BATCH_LUA =
			"local out = {} "
			+ "for i = 1, #KEYS do "
			+ "  local k = KEYS[i] "
			+ "  if redis.call('HEXISTS', k, '" + DONE + "') == 1 then out[i] = {'DONE'} "
			+ "  else "
			+ "    local busy = false "
			+ "    local lease = redis.call('HGET', k, '" + LEASE + "') "
			+ "    if lease then "
			+ "      local sep = string.find(lease, '|', 1, true) "
			+ "      if sep then "
			+ "        local owner = string.sub(lease, 1, sep - 1) "
			+ "        local at = tonumber(string.sub(lease, sep + 1)) "
			+ "        if owner ~= ARGV[1] and at and tonumber(ARGV[2]) - at < tonumber(ARGV[3]) then busy = true end "
			+ "      end "
			+ "    end "
			+ "    if busy then out[i] = {'BUSY'} "
			+ "    else "
			+ "      redis.call('HSET', k, '" + LEASE + "', ARGV[1] .. '|' .. ARGV[2]) "
			+ "      redis.call('EXPIRE', k, ARGV[4]) "
			+ "      local all = redis.call('HGETALL', k) "
			+ "      table.insert(all, 1, 'RUN') "
			+ "      out[i] = all "
			+ "    end "
			+ "  end "
			+ "end "
			+ "return out";

	// KEYS[1]=stream KEYS[2..]=markers ARGV[1]=group ARGV[2..]=ids: ACK_LUA for each entry, returns the XACK results
	private static final String ACK_BATCH_LUA =
			"local r = {} "
			+ "for i = 2, #KEYS do "
			+ "  local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[i]) "
			+ "  if acked > 0 then redis.call('XDEL', KEYS[1], ARGV[i]) end "
			+ "  redis.call('DEL', KEYS[i]) "
			+ "  r[#r + 1] = acked "
			+ "end "
			+ "return r";

	/** Called for each entry acked at the end of a batch: its stream id (as read) and whether its XACK removed it. */
	public interface AckListener {
		void acked(GmmsMessage msg, String redisId, boolean acked);
	}

	/**
	 * The attempts of one batch read from a stream: claimed in one Lua ({@link #claim}), each entry bound to the thread
	 * when it is processed ({@link #begin}), its flags and _done written when it is done ({@link #finished}, as
	 * {@link StreamAttempt#finish} does), all acked in one Lua at the end ({@link #ackAll}). The side effects, their
	 * flags and when they are written are those of one entry at a time; only the claim and the ack are shared.
	 */
	public static final class Batch {
		private final String streamKey;
		private final String group;
		private final RedisConnection redis;
		private final boolean trackDone;
		private final String owner;
		private final long claimedAt;
		/** stream entry id -> its claim; an entry whose marker repeats in the batch is not here */
		private final Map<String, Object[]> claims = new HashMap<String, Object[]>();
		/**
		 * The entry id of each message as read: processing changes it (a produce sets the id of the new entry on the
		 * same object, e.g. the MT written to stream:mt:routed), the ack needs the id in this stream.
		 */
		private final Map<GmmsMessage, String> readIds = new java.util.IdentityHashMap<GmmsMessage, String>();
		private final List<GmmsMessage> toAck = new ArrayList<GmmsMessage>();
		private final List<String> toAckIds = new ArrayList<String>();
		private final List<String> toAckMarkers = new ArrayList<String>();
		/** after-done actions of the entries whose flags stay in memory ({@link #finished}): run once they are acked */
		private final List<Runnable> afterAck = new ArrayList<Runnable>();

		private Batch(String streamKey, String group, RedisConnection redis, boolean trackDone, String owner, long claimedAt) {
			this.streamKey = streamKey;
			this.group = group;
			this.redis = redis;
			this.trackDone = trackDone;
			this.owner = owner;
			this.claimedAt = claimedAt;
		}

		/**
		 * Claim the entries of a batch in one Lua. entries=true: vendor DRs, results (marker per entry, _done);
		 * false: MTs (marker per msgId). An entry whose marker comes again in the batch is left out ({@link #covers}
		 * false): the caller processes it on its own after {@link #ackAll}, when the first one's marker is gone.
		 * Redis errors are thrown: nothing of the batch may be processed.
		 */
		public static Batch claim(String streamKey, String group, List<GmmsMessage> msgs, RedisConnection redis,
				boolean entries, String owner) {
			long now = System.currentTimeMillis();
			Batch b = new Batch(streamKey, group, redis, entries, owner, now);
			List<String> keys = new ArrayList<String>();
			List<String> ids = new ArrayList<String>();
			java.util.Set<String> seen = new java.util.HashSet<String>();
			for (GmmsMessage m : msgs) {
				String id = m.getRedisStreamID();
				String key = entries ? entryMarkerKey(streamKey, id) : markerKey(m);
				if (id == null || !seen.add(key)) {
					continue;
				}
				b.readIds.put(m, id);
				keys.add(key);
				ids.add(id);
			}
			if (keys.isEmpty()) {
				return b;
			}
			List<String> args = Arrays.asList(owner, String.valueOf(now),
					String.valueOf(longProperty("CoreMtLeaseMs", 60000L)),
					String.valueOf(longProperty("CoreMtMarkerTtlSeconds", 7L * 24 * 3600)));
			List<?> out = (List<?>) redis.evalScript(CLAIM_BATCH_LUA, keys, args);
			for (int i = 0; i < keys.size(); i++) {
				List<?> one = (List<?>) out.get(i);
				String state = String.valueOf(one.get(0));
				Map<String, String> recorded = new HashMap<String, String>();
				for (int j = 1; j + 1 < one.size(); j += 2) {
					recorded.put(String.valueOf(one.get(j)), String.valueOf(one.get(j + 1)));
				}
				b.claims.put(ids.get(i), new Object[] { keys.get(i), Start.valueOf(state), recorded });
			}
			return b;
		}

		/** Whether the entry was claimed by this batch (false: its marker came again, process it on its own) */
		public boolean covers(GmmsMessage msg) {
			return msg != null && readIds.containsKey(msg);
		}

		/**
		 * The claim of this entry, as {@link StreamAttempt#begin} returns it; on RUN the attempt is bound to the
		 * thread. After half the lease (CoreMtLeaseMs) since the batch claim the entry is claimed again on its own,
		 * so that the lease is fresh when its processing starts.
		 */
		public Start begin(GmmsMessage msg) {
			Object[] c = claims.get(readIds.get(msg));
			String key = (String) c[0];
			if (System.currentTimeMillis() - claimedAt > longProperty("CoreMtLeaseMs", 60000L) / 2) {
				return StreamAttempt.claim(key, redis, trackDone, owner, msg);
			}
			Start state = (Start) c[1];
			if (state == Start.BUSY) {
				return Start.BUSY;
			}
			if (state == Start.DONE) {
				log.info(msg, "Redelivered entry already processed completely, only acked. marker={}", key);
				return Start.DONE;
			}
			@SuppressWarnings("unchecked")
			Map<String, String> recorded = new HashMap<String, String>((Map<String, String>) c[2]);
			current.set(new StreamAttempt(key, redis, trackDone, recorded));
			if (recorded.size() > 1) {
				log.info(msg, "Redelivered, skipping side effects already done: {}", recorded.keySet());
			}
			return Start.RUN;
		}

		/**
		 * The entry is processed; the ack waits for {@link #ackAll}. Its flags (and _done for entries) are written and its
		 * after-done actions run now, as {@link StreamAttempt#finish} does, unless they can stay in memory
		 * ({@link #flagsInMemory}): the ack deletes the marker anyway, and the after-done actions run after the ack.
		 */
		public void finished(GmmsMessage msg) {
			StreamAttempt a = current.get();
			current.remove();
			String id = readIds.get(msg);
			Object[] c = claims.get(id);
			if (a != null && flagsInMemory(a)) {
				afterAck.addAll(a.afterDone);
			} else if (a != null) {
				a.flush(false, a.trackDone);
				for (Runnable action : a.afterDone) {
					try {
						action.run();
					} catch (Exception e) {
						log.warn("Attempt after-done action failed. marker={}", a.markerKey, e);
					}
				}
			}
			toAck.add(msg);
			toAckIds.add(id);
			toAckMarkers.add(a != null ? a.markerKey : (String) c[0]);
		}

		/**
		 * CoreStreamAttemptBatchFlagsInMemory (default true): the flags of a finished entry are not written when it is
		 * acked within half the lease and none of them guards a message written out (a produce, p:; a delivery to a
		 * customer session; a reroute or resend mark, x:). A crash before the ack then repeats the rest (CDR, statistics,
		 * a DR to the customer); a message is never written out twice (docs/Redis消息操作次数统计.md).
		 */
		private boolean flagsInMemory(StreamAttempt a) {
			if ("false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("CoreStreamAttemptBatchFlagsInMemory", "true").trim())
					|| System.currentTimeMillis() - claimedAt > longProperty("CoreMtLeaseMs", 60000L) / 2) {
				return false;
			}
			for (String field : a.pending.keySet()) {
				if (field.startsWith(PART_PREFIX) || field.startsWith(MARK_PREFIX) || field.startsWith("delivery#")) {
					return false;
				}
			}
			return true;
		}

		/** An entry an earlier attempt finished (Start.DONE): only acked, with the batch */
		public void done(GmmsMessage msg) {
			String id = readIds.get(msg);
			Object[] c = claims.get(id);
			toAck.add(msg);
			toAckIds.add(id);
			toAckMarkers.add((String) c[0]);
		}

		/** XACK+XDEL+DEL marker of the finished entries in one Lua; listener gets each entry's result. Errors are thrown. */
		public void ackAll(AckListener listener) {
			if (toAck.isEmpty()) {
				return;
			}
			List<String> keys = new ArrayList<String>(toAckMarkers.size() + 1);
			keys.add(streamKey);
			keys.addAll(toAckMarkers);
			List<String> args = new ArrayList<String>(toAck.size() + 1);
			args.add(group);
			args.addAll(toAckIds);
			List<?> r = (List<?>) redis.evalScript(ACK_BATCH_LUA, keys, args);
			for (int i = 0; i < toAck.size(); i++) {
				Object v = i < r.size() ? r.get(i) : null;
				listener.acked(toAck.get(i), toAckIds.get(i), v instanceof Long && (Long) v > 0);
			}
			toAck.clear();
			toAckIds.clear();
			toAckMarkers.clear();
			for (Runnable action : afterAck) {
				try {
					action.run();
				} catch (Exception e) {
					log.warn("Attempt after-done action failed after the batch ack. stream={}", streamKey, e);
				}
			}
			afterAck.clear();
		}
	}

	private static long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
