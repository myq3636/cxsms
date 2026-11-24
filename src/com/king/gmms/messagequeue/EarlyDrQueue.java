package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * Vendor DRs that came before their submit result was processed (no DR lookup entry yet), on the state Redis
 * (docs/DR早到重试与outbox应答超时改进设计.md 4). With T the response timeout of the DR's channel
 * ({@link A2PCustomerInfo#getSubmitRespTimeoutMs}) and t0 the time the Client received the DR, Core puts one here
 * ({@link #offer}) due at t0+T/2; the MQM job (EarlyDrDispatcher) takes the due ones ({@link #dispatchDue}), looks for
 * their originals itself (Redis, then WDQ) and writes only the matched ones back to stream:core:inbound-dr; one not
 * matched is due again at t0+T, and dropped after it. Whether to try again goes by the time only: a DR that waited in a
 * backlog is tried fewer times, never more, and the DRs without an original never go back to Core.
 *
 * <p>dr:early:{id} (hash: body, end = t0+T) with its member in zset:dr:early (score: when it is due). The MQM job moves
 * the due ones to zset:dr:early:processing (score: the end of its claim) and deletes them once settled; a claim that
 * ran out (MQM stopped) is moved back.
 *
 * <p>Where: with the DR lookup ({@link DrLookup}), on the report master of the outMsgId, so that the offer checks the
 * lookup in the same Lua: one written meanwhile is {@link Offer#FOUND}, the DR is matched at once
 * (docs/DR匹配数据迁移设计.md 3.4). The MQM job takes the due ones of every report master.
 */
public final class EarlyDrQueue {
	private static final SystemLogger log = SystemLogger.getSystemLogger(EarlyDrQueue.class);
	public static final String READY_ZSET = "zset:dr:early";
	public static final String PROCESSING_ZSET = "zset:dr:early:processing";
	public static final String PREFIX = "dr:early:";
	/** T of a DR whose channel is not configured */
	static final long DEFAULT_WAIT_MS = 60000L;

	public enum Offer {
		/** in the delay queue (or there already: a redelivery) */
		QUEUED,
		/** no original within T: dropped */
		DROPPED,
		/** not queued (Redis error): leave the DR pending, it is retried */
		ERROR,
		/** not queued: its DR lookup is there now (written since the DR was looked up): match it again */
		FOUND
	}

	// KEYS[1]=entry KEYS[2]=ready KEYS[3]=the DR lookup ARGV: 1 body, 2 end, 3 due, 4 ttl ms. 0: there already (a
	// redelivery of the DR), 2: the lookup is there now, not queued
	private static final String OFFER_LUA =
			"if redis.call('EXISTS', KEYS[3]) == 1 then return 2 end "
			+ "if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end "
			+ "redis.call('HSET', KEYS[1], 'body', ARGV[1], 'end', ARGV[2]) "
			+ "redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4])) "
			+ "redis.call('ZADD', KEYS[2], ARGV[3], KEYS[1]) "
			+ "return 1";

	// KEYS[1]=ready KEYS[2]=processing ARGV: 1 now, 2 claim end, 3 count. {entry, body, end, ...} of the due ones
	private static final String CLAIM_LUA =
			"local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[3])) "
			+ "local out = {} "
			+ "for i = 1, #m do "
			+ "  redis.call('ZREM', KEYS[1], m[i]) "
			+ "  redis.call('ZADD', KEYS[2], ARGV[2], m[i]) "
			+ "  local v = redis.call('HMGET', m[i], 'body', 'end') "
			+ "  out[#out + 1] = m[i] "
			+ "  out[#out + 1] = v[1] or '' "
			+ "  out[#out + 1] = v[2] or '0' "
			+ "end "
			+ "return out";

	// KEYS[1]=ready KEYS[2]=processing ARGV = {entry, at, ...}: at 0 deletes the entry (written back or dropped),
	// otherwise it is due again at at
	private static final String RESOLVE_LUA =
			"for i = 1, #ARGV, 2 do "
			+ "  redis.call('ZREM', KEYS[2], ARGV[i]) "
			+ "  if ARGV[i + 1] == '0' then redis.call('DEL', ARGV[i]) "
			+ "  elseif redis.call('EXISTS', ARGV[i]) == 1 then redis.call('ZADD', KEYS[1], ARGV[i + 1], ARGV[i]) end "
			+ "end "
			+ "return #ARGV / 2";

	// KEYS[1]=processing KEYS[2]=ready ARGV: 1 now, 2 count: claims that ran out go back to ready
	private static final String REQUEUE_LUA =
			"local m = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2])) "
			+ "for i = 1, #m do redis.call('ZREM', KEYS[1], m[i]) redis.call('ZADD', KEYS[2], ARGV[1], m[i]) end "
			+ "return #m";

	// counts for the [VDR-EARLY-STATS] line of each minute
	private static final AtomicLong queued = new AtomicLong(), matchedTry1 = new AtomicLong(),
			matchedTry2 = new AtomicLong(), dropped = new AtomicLong();
	private static final AtomicBoolean statsStarted = new AtomicBoolean(false);

	private EarlyDrQueue() {
	}

	/** EarlyDr.Enable (default true) */
	public static boolean enabled() {
		return !"false".equalsIgnoreCase(property("EarlyDr.Enable", "true"));
	}

	/** where the early DRs of this outMsgId are queued: with its DR lookup */
	private static RedisConnection redis(String outMsgId) {
		return RedisClient.getInstance() == null ? null : DrLookup.master(outMsgId);
	}

	static String entryKey(GmmsMessage dr) {
		String id = dr.getMsgID();
		return PREFIX + dr.getOutMsgID() + ":" + dr.getStatusCode() + ":" + (id == null ? "" : id);
	}

	/** T: how long the Client of the DR's channel waits for a submit response */
	public static long waitMs(GmmsMessage dr) {
		try {
			A2PCustomerInfo channel = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(dr.getRSsID());
			if (channel != null && channel.getSubmitRespTimeoutMs() > 0) {
				return channel.getSubmitRespTimeoutMs();
			}
		} catch (Exception ignored) {
			// not configured: the default
		}
		return DEFAULT_WAIT_MS;
	}

	/**
	 * Before t0+T: Core does not look in WDQ for a DR it did not find in Redis, it queues it at once; the MQM job looks
	 * in WDQ for a whole batch with one query at t0+T/2 (no SQL per early DR when the results lag)
	 */
	public static boolean young(GmmsMessage dr) {
		long now = System.currentTimeMillis();
		return now < receivedAt(dr, now) + waitMs(dr);
	}

	/** t0: when the Client received the DR (now when unknown or later than now) */
	static long receivedAt(GmmsMessage dr, long now) {
		long t0 = dr.getInTimeMs();
		return t0 > 0 && t0 <= now ? t0 : now;
	}

	/**
	 * Core, the DR has no original (Redis and WDQ): queue it for a try at t0+T/2 (or t0+T when that is past), or drop it
	 * when t0+T is past too.
	 */
	public static Offer offer(GmmsMessage dr) {
		startStats();
		long now = System.currentTimeMillis();
		long t = waitMs(dr);
		long t0 = receivedAt(dr, now);
		long end = t0 + t;
		if (now >= end || dr.getOutMsgID() == null) {
			drop(dr, now - t0, t, "no original at the first lookup");
			return Offer.DROPPED;
		}
		RedisConnection r = redis(dr.getOutMsgID());
		String body = SerializableHandler.convertGmmsMessage2RedisMessage(dr);
		if (body == null) {
			drop(dr, now - t0, t, "not serializable");
			return Offer.DROPPED;
		}
		if (r == null) {
			log.warn(dr, "[VDR-EARLY] not queued, no Redis: left pending. outMsgId={}", dr.getOutMsgID());
			return Offer.ERROR;
		}
		long due = now < t0 + t / 2 ? t0 + t / 2 : end;
		// only a safety net (an entry is deleted once settled): an hour at least, so that a lagging MQM job finds it
		long ttlMs = Math.max(TimeUnit.HOURS.toMillis(1), t + longProperty("EarlyDr.ClaimTimeoutMs", 60000L) + 60000L);
		try {
			Object n = r.evalScript(OFFER_LUA, Arrays.asList(entryKey(dr), READY_ZSET, DrLookup.key(dr.getOutMsgID())),
					Arrays.asList(body, String.valueOf(end), String.valueOf(due), String.valueOf(ttlMs)));
			if (Long.valueOf(2L).equals(n)) {
				if (log.isInfoEnabled()) {
					log.info(dr, "[VDR-EARLY] outMsgId={} status={} rssid={} its lookup came meanwhile, matched again",
							dr.getOutMsgID(), dr.getStatusCode(), dr.getRSsID());
				}
				return Offer.FOUND;
			}
			if (Long.valueOf(1L).equals(n)) {
				queued.incrementAndGet();
				if (log.isInfoEnabled()) {
					log.info(dr, "[VDR-EARLY] outMsgId={} status={} rssid={} has no original yet, tried again at +{} ms{}",
							dr.getOutMsgID(), dr.getStatusCode(), dr.getRSsID(), due - t0, due < end ? " and +" + t + " ms" : "");
				}
			}
			return Offer.QUEUED;
		} catch (Exception e) {
			log.warn(dr, "[VDR-EARLY] not queued: left pending. outMsgId={}", dr.getOutMsgID(), e);
			return Offer.ERROR;
		}
	}

	private static void drop(GmmsMessage dr, long waitedMs, long t, String reason) {
		dropped.incrementAndGet();
		log.warn(dr, "[VDR-DROP] outMsgId={} status={} rssid={} waitedMs={} waitMs={} reason={}", dr.getOutMsgID(),
				dr.getStatusCode(), dr.getRSsID(), waitedMs, t, reason);
	}

	/** one claimed entry */
	private static final class Due {
		final String key;
		final GmmsMessage dr;
		final long end;

		Due(String key, GmmsMessage dr, long end) {
			this.key = key;
			this.dr = dr;
			this.end = end;
		}
	}

	/**
	 * MQM job: the due DRs (at most EarlyDr.BatchSize): the ones whose original is there now are written back to
	 * stream:core:inbound-dr, the others are due again at t0+T or dropped after it. Returns how many were claimed. On
	 * an error the claimed ones come back after EarlyDr.ClaimTimeoutMs.
	 */
	public static int dispatchDue() {
		startStats();
		int n = 0;
		for (RedisConnection r : DrLookup.masters()) {
			try {
				n += dispatchDue(r);
			} catch (Exception e) {
				log.warn("[VDR-EARLY] due DRs not taken from a Redis, tried again later", e);
			}
		}
		return n;
	}

	private static int dispatchDue(RedisConnection r) {
		long now = System.currentTimeMillis();
		int batch = intProperty("EarlyDr.BatchSize", 500);
		r.evalScript(REQUEUE_LUA, Arrays.asList(PROCESSING_ZSET, READY_ZSET), Arrays.asList(String.valueOf(now), String.valueOf(batch)));
		List<?> claimed = (List<?>) r.evalScript(CLAIM_LUA, Arrays.asList(READY_ZSET, PROCESSING_ZSET), Arrays.asList(
				String.valueOf(now), String.valueOf(now + longProperty("EarlyDr.ClaimTimeoutMs", 60000L)), String.valueOf(batch)));
		if (claimed.isEmpty()) {
			return 0;
		}
		List<Due> due = new ArrayList<Due>();
		List<String> outMsgIds = new ArrayList<String>();
		for (int i = 0; i + 2 < claimed.size(); i += 3) {
			String body = String.valueOf(claimed.get(i + 1));
			GmmsMessage dr = body.isEmpty() ? null : SerializableHandler.convertRedisMssage2GmmsMessage(body);
			long end;
			try {
				end = Long.parseLong(String.valueOf(claimed.get(i + 2)));
			} catch (Exception e) {
				end = 0L;   // an entry of before (no end): this is its last try
			}
			due.add(new Due(String.valueOf(claimed.get(i)), dr, end));
			if (dr != null && dr.getOutMsgID() != null) {
				outMsgIds.add(dr.getOutMsgID());
			}
		}
		Set<String> found;
		try {
			found = originals(outMsgIds);
		} catch (Exception e) {
			log.warn("[VDR-EARLY] original lookup failed, {} DR(s) tried again after the claim", due.size(), e);
			return due.size();
		}
		// the matched ones are written back in parallel (EarlyDr.WriteBackThreads): one XADD each
		final StreamQueueManager q = StreamQueueManager.getInstance();
		java.util.Map<Due, java.util.concurrent.Future<Boolean>> written = new java.util.HashMap<Due, java.util.concurrent.Future<Boolean>>();
		for (final Due d : due) {
			if (d.dr != null && found.contains(d.dr.getOutMsgID())) {
				written.put(d, writeBackPool().submit(() -> q.produceInboundDeliveryReport(d.dr)));
			}
		}
		List<String> resolve = new ArrayList<String>();
		for (Due d : due) {
			if (d.dr == null) {
				log.warn("[VDR-DROP] entry without a readable DR. key={}", d.key);
				dropped.incrementAndGet();
				resolve.add(d.key);
				resolve.add("0");
				continue;
			}
			long t0 = receivedAt(d.dr, now);
			if (written.containsKey(d)) {
				try {
					if (!written.get(d).get()) {
						log.warn(d.dr, "[VDR-EARLY] not written back, tried again after the claim. key={}", d.key);
						continue;
					}
				} catch (Exception e) {
					log.warn(d.dr, "[VDR-EARLY] not written back, tried again after the claim. key={}", d.key, e);
					continue;
				}
				boolean first = now < d.end;
				(first ? matchedTry1 : matchedTry2).incrementAndGet();
				if (log.isInfoEnabled()) {
					log.info(d.dr, "[VDR-EARLY] outMsgId={} status={} original found on try {} after {} ms, written back",
							d.dr.getOutMsgID(), d.dr.getStatusCode(), first ? 1 : 2, now - t0);
				}
				resolve.add(d.key);
				resolve.add("0");
			} else if (now < d.end) {
				resolve.add(d.key);
				resolve.add(String.valueOf(d.end));
			} else {
				drop(d.dr, now - t0, waitMs(d.dr), "no original after the tries");
				resolve.add(d.key);
				resolve.add("0");
			}
		}
		if (!resolve.isEmpty()) {
			r.evalScript(RESOLVE_LUA, Arrays.asList(READY_ZSET, PROCESSING_ZSET), resolve);
		}
		return due.size();
	}

	private static volatile java.util.concurrent.ExecutorService writeBack;

	/** EarlyDr.WriteBackThreads (default 8, read once): the threads writing the matched DRs back to Core */
	private static java.util.concurrent.ExecutorService writeBackPool() {
		java.util.concurrent.ExecutorService p = writeBack;
		if (p == null) {
			synchronized (EarlyDrQueue.class) {
				if (writeBack == null) {
					int n = Math.max(1, intProperty("EarlyDr.WriteBackThreads", 8));
					java.util.concurrent.atomic.AtomicInteger seq = new java.util.concurrent.atomic.AtomicInteger();
					writeBack = Executors.newFixedThreadPool(n, runnable -> {
						Thread t = new Thread(runnable, "EarlyDrWriteBack-" + seq.incrementAndGet());
						t.setDaemon(true);
						return t;
					});
				}
				p = writeBack;
			}
		}
		return p;
	}

	/** the outMsgIds that have an original: their DR lookups (one MGET a Redis, DrLookup.existing), the others in WDQ with one query */
	private static Set<String> originals(List<String> outMsgIds) throws Exception {
		Set<String> found = new HashSet<String>();
		if (outMsgIds.isEmpty()) {
			return found;
		}
		found.addAll(DrLookup.existing(outMsgIds));
		List<String> missing = new ArrayList<String>();
		for (String id : outMsgIds) {
			if (!found.contains(id)) {
				missing.add(id);
			}
		}
		if (!missing.isEmpty()) {
			found.addAll(GmmsUtility.getInstance().getMessageStoreManager().existingOutMsgIDs(missing));
		}
		return found;
	}

	/** a [VDR-EARLY-STATS] line each minute with something to count (WARN when DRs were dropped) */
	private static void startStats() {
		if (!statsStarted.compareAndSet(false, true)) {
			return;
		}
		ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor(runnable -> {
			Thread t = new Thread(runnable, "EarlyDrStats");
			t.setDaemon(true);
			return t;
		});
		s.scheduleWithFixedDelay(() -> {
			long qd = queued.getAndSet(0), m1 = matchedTry1.getAndSet(0), m2 = matchedTry2.getAndSet(0),
					dr = dropped.getAndSet(0);
			if (qd + m1 + m2 + dr == 0) {
				return;
			}
			String line = "[VDR-EARLY-STATS] last minute: queued=" + qd + " matchedTry1=" + m1 + " matchedTry2=" + m2
					+ " dropped=" + dr;
			if (dr > 0) {
				log.warn(line);
			} else {
				log.info(line);
			}
		}, 60, 60, TimeUnit.SECONDS);
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
