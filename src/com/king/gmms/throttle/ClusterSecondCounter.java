package com.king.gmms.throttle;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.redis.RedisConnection;

import redis.clients.jedis.Response;

/**
 * Per-second counts shared by all nodes ({prefix}{id}:{second} on the state Redis, as the INCR per message wrote them),
 * counted in memory and synced every ThrottleInSyncIntervalMs: one pipeline of INCRBY (this node's count since the last
 * sync) per id, whose reply, the cluster total, gives the other nodes' count as of that sync. Until the first sync of a
 * second the other nodes are estimated from their last second. Redis down: this node counts alone.
 * docs/Redis消息操作次数统计.md
 */
final class ClusterSecondCounter {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ClusterSecondCounter.class);
	private static final int KEY_TTL_SECONDS = 2;

	private final String prefix;
	private final Map<Integer, Slot> slots = new ConcurrentHashMap<Integer, Slot>();
	private volatile ScheduledExecutorService syncer;

	private static final class Slot {
		long second;
		/** this node in this second; synced: the part of it already in Redis */
		long local, synced;
		/** the other nodes in this second as of the last sync */
		long others;
		/** the last second: its unsynced rest goes with the next sync, then prevOthers is its final count */
		long prevSecond = -1, prevLocal, prevSynced, prevOthers;
	}

	ClusterSecondCounter(String prefix) {
		this.prefix = prefix;
	}

	static long syncIntervalMs() {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty("ThrottleInSyncIntervalMs", "500").trim());
		} catch (Exception e) {
			return 500L;
		}
	}

	/** The cluster count of this second with this one; counted only when it is within limit. */
	long acquire(int id, int limit, long nowMs) {
		startSyncer();
		Slot s = slots.get(id);
		if (s == null) {
			slots.putIfAbsent(id, new Slot());
			s = slots.get(id);
		}
		long second = nowMs / 1000;
		synchronized (s) {
			roll(s, second);
			long others = Math.max(s.others, s.prevSecond == second - 1 ? s.prevOthers * (nowMs % 1000) / 1000 : 0);
			long count = s.local + others + 1;
			if (count <= limit) {
				s.local++;
			}
			return count;
		}
	}

	private static void roll(Slot s, long second) {
		if (s.second == second) {
			return;
		}
		if (s.second == second - 1) {
			s.prevSecond = s.second;
			s.prevLocal = s.local;
			s.prevSynced = s.synced;
			s.prevOthers = s.others;
		} else {
			s.prevSecond = -1;
			s.prevLocal = s.prevSynced = s.prevOthers = 0;
		}
		s.second = second;
		s.local = s.synced = s.others = 0;
	}

	private void startSyncer() {
		if (syncer != null) {
			return;
		}
		synchronized (this) {
			if (syncer == null) {
				ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor(r -> {
					Thread t = new Thread(r, "ThrottleSync-" + prefix);
					t.setDaemon(true);
					return t;
				});
				syncer = ses;
				ses.schedule(this::syncAndReschedule, Math.max(50L, syncIntervalMs()), TimeUnit.MILLISECONDS);
			}
		}
	}

	private void syncAndReschedule() {
		try {
			sync(System.currentTimeMillis());
		} catch (Throwable t) {
			log.warn("Throttle counter sync failed, counting locally until the next sync. prefix={}", prefix, t);
		} finally {
			syncer.schedule(this::syncAndReschedule, Math.max(50L, syncIntervalMs()), TimeUnit.MILLISECONDS);
		}
	}

	/** What one sync sends for one id */
	private static final class Push {
		int id;
		Slot slot;
		long second, delta, localAt;
		long prevSecond, prevDelta, prevLocalAt;
		Response<Long> total, prevTotal;
	}

	void sync(long nowMs) {
		long second = nowMs / 1000;
		final List<Push> pushes = new ArrayList<Push>();
		for (Map.Entry<Integer, Slot> e : slots.entrySet()) {
			Slot s = e.getValue();
			synchronized (s) {
				roll(s, Math.max(s.second, second));
				if (s.local == 0 && s.prevLocal == s.prevSynced && s.prevSecond != second - 1) {
					slots.remove(e.getKey(), s);   // idle: no count in this or the last second
					continue;
				}
				Push p = new Push();
				p.id = e.getKey();
				p.slot = s;
				p.second = s.second;
				p.delta = s.local - s.synced;
				p.localAt = s.local;
				s.synced = s.local;
				p.prevSecond = s.prevSecond;
				p.prevDelta = s.prevLocal - s.prevSynced;
				p.prevLocalAt = s.prevLocal;
				s.prevSynced = s.prevLocal;
				pushes.add(p);
			}
		}
		if (pushes.isEmpty()) {
			return;
		}
		RedisConnection redis = GmmsUtility.getInstance().getRedisClient() == null ? null
				: GmmsUtility.getInstance().getRedisClient().getStateRedis();
		if (redis == null) {
			undo(pushes);
			return;
		}
		try {
			redis.runPipeline(pl -> {
				for (Push p : pushes) {
					String key = prefix + p.id + ":" + p.second;
					p.total = pl.incrBy(key, p.delta);
					pl.expire(key, KEY_TTL_SECONDS);
					if (p.prevDelta > 0 && p.prevSecond >= 0) {
						String prevKey = prefix + p.id + ":" + p.prevSecond;
						p.prevTotal = pl.incrBy(prevKey, p.prevDelta);
						pl.expire(prevKey, KEY_TTL_SECONDS);
					}
				}
			});
		} catch (RuntimeException e) {
			undo(pushes);
			throw e;
		}
		for (Push p : pushes) {
			Slot s = p.slot;
			synchronized (s) {
				if (s.second == p.second) {
					s.others = Math.max(0, p.total.get() - p.localAt);
				} else if (s.prevSecond == p.second) {
					s.prevOthers = Math.max(0, p.total.get() - p.localAt);
				}
				if (p.prevTotal != null && s.prevSecond == p.prevSecond) {
					s.prevOthers = Math.max(0, p.prevTotal.get() - p.prevLocalAt);
				}
			}
		}
	}

	/** The sync did not reach Redis: its counts go with the next one. */
	private static void undo(List<Push> pushes) {
		for (Push p : pushes) {
			Slot s = p.slot;
			synchronized (s) {
				if (s.second == p.second) {
					s.synced -= p.delta;
				} else if (s.prevSecond == p.second) {
					s.prevSynced -= p.delta;
				}
				if (s.prevSecond == p.prevSecond) {
					s.prevSynced -= p.prevDelta;
				}
			}
		}
	}
}
