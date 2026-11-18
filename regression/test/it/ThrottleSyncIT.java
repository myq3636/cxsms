package it;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

/**
 * Incoming throttle counted in memory, synced to Redis (ThrottleInSyncIntervalMs, docs/Redis消息操作次数统计.md):
 * one node alone, two nodes sharing the limit after a sync, the estimate of the other nodes at the start of a second,
 * the cluster count in Redis, one key per second. Two counters stand for two nodes; the times are given, the background sync is held off.
 */
public class ThrottleSyncIT extends ItSupport {
	static Constructor<?> ctor;
	static Method acquire, sync;

	static Object counter() throws Exception {
		return ctor.newInstance("throttle:it:");
	}

	static long acquire(Object c, int limit, long at) throws Exception {
		return (Long) acquire.invoke(c, 7, limit, at);
	}

	/** how many of n are let through */
	static int take(Object c, int n, int limit, long at) throws Exception {
		int ok = 0;
		for (int i = 0; i < n; i++) if (acquire(c, limit, at) <= limit) ok++;
		return ok;
	}

	public static void main(String[] args) throws Exception {
		boot();
		props.setProperty("ThrottleInSyncIntervalMs", "100000");   // the background sync does not run during the checks
		Class<?> k = Class.forName("com.king.gmms.throttle.ClusterSecondCounter");
		ctor = k.getDeclaredConstructor(String.class);
		ctor.setAccessible(true);
		acquire = k.getDeclaredMethod("acquire", int.class, int.class, long.class);
		acquire.setAccessible(true);
		sync = k.getDeclaredMethod("sync", long.class);
		sync.setAccessible(true);
		long second = System.currentTimeMillis() / 1000;
		long t = second * 1000 + 100;

		// ---- TS-01 one node: exactly the limit, no Redis call when counting ----
		Object solo = counter();
		jedis.configResetStat();
		int ok = take(solo, 8, 5, t);
		check(ok == 5 && !jedis.info("commandstats").contains("cmdstat_incrby"), "TS-01 one node, limit 5, 8 at once: " + ok + " let through, nothing sent to Redis");
		jedis.del("throttle:it:7:" + second);

		// ---- TS-02 two nodes, limit 10: after a sync the second node knows the first one's count ----
		Object a = counter(), b = counter();
		int okA = take(a, 6, 10, t);
		sync.invoke(a, t + 10);
		int okB = take(b, 1, 10, t + 20);
		sync.invoke(b, t + 30);
		okB += take(b, 9, 10, t + 40);
		check(okA == 6 && okB == 4, "TS-02 node A 6, then node B after the sync: " + okB + " let through (10 in all)");
		sync.invoke(b, t + 50);
		sync.invoke(a, t + 60);
		check("10".equals(jedis.get("throttle:it:7:" + second)), "TS-03 the cluster count in Redis: " + jedis.get("throttle:it:7:" + second));
		check(acquire(a, 10, t + 70) == 11, "TS-04 node A after its sync sees B's 4: the next is the 11th, refused");

		// ---- TS-05 the next second, before its first sync: the other nodes estimated from their last second ----
		long half = (second + 1) * 1000 + 500;
		long n = acquire(a, 10, half);
		check(n == 3, "TS-05 half a second into the next second: B's 4 of the last second count as 2, A's first is the 3rd: " + n);

		// ---- TS-06 another second goes to its own key, with a TTL ----
		Object c = counter();
		long t2 = (second + 3) * 1000 + 100;
		take(c, 3, 100, t2);
		sync.invoke(c, t2 + 10);
		check("3".equals(jedis.get("throttle:it:7:" + (second + 3))), "TS-06 3 counted, synced: " + jedis.get("throttle:it:7:" + (second + 3)));
		check(jedis.ttl("throttle:it:7:" + (second + 3)) > 0, "TS-07 the count key has a TTL");
		exit();
	}
}
