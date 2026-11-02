package it;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.king.gmms.connectionpool.connection.ConnectionManagerImpl;
import com.king.gmms.customerconnectionfactory.AbstractConnectionFactory;
import com.king.gmms.customerconnectionfactory.MultiSmppServerFactory;
import com.king.gmms.ha.SmppServerSessionRegistry;
import com.king.gmms.listener.smppserver.MultiSmppServer;

/** Session registry: batched writes, PDU refresh as a fallback, reconcile that survives failures. On a real MultiSmppServer. */
public class RegistryBatchIT extends ItSupport {
	static final int PORT = 17776;
	static final String MODULE = "MultiSmppServer1";
	static MultiSmppServer server;

	static Map<String, Long> stats() {
		Map<String, Long> m = new HashMap<String, Long>();
		for (String l : jedis.info("commandstats").split("\r?\n")) {
			if (l.startsWith("cmdstat_")) {
				m.put(l.substring(8, l.indexOf(':')), Long.parseLong(l.replaceAll("^[^:]*:calls=(\\d+),.*", "$1")));
			}
		}
		return m;
	}
	static long d(Map<String, Long> a, Map<String, Long> b, String c) { return b.getOrDefault(c, 0L) - a.getOrDefault(c, 0L); }
	static long logs(String text) throws Exception {
		long n = 0;
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(text)) n++;
		return n;
	}
	static long rounds() throws Exception { return logs("registry reconcile finished"); }
	static boolean nextRound(int count) throws Exception {
		long r = rounds();
		return waitFor(() -> rounds() >= r + count, 30000);
	}
	static Set<String> active(int ssid) { return jedis.zrange("smpp:server:active:" + ssid + ":" + MODULE, 0, -1) == null ? new HashSet<String>() : new HashSet<String>(jedis.zrange("smpp:server:active:" + ssid + ":" + MODULE, 0, -1)); }
	static SmppTestClient client(int c) throws Exception {
		SmppTestClient cl = new SmppTestClient("127.0.0.1", PORT, 10, null);
		if (!cl.bind("c" + c, "p" + c)) throw new IllegalStateException("bind c" + c);
		return cl;
	}
	static Set<String> detailKeys(int ssid) { return new HashSet<String>(jedis.keys("smpp:server:session:" + ssid + ":*")); }
	static void delAll(int ssid) { for (String k : jedis.keys("smpp:server:*:" + ssid + "*")) jedis.del(k); }
	static void reconcileControl(String method) throws Exception {
		Method m = MultiSmppServer.class.getDeclaredMethod(method);
		m.setAccessible(true);
		m.invoke(server);
	}

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		}
		exit();
	}

	static void run() throws Exception {
		boot();
		props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
		props.setProperty("SmppServerSessionRegistryReconcileIntervalMs", "2000");
		// started with the registry off: reconcile must still take over when it is switched on
		props.setProperty("SmppServerSessionRegistryEnable", "false");
		server = new MultiSmppServer();
		server.startService();
		Thread.sleep(1000);
		SmppTestClient c0 = client(20);
		Thread.sleep(2500);
		check(active(20).isEmpty(), "RB-00a registry off: no entries");
		props.setProperty("SmppServerSessionRegistryEnable", "true");
		check(waitFor(() -> active(20).size() == 1, 10000), "RB-00b switched on by reload: next reconcile round registers (before: no reconcile task at all)");

		// ---- bind: 2 round trips, same content ----
		reconcileControl("stopSessionRegistryReconcile");
		Thread.sleep(500);
		Map<String, Long> s0 = stats();
		SmppTestClient c1 = client(21);
		waitFor(() -> active(21).size() == 1 && jedis.zcard("smpp:server:module:21") == 1, 3000);
		Thread.sleep(200);
		Map<String, Long> s1 = stats();
		check(d(s0, s1, "mget") == 1 && d(s0, s1, "setex") == 2 && d(s0, s1, "zadd") == 5, "RB-01a bind: 1 MGET + pipeline of 2 SETEX and 5 ZADD (get=" + d(s0, s1, "get") + " zadd=" + d(s0, s1, "zadd") + ")");
		String key = active(21).iterator().next();
		String detail = jedis.get("smpp:server:session:21:" + key);
		check(detail != null && detail.contains("module=" + MODULE) && detail.contains("lastSeen=") && detail.contains("ossid=21") && jedis.ttl("smpp:server:session:21:" + key) > 0,
				"RB-01b detail written with the same fields and a TTL");
		check(jedis.zrange("smpp:server:module:21", 0, -1).contains(MODULE) && jedis.zcard("smpp:server:active:21") == 1
				&& !jedis.keys("smpp:server:conn:21:*").isEmpty() && !jedis.keys("smpp:server:node:21:*").isEmpty() && !jedis.keys("smpp:server:slot:21:*").isEmpty(),
				"RB-01c all indexes written: module, active, conn, node, slot");
		check(SmppServerSessionRegistry.getInstance().hasActiveSessionInModule(21, MODULE), "RB-01d Core's lookup finds it");
		c1.socket.close();
		check(waitFor(() -> active(21).isEmpty() && detailKeys(21).isEmpty(), 8000), "RB-02 disconnect: entry removed");

		// ---- reconcile in batches ----
		List<SmppTestClient> all = new ArrayList<SmppTestClient>();
		all.add(c0);
		for (int c = 21; c < 30; c++) for (int k = 0; k < 6; k++) all.add(client(c)); // c30 has ConnectionSilentTime: 3
		props.setProperty("SmppServerSessionRegistryBatchSize", "200");
		props.setProperty("SmppServerSessionRegistryReconcileIntervalMs", "1000");
		reconcileControl("startSessionRegistryReconcile");
		nextRound(1);
		s0 = stats();
		long r0 = rounds();
		nextRound(2);
		s1 = stats();
		long done = rounds() - r0;
		check(d(s0, s1, "mget") / done == 1 && d(s0, s1, "get") == 0, "RB-03a reconcile of 55 connections, batch 200: 1 MGET per round, no single GET (rounds=" + done + ", mget=" + d(s0, s1, "mget") + ")");
		check(d(s0, s1, "setex") / done == 110, "RB-03b 2 SETEX per connection (" + d(s0, s1, "setex") / done + ")");
		props.setProperty("SmppServerSessionRegistryBatchSize", "10");
		nextRound(1);
		s0 = stats();
		r0 = rounds();
		nextRound(2);
		s1 = stats();
		done = rounds() - r0;
		check(d(s0, s1, "mget") / done == 6, "RB-03c batch 10 by reload: 6 MGET per round (" + d(s0, s1, "mget") / done + ")");
		props.setProperty("SmppServerSessionRegistryBatchSize", "200");
		check(logs("failed=0, removed=0") >= 2, "RB-03d each round logged with ok/failed/removed");

		// ---- PDU path does not write while reconcile is healthy ----
		props.setProperty("SmppServerSessionRegistryRefreshIntervalMs", "4000");
		long self0 = logs("refreshed itself");
		s0 = stats();
		r0 = rounds();
		long end = System.currentTimeMillis() + 7000;
		while (System.currentTimeMillis() < end) { c0.send(0x15, 0, 777, new byte[0]); Thread.sleep(100); }
		s1 = stats();
		done = rounds() - r0;
		check(d(s0, s1, "mget") <= done + 1 && logs("refreshed itself") == self0, "RB-04 enquire_link every 100ms, refresh interval 4s, reconcile each 1s: PDU path never writes (mget=" + d(s0, s1, "mget") + ", rounds=" + done + ")");

		// ---- reconcile stopped: a session with PDUs heals itself, at most one write per interval ----
		reconcileControl("stopSessionRegistryReconcile");
		Thread.sleep(500);
		props.setProperty("SmppServerSessionRegistryRefreshIntervalMs", "2000");
		delAll(20);
		check(active(20).isEmpty(), "RB-05a entries of customer 20 gone");
		final SmppTestClient c0f = c0;
		check(waitFor(() -> { c0f.send(0x15, 0, 778, new byte[0]); return active(20).size() == 1; }, 8000), "RB-05b no reconcile: the next PDU after the interval restores the entry");
		s0 = stats();
		end = System.currentTimeMillis() + 3000;
		while (System.currentTimeMillis() < end) { c0.send(0x15, 0, 779, new byte[0]); Thread.sleep(50); }
		s1 = stats();
		check(d(s0, s1, "mget") <= 2, "RB-05c 60 PDUs in 3s, interval 2s: at most 2 writes (" + d(s0, s1, "mget") + ")");
		check(logs("refreshed itself") > self0, "RB-05d logged: reconcile did not reach it");
		delAll(22);
		Thread.sleep(2500);
		check(active(22).isEmpty(), "RB-05e known limit: a session without PDUs is not restored while reconcile is stopped");
		// a reconcile that is stuck for long is an ERROR
		props.setProperty("SmppServerSessionRegistryReconcileIntervalMs", "200");
		Thread.sleep(1200);
		Field lastLog = SmppServerSessionRegistry.class.getDeclaredField("lastSelfRefreshLogMs");
		lastLog.setAccessible(true);
		lastLog.set(SmppServerSessionRegistry.getInstance(), 0L);
		delAll(20);
		waitFor(() -> { c0f.send(0x15, 0, 780, new byte[0]); return active(20).size() == 1; }, 8000);
		check(logs("reconcile has not finished for") >= 1, "RB-05f reconcile not finishing for over 5 intervals: ERROR");
		props.setProperty("SmppServerSessionRegistryReconcileIntervalMs", "1000");
		props.setProperty("SmppServerSessionRegistryRefreshIntervalMs", "180000");
		reconcileControl("startSessionRegistryReconcile");
		check(waitFor(() -> active(22).size() == 6, 12000), "RB-05g reconcile back: the session without PDUs is restored");

		// ---- Redis failure: connections stay, entries stay, next rounds repair ----
		int liveBefore = active(23).size();
		long warn0 = logs("Failed to write SMPP server bind registry");
		new Thread(() -> { try (redis.clients.jedis.Jedis j = new redis.clients.jedis.Jedis("127.0.0.1", 16379)) { j.clientPause(5000); } }).start();
		Thread.sleep(5500);
		check(waitFor(() -> logs("Failed to write SMPP server bind registry") > warn0, 10000), "RB-06a Redis paused longer than the socket timeout: batch failure logged");
		check(nextRound(1) && active(23).size() == liveBefore, "RB-06b after the pause: rounds go on, entries still there (" + active(23).size() + ")");
		boolean up = true;
		for (SmppTestClient c : all) if (c.socket.isClosed()) up = false;
		check(up, "RB-06c no client connection closed");
		Thread.sleep(1500);
		check(logs("failed=0, removed=0") > 0 && SmppServerSessionRegistry.getInstance().hasActiveSessionInModule(23, MODULE), "RB-06d registry healthy again");

		// ---- Error in a round must not end the rounds ----
		final AtomicBoolean once = new AtomicBoolean(true);
		Field mapF = AbstractConnectionFactory.class.getDeclaredField("ssid2connectionManagers");
		mapF.setAccessible(true);
		MultiSmppServerFactory factory = MultiSmppServerFactory.getInstance();
		@SuppressWarnings("unchecked") ConcurrentHashMap<String, Object> real = (ConcurrentHashMap<String, Object>) mapF.get(factory);
		ConcurrentHashMap<String, Object> throwing = new ConcurrentHashMap<String, Object>(real) {
			@Override public java.util.Collection<Object> values() {
				if (once.compareAndSet(true, false)) throw new OutOfMemoryError("test: unable to create native thread");
				return super.values();
			}
		};
		long err0 = logs("session registry reconcile failed");
		mapF.set(factory, throwing);
		check(waitFor(() -> logs("session registry reconcile failed") > err0, 8000), "RB-07a Error in a round: logged as ERROR");
		check(nextRound(2), "RB-07b rounds go on after the Error (before: the schedule was cancelled for good)");
		mapF.set(factory, real);

		// ---- one customer's sessions unreadable: nothing is removed in that round ----
		String module = MODULE;
		String zombie = "smpp:server:session:24:" + module + "|0|n|zombie|z";
		jedis.setex(zombie, 600, "module=" + module + ";lastSeen=1;");
		sun.misc.Unsafe u;
		Field uf = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
		uf.setAccessible(true);
		u = (sun.misc.Unsafe) uf.get(null);
		BadManager bad = (BadManager) u.allocateInstance(BadManager.class);
		long skip0 = logs("cleanupSkipped=true");
		real.put("BAD", bad);
		check(waitFor(() -> logs("cleanupSkipped=true") > skip0, 8000), "RB-08a a customer that cannot be read: round goes on, cleanup skipped");
		check(jedis.exists(zombie), "RB-08b nothing removed in that round");
		check(active(25).size() == 6 && SmppServerSessionRegistry.getInstance().hasActiveSessionInModule(25, MODULE), "RB-08c the other customers are still refreshed");
		real.remove("BAD");
		check(waitFor(() -> !jedis.exists(zombie), 8000), "RB-09a stale entry of this module removed by the next normal round");

		// ---- a session bound during a round is not removed ----
		String fresh = "smpp:server:session:24:" + module + "|0|n|fresh|f";
		reconcileControl("stopSessionRegistryReconcile");
		Thread.sleep(1000);
		long start = System.currentTimeMillis();
		jedis.setex(fresh, 600, "module=" + module + ";lastSeen=" + (start + 10) + ";");
		SmppServerSessionRegistry.getInstance().cleanupCurrentModule(new HashSet<String>(), start);
		check(jedis.exists(fresh), "RB-10a not in the live set but written since the round started: kept (before: removed)");
		jedis.del(fresh);
		// the live sessions were wiped by the empty live set: the next round puts them back
		reconcileControl("startSessionRegistryReconcile");
		check(waitFor(() -> active(25).size() == 6, 12000), "RB-10b reconcile restores what cleanup removed");

		// ---- reconcile time ----
		reconcileControl("stopSessionRegistryReconcile");
		for (int c = 31; c < 40; c++) for (int k = 0; k < 100; k++) all.add(client(c));
		Thread.sleep(1500);
		List<Long> costs = new ArrayList<Long>();
		for (int i = 0; i < 3; i++) {
			long t = System.nanoTime();
			factory.reconcileServerSessionRegistry();
			costs.add((System.nanoTime() - t) / 1000000);
		}
		check(costs.get(2) < 2000, "RB-11 reconcile of " + all.size() + " connections: " + costs + " ms (one by one: about 8 ms each on this machine)");
	}

	/** A connection manager that fails when read */
	static class BadManager extends ConnectionManagerImpl {
		BadManager() { super(null); }
		@Override public List<com.king.gmms.connectionpool.connection.Connection> getAvailableConnectionsSnapshot() { throw new IllegalStateException("test: unreadable customer"); }
	}
}
