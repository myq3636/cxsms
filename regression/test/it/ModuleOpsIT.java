package it;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * a2pctl add / remove / resume / cancel with real Core processes, the real tool and the registry on (MS-25 to MS-29):
 * plans and capacity on --dry-run, one operation at a time, a JOINING Core that touches no traffic, shards moved one at a
 * time under MT traffic, an old owner that stops answering, and cancel.
 */
public class ModuleOpsIT extends DrainCoreIT {
	static final String E1 = "CoreEngine1", E2 = "CoreEngine2", E3 = "CoreEngine3", E4 = "CoreEngine4", E5 = "CoreEngine5";

	/** the shards a running Core reports */
	static String owns(String module) {
		for (String k : jedis.keys("system:shards:core:" + module + ":*")) {
			String v = jedis.get(k);
			if (v != null && v.contains("|")) return v.substring(v.indexOf('|') + 1);
		}
		return null;
	}
	static boolean running(String module) { return !jedis.keys("module:status:*:" + module + ":*").isEmpty(); }
	static String step() { return jedis.hget("system:modules:op", "step"); }
	/** MTs sent every 20 ms while the tool runs */
	static String[] toolUnderTraffic(List<String> sent, String tag, String... args) throws Exception {
		final boolean[] stop = { false };
		Thread t = new Thread(() -> {
			int i = 0;
			while (!stop[0]) {
				sent.addAll(send(tag + (i++) + "_", 1));
				try { Thread.sleep(20); } catch (InterruptedException e) { return; }
			}
		});
		t.start();
		String[] r = tool(args);
		stop[0] = true;
		t.join();
		return r;
	}
	static void signal(Process p, String sig) throws Exception { new ProcessBuilder("kill", "-" + sig, String.valueOf(p.pid())).start().waitFor(); }
	static String last(String text, String start) {
		for (String l : text.split("\n")) if (l.trim().startsWith(start)) return l.trim();
		return "(no line " + start + ")";
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		appendCm(cust("V901", RSSID, "") + cust("R900", OSSID, "")
				+ "\n[V950]\nCCBSSID: 950\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nSMSOptionOutgoingThrottlingNum: 100\n"
				+ "OutgoingConnection: (ConnName=V950_c1,userName=u950,password=p,URL=127.0.0.1,Port=1,SessionNumber=4,BindMode=TRX,Version=V34,IsInit=true)\n");
		route("R900", OSSID, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V901)");
		Files.write(Paths.get(home + "conf/a2p_modules.conf"), ("1|" + E1 + "|core|x|0|core|10||\n1|" + E2 + "|core|x|0|core|10||\n"
				+ "1|MultiSmppServer1|smpp-server|x|0|server|40||\n1|MultiSmppClient1|smpp-client|x|0|client|20||\n1|MultiSmppClient2|smpp-client|x|0|client|20||\n").getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nTotalShards=4\nRedisStreamAutoClaimEnable=true\nCoreMtLeaseMs=10000\nRedisStreamAutoClaimIdleMs=10000"
				+ "\nRedisStreamAutoClaimIntervalMs=1000\nAsyncSqlWriterEnable=false\nModuleStatusHeartbeatSeconds=1\nModuleStatusTTLSeconds=3"
				+ "\nModuleShardIntervalSeconds=0\nModuleShardConfirmSeconds=5\nModuleOpLockSeconds=6\n").getBytes("UTF-8"));
		Process n1 = null, n2 = null, n3 = null, n4 = null;
		try {
			boot();
			Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleRegistryEnable=true\n").getBytes("UTF-8"));
			check("0".equals(tool("init")[0]), "MO-00a registry initialized");
			jedis.set("system:core:shards:" + E1, "0-1");
			jedis.set("system:core:shards:" + E2, "2-3");
			n1 = fresh(E1, 1);
			n2 = fresh(E2, 2);
			check(waitFor(() -> "0,1".equals(owns(E1)) && "2,3".equals(owns(E2)), 8000), "MO-00b two Cores: 0-1 and 2-3 of 4 shards");

			// ---- --dry-run: the plan, nothing changed (MS-25) ----
			String[] t = tool("add", E3, "--node-id", "3", "--dry-run");
			System.out.println("INFO " + last(t[1], "after"));
			check("0".equals(t[0]) && t[1].contains("after {CoreEngine1=0-1, CoreEngine2=2, CoreEngine3=3}") && t[1].contains("1 to move")
					&& t[1].contains("Nothing changed") && state(E3) == null && !jedis.exists("system:modules:op") && !jedis.exists("system:core:shards:" + E3),
					"MO-01 add --dry-run: even, one shard moves, nothing written");
			t = tool("add", "CoreEngine02", "--node-id", "3");
			check("1".equals(t[0]) && t[1].contains("number 2 is the number of " + E2), "MO-02 a number in use: refused");
			t = tool("add", "MultiSmppClient3", "--node-id", "3", "--dry-run");
			System.out.println("INFO " + last(t[1], "V950(950) V950_c1"));
			check("1".equals(t[0]) && t[1].contains("SessionNumber=4, connections 4 -> 6") && t[1].contains("MORE THAN SessionNumber")
					&& t[1].contains("total 200 -> 300"), "MO-03a a third Client: 2+2 connections and one more of 2 makes 6 of 4: refused; outgoing TPS 200 -> 300");
			t = tool("add", "MultiSmppClient3", "--node-id", "3", "--dry-run", "--force");
			check("0".equals(t[0]) && t[1].contains("Nothing changed"), "MO-03b --force: shown, not refused");
			t = tool("remove", "MultiSmppClient2", "--dry-run", "--force");
			check(t[1].contains("connections 4 -> 2") && t[1].contains("fewer") && t[1].contains("after restarting the others: 4"), "MO-03c removing a Client: 4 -> 2 until the other is restarted");

			// ---- add: JOINING, then started, then resume ----
			t = tool("add", E3, "--node-id", "3");
			check("0".equals(t[0]) && "JOINING".equals(state(E3)) && "none".equals(jedis.get("system:core:shards:" + E3)) && "REGISTERED".equals(step())
					&& t[1].contains("start it with the start script"), "MO-04 add: JOINING with no shard, it asks for the process");
			t = tool("add", E4, "--node-id", "4");
			check("1".equals(t[0]) && t[1].contains("Operation in progress: add " + E3) && state(E4) == null, "MO-05a a second operation: refused (MS-26)");
			t = tool("drain", E1);
			check("1".equals(t[0]) && t[1].contains("Operation in progress") && "ACTIVE".equals(state(E1)), "MO-05b drain meanwhile: refused");
			jedis.set("system:modules:lock", "someone|ops@host", new redis.clients.jedis.params.SetParams().ex(30));
			t = tool("resume");
			check("1".equals(t[0]) && t[1].contains("Another a2pctl is at work (someone|ops@host)"), "MO-05c another a2pctl holds the lock: refused");
			jedis.del("system:modules:lock");
			t = tool("resume");
			check("0".equals(t[0]) && t[1].contains("Deploy " + E3) && "REGISTERED".equals(step()), "MO-06 resume before it runs: asks again, nothing else");
			t = tool("status");
			check(t[1].contains("operation: add " + E3 + " at REGISTERED") && t[1].contains("nobody at work on it"), "MO-06b status shows the operation");

			n3 = fresh(E3, 3);
			List<String> r0 = results("JR", 100);
			waitFor(() -> readBy(E1, r0) + readBy(E2, r0) == 100, 20000);
			check(readBy(E3, r0) == 0 && readBy(E1, r0) + readBy(E2, r0) == 100, "MO-07a the JOINING Core reads no result: " + readBy(E3, r0) + " of 100");
			check(logHas(E3, "[MODULE-REG] " + E3 + " is JOINING"), "MO-07b it says why");

			List<String> m1 = Collections.synchronizedList(new ArrayList<>());
			t = toolUnderTraffic(m1, "MA", "resume");
			System.out.println("INFO " + last(t[1], "shard 3:"));
			check("0".equals(t[0]) && t[1].contains("shard 3: " + E2 + " -> " + E3) && "ACTIVE".equals(state(E3)) && !jedis.exists("system:modules:op"),
					"MO-08a resume: shard 3 moved, ACTIVE, operation done");
			check("2".equals(owns(E2)) && "3".equals(owns(E3)) && "0,1".equals(owns(E1)), "MO-08b the Cores report 0-1, 2, 3");
			check(t[1].contains("CoreShard." + E3 + "=3") && t[1].contains("CoreShard." + E2 + "=2"), "MO-08c the configuration lines to write are printed");
			allOnce(m1, "MO-08d MTs during the move");
			List<String> r1 = results("JS", 200);
			check(waitFor(() -> readBy(E3, r1) > 0 && readBy(E1, r1) + readBy(E2, r1) + readBy(E3, r1) == 200, 20000), "MO-08e ACTIVE: it reads results now: " + readBy(E3, r1));
			check(!jedis.exists("system:modules:maintenance") && !jedis.exists("system:modules:lock"), "MO-08f lock and maintenance mark gone");

			// ---- remove: the whole way ----
			t = tool("remove", E1, "--dry-run");
			check("0".equals(t[0]) && t[1].contains("after {CoreEngine2=0,2, CoreEngine3=1,3}") && "ACTIVE".equals(state(E1)), "MO-09 remove --dry-run: its 2 shards spread, nothing changed");
			List<String> m2 = Collections.synchronizedList(new ArrayList<>());
			t = toolUnderTraffic(m2, "MR", "remove", E1, "--shards-to", E2);
			System.out.println("INFO " + last(t[1], "drained:"));
			check("0".equals(t[0]) && "DRAINED".equals(state(E1)) && "none".equals(jedis.get("system:core:shards:" + E1)) && "0,1,2".equals(owns(E2))
					&& t[1].contains("Stop it with the stop script") && "WAIT_STOP".equals(step()), "MO-10a remove --shards-to: its shards to " + E2 + ", drained, asks to stop it");
			allOnce(m2, "MO-10b MTs during the remove");
			t = tool("resume");
			check("0".equals(t[0]) && t[1].contains("Stop it") && "DRAINED".equals(state(E1)), "MO-10c resume while it runs: asks again");
			n1.destroy();
			n1.waitFor(8, TimeUnit.SECONDS);
			waitFor(() -> !running(E1), 5000);
			t = tool("resume");
			check("0".equals(t[0]) && "REMOVED".equals(state(E1)) && !jedis.exists("system:core:shards:" + E1) && t[1].contains("delete CoreShard." + E1)
					&& !jedis.exists("system:modules:op"), "MO-10d stopped, resume: REMOVED, its shard value gone, the line to delete printed");

			// ---- the old owner stops answering (MS-29) ----
			t = tool("add", E4, "--node-id", "4");
			check("0".equals(t[0]) && t[1].contains("after {CoreEngine2=0-1, CoreEngine3=3, CoreEngine4=2}"), "MO-11a add " + E4 + ": shard 2 will move from " + E2);
			n4 = fresh(E4, 4);
			signal(n2, "STOP");
			waitFor(() -> !running(E2), 6000);
			t = tool("resume");
			System.out.println("INFO " + last(t[1], "STOPPED"));
			check("1".equals(t[0]) && t[1].contains("STOPPED: " + E2 + " is not running") && "MOVING".equals(step()) && "JOINING".equals(state(E4))
					&& !"2".equals(owns(E4)), "MO-11b " + E2 + " does not answer: stopped, shard 2 not given to " + E4 + ", the operation stays");
			t = tool("resume", "--force");
			check("0".equals(t[0]) && "2".equals(owns(E4)) && "ACTIVE".equals(state(E4)), "MO-11c resume --force: taken as gone, shard 2 to " + E4);
			signal(n2, "CONT");
			check(waitFor(() -> "0,1".equals(owns(E2)), 10000), "MO-11d it comes back: it reads its value again and keeps 0-1 only, no two owners: " + owns(E2));

			// ---- cancel ----
			t = tool("add", E5, "--node-id", "5");
			check("0".equals(t[0]) && "JOINING".equals(state(E5)), "MO-12a add " + E5);
			t = tool("cancel");
			check("0".equals(t[0]) && "REMOVED".equals(state(E5)) && !jedis.exists("system:core:shards:" + E5) && !jedis.exists("system:modules:op"),
					"MO-12b cancel before it runs: REMOVED, nothing left");
			t = tool("add", E5, "--node-id", "5", "--dry-run");
			check("0".equals(t[0]), "MO-12c its number is free again at once: it never took traffic");
			t = tool("remove", E4);
			check("0".equals(t[0]) && "DRAINED".equals(state(E4)) && "WAIT_STOP".equals(step()), "MO-13a remove " + E4 + ": drained, waits for the stop");
			t = tool("cancel");
			check("0".equals(t[0]) && "ACTIVE".equals(state(E4)) && !jedis.exists("system:modules:op") && t[1].contains("Shards now"), "MO-13b cancel: ACTIVE again, shards stay where they are");
			check("".equals(owns(E4)) && "0,1".equals(owns(E2)) && "2,3".equals(owns(E3)),
					"MO-13c its shard 2 stays with " + E3 + ": " + E2 + "=" + owns(E2) + " " + E3 + "=" + owns(E3) + " " + E4 + "=" + owns(E4));
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		finally {
			for (Process p : new Process[] { n1, n2, n3, n4 }) if (p != null) { signal(p, "CONT"); p.destroyForcibly(); }
		}
		exit();
	}
}
