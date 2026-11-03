package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/**
 * Two real Core processes with their own shards, killed with -9. Shards are static configuration: nothing takes over a
 * dead Core by itself; its backlog waits for a restart or for RELOAD_CORE_SHARDS. This process plays Server and Client.
 */
public class CoreFailoverIT extends CoreBizSupport {
	static final String C1 = "CoreEngine1", C2 = "CoreEngine2";
	static final int OSSID = 900, RSSID = 901;
	static final String[] MT = { "stream:mt:pending:" + OSSID + ":0", "stream:mt:pending:" + OSSID + ":1" };
	static int rcpt = 0;

	static Process launch(String module, int node, String h) throws Exception {
		new File(h + "node.out").delete();
		ProcessBuilder b = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx512m", "-Dit.keepRedis=true", "-Da2p_home=" + h, "-Dmodule=" + module,
				"-DNodeID=" + node, "-Dlog4j.configurationFile=" + h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.CoreNode");
		b.redirectErrorStream(true);
		b.redirectOutput(new File(h + "node.out"));
		Process p = b.start();
		final String out = h + "node.out";
		if (!waitFor(() -> new File(out).exists() && new String(Files.readAllBytes(Paths.get(out))).contains("READY"), 60000)) throw new IllegalStateException(module + " did not start");
		Thread.sleep(800);
		return p;
	}
	static String homeOf(String module) { return home.replaceAll("/$", "") + "_" + module + "/"; }
	static Process start(String module, int node) throws Exception {
		String h = homeOf(module);
		new ProcessBuilder("rm", "-rf", h).start().waitFor();
		new ProcessBuilder("cp", "-r", home, h).start().waitFor();
		return launch(module, node, h);
	}
	static void kill(Process p) throws Exception { p.destroyForcibly().waitFor(); }

	static List<String> send(String prefix, int n) {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			GmmsMessage m = msg(OSSID, "18667991226", "86138" + String.format("%08d", ++rcpt), "m");
			m.setMsgID(OSSID + "_" + prefix + i);
			m.setInMsgID("IN-" + m.getMsgID());
			StreamQueueManager.getInstance().produceSubmitMessage(m);
			ids.add(m.getMsgID());
		}
		return ids;
	}
	static long backlog(int shard) { return jedis.exists(MT[shard]) ? jedis.xlen(MT[shard]) : 0; }
	static long pel(int shard) {
		try { return jedis.xpending(MT[shard], "CoreMTGroup").getTotal(); } catch (Exception e) { return 0; }
	}
	/** how many times each MT went out to the channel */
	static Map<String, Integer> routed() {
		Map<String, Integer> r = new HashMap<>();
		for (String k : jedis.keys("stream:mt:routed:" + RSSID + ":*")) {
			StreamEntryID from = new StreamEntryID(0, 0);
			while (true) {
				List<StreamEntry> l = jedis.xrange(k, from, (StreamEntryID) null, 500);
				if (l.isEmpty()) break;
				for (StreamEntry e : l) {
					GmmsMessage g = SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
					if (g != null) r.merge(g.getMsgID(), 1, Integer::sum);
				}
				StreamEntryID last = l.get(l.size() - 1).getID();
				from = new StreamEntryID(last.getTime(), last.getSequence() + 1);
			}
		}
		return r;
	}
	static boolean allOnce(List<String> ids, String what) throws Exception {
		waitFor(() -> routed().keySet().containsAll(ids), 60000);
		Map<String, Integer> r = routed();
		int missing = 0, twice = 0;
		for (String id : ids) { Integer c = r.get(id); if (c == null) missing++; else if (c > 1) twice++; }
		check(missing == 0 && twice == 0, what + ": " + ids.size() + " MTs, missing " + missing + ", sent twice " + twice);
		return missing == 0;
	}
	static boolean logHas(String module, String text) throws Exception {
		Path p = Paths.get(homeOf(module) + "log/it.log");
		return Files.exists(p) && new String(Files.readAllBytes(p), "UTF-8").contains(text);
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		appendCm(cust("V901", RSSID, "") + cust("R900", OSSID, ""));
		route("R900", OSSID, "A2PRoutingRelay: (PhonePrefix=86,Relay=V901)");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedisStreamAutoClaimEnable=true\nCoreMtLeaseMs=10000\nRedisStreamAutoClaimIdleMs=10000\nRedisStreamAutoClaimIntervalMs=1000\n").getBytes("UTF-8"));
		// idle above the time a fresh process needs for its first MT, or auto-claim takes entries still being worked on (DEF-C2)
		Process n1 = null, n2 = null;
		try {
			boot();
			jedis.set("system:core:shards:" + C1, "0");
			jedis.set("system:core:shards:" + C2, "1");
			n1 = start(C1, 1);
			n2 = start(C2, 2);

			// ---- each Core consumes its own shard ----
			List<String> first = send("A", 100);
			allOnce(first, "FT-06a two Cores, one shard each: every MT out once");
			check(backlog(0) == 0 && backlog(1) == 0, "FT-06b both shards drained");
			check(logHas(C1, MT[0]) && !logHas(C1, MT[1]), "FT-06c Core 1 read only shard 0");
			check(logHas(C2, MT[1]) && !logHas(C2, MT[0]), "FT-06d Core 2 read only shard 1");

			// ---- idle Core killed: no takeover, its shard waits ----
			kill(n1);
			List<String> whileDown = send("B", 100);
			waitFor(() -> backlog(1) == 0, 15000);
			Thread.sleep(6000);
			long left = backlog(0);
			check(left > 0 && backlog(1) == 0, "FT-06e Core 1 killed: shard 1 goes on, shard 0 waits, no automatic takeover: left " + left);
			int out = 0; Map<String, Integer> r = routed();
			for (String id : whileDown) if (r.containsKey(id)) out++;
			check(out + left == 100, "FT-06f nothing lost while it is down: out " + out + " + waiting " + left);

			n1 = launch(C1, 1, homeOf(C1));
			allOnce(whileDown, "FT-06g Core 1 restarted: its backlog goes out, every MT once");

			// ---- killed while working, restarted ----
			final List<String> busy = new ArrayList<>();
			Thread sender = new Thread(() -> busy.addAll(send("C", 1500)));
			sender.start();
			Thread.sleep(400);
			kill(n1);
			sender.join();
			long pel0 = pel(0);
			check(backlog(0) > 0, "FT-06h killed under load: entries of shard 0 left, in flight " + pel0 + ", in the stream " + backlog(0));
			n1 = launch(C1, 1, homeOf(C1));
			allOnce(busy, "FT-06i restarted: in-flight entries and backlog finished, every MT once");
			check(waitFor(() -> backlog(0) == 0 && pel(0) == 0, 30000), "FT-06j shard 0 empty, nothing left in flight");

			// ---- killed, its shard moved to the other Core by hand ----
			final List<String> moved = new ArrayList<>();
			sender = new Thread(() -> moved.addAll(send("D", 1500)));
			sender.start();
			Thread.sleep(400);
			kill(n1);
			sender.join();
			check(backlog(0) > 0, "FT-06k killed again under load: shard 0 waits: " + backlog(0));
			jedis.set("system:core:shards:" + C2, "0-1");
			jedis.publish("channel:gmms:control", "RELOAD_CORE_SHARDS@" + C2);
			allOnce(moved, "FT-06l RELOAD_CORE_SHARDS gives shard 0 to Core 2: backlog and in-flight entries finished, every MT once");
			check(waitFor(() -> backlog(0) == 0 && pel(0) == 0, 30000), "FT-06m shard 0 empty, nothing left in flight");
			List<String> after = send("E", 50);
			allOnce(after, "FT-06n Core 2 now serves both shards");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		finally {
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}
}
