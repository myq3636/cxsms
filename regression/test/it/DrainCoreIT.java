package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/** Draining a Core: two real Core processes, the real A2pCtl, the registry on. Who reads results and inbound DRs, before and after. */
public class DrainCoreIT extends CoreFailoverIT {
	static final String D2 = "CoreEngine2", D3 = "CoreEngine3";
	static int seq = 0;

	static Process node(String module, int nodeId) throws Exception {
		String h = homeOf(module);
		new File(h + "node.out").delete();
		new File(h + "log/it.log").delete();
		ProcessBuilder b = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx512m", "-Dit.keepRedis=true", "-Da2p_home=" + h, "-Dmodule=" + module,
				"-DNodeID=" + nodeId, "-Dlog4j.configurationFile=" + h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.DrainCoreNode");
		b.redirectErrorStream(true); b.redirectOutput(new File(h + "node.out"));
		Process p = b.start();
		final String out = h + "node.out";
		if (!waitFor(() -> new File(out).exists() && new String(Files.readAllBytes(Paths.get(out))).contains("READY"), 60000)) throw new IllegalStateException(module + " did not start");
		Thread.sleep(800);
		return p;
	}
	static Process fresh(String module, int nodeId) throws Exception {
		String h = homeOf(module);
		new ProcessBuilder("rm", "-rf", h).start().waitFor();
		new ProcessBuilder("cp", "-r", home, h).start().waitFor();
		return node(module, nodeId);
	}
	static String[] tool(String... args) throws Exception {
		List<String> c = new ArrayList<>(List.of(System.getProperty("java.home") + "/bin/java", "-Xmx256m", "-Da2p_home=" + home, "-Dlog4j.configurationFile=" + home + "conf/log4j2.xml",
				"-cp", System.getProperty("java.class.path"), "it.A2pCtlNode"));
		c.addAll(List.of(args));
		File out = File.createTempFile("a2pctl", ".out");
		ProcessBuilder b = new ProcessBuilder(c);
		b.redirectErrorStream(true); b.redirectOutput(out);
		Process p = b.start();
		if (!p.waitFor(120, TimeUnit.SECONDS)) { p.destroyForcibly(); return new String[] { "-1", "timeout" }; }
		String text = new String(Files.readAllBytes(out.toPath()), "UTF-8");
		out.delete();
		return new String[] { String.valueOf(p.exitValue()), text };
	}
	/** results of MTs, as a Client writes them */
	static List<String> results(String tag, int n) {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			GmmsMessage m = mt(OSSID + "_" + tag + (++seq), OSSID, RSSID, 0);
			m.setRecipientAddress("86138" + String.format("%08d", seq));
			m.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			m.setStatusCode(0);
			m.setOutMsgID("OUT" + seq);
			StreamQueueManager.getInstance().produceResult(m);
			ids.add(m.getMsgID());
		}
		return ids;
	}
	static List<String> inboundDrs(String tag, int n) {
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			GmmsMessage d = mt(RSSID + "_" + tag + (++seq), OSSID, RSSID, 0);
			d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			d.setOutMsgID("NOPE" + seq);
			d.setStatusCode(10000);
			StreamQueueManager.getInstance().produceInboundDeliveryReport(d);
			ids.add(d.getMsgID());
		}
		return ids;
	}
	static long readBy(String module, List<String> ids) throws Exception {
		Path p = Paths.get(homeOf(module) + "log/it.log");
		if (!Files.exists(p)) return 0;
		Set<String> want = new HashSet<>(ids);
		return Files.lines(p, java.nio.charset.StandardCharsets.ISO_8859_1).filter(l -> l.contains("[STREAM-GET]"))
				.map(l -> l.replaceAll(".* ([0-9]+_[A-Za-z0-9]+) \\[STREAM-GET\\].*", "$1")).filter(want::contains).distinct().count();
	}
	static String stage(String module) { return jedis.hget("module:drain:" + module, "stage"); }
	static String state(String module) { return jedis.hget("system:module:" + module, "state"); }
	static void shards(String module, String value) {
		jedis.set("system:core:shards:" + module, value);
		jedis.publish("channel:gmms:control", "RELOAD_CORE_SHARDS@" + module);
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		appendCm(cust("V901", RSSID, "") + cust("R900", OSSID, ""));
		route("R900", OSSID, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V901)");
		Files.write(Paths.get(home + "conf/a2p_modules.conf"), ("1|" + D2 + "|core|x|0|core|10||\n1|" + D3 + "|core|x|0|core|10||\n"
				+ "1|MultiSmppServer1|smpp-server|x|0|server|40||\n1|MultiSmppClient1|smpp-client|x|0|client|20||\n").getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedisStreamAutoClaimEnable=true\nCoreMtLeaseMs=10000\nRedisStreamAutoClaimIdleMs=10000\nRedisStreamAutoClaimIntervalMs=1000\nAsyncSqlWriterEnable=false\nModuleStatusHeartbeatSeconds=1\nModuleStatusTTLSeconds=3\n").getBytes("UTF-8"));
		Process n2 = null, n3 = null;
		try {
			boot();
			// the nodes copy this home: the registry is on for them, not for this process
			Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleRegistryEnable=true\n").getBytes("UTF-8"));

			// ---- the same node id on one machine: STATUS_OFFLINE of one module ----
			GmmsUtility g = GmmsUtility.getInstance();
			ModuleStatusReporter.start(g, "server", "FakeServer1", "5");
			ModuleStatusReporter.start(g, "server", "FakeServer2", "5");
			ModuleStatusReporter.publishStatusCommand("STATUS_OFFLINE", "server,FakeServer1");
			Thread.sleep(800);
			check(!jedis.exists("module:status:server:FakeServer1:5") && jedis.sismember("system:server:nodes", "5"), "DC-00a STATUS_OFFLINE of one module: the other module of the machine keeps the node in system:server:nodes");
			ModuleStatusReporter.publishStatusCommand("STATUS_OFFLINE", "server,FakeServer2");
			Thread.sleep(800);
			check(!jedis.sismember("system:server:nodes", "5"), "DC-00b the last one: the node is gone");

			String[] t = tool("init");
			check("0".equals(t[0]), "DC-01a registry initialized");
			jedis.set("system:core:shards:" + D2, "0");
			jedis.set("system:core:shards:" + D3, "1");
			n2 = fresh(D2, 2);
			n3 = fresh(D3, 3);

			// ---- control: both Cores read results and inbound DRs ----
			List<String> r0 = results("R", 200);
			List<String> d0 = inboundDrs("I", 100);
			waitFor(() -> readBy(D2, r0) + readBy(D3, r0) == 200 && readBy(D2, d0) + readBy(D3, d0) == 100, 20000);
			System.out.println("INFO before: results " + readBy(D2, r0) + "/" + readBy(D3, r0) + ", inbound DRs " + readBy(D2, d0) + "/" + readBy(D3, d0));
			check(readBy(D2, r0) > 0 && readBy(D3, r0) > 0, "DC-01b control, before draining: results are read by both Cores, whatever their shards");

			// ---- refused ----
			t = tool("drain", "MultiSmppServer1");
			check("1".equals(t[0]) && t[1].contains("last ACTIVE"), "DC-02a drain of the only Server: refused, it is the last active one");
			t = tool("drain", D2);
			check("0".equals(t[0]) && "DRAINING".equals(state(D2)), "DC-02b drain " + D2 + ": DRAINING");
			t = tool("drain", D3);
			check("1".equals(t[0]) && t[1].contains("last ACTIVE"), "DC-02c the last active Core is not drained");
			check(waitFor(() -> "WAIT_SHARDS".equals(stage(D2)), 4000) && "0".equals(jedis.hget("module:drain:" + D2, "shards")), "DC-03a it waits for its shards to be moved: " + jedis.hgetAll("module:drain:" + D2));

			// ---- shards moved: it stops reading the rest ----
			shards(D2, "none");
			shards(D3, "0-1");
			check(waitFor(() -> "DONE".equals(stage(D2)), 15000), "DC-03b shards gone, nothing in flight: DONE: " + jedis.hgetAll("module:drain:" + D2));
			check(logHas(D2, "[DRAIN] " + D2 + " no shard left: stopped reading"), "DC-03c it stopped reading results, inbound DRs and held MTs");
			List<String> r1 = results("S", 200);
			List<String> d1 = inboundDrs("J", 100);
			List<String> m1 = send("M", 100);
			check(waitFor(() -> readBy(D3, r1) == 200 && readBy(D3, d1) == 100, 20000), "DC-04a after draining: the other Core reads all 200 results and 100 inbound DRs");
			check(readBy(D2, r1) == 0 && readBy(D2, d1) == 0, "DC-04b the drained Core read none: " + readBy(D2, r1) + "/" + readBy(D2, d1));
			allOnce(m1, "DC-04c MTs meanwhile: every one out once");

			// ---- not removed while it runs; cancelled ----
			t = tool("remove", D2);
			check("1".equals(t[0]) && t[1].contains("still running"), "DC-05a remove while it runs: refused");
			t = tool("cancel", D2);
			check("0".equals(t[0]) && "ACTIVE".equals(state(D2)), "DC-05b cancel: ACTIVE");
			check(waitFor(() -> stage(D2) == null, 4000), "DC-05c its drain progress is gone");
			shards(D3, "1");
			shards(D2, "0");
			Thread.sleep(1500);
			List<String> r2 = results("T", 200);
			check(waitFor(() -> readBy(D2, r2) + readBy(D3, r2) == 200, 20000) && readBy(D2, r2) > 0, "DC-05d it reads results again: " + readBy(D2, r2) + " of 200");

			// ---- restarted while draining ----
			tool("drain", D2);
			shards(D2, "none");
			shards(D3, "0-1");
			waitFor(() -> "DONE".equals(stage(D2)), 15000);
			kill(n2);
			n2 = node(D2, 2);
			check(waitFor(() -> "DONE".equals(stage(D2)), 15000), "DC-06a restarted while DRAINING: it drains again");
			Thread.sleep(1500);
			List<String> r3 = results("U", 200);
			check(waitFor(() -> readBy(D3, r3) == 200, 20000) && readBy(D2, r3) == 0, "DC-06b its readers started at startup are stopped again: read " + readBy(D2, r3));

			// ---- drained, stopped, removed ----
			t = tool("drain", D2, "--wait");
			check("1".equals(t[0]), "DC-07a drain of a module draining already: refused (not ACTIVE)");
			tool("cancel", D2);
			t = tool("drain", D2, "--wait");
			check("0".equals(t[0]) && "DRAINED".equals(state(D2)) && t[1].contains("stop the process now"), "DC-07b drain --wait: returns when done, DRAINED");
			n2.destroy();
			n2.waitFor(8, TimeUnit.SECONDS);
			t = tool("remove", D2);
			check("0".equals(t[0]) && "REMOVED".equals(state(D2)), "DC-07c stopped: removed");
			t = tool("status");
			check(t[1].contains(D2) && t[1].contains("REMOVED"), "DC-07d status shows it");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		finally {
			if (n2 != null) n2.destroyForcibly();
			if (n3 != null) n3.destroyForcibly();
		}
		exit();
	}
}
