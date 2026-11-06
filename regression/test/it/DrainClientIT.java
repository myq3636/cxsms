package it;

import java.nio.file.*;
import java.util.*;

import com.king.message.gmms.GmmsMessage;

/** Draining an SMPP Client: two real Client processes, one fake SMSC, the real A2pCtl, the registry on. */
public class DrainClientIT extends ClientFailoverIT {
	static String stage(String module) { return jedis.hget("module:drain:" + module, "stage"); }
	static String state(String module) { return jedis.hget("system:module:" + module, "state"); }
	static void shards(String module, String value) {
		jedis.set("system:client:shards:" + module, value);
		jedis.publish("channel:gmms:control", "RELOAD_CLIENT_SHARDS@" + module);
	}
	static int failedResults(List<String> ids, int code) {
		int n = 0;
		for (String id : ids) for (GmmsMessage r : inStreams("stream:core:results*", id)) if (r.getStatusCode() == code) n++;
		return n;
	}
	static int results(List<String> ids) {
		int n = 0;
		for (String id : ids) if (!inStreams("stream:core:results*", id).isEmpty()) n++;
		return n;
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + "\n[FOV601]\nCCBSSID: " + RSSID + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nWindowSize: 300\nBufferTimeout: 60\n"
				+ "OutgoingConnection: (ConnName=FOV601_c1,userName=u601,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=2,BindMode=TRX,Version=V34,IsInit=true)\n").getBytes("UTF-8"));
		Files.write(Paths.get(home + "conf/a2p_modules.conf"), ("1|CoreEngine1|core|x|0|core|10||\n1|" + C1 + "|smpp-client|x|0|client|20||\n1|" + C2 + "|smpp-client|x|0|client|20||\n").getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedisStreamAutoClaimEnable=true\nRedisStreamAutoClaimIdleMs=10000\nRedisStreamAutoClaimIntervalMs=1000\nClientSendLeaseMs=10000\n"
				+ "ModuleStatusHeartbeatSeconds=1\nModuleStatusTTLSeconds=3\nModuleDrainDrWaitSeconds=4\n").getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(PORT);
		FakeSmsc.Account a = smsc.account("u601");
		Process n1 = null, n2 = null;
		try {
			boot();
			q = com.king.gmms.messagequeue.StreamQueueManager.getInstance();
			Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleRegistryEnable=true\n").getBytes("UTF-8"));
			check("0".equals(DrainCoreIT.tool("init")[0]), "DL-00 registry initialized");
			jedis.set("system:client:shards:" + C1, "0");
			jedis.set("system:client:shards:" + C2, "1");
			n1 = start(C1, 1);
			n2 = start(C2, 2);
			check(waitFor(() -> a.binds.get() == 2, 15000), "DL-01a each Client binds one connection");
			List<String> first = send("A", 40);
			check(waitFor(() -> tally(a, first)[0] == 0, 15000) && tally(a, first)[1] == 0, "DL-01b both Clients send");

			// ---- responses held: MTs wait in the window ----
			a.answerSubmit = false;
			List<String> held = send("H", 40);
			check(waitFor(() -> tally(a, held)[0] == 0, 15000), "DL-02a 40 MTs sent, no response yet");
			String[] t = DrainCoreIT.tool("drain", C1);
			check("0".equals(t[0]) && "DRAINING".equals(state(C1)) && t[1].contains("system:client:shards:" + C1), "DL-02b drain " + C1 + ": DRAINING, told to move the client shards");
			check(waitFor(() -> "WAIT_SHARDS".equals(stage(C1)), 4000), "DL-02c it waits for its shards");
			shards(C1, "none");
			shards(C2, "0-1");
			check(waitFor(() -> "WAIT_WINDOW".equals(stage(C1)), 6000), "DL-03a no shard left, responses missing: it waits for its window: " + jedis.hgetAll("module:drain:" + C1));
			int closedBefore = a.closed.get();
			Thread.sleep(2000);
			check("WAIT_WINDOW".equals(stage(C1)) && a.closed.get() == closedBefore, "DL-03b still waiting, connection kept");
			a.answerSubmit = true;
			a.release();
			check(waitFor(() -> "WAIT_DR".equals(stage(C1)), 6000), "DL-03c responses in: it keeps the connection a while for receipts: " + jedis.hgetAll("module:drain:" + C1));
			check(waitFor(() -> results(held) == 40, 10000) && failedResults(held, 2700) == 0, "DL-03d every held MT has its result, none 2700 (unknown): " + results(held) + "/" + failedResults(held, 2700));
			check(waitFor(() -> "DONE".equals(stage(C1)), 10000) && a.closed.get() == closedBefore + 1, "DL-04a after ModuleDrainDrWaitSeconds: its connection closed, DONE: " + jedis.hgetAll("module:drain:" + C1));
			int binds = a.binds.get();
			Thread.sleep(4000);
			check(a.binds.get() == binds, "DL-04b and it does not connect again (ReconnectInterval 1 s)");
			jedis.publish("channel:gmms:control", "RELOAD_CUSTOMER");
			Thread.sleep(4000);
			check(a.binds.get() == binds, "DL-04c not even after RELOAD_CUSTOMER");
			List<String> after = send("B", 40);
			check(waitFor(() -> tally(a, after)[0] == 0, 15000) && tally(a, after)[1] == 0, "DL-04d MTs go out through the other Client, each once");

			// ---- cancelled after its connections were closed: back with a restart ----
			t = DrainCoreIT.tool("cancel", C1);
			check("0".equals(t[0]) && "ACTIVE".equals(state(C1)) && t[1].contains("restart it"), "DL-05a cancel after the close: ACTIVE, and told to restart it");
			check(waitFor(() -> stage(C1) == null, 4000), "DL-05b its drain progress is gone");
			kill(n1);
			n1 = launch(C1, 1, homeOf(C1));
			check(waitFor(() -> a.binds.get() == binds + 1, 10000), "DL-05c restarted: it connects again");
			shards(C2, "1");
			shards(C1, "0");
			Thread.sleep(1500);
			List<String> back = send("C", 40);
			check(waitFor(() -> tally(a, back)[0] == 0, 15000) && tally(a, back)[1] == 0, "DL-05d both serve again, each once");

			// ---- cancelled before the close: no restart needed ----
			a.answerSubmit = false;
			List<String> held2 = send("K", 40);
			waitFor(() -> tally(a, held2)[0] == 0, 15000);
			DrainCoreIT.tool("drain", C1);
			shards(C1, "none");
			shards(C2, "0-1");
			check(waitFor(() -> "WAIT_WINDOW".equals(stage(C1)), 6000), "DL-06a draining again, waiting for its window");
			int binds2 = a.binds.get(), closed2 = a.closed.get();
			t = DrainCoreIT.tool("cancel", C1);
			check("0".equals(t[0]) && !t[1].contains("restart it"), "DL-06b cancel before the close: no restart asked");
			a.answerSubmit = true;
			a.release();
			check(waitFor(() -> results(held2) == 40, 10000) && failedResults(held2, 2700) == 0, "DL-06c its MTs get their results");
			Thread.sleep(6000);
			check(a.closed.get() == closed2 && a.binds.get() == binds2, "DL-06d its connection was never closed");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		finally {
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}
}
