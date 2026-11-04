package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/** Two real Client processes with their own shards, killed with -9, against a fake SMSC. Nothing takes over a dead Client by itself. */
public class ClientFailoverIT extends ItSupport {
	static final String C1 = "MultiSmppClient1", C2 = "MultiSmppClient2";
	static final int PORT = 17795, RSSID = 601;
	static final String[] ROUTED = { "stream:mt:routed:" + RSSID + ":0", "stream:mt:routed:" + RSSID + ":1" };
	static StreamQueueManager q;
	static int no = 0;

	static String homeOf(String module) { return home.replaceAll("/$", "") + "_" + module + "/"; }
	static Process launch(String module, int node, String h) throws Exception {
		new File(h + "node.out").delete();
		ProcessBuilder b = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx512m", "-Dit.keepRedis=true", "-Da2p_home=" + h, "-Dmodule=" + module,
				"-DNodeID=" + node, "-Dlog4j.configurationFile=" + h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.ClientNode");
		b.redirectErrorStream(true);
		b.redirectOutput(new File(h + "node.out"));
		Process p = b.start();
		final String out = h + "node.out";
		if (!waitFor(() -> new File(out).exists() && new String(Files.readAllBytes(Paths.get(out))).contains("READY"), 60000)) throw new IllegalStateException(module + " did not start");
		return p;
	}
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
			String id = "8888_" + prefix + (++no);
			GmmsMessage m = mt(id, 8888, RSSID, 0);
			m.setTextContent(id);
			m.setMessageSize(id.length());
			m.setRecipientAddress("86138" + String.format("%08d", no));
			q.produceOutboundMessage(m);
			ids.add(id);
		}
		return ids;
	}
	static long len(int shard) { return jedis.exists(ROUTED[shard]) ? jedis.xlen(ROUTED[shard]) : 0; }
	static long pel(int shard) {
		try { return jedis.xpending(ROUTED[shard], "ClientOutboundSubmitGroup").getTotal(); } catch (Exception e) { return 0; }
	}
	static boolean logHas(String module, String text) throws Exception {
		Path p = Paths.get(homeOf(module) + "log/it.log");
		return Files.exists(p) && new String(Files.readAllBytes(p), "UTF-8").contains(text);
	}
	static int[] tally(FakeSmsc.Account a, List<String> ids) {
		int missing = 0, twice = 0;
		for (String id : ids) { int c = a.count(id); if (c == 0) missing++; else if (c > 1) twice++; }
		return new int[] { missing, twice };
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + "\n[FOV601]\nCCBSSID: " + RSSID + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 500\n"
				+ "OutgoingConnection: (ConnName=FOV601_c1,userName=u601,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=2,BindMode=TRX,Version=V34,IsInit=true)\n"
				+ "\n[FOV602]\nCCBSSID: 602\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nWindowSize: 300\nBufferTimeout: 60\nSMSOptionOutgoingThrottlingNum: 20\n"
				+ "OutgoingConnection: (ConnName=FOV602_c1,userName=u602,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=2,BindMode=TRX,Version=V34,IsInit=true)\n").getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedisStreamAutoClaimEnable=true\nRedisStreamAutoClaimIdleMs=10000\nRedisStreamAutoClaimIntervalMs=1000\nClientSendLeaseMs=10000\n").getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(PORT);
		FakeSmsc.Account a = smsc.account("u601");
		Process n1 = null, n2 = null;
		try {
			boot();
			q = StreamQueueManager.getInstance();
			jedis.set("system:client:shards:" + C1, "0");
			jedis.set("system:client:shards:" + C2, "1");
			n1 = start(C1, 1);
			n2 = start(C2, 2);
			check(waitFor(() -> a.binds.get() == 2, 15000), "FC-00 each Client binds one of SessionNumber=2: " + a.binds.get());

			// ---- each Client consumes its shard ----
			List<String> first = send("A", 100);
			check(waitFor(() -> tally(a, first)[0] == 0, 20000), "CS-02a two Clients: all 100 reach the vendor");
			check(tally(a, first)[1] == 0, "CS-02b none twice: " + tally(a, first)[1]);
			check(logHas(C1, ROUTED[0]) && !logHas(C1, ROUTED[1]), "CS-02c Client 1 read only shard 0");
			check(logHas(C2, ROUTED[1]) && !logHas(C2, ROUTED[0]), "CS-02d Client 2 read only shard 1");

			// ---- the rate of a channel is per Client ----
			FakeSmsc.Account limited = smsc.account("u602");
			check(waitFor(() -> limited.binds.get() == 2, 8000), "TW-08a second channel: one connection from each Client");
			for (int i = 0; i < 400; i++) {
				String id = "8888_T" + (++no);
				GmmsMessage m = mt(id, 8888, 602, 0);
				m.setTextContent(id);
				m.setMessageSize(id.length());
				m.setRecipientAddress("86138" + String.format("%08d", no));
				q.produceOutboundMessage(m);
			}
			int before = limited.texts.size();
			Thread.sleep(3000);
			int inThree = limited.texts.size() - before;
			System.out.println("INFO 20 TPS on two Clients: " + inThree + " in 3 s, backlog left " + (400 - limited.texts.size()));
			check(inThree > 85 && inThree <= 150 && limited.texts.size() < 400, "TW-08b known limit: each Client keeps its own limit, 20 TPS is about 40 on two: " + inThree + " in 3 s");

			// ---- killed: no takeover ----
			kill(n1);
			List<String> down = send("B", 100);
			Thread.sleep(5000);
			long left = len(0);
			check(left > 0 && len(1) == 0, "FT-01a Client 1 killed: shard 1 goes on, shard 0 waits: " + left);
			int[] t = tally(a, down);
			check(t[0] == left && t[1] == 0, "FT-01b nothing lost, nothing twice: waiting " + left + ", not sent " + t[0]);
			n1 = launch(C1, 1, homeOf(C1));
			check(waitFor(() -> tally(a, down)[0] == 0, 20000), "FT-01c restarted: its backlog goes out");
			check(tally(a, down)[1] == 0, "FT-01d each once: " + tally(a, down)[1]);

			// ---- killed with MTs sent and no response yet ----
			a.answerSubmit = false;
			Thread.sleep(500);
			List<String> wait = send("C", 40);
			check(waitFor(() -> tally(a, wait)[0] == 0, 15000), "FT-01e 40 MTs sent, no response yet");
			kill(n1);
			n1 = launch(C1, 1, homeOf(C1));
			Thread.sleep(13000);                       // past the auto-claim idle and the send lease
			int[] w = tally(a, wait);
			check(w[1] == 0, "FT-01f restarted with responses outstanding: not sent again: " + w[1] + " twice");
			a.answerSubmit = true;
			a.held.clear();

			// ---- killed under load ----
			final List<String> load = new ArrayList<>();
			Thread sender = new Thread(() -> load.addAll(send("D", 2000)));
			sender.start();
			Thread.sleep(300);
			kill(n1);
			sender.join();
			check(len(0) > 0, "FT-01g killed under load: shard 0 waits: " + len(0) + ", in flight " + pel(0));
			n1 = launch(C1, 1, homeOf(C1));
			waitFor(() -> len(0) == 0 && pel(0) == 0, 40000);
			Thread.sleep(1000);
			int[] l = tally(a, load);
			System.out.println("INFO after kill under load: not sent " + l[0] + ", twice " + l[1]);
			check(l[1] == 0, "FT-01h restarted: nothing twice: " + l[1]);
			check(l[0] <= pelBound, "FT-01i a send cut by the kill is not repeated, Core's outbox answers it: not sent " + l[0]);
			check(len(0) == 0 && pel(0) == 0, "FT-01j shard 0 empty, nothing pending");

			// ---- killed, shard moved by hand ----
			kill(n1);
			List<String> moved = send("E", 100);
			Thread.sleep(1000);
			check(len(0) > 0, "FT-06a killed: shard 0 waits: " + len(0));
			jedis.set("system:client:shards:" + C2, "0-1");
			jedis.publish("channel:gmms:control", "RELOAD_CLIENT_SHARDS@" + C2);
			check(waitFor(() -> tally(a, moved)[0] == 0, 20000), "CS-03a / FT-06b RELOAD_CLIENT_SHARDS gives shard 0 to Client 2: backlog goes out");
			check(tally(a, moved)[1] == 0, "FT-06c each once");
			List<String> after = send("F", 40);
			check(waitFor(() -> tally(a, after)[0] == 0, 15000), "CS-03b Client 2 now serves both shards");

			// ---- a Client without shards ----
			jedis.set("system:client:shards:" + C2, "none");
			jedis.publish("channel:gmms:control", "RELOAD_CLIENT_SHARDS@" + C2);
			Thread.sleep(1000);
			List<String> held = send("N", 40);
			Thread.sleep(3000);
			check(tally(a, held)[0] == 40, "MS-02a shards none: the Client sends nothing, not shard 0 either: left " + tally(a, held)[0] + " of 40");
			jedis.set("system:client:shards:" + C2, "0-1");
			jedis.publish("channel:gmms:control", "RELOAD_CLIENT_SHARDS@" + C2);
			check(waitFor(() -> tally(a, held)[0] == 0, 15000) && tally(a, held)[1] == 0, "MS-02b shards back: the backlog goes out, each once");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		finally {
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}
	/** entries a Client can have in hand when it is killed: one batch */
	static final int pelBound = 300;
}
