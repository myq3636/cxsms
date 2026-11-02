package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import it.ServerFuncIT.Pdu;
import it.ServerFuncIT.R;

/**
 * Several real server processes loading different customers (SmppServerModules), moving a customer to
 * another server with RELOAD_CUSTOMER, and a server killed and restarted. This process plays Core: it
 * resolves and writes the DRs with the product code. Two MultiSmppServer JVMs, shared Redis.
 */
public class MultiServerIT extends ItSupport {
	static final String S1 = "MultiSmppServer1", S2 = "MultiSmppServer2";
	static final int P1 = 17781, P2 = 17782;
	static Process n1, n2;
	static String base;
	static Path[] cms;

	static String cust(String name, int ssid, String modules) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\n"
				+ (modules == null ? "" : "SmppServerModules: " + modules + "\n")
				+ "IncomingConnection: (ConnName=" + name + "_c1,userName=" + name.toLowerCase() + ",password=pw" + ssid + ",URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n";
	}
	static boolean rejected(Pdu p) { return p != null && (p.cmd == -1 || p.status != 0); }
	static boolean bound(Pdu p) { return p != null && p.cmd == 0x80000009 && p.status == 0; }
	static Pdu tryBind(int port, String u, String pw) throws Exception { try (R r = new R(port)) { return r.bind(u, pw); } }

	static Process start(String module, int node, int port) throws Exception {
		String h = home.replaceAll("/$", "") + "_" + module + "/";
		new ProcessBuilder("rm", "-rf", h).start().waitFor();
		new ProcessBuilder("cp", "-r", home, h).start().waitFor();
		return launch(module, node, port, h);
	}
	static Process launch(String module, int node, int port, String h) throws Exception {
		new File(h + "log/it.log").delete();
		ProcessBuilder b = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx512m", "-Dit.keepRedis=true", "-Da2p_home=" + h, "-Dmodule=" + module,
				"-DNodeID=" + node, "-Dlog4j.configurationFile=" + h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.MultiServerNode", String.valueOf(port));
		b.redirectErrorStream(true); b.redirectOutput(new File(h + "node.out"));
		Process p = b.start();
		final String out = h + "node.out";
		boolean up = waitFor(() -> new File(out).exists() && new String(Files.readAllBytes(Paths.get(out))).contains("READY"), 60000);
		if (!up) throw new IllegalStateException(module + " did not start");
		Thread.sleep(800);
		return p;
	}
	/** the session still answers; a closed socket is not an answer */
	static boolean alive(R r, int seq) throws Exception {
		try { r.send(0x15, seq, new byte[0]); } catch (java.io.IOException e) { return false; }
		Pdu q = r.read();
		return q != null && q.cmd == 0x80000015 && q.status == 0 && q.seq == seq;
	}
	static boolean active(int ssid, String module) { return com.king.gmms.ha.SmppServerSessionRegistry.getInstance().hasActiveSessionInModule(ssid, module); }
	static String h(String module) { return home.replaceAll("/$", "") + "_" + module + "/"; }

	/** every process sees the new cm.cfg, then RELOAD_CUSTOMER; returns when all have reloaded */
	static void reload(String text) throws Exception {
		String[] keys = { "config:files:CoreEngine1:1", "config:files:" + S1 + ":1", "config:files:" + S2 + ":2" };
		Map<String, String> before = new HashMap<String, String>();
		for (String k : keys) before.put(k, String.valueOf(jedis.hget(k, "cm.loadedAt")));
		for (Path c : cms) Files.write(c, text.getBytes("UTF-8"));
		Thread.sleep(700);
		jedis.publish("channel:gmms:control", "RELOAD_CUSTOMER");
		for (String k : keys) { final String key = k; waitFor(() -> !before.get(key).equals(String.valueOf(jedis.hget(key, "cm.loadedAt"))), 10000); }
		Thread.sleep(1500);
	}
	static void sendDr(String id, int ossid, String originalModule) throws Exception {
		GmmsMessage d = mt(id, ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInMsgID(id);
		d.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", originalModule), "conn"));
		StreamQueueManager q = StreamQueueManager.getInstance();
		q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
	}
	static boolean gets(R r, String id, int ms) throws Exception {
		Pdu p = ServerFuncIT.nextDeliver(r, ms);
		if (p == null) return false;
		r.send(0x80000005, p.seq, new byte[] { 0 });
		return ServerFuncIT.deliver(p).get("text").contains(id);
	}
	static Set<String> modulesOf(int ssid) { return jedis.zrange("smpp:server:module:" + ssid, 0, -1) == null ? new HashSet<String>() : new HashSet<String>(jedis.zrange("smpp:server:module:" + ssid, 0, -1)); }

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg");
		base = new String(Files.readAllBytes(cm), "UTF-8")
				+ cust("MA", 120, S1) + cust("MC", 121, S2) + cust("MB", 122, S1 + "," + S2) + cust("MN", 123, null) + cust("MD", 124, S1);
		Files.write(cm, base.getBytes("UTF-8"));
		// a killed server is noticed within the heartbeat TTL; short here, so the test need not wait 30 s
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleStatusTTLSeconds=5\nModuleStatusHeartbeatSeconds=1\n").getBytes("UTF-8"));
		try {
			boot();
			jedis.set("thcon", "1");
			n1 = start(S1, 1, P1);
			n2 = start(S2, 2, P2);
			cms = new Path[] { cm, Paths.get(h(S1) + "conf/cm.cfg"), Paths.get(h(S2) + "conf/cm.cfg") };
			loading();
			submitAndDr();
			moveCustomer();
			addAndRemove();
			killAndRestart();
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		} finally {
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}

	/** each server loads its own customers */
	static void loading() throws Exception {
		check(bound(tryBind(P1, "ma", "pw120")), "MS-01a MA (only server 1): binds on server 1");
		Pdu p = tryBind(P2, "ma", "pw120"); check(rejected(p), "MS-01b MA: refused by server 2: " + p);
		check(bound(tryBind(P2, "mc", "pw121")), "MS-01c MC (only server 2): binds on server 2");
		p = tryBind(P1, "mc", "pw121"); check(rejected(p), "MS-01d MC: refused by server 1: " + p);
		check(bound(tryBind(P1, "mb", "pw122")) && bound(tryBind(P2, "mb", "pw122")), "MS-01e MB (both listed): binds on both");
		check(bound(tryBind(P1, "mn", "pw123")) && bound(tryBind(P2, "mn", "pw123")), "MS-01f MN (no SmppServerModules): binds on both");
		Thread.sleep(800);
		try (R r = new R(P1)) { r.bind("ma", "pw120"); try (R q = new R(P2)) { q.bind("mc", "pw121");
			check(waitFor(() -> modulesOf(120).equals(Collections.singleton(S1)) && modulesOf(121).equals(Collections.singleton(S2)), 4000), "MS-02 registry: each customer under its own server (" + modulesOf(120) + "," + modulesOf(121) + ")");
		} }
	}

	static void submitAndDr() throws Exception {
		try (R ma = new R(P1); R mc = new R(P2)) {
			ma.bind("ma", "pw120"); mc.bind("mc", "pw121");
			Pdu p = ma.submit(ServerFuncIT.sub("from s1")); GmmsMessage m = ServerFuncIT.find(120, ServerFuncIT.msgId(p));
			check(m != null && S1.equals(m.getInnerTransaction().getModule().getModule()), "MS-03a submit on server 1 records server 1");
			p = mc.submit(ServerFuncIT.sub("from s2")); m = ServerFuncIT.find(121, ServerFuncIT.msgId(p));
			check(m != null && S2.equals(m.getInnerTransaction().getModule().getModule()), "MS-03b submit on server 2 records server 2");
			sendDr("2000001", 120, S1); check(gets(ma, "2000001", 6000), "MS-04a DR of MA comes out of server 1");
			sendDr("2000002", 121, S2); check(gets(mc, "2000002", 6000), "MS-04b DR of MC comes out of server 2");
		}
		try (R a = new R(P1); R b = new R(P2)) { a.bind("mb", "pw122"); b.bind("mb", "pw122"); Thread.sleep(800);
			sendDr("2000003", 122, S1); check(gets(a, "2000003", 6000) , "MS-05a MB on both, message came through server 1: DR on server 1");
			sendDr("2000004", 122, S2); check(gets(b, "2000004", 6000), "MS-05b MB on both, message came through server 2: DR on server 2");
		}
		Thread.sleep(1000);
		try (R b = new R(P2)) { b.bind("mb", "pw122"); Thread.sleep(800); sendDr("2000005", 122, S1); check(gets(b, "2000005", 8000), "MS-06 MB now only on server 2, message came through server 1: DR follows to server 2"); }
		Thread.sleep(1000);
		try (R a = new R(P1)) { a.bind("mn", "pw123"); a.submit(ServerFuncIT.sub("x")); }
		Thread.sleep(1000);
		try (R b = new R(P2)) { b.bind("mn", "pw123"); Thread.sleep(800); sendDr("2000006", 123, S1); check(gets(b, "2000006", 8000), "MS-07 customer reconnected from server 1 to server 2: DR follows"); }
	}

	/** SmppServerModules changed and RELOAD_CUSTOMER: the customer moves, the others stay */
	static void moveCustomer() throws Exception {
		Thread.sleep(1000);
		R live = new R(P1); check(bound(live.bind("ma", "pw120")), "MS-10a MA bound on server 1 before the move");
		R other = new R(P2); other.bind("mc", "pw121"); R other1 = new R(P1); other1.bind("md", "pw124");
		String moved = base.replace(cust("MA", 120, S1), cust("MA", 120, S2));
		reload(moved);
		Pdu p = tryBind(P2, "ma", "pw120"); check(bound(p), "MS-10b moved to server 2: binds on server 2: " + p);
		p = tryBind(P1, "ma", "pw120"); check(rejected(p), "MS-10c moved away: refused by server 1: " + p);
		check(live.closedWithin(3000), "MS-10d its session on server 1 is closed");
		check(alive(other, 11), "MS-10e another customer on server 2 not touched");
		check(alive(other1, 12), "MS-10f another customer on server 1 not touched");
		try (R r = new R(P2)) { check(bound(r.bind("ma", "pw120")), "MS-10g client reconnects to the new server"); Thread.sleep(800);
			sendDr("2000010", 120, S1); check(gets(r, "2000010", 8000), "MS-10h DR of a message taken by server 1 reaches the customer on server 2");
			Pdu s = r.submit(ServerFuncIT.sub("after move")); check(s != null && s.cmd == 0x80000004 && s.status == 0, "MS-10i submit on the new server works: " + s); }
		Thread.sleep(800);
		check(waitFor(() -> !active(120, S1), 8000), "MS-10j Core no longer finds an active session of MA on server 1");
		reload(base);
		p = tryBind(P1, "ma", "pw120"); check(bound(p), "MS-11a moved back: binds on server 1: " + p);
		p = tryBind(P2, "ma", "pw120"); check(rejected(p), "MS-11b moved back: refused by server 2: " + p);
		// from one server to both, and the other way
		reload(base.replace(cust("MD", 124, S1), cust("MD", 124, S1 + "," + S2)));
		check(other1.closedWithin(3000), "MS-12a MD changed (widened to both): its session is rebuilt, the client reconnects");
		other1.close(); other1 = new R(P1); check(bound(other1.bind("md", "pw124")), "MS-12a2 reconnects on server 1");
		check(bound(tryBind(P2, "md", "pw124")), "MS-12b MD binds on server 2 as well");
		reload(base);
		p = tryBind(P2, "md", "pw124"); check(rejected(p), "MS-12c MD narrowed to server 1: refused by server 2: " + p);
		other1.close(); check(bound(tryBind(P1, "md", "pw124")), "MS-12d narrowed: still binds on server 1");
		live.close(); other.close(); other1.close();
	}

	static void addAndRemove() throws Exception {
		reload(base + cust("ME", 125, S2));
		check(bound(tryBind(P2, "me", "pw125")), "MS-13a new customer for server 2 by reload: binds there");
		Pdu p = tryBind(P1, "me", "pw125"); check(rejected(p), "MS-13b refused by server 1: " + p);
		R s = new R(P2); s.bind("me", "pw125");
		reload(base);
		p = tryBind(P2, "me", "pw125"); check(rejected(p), "MS-13c customer removed by reload: refused: " + p);
		check(s.closedWithin(3000), "MS-13d its session is closed"); s.close();
		reload(base.replace("password=pw121", "password=new121"));
		p = tryBind(P2, "mc", "pw121"); check(rejected(p), "MS-14a password changed: old one refused on its server: " + p);
		check(bound(tryBind(P2, "mc", "new121")), "MS-14b new one accepted");
		reload(base);
	}

	/** one server killed: the other carries on, DRs follow or wait; back up after a restart */
	static void killAndRestart() throws Exception {
		R mn = new R(P1); mn.bind("mn", "pw123"); R mc = new R(P2); mc.bind("mc", "pw121"); Thread.sleep(800);
		n1.destroyForcibly(); n1.waitFor();
		check(mn.closedWithin(3000), "MS-20a server 1 killed: its clients are disconnected");
		check(alive(mc, 21), "MS-20b customer on server 2 not touched");
		Pdu p = tryBind(P2, "ma", "pw120"); check(rejected(p), "MS-21a MA (only server 1) cannot move by itself: " + p);
		try (R b = new R(P2)) { check(bound(b.bind("mn", "pw123")), "MS-21b MN (unrestricted) reconnects to server 2"); Thread.sleep(7000);
			sendDr("2000020", 123, S1); check(gets(b, "2000020", 8000), "MS-21c DR of a message taken by the dead server reaches MN on server 2"); }
		// the only server of this customer is dead: Core gets no stream and hands the DR to its retry policy
		// (DirectInDRDispatchService, MessageProcessorThread), so nothing is written to a stream nobody reads
		GmmsMessage held = mt("2000021", 120, 8, 0);
		held.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		held.setStatus(GmmsStatus.DELIVERED);
		held.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", S1), "conn"));
		StreamQueueManager q = StreamQueueManager.getInstance();
		check(q.getDrPendingQueue(held) == null, "MS-22a customer only on the dead server: Core gets no DR stream (goes to the DR retry policy)");
		n1 = launch(S1, 1, P1, h(S1));
		final GmmsMessage heldDr = held;
		check(waitFor(() -> String.valueOf(q.getDrPendingQueue(heldDr)).contains(S1), 12000), "MS-22b server 1 restarted: Core finds its DR stream again");
		try (R r = new R(P1)) { check(bound(r.bind("ma", "pw120")), "MS-22c server 1 restarted: MA binds again"); Thread.sleep(800);
			sendDr("2000022", 120, S1); check(gets(r, "2000022", 8000), "MS-22d new DR delivered after the restart"); }
		mn.close(); mc.close();
	}
}
