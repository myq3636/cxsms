package it;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.gmms.GmmsUtility;
import com.king.gmms.RetryPolicyManager;
import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * cm.cfg reload with a changed shortName and/or CCBSSID on real Core, Server and Client modules, the
 * module type coming from a2p_modules.conf (not part of the repo). Only the changed customer may be touched.
 */
public class RenameReloadIT extends ItSupport {
	static Path cm;
	static String mod;
	static int msgNo = 0;

	static A2PCustomerManager ctm() { return GmmsUtility.getInstance().getCustomerManager(); }
	static String who(int ssid) { A2PCustomerInfo c = ctm().getCustomerBySSID(ssid); return c == null ? null : c.getShortName(); }
	static boolean policy(int ssid) { return RetryPolicyManager.getInstance().getSsid_policy_map().containsKey(ssid); }
	static String files(String f) { return jedis.hget("config:files:" + mod + ":1", f); }

	static void control(String cmd) { jedis.publish("channel:gmms:control", cmd); }

	static void load(String text) throws Exception {
		String at = files("cm.loadedAt");
		Files.write(cm, text.getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		waitFor(() -> !String.valueOf(at).equals(files("cm.loadedAt")), 6000);
		Thread.sleep(1200);
	}

	static long logs(String text) throws Exception {
		long n = 0;
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(text)) n++;
		return n;
	}

	public static void main(String[] args) throws Exception {
		byte[] orig = null;
		home = System.getProperty("a2p_home");
		cm = Paths.get(home + "conf/cm.cfg");
		mod = System.getProperty("module");
		orig = Files.readAllBytes(cm);
		Path phone = Paths.get(home + "conf/senderReplacement.cfg"), route = Paths.get(home + "conf/routing/CUST8888_routingInfo.cfg");
		byte[] phoneOrig = Files.readAllBytes(phone), routeOrig = Files.readAllBytes(route);
		try {
			if (mod.startsWith("Core")) core(new String(orig, "UTF-8"), phone, route);
			else if (mod.contains("Server")) server(new String(orig, "UTF-8"));
			else client(new String(orig, "UTF-8"));
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		} finally {
			Files.write(cm, orig);
			Files.write(phone, phoneOrig);
			Files.write(route, routeOrig);
		}
		exit();
	}

	// ==================== Core ====================

	/** the MT ends: routed key, or null when it failed */
	static String sendMt(String id, int ossid, int rssid) throws Exception {
		StreamQueueManager.getInstance().produceSubmitMessage(mt(id, ossid, rssid, 0));
		waitFor(() -> logLine(id, "[STREAM-ACK] flow=server->core") != null || logLine(id, "[STREAM-NACK]") != null, 8000);
		for (String k : jedis.keys("stream:mt:routed:*")) if (inStreams(k, id).size() > 0) return k;
		return null;
	}

	static void noChannelFails(String tag, String id, int rssid) throws Exception {
		String routed = sendMt(id, 8888, rssid);
		check(routed == null && logLine(id, "[STREAM-ACK] flow=server->core") != null && logLine(id, "[STREAM-NACK]") == null,
				tag + "a MT to a channel that is gone: not routed, acked, not retried");
		check(waitFor(() -> cdrLines(id, "OUT_SUBMIT").size() == 1, 5000) && logLine(id, "[CDR] type=OUT_SUBMIT status=2120") != null,
				tag + "b OUT_SUBMIT CDR status 2120");
		check(waitFor(() -> inStreams("stream:dr:pending:8888:*", id).size() == 1, 5000), tag + "c failure DR written for the customer");
	}

	static void core(String text, Path phone, Path route) throws Exception {
		boot();
		MTStreamConsumer.getInstance().start();
		ResultStreamConsumer.getInstance().start();
		waitFor(() -> files("cm.version") != null, 15000);
		String base = text.replace("[VENDOR8]\nCCBSSID: 8\n", "[VENDOR8]\nCCBSSID: 8\nSMSOptionMTRetryPolicy: 5M*3\n");
		load(base);
		A2PCustomerInfo vendor9 = ctm().getCustomerBySSID(9), cust8888 = ctm().getCustomerBySSID(8888);
		check("stream:mt:routed:8:1".equals(sendMt("8888_RC0", 8888, -1)) || String.valueOf(sendMt("8888_RC00", 8888, -1)).startsWith("stream:mt:routed:8:"), "RC-00 baseline: MT routed to rssid 8");

		// rename, same ssid
		load(base.replace("[VENDOR8]", "[VENDOR8N]"));
		check("ok".equals(files("cm.result")) && "VENDOR8N".equals(who(8)) && ctm().getSSidbyCustomerNameshort("VENDOR8") < 0
				&& ctm().getSSidbyCustomerNameshort("VENDOR8N") == 8, "RC-01a rename VENDOR8 -> VENDOR8N: ssid 8 resolves to the new name, the old name is gone");
		check(policy(8), "RC-01b retry policy of ssid 8 kept (before: lost)");
		check(ctm().getCustomerBySSID(9) == vendor9 && ctm().getCustomerBySSID(8888) == cust8888,
				"RC-01c other customers keep the very same object (not rebuilt)");
		check(String.valueOf(sendMt("8888_RC1", 8888, -1)).startsWith("stream:mt:routed:8:"), "RC-01d routing tables keep ssid 8, MT still routed");
		control("RELOAD_ROUTING:-f"); Thread.sleep(1500);
		noChannelFails("RC-02", "8888_RC2", -1);       // routing file still says Relay=VENDOR8
		load(base); control("RELOAD_ROUTING:-f"); Thread.sleep(1500);
		check(String.valueOf(sendMt("8888_RC3", 8888, -1)).startsWith("stream:mt:routed:8:"), "RC-03 name restored and RELOAD_ROUTING: routed again");

		// ssid change: routing changes only with RELOAD_ROUTING
		load(base.replace("[VENDOR8]\nCCBSSID: 8\n", "[VENDOR8]\nCCBSSID: 108\n"));
		check(who(8) == null && "VENDOR8".equals(who(108)) && !policy(8) && policy(108), "RC-04a VENDOR8 ssid 8 -> 108: lookups and retry policy moved");
		noChannelFails("RC-05", "8888_RC5", -1);       // routing table still has rssid 8
		noChannelFails("RC-06", "8888_RC6", 8);        // a retried MT that already has rssid 8
		control("RELOAD_ROUTING"); Thread.sleep(1500);
		noChannelFails("RC-06b", "8888_RC6b", -1);     // incremental: the routing file did not change
		Path rf = Paths.get(home + "conf/routing/CUST8888_routingInfo.cfg");
		Files.setLastModifiedTime(rf, java.nio.file.attribute.FileTime.fromMillis(System.currentTimeMillis()));
		control("RELOAD_ROUTING"); Thread.sleep(1500);
		check(String.valueOf(sendMt("8888_RC7", 8888, -1)).startsWith("stream:mt:routed:108:"), "RC-07 file of the customer touched + RELOAD_ROUTING: MT goes to rssid 108");
		load(base); control("RELOAD_ROUTING:-f"); Thread.sleep(1500);

		// rename + occupied ssid: the channel is not lost
		load(base.replace("[VENDOR8]\nCCBSSID: 8\n", "[VENDOR8N]\nCCBSSID: 9\n"));
		check("VENDOR8".equals(who(8)) && "VENDOR9".equals(who(9)) && ctm().getSSidbyCustomerNameshort("VENDOR8") == 8,
				"RC-08a rename onto a used ssid: old VENDOR8 kept (before: channel lost)");
		check("partial".equals(files("cm.result")) && "VENDOR8N".equals(files("cm.failed")) && "VENDOR8".equals(files("cm.kept")),
				"RC-08b cm.result=partial, cm.failed=VENDOR8N, cm.kept=VENDOR8");
		check(String.valueOf(sendMt("8888_RC8", 8888, -1)).startsWith("stream:mt:routed:8:"), "RC-08c MT still routed through the kept channel");
		load(base.replace("[VENDOR8]\nCCBSSID: 8\n", "[VENDOR8N]\nCCBSSID: 8\n"));
		check("VENDOR8N".equals(who(8)) && "ok".equals(files("cm.result")) && "".equals(files("cm.kept")), "RC-08d fixed the ssid: renamed normally, nothing kept");
		load(base);
		String noVendor = base.substring(0, base.indexOf("[VENDOR8]")) + base.substring(base.indexOf("[VENDOR9]"));
		load(noVendor);
		check(who(8) == null && "ok".equals(files("cm.result")), "RC-08e a real removal (no failed block): the customer is removed");
		load(base);

		// swap
		load(base.replace("[VENDOR8]\nCCBSSID: 8\n", "[VENDOR8]\nCCBSSID: X\n").replace("[VENDOR9]\nCCBSSID: 9\n", "[VENDOR9]\nCCBSSID: 8\n").replace("CCBSSID: X", "CCBSSID: 9"));
		check("VENDOR9".equals(who(8)) && "VENDOR8".equals(who(9)) && policy(8) && policy(9), "RC-09 swap ssids: both lookups right, both retry policies present");
		load(base);

		// routing and phone prefix are written to Redis when they take effect, not on a cm.cfg reload
		String rv = files("routing.version"), pv = files("phonePrefix.version");
		Files.write(route, "CCBSSID: 8888\nA2PMainRouting: (Priority=30,PhonePrefix=86,Relay=VENDOR12)\n".getBytes("UTF-8"));
		Files.write(phone, "[VENDOR8]\nCCBSSID: 8\nSenderReplacement: (PhonePrefix=86,ReplaceTo=1069,SenderAddress=all,Ossid=all,Rssid=8)\n".getBytes("UTF-8"));
		load(base + "\n[CUSTNEW90]\nCCBSSID: 90\nA2PPlayerType: operator\nProtocol: SMPP\n");
		Thread.sleep(500);
		check(rv.equals(files("routing.version")) && String.valueOf(sendMt("8888_RC10", 8888, -1)).startsWith("stream:mt:routed:8:"),
				"RC-10a routing file changed + cm.cfg reload only: Redis version unchanged, MT still by the old table");
		check(pv.equals(files("phonePrefix.version")), "RC-10b senderReplacement changed + cm.cfg reload only: Redis version unchanged");
		control("RELOAD_ROUTING"); control("RELOAD_PHONE_PREFIX");
		check(waitFor(() -> !rv.equals(files("routing.version")), 6000) && String.valueOf(sendMt("8888_RC11", 8888, -1)).startsWith("stream:mt:routed:12:"),
				"RC-11a RELOAD_ROUTING: Redis version changes and MT follows the new table");
		check(waitFor(() -> !pv.equals(files("phonePrefix.version")), 6000), "RC-11b RELOAD_PHONE_PREFIX after that cm.cfg reload: new content takes effect and is written");
	}

	// ==================== Server ====================

	static final int SPORT = 17786;

	static SmppTestClient connect(String user, String pwd) throws Exception {
		SmppTestClient c = new SmppTestClient("127.0.0.1", SPORT, 5, null);
		return c.bind(user, pwd) ? c : null;
	}

	static boolean submitWorks(SmppTestClient c, int ssid) throws Exception {
		long before = streamLen("stream:mt:pending:" + ssid + ":*");
		c.submit("18667991226", "8613800000000", "x");
		return waitFor(() -> streamLen("stream:mt:pending:" + ssid + ":*") > before, 4000);
	}

	static boolean drWorks(SmppTestClient c, int ossid) throws Exception {
		GmmsMessage d = mt("RSDR" + (++msgNo), ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInMsgID("IN-" + d.getMsgID());
		long before = c.delivers.get();
		StreamQueueManager q = StreamQueueManager.getInstance();
		q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
		return waitFor(() -> c.delivers.get() > before, 5000);
	}

	/** the customer with user/pwd after a reload: old connection closed, rebind, submit and DR work */
	static void afterChange(String tag, SmppTestClient old, String user, String pwd, int ssid) throws Exception {
		check(waitFor(() -> old.isClosedByServer(), 4000), tag + "a old connection of the changed customer closed");
		old.close();
		SmppTestClient c = connect(user, pwd);
		check(c != null, tag + "b rebind accepted (before, for a rename: rejected)");
		if (c != null) {
			check(submitWorks(c, ssid), tag + "c submit written to stream:mt:pending:" + ssid);
			check(drWorks(c, ssid), tag + "d DR delivered on the new connection");
			c.close();
			Thread.sleep(300);
		}
	}

	/** c33 was never changed: same connection, still working */
	static void untouched(String tag, SmppTestClient c33) throws Exception {
		check(!c33.isClosedByServer() && submitWorks(c33, 33) && drWorks(c33, 33), tag + " unchanged customer c33: connection not closed, submit and DR work");
	}

	static void server(String text) throws Exception {
		boot();
		jedis.set("thcon", "1");
		props.setProperty("MultiSmppServer1.Port", String.valueOf(SPORT));
		new MultiSmppServer().startService();
		Thread.sleep(1500);
		waitFor(() -> files("cm.version") != null, 15000);
		String hdr = "[SMPPCUST31]\nCCBSSID: 31\n", hdr32 = "[SMPPCUST32]\nCCBSSID: 32\n";
		SmppTestClient c33 = connect("c33", "p33");
		check(c33 != null, "RS-00 unchanged customer c33 bound for the whole test");
		long binds = logs("bind_transceiver ") ;

		SmppTestClient old = connect("c31", "p31");
		load(text.replace("[SMPPCUST31]", "[SMPPCUST31N]"));
		afterChange("RS-01", old, "c31", "p31", 31);       // rename only
		untouched("RS-01e", c33);
		old = connect("c31", "p31");
		load(text);
		afterChange("RS-02", old, "c31", "p31", 31);       // rename back

		old = connect("c31", "p31");
		load(text.replace(hdr, "[SMPPCUST31]\nCCBSSID: 131\n"));
		afterChange("RS-03", old, "c31", "p31", 131);      // ssid only
		untouched("RS-03e", c33);
		old = connect("c31", "p31");
		load(text);
		old.close();

		old = connect("c31", "p31");
		load(text.replace(hdr, "[SMPPCUST31N]\nCCBSSID: 131\n"));
		afterChange("RS-04", old, "c31", "p31", 131);      // both
		untouched("RS-04e", c33);
		load(text);

		old = connect("c31", "p31");
		SmppTestClient old32 = connect("c32", "p32");
		load(text.replace(hdr, "[SMPPCUST31]\nCCBSSID: X\n").replace(hdr32, "[SMPPCUST32]\nCCBSSID: 31\n").replace("CCBSSID: X", "CCBSSID: 32"));
		afterChange("RS-05", old, "c31", "p31", 32);       // swap, both customers work
		afterChange("RS-06", old32, "c32", "p32", 31);
		untouched("RS-06e", c33);
		load(text);
		SmppTestClient a = connect("c31", "p31"), b = connect("c32", "p32");
		check(a != null && b != null, "RS-07 swapped back: both bind");
		if (a != null) a.close();
		if (b != null) b.close();
	}

	// ==================== Client ====================

	static final int VPORT = 17787;

	/** a vendor: answers bind, enquire_link and submit_sm; counts per system_id */
	static final class Vendor implements Runnable {
		final Map<String, AtomicInteger> binds = new ConcurrentHashMap<>(), closed = new ConcurrentHashMap<>(), submits = new ConcurrentHashMap<>();
		final ServerSocket server;
		Vendor(int port) throws Exception { server = new ServerSocket(port); }
		int get(Map<String, AtomicInteger> m, String k) { AtomicInteger n = m.get(k); return n == null ? 0 : n.get(); }
		public void run() {
			while (true) {
				try {
					final Socket s = server.accept();
					new Thread(() -> serve(s)).start();
				} catch (Exception e) { return; }
			}
		}
		void serve(Socket s) {
			String sys = "?";
			try {
				DataInputStream in = new DataInputStream(s.getInputStream());
				DataOutputStream out = new DataOutputStream(new java.io.BufferedOutputStream(s.getOutputStream()));
				int id = 0;
				while (true) {
					int len = in.readInt(), cmd = in.readInt(); in.readInt(); int seq = in.readInt();
					byte[] body = new byte[len - 16];
					in.readFully(body);
					byte[] resp = new byte[0];
					if (cmd == 9 || cmd == 1 || cmd == 2) {
						int z = 0;
						while (body[z] != 0) z++;
						sys = new String(body, 0, z, "US-ASCII");
						binds.computeIfAbsent(sys, k -> new AtomicInteger()).incrementAndGet();
						resp = "stub\0".getBytes("US-ASCII");
					} else if (cmd == 4) {
						submits.computeIfAbsent(sys, k -> new AtomicInteger()).incrementAndGet();
						resp = ("V" + (++id) + "\0").getBytes("US-ASCII");
					} else if (cmd < 0) {
						continue;
					}
					out.writeInt(16 + resp.length); out.writeInt(cmd | 0x80000000); out.writeInt(0); out.writeInt(seq); out.write(resp); out.flush();
				}
			} catch (Exception e) {
				closed.computeIfAbsent(sys, k -> new AtomicInteger()).incrementAndGet();
			}
		}
	}

	static boolean mtReachesVendor(Vendor v, String sys, int rssid) throws Exception {
		int before = v.get(v.submits, sys);
		StreamQueueManager.getInstance().produceOutboundMessage(mt("8888_RK" + (++msgNo), 8888, rssid, 0));
		return waitFor(() -> v.get(v.submits, sys) > before, 8000);
	}

	static void client(String text) throws Exception {
		String blocks = "\n[CLIV41]\nCCBSSID: 141\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nWindowSize: 20\n"
				+ "OutgoingConnection: (ConnName=CLIV41_c1,userName=u141,password=p141,URL=127.0.0.1,Port=" + VPORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n"
				+ "\n[CLIV43]\nCCBSSID: 143\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nWindowSize: 20\n"
				+ "OutgoingConnection: (ConnName=CLIV43_c1,userName=u143,password=p143,URL=127.0.0.1,Port=" + VPORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n";
		String base = text + blocks;
		Files.write(cm, base.getBytes("UTF-8"));
		Vendor v = new Vendor(VPORT);
		Thread t = new Thread(v); t.setDaemon(true); t.start();
		boot();
		new MultiSmppClient().startService();
		OutboundStreamConsumer.getInstance().start();
		check(waitFor(() -> v.get(v.binds, "u141") == 1 && v.get(v.binds, "u143") == 1, 15000), "RK-00 both vendors bound once at startup");
		check(mtReachesVendor(v, "u141", 141) && mtReachesVendor(v, "u143", 143), "RK-00b MT reaches both vendors");
		waitFor(() -> files("cm.version") != null, 15000);

		// rename
		load(base.replace("[CLIV41]", "[CLIV41N]"));
		check(waitFor(() -> v.get(v.closed, "u141") == 1 && v.get(v.binds, "u141") == 2, 15000), "RK-01a rename: old connection closed, one new bind");
		check(mtReachesVendor(v, "u141", 141), "RK-01b MT reaches the renamed vendor");
		check(v.get(v.closed, "u143") == 0 && v.get(v.binds, "u143") == 1 && mtReachesVendor(v, "u143", 143), "RK-01c unchanged vendor: no reconnect, MT still works");
		load(base);
		check(waitFor(() -> v.get(v.binds, "u141") == 3, 15000) && mtReachesVendor(v, "u141", 141), "RK-02 renamed back works");

		// ssid only
		int b0 = v.get(v.binds, "u141");
		load(base.replace("[CLIV41]\nCCBSSID: 141\n", "[CLIV41]\nCCBSSID: 241\n"));
		check(waitFor(() -> v.get(v.binds, "u141") == b0 + 1, 15000) && mtReachesVendor(v, "u141", 241), "RK-03 ssid 141 -> 241: rebound, MT to rssid 241 reaches it");
		check(v.get(v.closed, "u143") == 0 && v.get(v.binds, "u143") == 1, "RK-03b unchanged vendor untouched");

		// both
		load(base);
		int b1 = v.get(v.binds, "u141");
		waitFor(() -> v.get(v.binds, "u141") > b0 + 1, 15000);
		int b2 = v.get(v.binds, "u141");
		load(base.replace("[CLIV41]\nCCBSSID: 141\n", "[CLIV41N]\nCCBSSID: 241\n"));
		check(waitFor(() -> v.get(v.binds, "u141") == b2 + 1, 15000) && mtReachesVendor(v, "u141", 241), "RK-04 name and ssid both changed: rebound, MT reaches it");
		check(v.get(v.closed, "u143") == 0 && v.get(v.binds, "u143") == 1, "RK-04b unchanged vendor untouched");
	}
}
