package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/** The real SMPP client against a fake SMSC: bind refused, port closed, disconnects, keep-alive, bind modes, sessions, window. */
public class ClientConnectionIT extends ItSupport {
	static final int PORT = 17791, PORT2 = 17792;
	static StreamQueueManager q;
	static int no = 0;

	static String vendor(String name, int ssid, String user, int port, String opts, String bindMode, int sessions) {
		// a key written twice in one block is joined (20,2), fails to parse and falls back to the default: set a default here only when opts does not
		String base = "";
		if (!opts.contains("EnquireLink:")) base += "EnquireLink: false\n";
		if (!opts.contains("WindowSize:")) base += "WindowSize: 20\n";
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nBufferTimeout: 60\n" + base + opts
				+ "OutgoingConnection: (ConnName=" + name + "_c1,userName=" + user + ",password=p,URL=127.0.0.1,Port=" + port + ",SessionNumber=" + sessions + ",BindMode=" + bindMode + ",Version=V34,IsInit=true)\n";
	}
	/** an MT out of Core for this channel; its text is its id, so the SMSC can tell the messages apart */
	static String send(int rssid) {
		String id = "8888_CC" + (++no);
		GmmsMessage m = mt(id, 8888, rssid, 0);
		m.setTextContent(id);
		m.setMessageSize(id.length());
		q.produceOutboundMessage(m);
		return id;
	}
	static GmmsMessage result(String id, long ms) throws Exception {
		waitFor(() -> !inStreams("stream:core:results*", id).isEmpty(), ms);
		List<GmmsMessage> l = inStreams("stream:core:results*", id);
		return l.isEmpty() ? null : l.get(0);
	}
	static String st(GmmsMessage g) { return g == null ? "none" : String.valueOf(g.getStatusCode()); }
	/** refuse binds from now on and close the link, returns the refusals so far */
	static int a301Rejected2(FakeSmsc.Account a) throws Exception {
		if (a.bindStatus == 0) { a.bindStatus = 0x0E; a.closeAll(); Thread.sleep(1500); }
		return a.rejected.get();
	}
	static boolean reached(FakeSmsc.Account a, String id) throws Exception { return waitFor(() -> a.count(id) >= 1, 8000); }

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder();
		cm.append(vendor("CV301", 301, "u301", PORT, "", "TRX", 1));       // bind refused
		cm.append(vendor("CV302", 302, "u302", PORT2, "", "TRX", 1));      // port closed
		cm.append(vendor("CV303", 303, "u303", PORT, "EnquireLink: true\nEnquireLinkTime: 1\n", "TRX", 1));       // disconnects
		cm.append(vendor("CV304", 304, "u304", PORT, "EnquireLink: true\nEnquireLinkTime: 1\nEnquireLinkFailureNum: 3\n", "TRX", 1));
		cm.append(vendor("CV305", 305, "u305", PORT, "EnquireLink: true\nEnquireLinkTime: 1\nEnquireLinkFailureNum: 3\n", "TRX", 1));
		cm.append(vendor("CV306", 306, "u306", PORT, "", "T", 1));
		cm.append(vendor("CV307", 307, "u307", PORT, "", "R", 1));
		cm.append(vendor("CV308", 308, "u308", PORT, "", "TRX", 2));
		cm.append(vendor("CV309", 309, "u309", PORT, "WindowSize: 2\n", "TRX", 1));
		cm.append(vendor("CV310", 310, "u310", PORT, "ConnectionSilentTime: 2\n", "TRX", 1));
		cm.append(vendor("CV311", 311, "u311", PORT, "", "TRX", 1));       // SMSC asks enquire_link
		cm.append(vendor("CV312", 312, "u312", PORT, "ConnectionSilentTime: 12\n", "TRX", 1));      // reconnect, no keep-alive
		cm.append(vendor("CV313", 313, "u313", PORT, "EnquireLink: true\nEnquireLinkTime: 8\n", "TRX", 1));      // reconnect, keep-alive
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + cm).getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(PORT);
		FakeSmsc.Account a301 = smsc.account("u301"); a301.bindStatus = 0x0E;
		FakeSmsc.Account a303 = smsc.account("u303"), a304 = smsc.account("u304"), a305 = smsc.account("u305"), a306 = smsc.account("u306"), a307 = smsc.account("u307"), a308 = smsc.account("u308"), a309 = smsc.account("u309"), a310 = smsc.account("u310"), a311 = smsc.account("u311"), a312 = smsc.account("u312"), a313 = smsc.account("u313");
		a305.answerEnquire = false;
		try {
			boot();
			q = StreamQueueManager.getInstance();
			new MultiSmppClient().startService();
			OutboundStreamConsumer.getInstance().start();

			// ---- a lost connection wakes the session thread: it reconnects after ReconnectInterval, whatever the keep-alive sleep. Closed right after it went to sleep. ----
			check(waitFor(() -> a312.binds.get() == 1 && a313.binds.get() == 1, 8000), "CN-05i bound");
			check(waitFor(() -> a313.enquires.get() >= 1, 12000), "CN-05j keep-alive thread just sent enquire_link and sleeps EnquireLinkTime (8 s)");
			a313.closeAll();
			long wake = a312.firstBindAt + 11900 + 300;      // its first sleep of ConnectionSilentTime - 100 is over, the next one started
			while (System.currentTimeMillis() < wake) Thread.sleep(50);
			a312.closeAll();
			check(waitFor(() -> a313.binds.get() == 2, 15000), "CN-05k closed by the vendor: connected again");
			long ka = a313.nextBindAt - a313.firstCloseAt;
			System.out.println("INFO reconnect with keep-alive: " + ka + " ms");
			check(ka >= 900 && ka < 2500, "CN-05l ReconnectInterval 1 s, EnquireLinkTime 8 s: about 1 s, not the rest of the keep-alive sleep: " + ka + " ms");
			check(waitFor(() -> a312.binds.get() == 2, 20000), "CN-05m EnquireLink off: connected again");
			long si = a312.nextBindAt - a312.firstCloseAt;
			System.out.println("INFO reconnect without keep-alive: " + si + " ms");
			check(si >= 900 && si < 2500, "CN-05n ReconnectInterval 1 s, ConnectionSilentTime 12 s: about 1 s: " + si + " ms");

			// ---- bind refused ----
			check(waitFor(() -> a301.rejected.get() >= 3, 15000), "CN-03a bind refused: tried again at ReconnectInterval: " + a301.rejected.get() + " tries");
			String id = send(301);
			check("2500".equals(st(result(id, 5000))) && a301.count(id) == 0, "CN-03b no session: failure result 2500, nothing sent: " + st(result(id, 100)));
			a301.bindStatus = 0;
			check(waitFor(() -> a301.binds.get() == 1, 8000), "CN-03c vendor accepts again: bound");
			Thread.sleep(300);
			id = send(301);
			check(reached(a301, id), "CN-03d MT goes out");

			// ---- port closed ----
			id = send(302);
			check("2500".equals(st(result(id, 5000))), "CN-04a port closed: failure result 2500: " + st(result(id, 100)));
			FakeSmsc smsc2 = new FakeSmsc(PORT2);
			FakeSmsc.Account a302 = smsc2.account("u302");
			check(waitFor(() -> a302.binds.get() == 1, 8000), "CN-04b port open: connected and bound");
			Thread.sleep(300);
			id = send(302);
			check(reached(a302, id), "CN-04c MT goes out");

			// ---- vendor disconnects with MTs waiting for a response ----
			check(waitFor(() -> a303.binds.get() == 1, 8000), "CN-05a bound");
			a303.answerSubmit = false;
			String w1 = send(303), w2 = send(303);
			check(reached(a303, w1) && reached(a303, w2), "CN-05b two MTs sent, no response yet");
			check(inStreams("stream:core:results*", w1).isEmpty(), "CN-05c waiting: no result yet");
			a303.closeAll();
			GmmsMessage r1 = result(w1, 8000), r2 = result(w2, 8000);
			check(r1 != null && r2 != null, "CN-05d vendor disconnects: both waiting MTs get a failure result: " + st(r1) + " " + st(r2));
			check(waitFor(() -> a303.binds.get() == 2, 8000), "CN-05e reconnected at ReconnectInterval");
			a303.answerSubmit = true;
			Thread.sleep(300);
			id = send(303);
			check(reached(a303, id), "CN-05f MT after the reconnect goes out");
			Thread.sleep(500);
			check(a303.count(w1) == 1 && a303.count(w2) == 1, "CN-05g the waiting MTs were not sent again by the Client");
			check(inStreams("stream:core:results*", w1).size() == 1, "CN-05h one result each");

			// ---- keep-alive ----
			check(waitFor(() -> a304.enquires.get() >= 3, 10000), "CN-06 idle link: enquire_link at EnquireLinkTime: " + a304.enquires.get());
			check(a304.closed.get() == 0 && a304.binds.get() == 1, "CN-06b answered: connection stays");
			check(waitFor(() -> a305.closed.get() >= 1, 15000), "CN-07a enquire_link not answered: connection closed");
			check(a305.enquiresAtFirstClose == 3, "CN-07b closed after EnquireLinkFailureNum (3) unanswered: " + a305.enquiresAtFirstClose);
			check(waitFor(() -> a305.binds.get() >= 2, 8000), "CN-07c connected again");
			long sel = a305.nextBindAt - a305.firstCloseAt;
			check(sel >= 900, "CN-07d the session thread stopped it itself: ReconnectInterval is still waited, no fast reconnect: " + sel + " ms");
			check(waitFor(() -> a311.binds.get() == 1, 8000), "CN-08a bound");
			a311.open().get(0).pdu(0x15, 0, 77, new byte[0]);
			check(waitFor(() -> a311.enquireResps.get() == 1, 5000), "CN-08b SMSC asks enquire_link: answered");

			// ---- silent ----
			check(waitFor(() -> a310.closed.get() >= 1, 15000), "CN-09a ConnectionSilentTime 2 s, no traffic: connection closed");
			check(waitFor(() -> a310.binds.get() >= 2, 8000), "CN-09b connected again");

			// ---- bind modes ----
			check(waitFor(() -> a306.binds.get() == 2 && a307.binds.get() == 1, 8000), "CN-02a BindMode=T is split into a transmitter and a receiver connection: " + a306.bindCmds);
			check(a306.bindCmds.contains(1) && a306.bindCmds.contains(2), "CN-02b T: bind_transmitter and bind_receiver: " + a306.bindCmds);
			check(a307.bindCmds.size() == 1 && a307.bindCmds.get(0) == 1, "CN-02c BindMode=R: only bind_receiver: " + a307.bindCmds);
			Thread.sleep(300);
			id = send(306);
			check(reached(a306, id), "CN-02d T session sends");
			id = send(307);
			GmmsMessage rr = result(id, 5000);
			check(rr != null && a307.count(id) == 0, "CN-02e R session does not send, failure result: " + st(rr));

			// ---- several sessions ----
			check(waitFor(() -> a308.binds.get() == 2, 8000), "CN-10a SessionNumber=2: two binds: " + a308.binds.get());
			Thread.sleep(300);
			List<String> many = new ArrayList<>();
			for (int i = 0; i < 20; i++) many.add(send(308));
			check(waitFor(() -> a308.texts.size() >= 20, 8000), "CN-10b all sent");
			int used = 0; for (java.util.concurrent.atomic.AtomicInteger n : a308.perLink.values()) if (n.get() > 0) used++;
			check(used == 2, "CN-10c both sessions used: " + a308.perLink.values());

			// ---- a dead vendor is not hammered: bind tries stay at ReconnectInterval ----
			int t0 = a301Rejected2(a301);
			Thread.sleep(3000);
			int tries = a301Rejected2(a301) - t0;
			check(tries >= 2 && tries <= 4, "CN-03e bind refused again, 3 s: tries at ReconnectInterval 1 s, no storm: " + tries);
			a301.bindStatus = 0;

			// ---- window ----
			check(waitFor(() -> a309.binds.get() == 1, 8000), "TW-04a bound, WindowSize 2");
			Thread.sleep(300);
			a309.answerSubmit = false;
			List<String> win = new ArrayList<>();
			for (int i = 0; i < 6; i++) win.add(send(309));
			waitFor(() -> a309.texts.size() >= 2, 5000);
			Thread.sleep(1500);
			check(a309.texts.size() == 2, "TW-04b window full: only 2 sent: " + a309.texts.size());
			int fails = 0; for (String w : win) if (!inStreams("stream:core:results*", w).isEmpty()) fails++;
			check(fails == 0, "TW-04c no failure result for the ones waiting: " + fails);
			a309.answerSubmit = true;
			a309.release();
			check(waitFor(() -> a309.texts.size() >= 6, 15000), "TW-04d responses arrive: the rest goes out: " + a309.texts.size());
			Thread.sleep(800);
			int once = 0; for (String w : win) if (a309.count(w) == 1) once++;
			check(once == 6 && a309.texts.size() == 6, "TW-04e every MT sent exactly once: " + once + " of 6, total " + a309.texts.size());
			int ok = 0; for (String w : win) { GmmsMessage g = result(w, 5000); if (g != null && g.getStatusCode() == 0) ok++; }
			check(ok == 6, "TW-04f every MT has a success result: " + ok);
			long left = 0; for (String k : jedis.keys("stream:mt:routed:309:*")) left += jedis.xlen(k);
			check(left == 0, "TW-04g stream empty: " + left);
			check(jedis.keys("client:sent:8888_CC*").isEmpty(), "TW-04h no marker left");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
