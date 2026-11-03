package it;

import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;

/** The real SMPP client when Redis really goes away (a cut TCP proxy) and when responses come late. Checks read Redis directly. */
public class ClientFaultIT extends ItSupport {
	static final int PORT = 17794, GATE = 16381;
	static final String GROUP = "ClientOutboundSubmitGroup";
	static StreamQueueManager q;
	static RedisGate gate;
	static int no = 0;

	static String vendor(String name, int ssid, String user, String opts) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 3\n" + opts
				+ "OutgoingConnection: (ConnName=" + name + "_c1,userName=" + user + ",password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n";
	}
	static GmmsMessage msg(int rssid) {
		String id = "8888_CF" + (++no);
		GmmsMessage m = mt(id, 8888, rssid, 0);
		m.setTextContent(id);
		m.setMessageSize(id.length());
		return m;
	}
	static String send(int rssid) { GmmsMessage m = msg(rssid); q.produceOutboundMessage(m); return m.getMsgID(); }
	static int results(String id) { return inStreams("stream:core:results*", id).size(); }
	static GmmsMessage result(String id) { List<GmmsMessage> l = inStreams("stream:core:results*", id); return l.isEmpty() ? null : l.get(0); }
	static long pel(int rssid) {
		long n = 0;
		for (String k : jedis.keys("stream:mt:routed:" + rssid + ":*")) { try { n += jedis.xpending(k, GROUP).getTotal(); } catch (Exception e) { } }
		return n;
	}
	static long drs(int ssid) { long n = 0; for (String k : jedis.keys("stream:core:inbound-dr:" + ssid + "*")) n += jedis.xlen(k); return n; }

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedis_master_port=" + GATE + "\nRedisStreamAutoClaimEnable=true\nRedisStreamAutoClaimIdleMs=2000\nRedisStreamAutoClaimIntervalMs=500\nCoreMtLeaseMs=2000\nClientSendLeaseMs=2000\nSMPPSubmitPendingResultMaxRetry=4\n").getBytes("UTF-8"));
		StringBuilder cm = new StringBuilder();
		cm.append(vendor("FV501", 501, "u501", "WindowSize: 20\n"));
		cm.append(vendor("FV502", 502, "u502", "WindowSize: 1\n"));
		cm.append(vendor("FV503", 503, "u503", "WindowSize: 20\nSMSOptionOutgoingThrottlingNum: 10\n"));
		cm.append(vendor("FV504", 504, "u504", "WindowSize: 20\n"));
		cm.append(vendor("FV505", 505, "u505", "WindowSize: 20\n"));
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + cm).getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(PORT);
		FakeSmsc.Account a1 = smsc.account("u501"), a2 = smsc.account("u502"), a3 = smsc.account("u503"), a4 = smsc.account("u504"), a5 = smsc.account("u505");
		gate = new RedisGate(GATE, 16379);
		try {
			boot();
			q = StreamQueueManager.getInstance();
			new MultiSmppClient().startService();
			check(waitFor(() -> a1.binds.get() == 1 && a2.binds.get() == 1 && a3.binds.get() == 1 && a4.binds.get() == 1 && a5.binds.get() == 1, 15000), "CF-00 all bound");
			Thread.sleep(300);

			// ---- send marker not reachable: the worker's attempt, then Redis back ----
			Method pm = OutboundStreamConsumer.class.getDeclaredMethod("processMessage", String.class, GmmsMessage.class);
			pm.setAccessible(true);
			GmmsMessage m9 = msg(504);
			q.produceOutboundMessage(m9);
			String stream9 = null;
			for (String k : jedis.keys("stream:mt:routed:504:*")) if (jedis.xlen(k) > 0) stream9 = k;
			q.createGroup(stream9, GROUP, true);
			Map<String, StreamEntryID> query = new HashMap<>();
			query.put(stream9, StreamEntryID.UNRECEIVED_ENTRY);
			GmmsMessage read = q.consumeBatch(GROUP, "client-outbound-submit-1", 10, query, true).get(0);
			gate.cut();
			Thread.sleep(300);
			pm.invoke(OutboundStreamConsumer.getInstance(), stream9, read);
			check(a4.count(m9.getMsgID()) == 0, "ID-09a Redis gone, marker not reachable: not sent");
			check(pel(504) == 1, "ID-09b entry stays pending");
			gate.restore();
			Thread.sleep(500);
			GmmsMessage again = q.recoverPending(stream9, GROUP, "client-outbound-submit-1", 0L, 10, true).get(0);
			pm.invoke(OutboundStreamConsumer.getInstance(), stream9, again);
			check(a4.count(m9.getMsgID()) == 1 && pel(504) == 0, "ID-09c Redis back: sent once, acked: " + a4.count(m9.getMsgID()));

			OutboundStreamConsumer.getInstance().start();
			Thread.sleep(500);

			// ---- response after the timeout ----
			a1.answerSubmit = false;
			String late = send(501);
			check(waitFor(() -> a1.count(late) == 1, 5000), "RS-06a sent, no response");
			check(waitFor(() -> results(late) == 1, 8000), "RS-06b BufferTimeout 3 s: failure result");
			GmmsMessage lr = result(late);
			check(lr != null && lr.getStatusCode() == 2700, "RS-06c it is 2700: " + (lr == null ? "none" : lr.getStatusCode()));
			a1.answerSubmit = true;
			a1.release();
			Thread.sleep(800);
			check(results(late) == 1, "RS-06d the response comes late: no second result: " + results(late));
			check(new String(Files.readAllBytes(Paths.get(home + "log/it.log")), "UTF-8").contains("Can't find the original message"), "RS-06e logged: response without a waiting message");
			check(a1.open().size() == 1, "RS-06f connection stays");

			// ---- result cannot be written ----
			a1.answerSubmit = false;
			String w = send(501);
			check(waitFor(() -> a1.count(w) == 1, 5000), "RS-07a sent, response held");
			gate.cut();
			Thread.sleep(300);
			a1.answerSubmit = true;
			a1.release();
			Thread.sleep(800);
			check(results(w) == 0, "RS-07b Redis gone, response received: no result yet");
			gate.restore();
			check(waitFor(() -> results(w) == 1, 8000), "RS-07c Redis back: result written once by the retry");
			Thread.sleep(1500);
			GmmsMessage wr = result(w);
			check(results(w) == 1 && wr != null && wr.getStatusCode() == 0, "RS-07d success, one result: " + results(w) + " " + (wr == null ? "none" : wr.getStatusCode()));

			// ---- result never written: give up, window free ----
			a2.answerSubmit = false;
			String g = send(502);
			check(waitFor(() -> a2.count(g) == 1, 5000), "RS-07e WindowSize 1: sent, response held");
			gate.cut();
			Thread.sleep(300);
			a2.answerSubmit = true;
			a2.release();
			Thread.sleep(8000);                       // retries every second, SMPPSubmitPendingResultMaxRetry 4
			gate.restore();
			Thread.sleep(1500);
			check(results(g) == 0, "RS-07f retries used up: no result, Core outbox answers later: " + results(g));
			String next = send(502);
			check(waitFor(() -> a2.count(next) == 1, 8000), "RS-07g window freed: the next MT goes out");

			// ---- vendor DR, Redis gone ----
			long d0 = drs(505);
			gate.cut();
			Thread.sleep(300);
			byte[] receipt = "id:GATE1 sub:001 dlvrd:001 submit date:2611031200 done date:2611031201 stat:DELIVRD err:000 text:x".getBytes("US-ASCII");
			int seq = a5.deliver("8613800000000", "18667991226", 4, receipt, null);
			check(waitFor(() -> a5.deliverResp.containsKey(seq), 15000) && a5.deliverResp.get(seq) == 0x64, "VD-02e Redis really gone: ESME_RX_T_APPN: " + a5.deliverResp.get(seq));
			check(drs(505) == d0, "VD-02f nothing written");
			gate.restore();
			Thread.sleep(800);
			int seq2 = a5.deliver("8613800000000", "18667991226", 4, receipt, null);
			check(waitFor(() -> a5.deliverResp.containsKey(seq2), 8000) && a5.deliverResp.get(seq2) == 0 && drs(505) == d0 + 1, "VD-02g the vendor's next try is accepted");

			// ---- Redis gone while a channel is sending ----
			List<String> many = new ArrayList<>();
			for (int i = 0; i < 40; i++) many.add(send(503));
			waitFor(() -> a3.texts.size() >= 8, 8000);
			gate.cut();
			Thread.sleep(3000);
			int during = a3.texts.size();
			gate.restore();
			check(waitFor(() -> { int n = 0; for (String id : many) if (a3.count(id) >= 1) n++; return n == 40; }, 40000), "FT-03a Redis back after 3 s: every MT reaches the vendor");
			Thread.sleep(3000);
			int twice = 0, missing = 0;
			for (String id : many) { int c = a3.count(id); if (c > 1) twice++; if (c == 0) missing++; }
			System.out.println("INFO during=" + during + " twice=" + twice + " missing=" + missing + " total=" + a3.texts.size());
			check(twice == 0 && missing == 0, "FT-03b every MT exactly once: twice " + twice + ", missing " + missing);
			long left = 0;
			for (String k : jedis.keys("stream:mt:routed:503:*")) left += jedis.xlen(k);
			check(waitFor(() -> pel(503) == 0, 15000), "FT-03c nothing left pending: " + pel(503) + ", in the stream " + left);
			int noResult = 0; for (String id : many) if (results(id) != 1) noResult++;
			check(noResult == 0, "FT-03d one result for each: " + noResult + " without exactly one");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
