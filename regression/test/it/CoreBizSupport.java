package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Business cases of Core on the real MT consumer: configuration written before boot, MTs through stream:mt:pending. */
public class CoreBizSupport extends ItSupport {
	static final String DST = "8613800000000";
	static int seq = 0;

	static String cust(String name, int ssid, String opts) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\n" + opts;
	}
	static void route(String name, int ssid, String... lines) throws Exception {
		StringBuilder b = new StringBuilder("CCBSSID: " + ssid + "\n");
		for (String l : lines) b.append(l).append('\n');
		Files.write(Paths.get(home + "conf/routing/" + name + "_routingInfo.cfg"), b.toString().getBytes("UTF-8"));
	}
	static void conf(String relative, String text) throws Exception {
		Path p = Paths.get(home + "conf/" + relative);
		Files.createDirectories(p.getParent());
		Files.write(p, text.getBytes("UTF-8"));
	}
	static void appendCm(String text) throws Exception {
		Path cm = Paths.get(home + "conf/cm.cfg");
		Files.write(cm, (new String(Files.readAllBytes(cm), "UTF-8") + text).getBytes("UTF-8"));
	}
	static void start() throws Exception {
		boot();
		MTStreamConsumer.getInstance().start();
	}

	/** what became of one MT */
	static class Out {
		String id; int rssid = -1; GmmsMessage dr; int inCdr; boolean acked;
		public String toString() { return id + " rssid=" + rssid + " drStatus=" + (dr == null ? "none" : String.valueOf(dr.getStatusCode())) + " inCdr=" + inCdr + " acked=" + acked; }
		boolean routedTo(int r) { return rssid == r && dr == null; }
		boolean rejected(GmmsStatus s) { return rssid == -1 && dr != null && dr.getStatusCode() == s.getCode(); }
	}
	static GmmsMessage msg(int ossid, String sender, String recipient, String text) {
		String id = ossid + "_T" + (++seq);
		GmmsMessage m = mt(id, ossid, -1, 0);
		m.setSenderAddress(sender);
		m.setRecipientAddress(recipient);
		m.setTextContent(text);
		m.setMessageSize(text.length());
		return m;
	}
	static Out send(GmmsMessage m) throws Exception {
		final String id = m.getMsgID();
		StreamQueueManager.getInstance().produceSubmitMessage(m);
		Out o = new Out(); o.id = id;
		o.acked = waitFor(() -> logLine(id, "[STREAM-ACK] flow=server->core") != null, 8000);
		for (String k : jedis.keys("stream:mt:routed:*")) if (!inStreams(k, id).isEmpty()) o.rssid = Integer.parseInt(k.split(":")[3]);
		if (o.rssid == -1) {
			waitFor(() -> !inStreams("stream:dr:pending:" + m.getOSsID() + ":*", id).isEmpty() || logLine(id, "[CDR] type=OUT_SUBMIT") != null, 3000);
			waitFor(() -> !inStreams("stream:dr:pending:" + m.getOSsID() + ":*", id).isEmpty(), 1500);
		}
		List<GmmsMessage> drs = inStreams("stream:dr:pending:" + m.getOSsID() + ":*", id);
		if (!drs.isEmpty()) o.dr = drs.get(0);
		waitFor(() -> cdrCount(id, "IN_SUBMIT") >= 1, 2500);
		o.inCdr = cdrCount(id, "IN_SUBMIT");
		return o;
	}
	static Out send(int ossid, String sender, String recipient, String text) throws Exception { return send(msg(ossid, sender, recipient, text)); }
	static Out send(int ossid) throws Exception { return send(ossid, "18667991226", DST, "hello"); }
}
