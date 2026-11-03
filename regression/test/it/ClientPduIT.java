package it;

import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.throttle.DistributedThrottlingManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** The submit_sm the vendor really receives, field by field; connection policy; rate and send-marker details. */
public class ClientPduIT extends ItSupport {
	static final int PORT = 17796;
	static StreamQueueManager q;
	static int no = 0;
	static FakeSmsc smsc;

	static String vendor(String name, int ssid, String user, String opts) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\n" + opts
				+ "OutgoingConnection: (ConnName=" + name + "_c1,userName=" + user + ",password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n";
	}
	static GmmsMessage msg(int rssid) {
		String id = "8888_PD" + (++no);
		GmmsMessage m = mt(id, 8888, rssid, 0);
		m.setTextContent(id);
		m.setMessageSize(id.length());
		return m;
	}
	/** the message goes through the real consumer and session; what the vendor received */
	static FakeSmsc.Submit send(FakeSmsc.Account a, GmmsMessage m) throws Exception {
		int before = a.submits.size();
		q.produceOutboundMessage(m);
		waitFor(() -> a.submits.size() > before, 8000);
		return a.submits.size() > before ? a.submits.get(a.submits.size() - 1) : null;
	}
	static GmmsMessage result(String id, long ms) throws Exception {
		waitFor(() -> !inStreams("stream:core:results*", id).isEmpty(), ms);
		List<GmmsMessage> l = inStreams("stream:core:results*", id);
		return l.isEmpty() ? null : l.get(0);
	}
	static String hex(byte[] b) { if (b == null) return "null"; StringBuilder s = new StringBuilder(); for (byte x : b) s.append(String.format("%02X", x)); return s.toString(); }

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nClientStreamTaskMaxBatches=1\nClientStreamTaskMaxMs=2000\nRedisStreamConsumer.ClientOutboundSubmit.WorkerMaxPoolSize=2\n").getBytes("UTF-8"));
		StringBuilder cm = new StringBuilder();
		cm.append(vendor("PV701", 701, "u701", ""));
		cm.append(vendor("PV702", 702, "u702", "ServiceType: CMT\nSMSOptionSourceAddrTon: 2\nSMSOptionSourceAddrNpi: 3\nSMSOptionDestAddrTon: 4\nSMSOptionDestAddrNpi: 5\nSMSOptionProtocolId: 3F\n"));
		cm.append(vendor("PV703", 703, "u703", "SMSOptionSupportOutgoingGSM7bit: 1\n"));
		cm.append(vendor("PV704", 704, "u704", "SMSOptionSupportOutgoingGSM7bit: 2\n"));
		cm.append(vendor("PV705", 705, "u705", "ChlUdhConcatenated: true\n"));
		cm.append(vendor("PV706", 706, "u706", "SMSOptionTransferValidityPeriod: true\n"));
		cm.append(vendor("PV707", 707, "u707", "SMSOptionTransferValidityPeriod: true\nSMSOptionIsRelateValidityPeriod: true\n"));
		cm.append(vendor("PV708", 708, "u708", "ChlIsSupportDeliveryReport: No\n"));
		cm.append(vendor("PV709", 709, "u709", "SMSOptionHttpCustomParameter: OTP\n"));
		cm.append(vendor("PV710", 710, "u710", "SMSOptionOutgoingThrottlingNum: 7\n"));
		cm.append(vendor("PV711", 711, "u711", ""));
		cm.append(vendor("PV712", 712, "u712", ""));
		cm.append("\n[PV713]\nCCBSSID: 713\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\nSubmitPolicy_Conn: (Policy=primary,Primary=PV713_b)\n"
				+ "OutgoingConnection: (ConnName=PV713_a,userName=u713a,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true);(ConnName=PV713_b,userName=u713b,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n");
		cm.append("\n[PV714]\nCCBSSID: 714\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nBufferTimeout: 30\nWindowSize: 100\nSubmitPolicy_Conn: (Policy=loadbalance)\n"
				+ "OutgoingConnection: (ConnName=PV714_a,userName=u714a,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true);(ConnName=PV714_b,userName=u714b,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n");
		Path p = Paths.get(home + "conf/cm.cfg");
		final String base = new String(Files.readAllBytes(p), "UTF-8") + cm;
		Files.write(p, base.getBytes("UTF-8"));
		smsc = new FakeSmsc(PORT);
		try {
			boot();
			q = StreamQueueManager.getInstance();
			new MultiSmppClient().startService();
			OutboundStreamConsumer.getInstance().start();
			final String[] users = { "u701", "u702", "u703", "u704", "u705", "u706", "u707", "u708", "u709", "u710", "u711", "u712", "u713a", "u713b", "u714a", "u714b" };
			check(waitFor(() -> { for (String u : users) if (smsc.account(u).binds.get() != 1) return false; return true; }, 20000), "PD-00 all bound");
			Thread.sleep(300);
			FakeSmsc.Account a1 = smsc.account("u701"), a2 = smsc.account("u702"), a3 = smsc.account("u703"), a4 = smsc.account("u704"), a5 = smsc.account("u705"), a6 = smsc.account("u706"), a7 = smsc.account("u707"), a8 = smsc.account("u708"), a9 = smsc.account("u709");

			// ---- addresses, service type ----
			GmmsMessage m = msg(701);
			FakeSmsc.Submit s = send(a1, m);
			check(s != null && "18667991226".equals(s.source) && "8613800000000".equals(s.dest), "PD-01a source and dest: " + (s == null ? "" : s.source + " " + s.dest));
			check(s != null && m.getMsgID().equals(s.text()), "PD-01b short_message is the text");
			check(s != null && s.srcTon == 1 && s.srcNpi == 1 && s.dstTon == 1 && s.dstNpi == 1, "PD-06a default TON and NPI: 1/1: " + (s == null ? "" : s.srcTon + "/" + s.srcNpi + " " + s.dstTon + "/" + s.dstNpi));
			check(s != null && s.service.isEmpty() && s.protocol == 0 && s.esm == 0 && s.validity.isEmpty(), "PD-01c defaults: no service type, protocol 0, esm 0, no validity");
			m = msg(702);
			s = send(a2, m);
			check(s != null && "CMT".equals(s.service), "PD-01d ServiceType: " + (s == null ? "" : s.service));
			check(s != null && s.srcTon == 2 && s.srcNpi == 3 && s.dstTon == 4 && s.dstNpi == 5, "PD-06b configured TON and NPI: " + (s == null ? "" : s.srcTon + "/" + s.srcNpi + " " + s.dstTon + "/" + s.dstNpi));
			check(s != null && s.protocol == 0x3F, "PD-06c SMSOptionProtocolId 3F: " + (s == null ? "" : s.protocol));
			m = msg(702);
			m.setSenderAddress("SENDERID");
			s = send(a2, m);
			check(s != null && "SENDERID".equals(s.source) && s.srcTon == 5 && s.srcNpi == 0, "PD-06d alphanumeric sender: TON 5, NPI 0: " + (s == null ? "" : s.srcTon + "/" + s.srcNpi));

			// ---- encoding ----
			String zh = "你好";
			m = msg(701);
			m.setContentType(GmmsMessage.AIC_CS_UCS2); m.setTextContent(zh); m.setMessageSize(4);
			s = send(a1, m);
			System.out.println("INFO default channel UCS2 dcs=" + (s == null ? "" : s.dcs) + " sm=" + (s == null ? "" : hex(s.sm)));
			check(s != null && hex(s.sm).equals("4F60597D") && s.dcs == 8, "PD-02a UCS2 on a default channel: data_coding 8, UTF-16BE bytes: dcs " + (s == null ? "" : s.dcs) + " " + (s == null ? "" : hex(s.sm)));
			m = msg(703);
			m.setContentType(GmmsMessage.AIC_CS_UCS2); m.setTextContent(zh); m.setMessageSize(4);
			s = send(a3, m);
			check(s != null && hex(s.sm).equals("4F60597D") && s.dcs == 8, "PD-02b UCS2, channel with GSM7bit 1: data_coding 8: dcs " + (s == null ? "" : s.dcs));
			m = msg(701);
			m.setTextContent("hello"); m.setMessageSize(5);
			s = send(a1, m);
			check(s != null && s.dcs == 0 && "hello".equals(s.text()), "PD-02c ASCII: data_coding 0: " + (s == null ? "" : s.dcs));
			m = msg(703);
			m.setTextContent("hello"); m.setMessageSize(5); m.setGsm7bit(true);
			s = send(a3, m);
			check(s != null && s.dcs == 0 && "hello".equals(s.text()), "PD-02d GSM7bit 1: unpacked GSM: " + (s == null ? "" : hex(s.sm)));
			m = msg(704);
			m.setTextContent("hello"); m.setMessageSize(5); m.setGsm7bit(true);
			s = send(a4, m);
			check(s != null && s.dcs == 0 && hex(s.sm).equals("E8329BFD06"), "PD-02e GSM7bit 2: packed: " + (s == null ? "" : hex(s.sm)));

			// ---- segments ----
			m = msg(705);
			m.setTextContent("part"); m.setMessageSize(4);
			m.setUdh(new byte[] { 5, 0, 3, 9, 2, 1 });
			m.setSarMsgRefNum("09"); m.setSarTotalSegments(2); m.setSarSegmentSeqNum(1); m.setSplitStatus(1);
			s = send(a5, m);
			check(s != null && s.esm == 0x40 && hex(s.sm).equals("050003090201" + hex("part".getBytes())), "PD-03a ChlUdhConcatenated: esm 0x40, UDH in front of the text: " + (s == null ? "" : s.esm + " " + hex(s.sm)));
			check(s != null && !s.tlvs.containsKey(0x020C), "PD-03b no SAR parameters");
			m = msg(701);
			m.setTextContent("part"); m.setMessageSize(4);
			m.setSarMsgRefNum("09"); m.setSarTotalSegments(2); m.setSarSegmentSeqNum(1); m.setSplitStatus(1);
			s = send(a1, m);
			check(s != null && s.esm == 0 && hex(s.tlvs.get(0x020C)).equals("0009") && hex(s.tlvs.get(0x020E)).equals("02") && hex(s.tlvs.get(0x020F)).equals("01"), "PD-03c default: SAR parameters: " + (s == null ? "" : hex(s.tlvs.get(0x020C)) + " " + hex(s.tlvs.get(0x020E)) + " " + hex(s.tlvs.get(0x020F))));

			// ---- validity ----
			s = send(a6, msg(706));
			check(s != null && s.validity.matches("\\d{12}\\d{3}[+-]"), "PD-04a SMSOptionTransferValidityPeriod: absolute: " + (s == null ? "" : s.validity));
			s = send(a7, msg(707));
			check(s != null && s.validity.matches("000000\\d{6}000R"), "PD-04b SMSOptionIsRelateValidityPeriod: relative: " + (s == null ? "" : s.validity));

			// ---- DR request ----
			s = send(a1, msg(701));
			check(s != null && s.registered == 1, "PD-05a default: registered_delivery 1");
			s = send(a8, msg(708));
			check(s != null && s.registered == 0, "PD-05b ChlIsSupportDeliveryReport No: registered_delivery 0");

			// ---- custom parameter ----
			s = send(a9, msg(709));
			boolean found = false;
			if (s != null) for (byte[] v : s.tlvs.values()) found |= hex(v).startsWith(hex("OTP".getBytes()));
			check(found, "PD-07 SMSOptionHttpCustomParameter: in an optional parameter: " + (s == null ? "" : s.tlvs.keySet()));

			// ---- cannot be built ----
			m = msg(701);
			m.setContentType("NO-SUCH-CHARSET");
			final String bad = m.getMsgID();
			int b0 = a1.submits.size();
			q.produceOutboundMessage(m);
			GmmsMessage rb = result(bad, 8000);
			check(rb != null && rb.getStatusCode() != 0 && a1.submits.size() == b0, "PD-09 no PDU can be built: nothing sent, failure result: " + (rb == null ? "none" : rb.getStatusCode()));
			String after = msg(701).getMsgID();
			GmmsMessage ok = msg(701);
			s = send(a1, ok);
			check(s != null && ok.getMsgID().equals(s.text()), "PD-09b the channel goes on");

			// ---- send marker: one per part and per try ----
			FakeSmsc.Account a11 = smsc.account("u711");
			GmmsMessage t0 = mt("8888_ID8", 8888, 711, 0); t0.setTextContent("try"); t0.setMessageSize(3);
			send(a11, t0);
			GmmsMessage t1 = mt("8888_ID8", 8888, 711, 0); t1.setTextContent("try"); t1.setMessageSize(3); t1.setRetriedNumber(1);
			send(a11, t1);
			GmmsMessage t2 = mt("8888_ID8", 8888, 711, 2); t2.setTextContent("try"); t2.setMessageSize(3); t2.setSarSegmentSeqNum(2);
			send(a11, t2);
			check(a11.count("try") == 3, "ID-08a same msgId: second try of Core and another part each go out: " + a11.count("try"));
			GmmsMessage t3 = mt("8888_ID8", 8888, 711, 0); t3.setTextContent("try"); t3.setMessageSize(3);
			waitFor(() -> jedis.keys("client:sent:8888_ID8*").isEmpty(), 3000);
			check(jedis.keys("client:sent:8888_ID8*").isEmpty(), "ID-08b markers removed");

			// ---- connection policy ----
			FakeSmsc.Account pa = smsc.account("u713a"), pb = smsc.account("u713b");
			for (int i = 0; i < 10; i++) q.produceOutboundMessage(msg(713));
			check(waitFor(() -> pb.texts.size() == 10, 8000) && pa.texts.isEmpty(), "CN-11a Policy=primary, Primary=b: all on b: a " + pa.texts.size() + " b " + pb.texts.size());
			pb.bindStatus = 0x0E;
			pb.closeAll();
			Thread.sleep(500);
			int b13 = pa.texts.size();
			for (int i = 0; i < 5; i++) q.produceOutboundMessage(msg(713));
			check(waitFor(() -> pa.texts.size() == b13 + 5, 8000), "CN-11b primary down: the other connection takes over: a " + pa.texts.size());
			pb.bindStatus = 0;
			check(waitFor(() -> pb.binds.get() == 2, 8000), "CN-11c primary back: bound");
			Thread.sleep(500);
			int bb = pb.texts.size();
			for (int i = 0; i < 5; i++) q.produceOutboundMessage(msg(713));
			check(waitFor(() -> pb.texts.size() == bb + 5, 8000), "CN-11d and used again");
			FakeSmsc.Account la = smsc.account("u714a"), lb = smsc.account("u714b");
			for (int i = 0; i < 20; i++) q.produceOutboundMessage(msg(714));
			check(waitFor(() -> la.texts.size() + lb.texts.size() == 20, 8000) && la.texts.size() > 0 && lb.texts.size() > 0, "CN-11e Policy=loadbalance: both used: " + la.texts.size() + "/" + lb.texts.size());

			// ---- rate details ----
			Method bm = OutboundStreamConsumer.class.getDeclaredMethod("batchSize", String.class);
			bm.setAccessible(true);
			check((Integer) bm.invoke(OutboundStreamConsumer.getInstance(), "stream:mt:routed:710:0") == 7, "TW-06 limited channel, 7 TPS: reads 7 at a time");
			check((Integer) bm.invoke(OutboundStreamConsumer.getInstance(), "stream:mt:routed:701:0") > 7, "TW-06b unlimited channel: the normal batch");
			Method rm = OutboundStreamConsumer.class.getDeclaredMethod("getWindowFullRetryMs");
			rm.setAccessible(true);
			props.setProperty("ClientOutboundWindowFullRetryMs", "1");
			long lo = (Long) rm.invoke(OutboundStreamConsumer.getInstance());
			props.setProperty("ClientOutboundWindowFullRetryMs", "999999");
			long hi = (Long) rm.invoke(OutboundStreamConsumer.getInstance());
			props.setProperty("ClientOutboundWindowFullRetryMs", "bad");
			long df = (Long) rm.invoke(OutboundStreamConsumer.getInstance());
			props.remove("ClientOutboundWindowFullRetryMs");
			check(lo == 10 && hi == 60000 && df == 100, "TW-05 ClientOutboundWindowFullRetryMs limited to 10..60000, 100 when invalid: " + lo + " " + hi + " " + df);

			// ---- task budget: with 2 workers and 3 big backlogs, a small channel still gets its turn ----
			FakeSmsc.Account big = smsc.account("u712");
			for (int i = 0; i < 3000; i++) q.produceOutboundMessage(msg(712));
			Thread.sleep(100);
			FakeSmsc.Account sm = smsc.account("u711");
			int s0 = sm.texts.size();
			long t = System.currentTimeMillis();
			q.produceOutboundMessage(msg(711));
			check(waitFor(() -> sm.texts.size() == s0 + 1, 8000), "CS-06a ClientStreamTaskMaxBatches 1: small channel served during a 3000 backlog");
			int done = big.texts.size();
			System.out.println("INFO small channel after " + (System.currentTimeMillis() - t) + " ms, big channel at " + done + " of 3000");
			check(waitFor(() -> big.texts.size() == 3000, 30000), "CS-06b the backlog finishes too: " + big.texts.size());
			Set<String> uniq = new HashSet<>(big.texts);
			check(uniq.size() == 3000, "CS-06c task re-queued between batches: nothing twice: " + uniq.size());
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		exit();
	}
}
