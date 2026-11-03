package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.messagequeue.ClientSendMarker;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/** The real client over time: backlog at start, dead letter, cm.cfg reload of rate and channels, Core not reading results, response against timeout; multi-node and SMPP 3.3 channels. */
public class ClientLifecycleIT extends ItSupport {
	static final int PORT = 17797;
	static StreamQueueManager q;
	static int no = 0;
	static Path cmFile;
	static FakeSmsc smsc;

	static String vendor(String name, int ssid, String user, String opts) {
		String base = "ReconnectInterval: 1\nEnquireLink: false\n";
		if (!opts.contains("WindowSize:")) base += "WindowSize: 300\n";
		if (!opts.contains("BufferTimeout:")) base += "BufferTimeout: 60\n";
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\n" + base + opts
				+ "OutgoingConnection: (ConnName=" + name + "_c1,userName=" + user + ",password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n";
	}
	static GmmsMessage msg(int rssid) {
		String id = "8888_LC" + (++no);
		GmmsMessage m = mt(id, 8888, rssid, 0);
		m.setTextContent(id);
		m.setMessageSize(id.length());
		return m;
	}
	static String send(int rssid) { GmmsMessage m = msg(rssid); q.produceOutboundMessage(m); return m.getMsgID(); }
	static List<GmmsMessage> results(String id) { return inStreams("stream:core:results*", id); }
	static GmmsMessage one(String id, long ms) throws Exception {
		waitFor(() -> !results(id).isEmpty(), ms);
		List<GmmsMessage> l = results(id);
		return l.isEmpty() ? null : l.get(0);
	}
	static void reload() throws Exception {
		jedis.publish("channel:gmms:control", "RELOAD_CUSTOMER");
		Thread.sleep(2500);
	}
	static boolean inDlq(String id) {
		if (!jedis.exists("stream:dlq:submit")) return false;
		for (StreamEntry e : jedis.xrange("stream:dlq:submit", (StreamEntryID) null, (StreamEntryID) null)) if (e.getFields().toString().contains(id)) return true;
		return false;
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedisStreamAutoClaimEnable=true\nRedisStreamAutoClaimIdleMs=500\nRedisStreamAutoClaimIntervalMs=300\nRedisStreamMaxDeliveries=3\n").getBytes("UTF-8"));
		StringBuilder cm = new StringBuilder();
		cm.append(vendor("LV801", 801, "u801", ""));
		cm.append(vendor("LV802", 802, "u802", "SMSOptionOutgoingThrottlingNum: 2\n"));
		cm.append(vendor("LV803", 803, "u803", "BufferTimeout: 1\n"));
		cm.append(vendor("LV804", 804, "u804", "WindowSize: 1\n"));
		cm.append(vendor("LV805", 805, "u805", ""));
		cm.append(vendor("LV806", 806, "u806", "BufferTimeout: 30\n"));
		cm.append("\n[LV807]\nCCBSSID: 807\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MN\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nWindowSize: 100\nBufferTimeout: 30\nConnectionSilentTime: 300\nSubmitPolicy_Node: (Policy=primary,Primary=N1)\nMaxRetryTimes: 1\n"
				+ "OutgoingConnection: (NodeName=N1,ConnName=LV807_a,userName=u807a,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true);(NodeName=N2,ConnName=LV807_b,userName=u807b,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n");
		cm.append("\n[LV808]\nCCBSSID: 808\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nWindowSize: 100\nBufferTimeout: 30\nSMSOptionSMPPVersion: V33\n"
				+ "OutgoingConnection: (ConnName=LV808_c1,userName=u808,password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V33,IsInit=true)\n");
		cmFile = Paths.get(home + "conf/cm.cfg");
		final String base = new String(Files.readAllBytes(cmFile), "UTF-8") + cm;
		Files.write(cmFile, base.getBytes("UTF-8"));
		smsc = new FakeSmsc(PORT);
		FakeSmsc.Account a1 = smsc.account("u801"), a2 = smsc.account("u802"), a3 = smsc.account("u803"), a4 = smsc.account("u804"), a5 = smsc.account("u805"), a6 = smsc.account("u806"), a7 = smsc.account("u807a"), a7b = smsc.account("u807b"), a8 = smsc.account("u808");
		try {
			boot();
			q = StreamQueueManager.getInstance();
			new MultiSmppClient().startService();
			check(waitFor(() -> a1.binds.get() == 1 && a2.binds.get() == 1 && a3.binds.get() == 1 && a4.binds.get() == 1 && a5.binds.get() == 1 && a6.binds.get() == 1, 15000), "LC-00 all bound");
			Thread.sleep(300);

			// ---- backlog from before the start, doorbell gone ----
			List<String> old = new ArrayList<>();
			for (int i = 0; i < 20; i++) old.add(send(801));
			jedis.del("zset:mt:routed:active");
			OutboundStreamConsumer.getInstance().start();
			check(waitFor(() -> a1.texts.containsAll(old), 8000), "CS-09 backlog with its doorbell gone: found and sent when the consumer starts");

			// ---- a stuck entry ends in the dead letter stream ----
			GmmsMessage stuck = msg(801);
			jedis.setex(ClientSendMarker.key(stuck), 600, "S|another:x:1|" + (System.currentTimeMillis() + 600000));
			q.produceOutboundMessage(stuck);
			check(waitFor(() -> inDlq(stuck.getMsgID()), 15000), "CS-08a entry never finished: dead letter after RedisStreamMaxDeliveries 3");
			check(a1.count(stuck.getMsgID()) == 0, "CS-08b not sent");
			jedis.del(ClientSendMarker.key(stuck));

			// ---- window full: put off for ClientOutboundWindowFullRetryMs ----
			props.setProperty("RedisStreamAutoClaimIdleMs", "600000");      // else auto-claim retries the entry sooner
			props.setProperty("ClientOutboundWindowFullRetryMs", "4000");
			a4.answerSubmit = false;
			String w1 = send(804), w2 = send(804);
			check(waitFor(() -> a4.count(w1) == 1, 5000), "TW-05a WindowSize 1: first sent");
			Thread.sleep(500);
			a4.answerSubmit = true;
			long t0 = System.currentTimeMillis();
			a4.release();
			check(waitFor(() -> a4.count(w2) == 1, 8000), "TW-05b window free: second sent");
			long took = System.currentTimeMillis() - t0;
			System.out.println("INFO retry after " + took + " ms");
			check(took > 1500 && took < 5000, "TW-05c it waits for the retry delay of 4 s, counted from when it was put off: " + took + " ms");
			props.remove("ClientOutboundWindowFullRetryMs");
			props.setProperty("RedisStreamAutoClaimIdleMs", "500");

			// ---- response against timeout: one result each ----
			a3.answerSubmit = false;
			List<String> race = new ArrayList<>();
			for (int i = 0; i < 100; i++) { race.add(send(803)); Thread.sleep(15); }      // over 1.5 s: the oldest pass BufferTimeout 1 s while the newest do not
			a3.answerSubmit = true;
			a3.release();
			Thread.sleep(3000);
			int ok = 0, timedOut = 0, wrong = 0;
			for (String id : race) {
				List<GmmsMessage> l = results(id);
				if (l.size() != 1) wrong++;
				else if (l.get(0).getStatusCode() == 0) ok++;
				else timedOut++;
			}
			System.out.println("INFO race ok=" + ok + " timedOut=" + timedOut + " wrong=" + wrong);
			check(wrong == 0, "RS-09a response against BufferTimeout: exactly one result each: success " + ok + ", timeout " + timedOut + ", not one " + wrong);
			check(ok > 0 && timedOut > 0, "RS-09b the run did cover both sides: success " + ok + ", timeout " + timedOut);

			// ---- Core does not read results, stream over the limit ----
			props.setProperty("RedisStreamBackPressureEnable", "true");
			props.setProperty("RedisStreamMonitor.BlockXLEN", "1");
			props.setProperty("RedisStreamBackPressureEnable", "false");
			for (int i = 0; i < 30 && !(jedis.exists("stream:core:results:805:0") && jedis.exists("stream:core:results:805:1")); i++) one(send(805), 8000);
			check(jedis.exists("stream:core:results:805:0") && jedis.exists("stream:core:results:805:1"), "FT-07 both result shards hold an entry");
			props.setProperty("RedisStreamBackPressureEnable", "true");
			final String sent = send(805);
			check(waitFor(() -> a5.count(sent) == 1, 8000), "FT-07a result stream full: sending goes on");
			Thread.sleep(1500);
			int before = results(sent).size();
			props.remove("RedisStreamBackPressureEnable");
			props.remove("RedisStreamMonitor.BlockXLEN");
			check(before == 0, "FT-07b no result while it is full: " + before);
			check(one(sent, 15000) != null && results(sent).size() == 1, "FT-07c room again: the retry writes one result");

			// ---- rate changed by reload ----
			List<String> slow = new ArrayList<>();
			for (int i = 0; i < 30; i++) slow.add(send(802));
			Thread.sleep(4000);
			int at2 = a2.texts.size();
			check(at2 >= 6 && at2 <= 13, "TW-07a 2 TPS: in 4 s: " + at2);
			String now = new String(Files.readAllBytes(cmFile), "UTF-8");
			Files.write(cmFile, now.replace("SMSOptionOutgoingThrottlingNum: 2\n", "SMSOptionOutgoingThrottlingNum: 50\n").getBytes("UTF-8"));
			reload();
			check(GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(802).getOutgoingThrottlingNum() == 50, "TW-07b RELOAD_CUSTOMER: 50 TPS in effect");
			check(waitFor(() -> a2.binds.get() == 2, 8000), "TW-07c the changed channel reconnects");
			check(a1.binds.get() == 1, "TW-07d other channels do not");
			check(waitFor(() -> { for (String id : slow) if (results(id).isEmpty()) return false; return true; }, 15000), "TW-07e the rest of the backlog is handled soon after: a result for each");
			int failed = 0; for (String id : slow) if (results(id).get(0).getStatusCode() != 0) failed++;
			System.out.println("INFO during the reconnect " + failed + " of 30 got a failure result, vendor saw " + a2.texts.size());

			// ---- channel added ----
			String more = new String(Files.readAllBytes(cmFile), "UTF-8") + vendor("LV809", 809, "u809", "");
			Files.write(cmFile, more.getBytes("UTF-8"));
			reload();
			FakeSmsc.Account a9 = smsc.account("u809");
			check(waitFor(() -> a9.binds.get() == 1, 10000), "CF-03a channel added by reload: connected");
			Thread.sleep(300);
			String nw = send(809);
			check(waitFor(() -> a9.count(nw) == 1, 8000), "CF-03b MT reaches it");
			check(a1.binds.get() == 1, "CF-03c other channels untouched");

			// ---- channel removed with MTs waiting ----
			a6.answerSubmit = false;
			String p1 = send(806), p2 = send(806);
			check(waitFor(() -> a6.count(p1) == 1 && a6.count(p2) == 1, 5000), "CF-04a two MTs waiting for a response");
			String cut = new String(Files.readAllBytes(cmFile), "UTF-8");
			int i = cut.indexOf("\n[LV806]");
			int j = cut.indexOf("\n[", i + 2);
			Files.write(cmFile, (cut.substring(0, i) + cut.substring(j)).getBytes("UTF-8"));
			reload();
			check(waitFor(() -> a6.closed.get() >= 1, 10000), "CF-04b channel removed by reload: connection closed");
			GmmsMessage r1 = one(p1, 8000), r2 = one(p2, 8000);
			System.out.println("INFO removed channel " + (r1 == null ? "none" : r1.getStatusCode()) + " " + (r2 == null ? "none" : r2.getStatusCode()));
			check(r1 != null && r2 != null && r1.getStatusCode() != 0 && r2.getStatusCode() != 0, "CF-04c the waiting MTs get a failure result: " + (r1 == null ? "none" : r1.getStatusCode()));
			String gone = send(806);
			GmmsMessage rg = one(gone, 8000);
			check(rg != null && rg.getStatusCode() != 0, "CF-04d next MT to it: failure result: " + (rg == null ? "none" : rg.getStatusCode()));
			check(a1.closed.get() == 0, "CF-04e other channels untouched");

			// ---- multi-node ----
			check(waitFor(() -> a7.binds.get() == 1 && a7b.binds.get() == 1, 10000), "CN-12a ConnectionType MN: both nodes bound");
			for (int n = 0; n < 10; n++) send(807);
			check(waitFor(() -> a7.texts.size() == 10, 8000) && a7b.texts.isEmpty(), "CN-12b SubmitPolicy_Node primary N1: all on N1: " + a7.texts.size() + "/" + a7b.texts.size());
			Thread.sleep(300);
			a7.closeAll();
			check(waitFor(() -> a7.binds.get() == 2, 10000), "CN-12c node connection lost: connected again");
			long nd = a7.nextBindAt - a7.firstCloseAt;
			System.out.println("INFO multi-node reconnect " + nd + " ms");
			check(nd < 3000, "CN-12d NodeClientSessionThread: about ReconnectInterval, not the rest of its sleep (ConnectionSilentTime 300 s): " + nd + " ms");

			// ---- SMPP 3.3 ----
			check(waitFor(() -> a8.binds.get() == 1, 8000), "CN-13a V33: bound");
			check(a8.bindVersion == 0x33, "CN-13b interface_version 0x33: 0x" + Integer.toHexString(a8.bindVersion));
			Thread.sleep(300);
			String v3 = send(808);
			check(waitFor(() -> a8.count(v3) == 1, 8000), "CN-13c MT goes out");
			GmmsMessage rv = one(v3, 8000);
			check(rv != null && rv.getStatusCode() == 0, "CN-13d success result: " + (rv == null ? "none" : rv.getStatusCode() + " " + rv.getOutMsgID()));
			int seq = a8.deliver("8613800000000", "18667991226", 4, "id:1234567890 sub:001 dlvrd:001 submit date:2611031200 done date:2611031201 stat:DELIVRD err:000 text:x".getBytes("US-ASCII"), null);
			check(waitFor(() -> a8.deliverResp.containsKey(seq), 5000) && a8.deliverResp.get(seq) == 0, "CN-13e receipt: deliver_sm_resp ROK");
			List<GmmsMessage> ds = new ArrayList<>();
			waitFor(() -> jedis.keys("stream:core:inbound-dr:808*").size() > 0, 5000);
			for (String k : jedis.keys("stream:core:inbound-dr:808*")) for (StreamEntry e : jedis.xrange(k, (StreamEntryID) null, (StreamEntryID) null)) ds.add(com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body")));
			System.out.println("INFO v33 dr " + (ds.isEmpty() ? "none" : ds.get(0).getOutMsgID() + " " + ds.get(0).getStatusCode()));
			check(!ds.isEmpty() && "499602d2".equals(ds.get(0).getOutMsgID()) && ds.get(0).getStatusCode() == 10000, "CN-13f decimal id of the receipt turned into hex, as SMPP 3.3 ids are: " + (ds.isEmpty() ? "none" : ds.get(0).getOutMsgID()));
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		exit();
	}
}
