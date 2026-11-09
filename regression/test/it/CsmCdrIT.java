package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * CDRs of a 3-segment long SMS reassembled in Redis (CsmReassembleMode=redis): each segment through the real Core
 * entry, then this program plays the Client (a submit_resp and a vendor DR per MT sent out). Twice: a channel
 * that takes the long message as it is, and one that splits it again by its own length (ChlMsgSplit, MsgSplitLen).
 * Every CDR line of the message is counted by type.
 */
public class CsmCdrIT extends CoreBizSupport {
	static final String[] TYPES = { "IN_SUBMIT", "IN_SUBMIT_PARTIAL", "OUT_SUBMIT", "IN_DELI_REP", "OUT_DELI_REP", "IN_DELI", "IN_DELI_PARTIAL",
			"OUT_DELI", "IN_SUBMIT_REQ", "IN_SUBMIT_RES", "OUT_SUBMIT_REQ", "OUT_SUBMIT_RES", "IN_DELI_REP_REQ", "IN_DELI_REP_RES",
			"OUT_DELI_REP_REQ", "OUT_DELI_REP_RES" };
	static int out = 0;

	static GmmsMessage segment(String tag, int ossid, int i, String text) {
		GmmsMessage s = mt("" + ossid + "_" + tag + "_" + i, ossid, 0, 0);
		s.setSarMsgRefNum(tag);
		s.setSarTotalSegments(3);
		s.setSarSegmentSeqNum(i);
		s.setTextContent(text);
		s.setMessageSize(text.length());
		s.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", "MultiSmppServer1"), "conn1"));
		return s;
	}

	/** what MTStreamConsumer does with one entry */
	static boolean process(GmmsMessage m) {
		StreamAttempt.begin(m, "core:it");
		boolean ok = false;
		try {
			ok = InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(m);
		} finally {
			if (ok) StreamAttempt.finish("stream:it:none", "g", "0-1"); else StreamAttempt.abandon();
		}
		return ok;
	}

	/** MTs Core sent towards the channel for this long SMS */
	static List<GmmsMessage> sentOut(int rssid, String tag) {
		List<GmmsMessage> found = new ArrayList<>();
		for (String k : jedis.keys("stream:mt:routed:" + rssid + ":*")) {
			for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
				GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && m.getMsgID() != null && m.getMsgID().contains("_" + tag + "_")) found.add(m);
			}
		}
		return found;
	}

	/** every CDR line that mentions the long SMS, by type; one sample line per type */
	static Map<String, Integer> cdrs(String tag, Map<String, String> sample) throws Exception {
		Map<String, Integer> byType = new TreeMap<>();
		List<File> files = new ArrayList<>();
		collectFiles(new File(home + "cdr"), files);
		for (File f : files) {
			for (String l : Files.readAllLines(f.toPath())) {
				if (!l.contains("_" + tag + "_")) continue;
				String type = "?";
				for (String t : TYPES) if (l.contains("," + t + ",")) type = t;
				byType.merge(type, 1, Integer::sum);
				sample.putIfAbsent(type, l.length() > 230 ? l.substring(0, 230) + "..." : l);
			}
		}
		return byType;
	}
	static void collectFiles(File dir, List<File> outFiles) {
		File[] fs = dir.listFiles();
		if (fs == null) return;
		for (File f : fs) if (f.isDirectory()) collectFiles(f, outFiles); else outFiles.add(f);
	}
	static long drsToCustomer(int ossid, String tag) {
		long n = 0;
		for (String k : jedis.keys("stream:dr:pending:" + ossid + ":*")) n += inStreamsLike(k, "_" + tag + "_");
		return n;
	}
	static long inStreamsLike(String key, String part) {
		long n = 0;
		for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(key, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
			GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
			if (m != null && ((m.getMsgID() != null && m.getMsgID().contains(part)) || (m.getInMsgID() != null && m.getInMsgID().contains(part)))) n++;
		}
		return n;
	}

	static void scenario(String name, String tag, int ossid, int rssid, String text) throws Exception {
		scenario(name, tag, ossid, rssid, text, 3);
	}

	/** segments 0: one plain SMS (the control) */
	static void scenario(String name, String tag, int ossid, int rssid, String text, int segments) throws Exception {
		System.out.println("INFO ==== " + name);
		List<String> ids = new ArrayList<>();
		if (segments == 0) {
			GmmsMessage single = mt("" + ossid + "_" + tag + "_1", ossid, 0, 0);
			single.setTextContent(text);
			single.setMessageSize(text.length());
			single.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", "MultiSmppServer1"), "conn1"));
			ids.add(single.getMsgID());
			check(process(single), name + ": handled and acked");
		}
		for (int i = 1; i <= segments; i++) {
			GmmsMessage s = segment(tag, ossid, i, text);
			ids.add(s.getMsgID());
			check(process(s), name + ": segment " + i + " handled and acked");
		}
		check(waitFor(() -> !sentOut(rssid, tag).isEmpty(), 5000), name + ": it went out to channel " + rssid);
		List<GmmsMessage> sent = sentOut(rssid, tag);
		for (GmmsMessage m : sent)
			System.out.println("INFO   sent out: msgId=" + m.getMsgID() + " inMsgId=" + m.getInMsgID() + " split=" + m.getSplitStatus() + " size=" + m.getMessageSize());

		// ---- the Client: a submit_resp and a vendor DR for every MT sent out ----
		StreamQueueManager q = StreamQueueManager.getInstance();
		for (GmmsMessage m : sent) {
			String outId = "OUT-" + tag + "-" + (++out);
			GmmsMessage resp = new GmmsMessage(m);
			resp.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			resp.setStatus(GmmsStatus.SUCCESS);
			resp.setOutMsgID(outId);
			q.produceResult(resp);
			final String o = outId;
			waitFor(() -> jedis.exists(o), 8000);
			GmmsMessage dr = new GmmsMessage();
			dr.setMsgID(rssid + "_DR_" + tag + "_" + out);
			dr.setOutMsgID(outId);
			dr.setRSsID(rssid);
			dr.setOSsID(-1);
			dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			dr.setStatus(GmmsStatus.DELIVERED);
			q.produceInboundDeliveryReport(dr);
		}
		final int expected = segments == 0 ? 1 : segments;
		waitFor(() -> drsToCustomer(ossid, tag) >= expected, 8000);
		Thread.sleep(3000);   // CDR files are written asynchronously
		long drs = drsToCustomer(ossid, tag);
		Map<String, String> sample = new TreeMap<>();
		Map<String, Integer> byType = cdrs(tag, sample);
		System.out.println("INFO   MTs sent out: " + sent.size() + ", DRs to the customer: " + drs + ", CDRs by type: " + byType);
		for (Map.Entry<String, String> e : sample.entrySet()) System.out.println("INFO   sample " + e.getKey() + ": " + e.getValue());
		int partial = 0, full = 0;
		for (String id : ids) { partial += cdrCount(id, "IN_SUBMIT_PARTIAL"); full += cdrCount(id, "IN_SUBMIT"); }
		check(partial == segments && full == expected, name + ": in-submit CDRs per segment: IN_SUBMIT_PARTIAL " + partial + ", IN_SUBMIT " + full);
		// logOutSubmitRes writes the type OUT_SUBMIT
		check(byType.getOrDefault("OUT_SUBMIT", 0) == sent.size(), name + ": one OUT_SUBMIT per MT sent out (its result): " + byType.getOrDefault("OUT_SUBMIT", 0) + " for " + sent.size());
		check(byType.getOrDefault("OUT_DELI_REP", 0) == sent.size(), name + ": one OUT_DELI_REP per vendor DR: " + byType.getOrDefault("OUT_DELI_REP", 0) + " for " + sent.size());
		// the DR of the first part sent out is split back into one per segment the customer sent (CsmUtility.splitCsmDr)
		check(drs == expected, name + ": DRs to the customer: one per message or segment it sent: " + drs);
		if (segments > 0) {
			Set<String> inIds = new TreeSet<>();
			for (String k : jedis.keys("stream:dr:pending:" + ossid + ":*"))
				for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
					GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
					if (m != null && m.getInMsgID() != null && m.getInMsgID().contains("_" + tag + "_")) inIds.add(m.getInMsgID() + "#" + m.getSarSegmentSeqNum());
				}
			check(inIds.size() == segments, name + ": each with the in-id of its own segment: " + inIds);
		}
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		appendCm("\n[CUST8889]\nCCBSSID: 8889\nA2PPlayerType: operator\nProtocol: SMPP\n"
				+ "\n[VENDOR18]\nCCBSSID: 18\nA2PPlayerType: operator\nProtocol: SMPP\nChlMsgSplit: yes\nMsgSplitLen: 100\n");
		route("CUST8889", 8889, "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR18)");
		try {
			boot();
			ResultStreamConsumer.getInstance().start();
			InboundDRStreamConsumer.getInstance().start();
			String sixty = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWX";
			scenario("A channel takes it whole", "CA", 8888, 8, sixty);
			scenario("B channel splits by 100", "CB", 8889, 18, sixty);
			scenario("C one plain SMS (control)", "CS", 8888, 8, "hello", 0);
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		exit();
	}
}
