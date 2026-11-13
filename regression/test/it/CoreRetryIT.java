package it;

import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.*;

import com.king.gmms.messagequeue.CoreMQMStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.mqm.MQMMessageSender;
import com.king.gmms.mqm.MqmActiveState;
import com.king.gmms.util.ExpiredMessageQueueWithSafeExit;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/**
 * Core retry on the real result and MQM consumers. No database here: the store records its SQL, and this
 * program plays the MQM query, handing back the message as getResendMessage would read it from RMQ.
 */
public class CoreRetryIT extends CoreBizSupport {
	static final List<String> sql = Collections.synchronizedList(new ArrayList<String>());
	static class RecordingStore extends MessageStoreManager {
		@Override protected int doUpdate(String s) { sql.add(s); return 1; }
	}
	static StreamQueueManager q;

	static List<String> sqlOf(String verb, String table, String id) {
		List<String> r = new ArrayList<>();
		String head = "DELETE".equals(verb) ? "DELETE FROM " + table : "UPDATE".equals(verb) ? "UPDATE " + table : "INSERT INTO " + table;
		synchronized (sql) { for (String s : sql) if (s.startsWith(head) && s.contains("IN-" + id)) r.add(s); }
		return r;
	}
	static int retried(String s) {
		Matcher m = Pattern.compile("RetriedNumber=(\\d+)").matcher(s);
		return m.find() ? Integer.parseInt(m.group(1)) : -1;
	}
	/** seconds from now to NextRetryTime in the SQL, -1 when the column is null */
	static long nextIn(String s) throws Exception {
		Matcher m = Pattern.compile("NextRetryTime='([^']+)'").matcher(s);
		if (!m.find()) return -1;
		return (new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(m.group(1)).getTime() - System.currentTimeMillis()) / 1000;
	}
	static boolean near(long v, long want) { return v >= want - 4 && v <= want + 1; }

	/** the newest copy of the MT in the channel's outbound stream, waiting for attempt number n */
	static GmmsMessage out(int rssid, String id, int n) throws Exception {
		waitFor(() -> inStreams("stream:mt:routed:" + rssid + ":*", id).size() >= n, 8000);
		List<GmmsMessage> l = inStreams("stream:mt:routed:" + rssid + ":*", id);
		return l.size() < n ? null : l.get(n - 1);
	}
	static void fail(GmmsMessage m, GmmsStatus st) {
		m.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		m.setStatus(st);
		q.produceResult(m);
	}
	static boolean sqlSeen(String verb, String table, String id, int n) throws Exception {
		return waitFor(() -> sqlOf(verb, table, id).size() >= n, 8000);
	}
	static GmmsMessage dr(int ossid, String id, long ms) throws Exception {
		waitFor(() -> !inStreams("stream:dr:pending:" + ossid + ":*", id).isEmpty(), ms);
		List<GmmsMessage> l = inStreams("stream:dr:pending:" + ossid + ":*", id);
		return l.isEmpty() ? null : l.get(l.size() - 1);
	}
	/** what MQM reads back from RMQ and sends through its stream */
	static GmmsMessage fromRmq(GmmsMessage m, int retriedNumber) {
		GmmsMessage r = new GmmsMessage(m);
		r.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		r.setRetriedNumber(retriedNumber);
		r.setOriginalQueue(MessageStoreManager.RMQ);
		r.setRowId(7);
		r.setActionCode(0);
		r.setStatus(GmmsStatus.SUCCESS);
		return r;
	}
	static GmmsMessage first(int ossid, String sender, int rssid) throws Exception {
		GmmsMessage m = msg(ossid, sender, DST, "hello");
		q.produceSubmitMessage(m);
		return out(rssid, m.getMsgID(), 1);
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		String retry = "SMSOptionSubmitRetryCode: 2500\n";
		StringBuilder cm = new StringBuilder();
		cm.append(cust("V601", 601, retry + "SMSOptionMTRetryPolicy: 5S,1M,2M\n"));
		cm.append(cust("V602", 602, ""));
		cm.append(cust("V603", 603, retry));
		cm.append(cust("V604", 604, retry + "SMSOptionMTRetryPolicy: 1S\n"));
		cm.append(cust("V605", 605, retry + "SMSOptionMTRetryPolicy: 2D,2D\n"));
		cm.append(cust("V606", 606, retry));   // reroute only for a code in the channel's SMSOptionSubmitRetryCode (docs/路由重构设计.md 2.9)
		cm.append(cust("V607", 607, ""));
		cm.append(cust("R700", 700, ""));
		cm.append(cust("R701", 701, ""));
		cm.append(cust("R702", 702, "SMSOptionSenderRetryPolicy: (prefix=SEND,policy=7S*5)\nSMSOptionMORetryPolicy: 9S*5\n"));
		cm.append(cust("R703", 703, ""));
		cm.append(cust("R704", 704, "SMSOptionSubmitErrorCodesForFakeDR: 2500\n"));
		cm.append(cust("R705", 705, ""));
		cm.append(cust("R706", 706, ""));
		cm.append(cust("R707", 707, "SMSOptionOnlySupportDRRetry: true\n"));
		cm.append(cust("R708", 708, "SMSOptionSenderRetryPolicy: (prefix=SEND,policy=7S*5)\n"));
		appendCm(cm.toString());
		route("R700", 700, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V601)");
		route("R701", 701, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V602)");
		route("R702", 702, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V603)");
		route("R703", 703, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V603)");
		route("R704", 704, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V604)");
		route("R705", 705, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V604)");
		route("R706", 706, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V606)", "A2PBackupRouting: (Priority=30,PhonePrefix=86,Relay=V607)");
		route("R707", 707, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V603)");
		route("R708", 708, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V601)");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\n").getBytes("UTF-8"));
		msmFactory = RecordingStore::new;
		try {
			start();
			q = StreamQueueManager.getInstance();
			ResultStreamConsumer.getInstance().start();
			CoreMQMStreamConsumer.getInstance().start();
			Thread.sleep(800);

			// ---- whole life of one MT: three sends, two retries, then the failure DR ----
			GmmsMessage m = first(700, "18667991226", 601);
			final String id = m.getMsgID();
			fail(m, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("INSERT", "RMQ", id, 1), "RE-02a retry code on the channel: written to RMQ");
			String ins = sqlOf("INSERT", "RMQ", id).get(0);
			check(retried(ins) == 1 && near(nextIn(ins), 5), "RE-02b retried number 1, next try after the first point of the policy: retried=" + retried(ins) + " in " + nextIn(ins) + "s");
			check(dr(700, id, 800) == null, "RE-02c still retrying: no DR to the customer");
			check(waitFor(() -> cdrCount(id, "OUT_SUBMIT") == 1, 3000), "RE-02d OUT_SUBMIT CDR of the failed try");

			GmmsMessage back = fromRmq(m, retried(ins));
			check(q.produceMqmSubmitMessage(back), "RE-07a MQM writes to its stream");
			GmmsMessage m2 = out(601, id, 2);
			check(m2 != null, "RE-07b Core sends it out again on the same channel");
			check(m2 != null && m2.getRetriedNumber() == 1 && "RMQ".equals(m2.getOriginalQueue()), "RE-07c the try keeps retried number and origin: " + (m2 == null ? "" : m2.getRetriedNumber() + " " + m2.getOriginalQueue()));
			check(waitFor(() -> logLine(id, "flow=mqm->core-submit") != null, 3000), "RE-07d consumed from the MQM stream and acked");
			check(cdrCount(id, "IN_SUBMIT") == 1, "RE-07e no second IN_SUBMIT CDR");

			fail(m2, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("UPDATE", "RMQ", id, 1), "RE-04a second failure: the RMQ row is updated, not inserted again");
			String up = sqlOf("UPDATE", "RMQ", id).get(0);
			check(retried(up) == 2 && near(nextIn(up), 60) && sqlOf("INSERT", "RMQ", id).size() == 1, "RE-04b retried number 2, next try after the second point: retried=" + retried(up) + " in " + nextIn(up) + "s");
			check(dr(700, id, 500) == null, "RE-04c no DR yet");

			check(q.produceMqmSubmitMessage(fromRmq(m2, retried(up))), "RE-04d next try");
			GmmsMessage m3 = out(601, id, 3);
			check(m3 != null, "RE-04e third send");
			fail(m3, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("DELETE", "RMQ", id, 1), "RE-04f policy of 3 points: third failure removes the row (3 sends, 2 retries)");
			GmmsMessage fd = dr(700, id, 5000);
			check(fd != null && fd.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "RE-04g failure DR to the customer: " + (fd == null ? "none" : String.valueOf(fd.getStatusCode())));
			check(fd != null && fd.getRSsID() == 601, "RE-04h failure DR keeps the channel");
			check(waitFor(() -> cdrCount(id, "OUT_SUBMIT") == 3, 3000) && cdrCount(id, "IN_SUBMIT") == 1, "RE-04i one OUT_SUBMIT per send, one IN_SUBMIT: " + cdrCount(id, "OUT_SUBMIT"));

			// ---- no retry code ----
			GmmsMessage n = first(701, "18667991226", 602);
			final String nid = n.getMsgID();
			fail(n, GmmsStatus.COMMUNICATION_ERROR);
			GmmsMessage nd = dr(701, nid, 5000);
			check(nd != null && nd.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "RE-03a code not on the retry list: failure DR at once: " + (nd == null ? "none" : String.valueOf(nd.getStatusCode())));
			check(sqlOf("INSERT", "RMQ", nid).isEmpty(), "RE-03b nothing in RMQ");

			// ---- which policy ----
			GmmsMessage p = first(702, "SENDER1", 603);
			fail(p, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("INSERT", "RMQ", p.getMsgID(), 1) && near(nextIn(sqlOf("INSERT", "RMQ", p.getMsgID()).get(0)), 7), "RE-05a no MT policy on the channel: sender policy of the customer (7S)");
			p = first(708, "SENDER1", 601);
			fail(p, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("INSERT", "RMQ", p.getMsgID(), 1) && near(nextIn(sqlOf("INSERT", "RMQ", p.getMsgID()).get(0)), 5), "RE-05d MT policy of the channel (5S) beats the sender policy of the customer");
			p = first(702, "18667991226", 603);
			fail(p, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("INSERT", "RMQ", p.getMsgID(), 1) && near(nextIn(sqlOf("INSERT", "RMQ", p.getMsgID()).get(0)), 9), "RE-05b sender does not match: MO policy of the customer (9S)");
			p = first(703, "18667991226", 603);
			fail(p, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("INSERT", "RMQ", p.getMsgID(), 1) && near(nextIn(sqlOf("INSERT", "RMQ", p.getMsgID()).get(0)), 600), "RE-05c no policy at all: default 10M");

			// ---- next try would be past the expiry ----
			GmmsMessage e = msg(703, "18667991226", DST, "hello");
			e.setRSsID(605);
			q.produceSubmitMessage(e);
			GmmsMessage e1 = out(605, e.getMsgID(), 1);
			check(e1 != null, "RE-06a MT with its channel set");
			if (e1 != null) {
				fail(e1, GmmsStatus.COMMUNICATION_ERROR);
				check(sqlSeen("INSERT", "RMQ", e.getMsgID(), 1) && nextIn(sqlOf("INSERT", "RMQ", e.getMsgID()).get(0)) == -1, "RE-06b next try would pass the expiry: in RMQ with no NextRetryTime, so the resend query never selects it");
				check(dr(703, e.getMsgID(), 500) == null, "RE-06c no DR here, the expiry task of MQM answers later");
			}

			// ---- fake DR on the last failure ----
			GmmsMessage f = first(704, "18667991226", 604);
			fail(f, GmmsStatus.COMMUNICATION_ERROR);
			GmmsMessage fk = dr(704, f.getMsgID(), 5000);
			check(fk != null && fk.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "RE-10a code on the fake DR list, last failure: DELIVERED: " + (fk == null ? "none" : String.valueOf(fk.getStatusCode())));
			f = first(705, "18667991226", 604);
			fail(f, GmmsStatus.COMMUNICATION_ERROR);
			fk = dr(705, f.getMsgID(), 5000);
			check(fk != null && fk.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "RE-10b customer without the option, same channel: failure DR: " + (fk == null ? "none" : String.valueOf(fk.getStatusCode())));

			// ---- backup route on failure ----
			GmmsMessage r = first(706, "18667991226", 606);
			final String rid = r.getMsgID();
			fail(r, GmmsStatus.COMMUNICATION_ERROR);
			GmmsMessage r2 = out(607, rid, 1);
			check(r2 != null && dr(706, rid, 300) == null, "RE-12a failure: sent again on the backup route, no DR");
			if (r2 != null) {
				fail(r2, GmmsStatus.COMMUNICATION_ERROR);
				GmmsMessage rd = dr(706, rid, 5000);
				check(rd != null && out(607, rid, 2) == null, "RE-12b backup route fails too: no second reroute, failure DR");
			}

			// ---- DR retry only ----
			GmmsMessage o = first(707, "18667991226", 603);
			fail(o, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("INSERT", "RMQ", o.getMsgID(), 1), "RE-13a SMSOptionOnlySupportDRRetry: the MT still goes to RMQ");
			if (!sqlOf("INSERT", "RMQ", o.getMsgID()).isEmpty()) check(near(nextIn(sqlOf("INSERT", "RMQ", o.getMsgID()).get(0)), 600), "RE-13b with the default policy, 10M: in " + nextIn(sqlOf("INSERT", "RMQ", o.getMsgID()).get(0)) + "s");

			// ---- MQM: the sender only works while active, an old term is refused ----
			ExpiredMessageQueueWithSafeExit queue = new ExpiredMessageQueueWithSafeExit(60000, false, "it-mqm");
			queue.start();
			Thread sender = new Thread(new MQMMessageSender(queue));
			sender.setDaemon(true);
			sender.start();
			GmmsMessage s = first(700, "18667991226", 601);
			final String sid = s.getMsgID();
			MqmActiveState.setActive(false, "it");
			queue.put(fromRmq(s, 1));
			Thread.sleep(1500);
			check(out(601, sid, 2) == null, "RE-07f standby MQM does not send");
			MqmActiveState.setActive(true, "it");
			check(out(601, sid, 2) != null, "RE-07g active MQM sends it");

			// ---- report lane: DR again from MQM, and its result ----
			// a server module online, as ModuleStatusReporter writes it: without one the DR has nowhere to go and returns to RDQ
			jedis.sadd("system:server:nodes", "1");
			jedis.set("module:status:server:MultiSmppServer1:1", "ONLINE");
			GmmsMessage rep = first(700, "18667991226", 601);
			final String did = rep.getMsgID();
			GmmsMessage d = new GmmsMessage(rep);
			d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			d.setStatus(GmmsStatus.DELIVERED);
			d.setOriginalQueue(MessageStoreManager.RDQ);
			d.setRowId(9);
			check(q.produceMqmReportMessage(d), "RE-08a MQM writes the DR to its stream");
			GmmsMessage dro = dr(700, did, 8000);
			check(dro != null && dro.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "RE-08b the DR goes to the customer's stream");
			check(waitFor(() -> logLine(did, "flow=mqm->core-report") != null, 3000), "RE-08c consumed from the MQM report stream and acked");
			if (dro != null) {
				GmmsMessage ok = new GmmsMessage(dro);
				ok.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
				ok.setOriginalQueue(MessageStoreManager.RDQ);
				ok.setRowId(9);
				q.produceResult(ok);
				check(waitFor(() -> { synchronized (sql) { return sql.contains("DELETE FROM RDQ WHERE ID=9"); } }, 8000), "RE-08d customer got it: the RDQ row is removed");
			}
			GmmsMessage drf = new GmmsMessage(d);
			drf.setMsgID(did + "F");
			drf.setInMsgID("IN-" + did + "F");
			drf.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
			drf.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
			drf.setOriginalQueue(null);
			drf.setRowId(0);
			q.produceResult(drf);
			check(sqlSeen("INSERT", "RDQ", did + "F", 1), "RE-08e DR could not be sent: into RDQ for another try");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
