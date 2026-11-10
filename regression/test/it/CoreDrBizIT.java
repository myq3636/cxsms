package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.messagequeue.CoreMQMStreamConsumer;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.processor.DelayedInDRDispatcher;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/** What the customer sees as DR: fake DR, DR on receipt, delayed DR, reroute on a vendor DR, no server online; failover by Relay=a;b. */
public class CoreDrBizIT extends CoreRetryIT {
	static int outSeq = 0;

	/** the vendor accepted the MT: submit_sm_resp with an outMsgID */
	static GmmsMessage accept(GmmsMessage m) throws Exception {
		m.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		m.setStatus(GmmsStatus.SUCCESS);
		m.setOutMsgID("OUT-" + (++outSeq));
		q.produceResult(m);
		final String out = m.getOutMsgID();
		final String id = m.getMsgID();
		waitFor(() -> cdrCount(id, "OUT_SUBMIT") >= 1, 5000);
		return m;
	}
	/** the vendor's DR for an accepted MT */
	static void vendorDr(GmmsMessage m, GmmsStatus st) throws Exception {
		final String out = m.getOutMsgID();
		waitFor(() -> jedis.exists(out), 5000);
		GmmsMessage d = new GmmsMessage();
		d.setMsgID(m.getMsgID() + "#VDR");
		d.setOutMsgID(out);
		d.setRSsID(m.getRSsID());
		d.setOSsID(-1);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(st);
		q.produceInboundDeliveryReport(d);
	}
	static int drs(int ossid, String id) { return inStreams("stream:dr:pending:" + ossid + ":*", id).size(); }
	static GmmsMessage withTime(int ossid, long age) {
		GmmsMessage m = msg(ossid, "18667991226", DST, "hello");
		m.setTimeStamp(new Date(System.currentTimeMillis() - age));
		return m;
	}
	static GmmsMessage firstMsg(GmmsMessage m, int rssid) throws Exception {
		q.produceSubmitMessage(m);
		return out(rssid, m.getMsgID(), 1);
	}

	static GmmsMessage readOne(String stream, String group, boolean submit) {
		q.createGroup(stream, group, submit);
		Map<String, redis.clients.jedis.StreamEntryID> query = new HashMap<>();
		query.put(stream, redis.clients.jedis.StreamEntryID.UNRECEIVED_ENTRY);
		return q.consumeBatch(group, "worker", 10, query, submit).get(0);
	}
	static String withEntries(String pattern) {
		for (String k : jedis.keys(pattern)) if (jedis.xlen(k) > 0) return k;
		throw new IllegalStateException("no entries: " + pattern);
	}
	static void invoke(Object consumer, String method, String stream, GmmsMessage m, boolean claimed) throws Exception {
		java.lang.reflect.Method pm = consumer.getClass().getDeclaredMethod(method, String.class, GmmsMessage.class, boolean.class);
		pm.setAccessible(true);
		pm.invoke(consumer, stream, m, claimed);
	}

	/** auto-claim reads an entry just before its first consumer acks it: the copy in memory must not run again */
	static void claimRaces() throws Exception {
		com.king.gmms.messagequeue.MTStreamConsumer mt = com.king.gmms.messagequeue.MTStreamConsumer.getInstance();
		mt.stop();
		ResultStreamConsumer.getInstance().stop();
		InboundDRStreamConsumer.getInstance().stop();
		Thread.sleep(800);

		// MT
		GmmsMessage race = msg(817, "18667991226", DST, "race");
		q.produceSubmitMessage(race);
		String stream = withEntries("stream:mt:pending:817:*");
		GmmsMessage read = readOne(stream, "CoreMTGroup", true);
		GmmsMessage claimed = q.recoverPending(stream, "CoreMTGroup", "claimer", 0L, 10, true).get(0);
		invoke(mt, "processMessage", stream, read, false);
		invoke(mt, "processMessage", stream, claimed, true);
		int sent = inStreams("stream:mt:routed:801:*", race.getMsgID()).size();
		check(sent == 1, "FT-08a MT claimed just before its worker acked it: sent once: " + sent);
		check(cdrCount(race.getMsgID(), "IN_SUBMIT") == 1, "FT-08b one IN_SUBMIT CDR: " + cdrCount(race.getMsgID(), "IN_SUBMIT"));
		check(!jedis.exists("core:mt:" + race.getMsgID()), "FT-08c no marker left behind");
		check(logLine(race.getMsgID(), "MT already acked by another consumer") != null, "FT-08d logged");

		// the other way round: the claim finished first, the worker's copy waited in its batch past the idle time
		GmmsMessage slow = msg(817, "18667991226", DST, "slow");
		q.produceSubmitMessage(slow);
		stream = withEntries("stream:mt:pending:817:*");
		read = readOne(stream, "CoreMTGroup", true);
		invoke(mt, "processMessage", stream, q.recoverPending(stream, "CoreMTGroup", "claimer", 0L, 10, true).get(0), true);
		invoke(mt, "processMessage", stream, read, true);
		sent = inStreams("stream:mt:routed:801:*", slow.getMsgID()).size();
		check(sent == 1 && cdrCount(slow.getMsgID(), "IN_SUBMIT") == 1, "FT-08k claim finished first, the worker's old copy comes later: sent once: " + sent);
		check(!jedis.exists("core:mt:" + slow.getMsgID()), "FT-08l no marker left behind");

		// MT still in the stream, its worker died: the claim must still process it
		GmmsMessage dead = msg(817, "18667991226", DST, "dead");
		q.produceSubmitMessage(dead);
		stream = withEntries("stream:mt:pending:817:*");
		readOne(stream, "CoreMTGroup", true);
		invoke(mt, "processMessage", stream, q.recoverPending(stream, "CoreMTGroup", "claimer", 0L, 10, true).get(0), true);
		check(inStreams("stream:mt:routed:801:*", dead.getMsgID()).size() == 1, "FT-08e worker died, entry still there: the claim processes it");
		check(!jedis.exists(stream) || jedis.xlen(stream) == 0, "FT-08f and acks it");

		// outbound result
		GmmsMessage out = inStreams("stream:mt:routed:801:*", dead.getMsgID()).get(0);
		out.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		out.setStatus(GmmsStatus.SUCCESS);
		out.setOutMsgID("OUT-RACE");
		q.produceResult(out);
		stream = withEntries("stream:core:results:801:*");
		String group = "CoreResultGroup";
		read = readOne(stream, group, false);
		claimed = q.recoverPending(stream, group, "claimer", 0L, 10, false).get(0);
		invoke(ResultStreamConsumer.getInstance(), "processResult", stream, read, false);
		invoke(ResultStreamConsumer.getInstance(), "processResult", stream, claimed, true);
		check(cdrCount(dead.getMsgID(), "OUT_SUBMIT") == 1, "FT-08g result claimed just before its ack: one OUT_SUBMIT CDR: " + cdrCount(dead.getMsgID(), "OUT_SUBMIT"));
		check(logLine(dead.getMsgID(), "Result already acked by another consumer") != null, "FT-08h logged");

		// vendor DR
		waitFor(() -> jedis.exists("OUT-RACE"), 3000);
		GmmsMessage v = new GmmsMessage();
		v.setMsgID(dead.getMsgID());
		v.setOutMsgID("OUT-RACE");
		v.setRSsID(801);
		v.setOSsID(-1);
		v.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		v.setStatus(GmmsStatus.DELIVERED);
		q.produceInboundDeliveryReport(v);
		stream = withEntries("stream:core:inbound-dr:801*");
		group = "CoreInboundDRGroup";
		read = readOne(stream, group, false);
		claimed = q.recoverPending(stream, group, "claimer", 0L, 10, false).get(0);
		invoke(InboundDRStreamConsumer.getInstance(), "processMessage", stream, read, false);
		invoke(InboundDRStreamConsumer.getInstance(), "processMessage", stream, claimed, true);
		check(drs(817, dead.getMsgID()) == 1, "FT-08i vendor DR claimed just before its ack: one DR to the customer: " + drs(817, dead.getMsgID()));
		check(cdrCount(dead.getMsgID(), "OUT_DELI_REP") == 1, "FT-08j one OUT_DELI_REP CDR: " + cdrCount(dead.getMsgID(), "OUT_DELI_REP"));
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder();
		cm.append(cust("V801", 801, ""));
		cm.append(cust("V802", 802, "SMSOptionSendDRBeforeOutSubmitInDC: true\n"));
		cm.append(cust("V803", 803, "NeedDRReroute: true\nSMSOptionDRRerouteCode: " + GmmsStatus.UNDELIVERABLE.getCode() + "\n"));
		cm.append(cust("V804", 804, ""));
		cm.append(cust("V805", 805, ""));
		cm.append(cust("V806", 806, ""));
		cm.append(cust("V807", 807, "SMSOptionSendDRByOutSubmit: true\n"));
		cm.append(cust("V808", 808, "SMSOptionSendDRByOutSubmit: true\nSMSOptionDRDelayTimeMinMaxInSec: 3,4\n"));
		cm.append(cust("R810", 810, ""));
		cm.append(cust("R811", 811, "SMSOptionFakeDRSuccessRatio: 0\n"));
		cm.append(cust("R812", 812, "SMSOptionFakeDRSuccessRatio: 70\n"));
		cm.append(cust("R813", 813, "SMSOptionSendFakeDR: true\n"));
		cm.append(cust("R814", 814, "SMSOptionSendDRByInSubmitInCU: true\n"));
		cm.append(cust("R815", 815, "SMSOptionSendFakeDRByInSubmitRecPrefixInCU: 8613\n"));
		cm.append(cust("R816", 816, ""));
		cm.append(cust("R817", 817, ""));
		cm.append(cust("R818", 818, ""));
		cm.append(cust("R819", 819, ""));
		appendCm(cm.toString());
		for (int r : new int[] { 814, 815, 817 }) route("R" + r, r, "A2PRoutingRelay: (PhonePrefix=86;852,Relay=V801)");
		for (int r : new int[] { 810, 811, 812 }) route("R" + r, r, "A2PRoutingRelay: (PhonePrefix=86;852,Relay=V807)");
		route("R816", 816, "A2PRoutingRelay: (PhonePrefix=86,Relay=V808)");
		route("R813", 813, "A2PRoutingRelay: (PhonePrefix=86,Relay=V802)");
		route("R818", 818, "A2PRoutingRelay: (PhonePrefix=86,Relay=V803)", "A2PBackupRoutingRelay: (PhonePrefix=86,Relay=V804)");
		route("R819", 819, "A2PRoutingRelay: (PhonePrefix=86,Relay=V805;V806)");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\nAutoInDR.DirectStreamEnable=true\nAutoInDR.DispatcherIdleSleepMs=100\n").getBytes("UTF-8"));
		msmFactory = RecordingStore::new;
		try {
			start();
			q = StreamQueueManager.getInstance();
			ResultStreamConsumer.getInstance().start();
			InboundDRStreamConsumer.getInstance().start();
			CoreMQMStreamConsumer.getInstance().start();
			DelayedInDRDispatcher.getInstance().startDispatcher();
			Thread.sleep(800);

			// ---- fake DR after the vendor accepted ----
			GmmsMessage m = accept(first(810, "18667991226", 807));
			GmmsMessage d = dr(810, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "DR-07a channel SMSOptionSendDRByOutSubmit, fake ratio not configured (100): DELIVERED after the vendor accepted: " + (d == null ? "none" : String.valueOf(d.getStatusCode())));
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			final String id1 = m.getMsgID();
			waitFor(() -> cdrCount(id1, "OUT_DELI_REP") >= 1, 5000);
			Thread.sleep(500);
			check(drs(810, id1) == 1, "DR-07b the vendor's real DR is not sent to the customer again: " + drs(810, id1) + " DR");

			m = first(810, "18667991226", 807);
			fail(m, GmmsStatus.UNDELIVERABLE);
			d = dr(810, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "DR-07c the vendor refused the MT: failure DR, no fake success: " + (d == null ? "none" : String.valueOf(d.getStatusCode())));

			m = accept(first(811, "18667991226", 807));
			d = dr(811, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "DR-07d SMSOptionFakeDRSuccessRatio 0: the fake DR is a failure: " + (d == null ? "none" : String.valueOf(d.getStatusCode())));

			// ---- success ratio ----
			int ok = 0, none = 0;
			for (int i = 0; i < 100; i++) {
				GmmsMessage x = accept(first(812, "18667991226", 807));
				GmmsMessage y = dr(812, x.getMsgID(), 3000);
				if (y == null) none++; else if (y.getStatusCode() == GmmsStatus.DELIVERED.getCode()) ok++;
			}
			check(none == 0 && ok >= 50 && ok <= 90, "DR-12 SMSOptionFakeDRSuccessRatio 70 over 100 MTs: " + ok + " DELIVERED, " + none + " without DR");

			// ---- removed: fake DR before the MT goes out (channel), fake DR switched on by the customer ----
			m = first(813, "18667991226", 802);
			check(m != null && dr(813, m.getMsgID(), 1500) == null, "DR-08 SMSOptionSendDRBeforeOutSubmitInDC no longer used: no DR before the vendor answers");
			accept(m);
			check(dr(813, m.getMsgID(), 1500) == null, "DR-08b customer SMSOptionSendFakeDR no longer used: no fake DR after the accept");
			vendorDr(m, GmmsStatus.DELIVERED);
			d = dr(813, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode() && drs(813, m.getMsgID()) == 1, "DR-08c the vendor's DR goes to the customer: " + drs(813, m.getMsgID()));

			// ---- DR on receipt ----
			m = first(814, "18667991226", 801);
			d = dr(814, m.getMsgID(), 5000);
			check(m != null && d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "DR-09a SMSOptionSendDRByInSubmitInCU: DR on receipt: " + (d == null ? "none" : String.valueOf(d.getStatusCode())));
			accept(m);
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			final String id9 = m.getMsgID();
			waitFor(() -> cdrCount(id9, "OUT_DELI_REP") >= 1, 5000);
			Thread.sleep(500);
			check(drs(814, id9) == 1, "DR-09b the vendor's real DR is not sent again: " + drs(814, id9));
			m = first(815, "18667991226", 801);
			check(dr(815, m.getMsgID(), 5000) != null, "DR-09c by recipient prefix, 8613: DR on receipt");
			GmmsMessage hk = msg(815, "18667991226", "85212345678", "hello");
			hk = firstMsg(hk, 801);
			check(dr(815, hk.getMsgID(), 1000) == null, "DR-09d other prefix: no DR on receipt");

			// ---- delayed DR ----
			long t0 = System.currentTimeMillis();
			m = accept(first(816, "18667991226", 808));
			final String idd = m.getMsgID();
			Thread.sleep(1500);
			check(drs(816, idd) == 0, "DR-10a delay 3 to 4 s: nothing yet after 1.5 s");
			check(waitFor(() -> drs(816, idd) == 1, 8000), "DR-10b the DR arrives");
			long took = System.currentTimeMillis() - t0;
			check(took >= 2500 && took <= 7000, "DR-10c inside the range (time includes the vendor's answer): " + took + " ms");

			DelayedInDRDispatcher.getInstance().stopDispatcher();
			Thread.sleep(500);
			m = accept(first(816, "18667991226", 808));
			final String ids = m.getMsgID();
			Thread.sleep(6000);
			check(drs(816, ids) == 0, "DR-11a dispatcher stopped (Core down): a due DR waits in Redis");
			DelayedInDRDispatcher.getInstance().startDispatcher();
			check(waitFor(() -> drs(816, ids) == 1, 8000), "DR-11b dispatcher started again: the due DR goes out");

			// ---- reroute on a vendor DR: the window counts from a GMT+8 stamp ----
			long eight = 8 * 3600 * 1000L;
			m = accept(firstMsg(withTime(818, eight), 803));
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			GmmsMessage rr = out(804, m.getMsgID(), 1);
			check(rr != null && dr(818, m.getMsgID(), 300) == null, "RE-11a DR with a reroute code, inside RerouteDRDelayTimeInSec: sent again on the backup route, no DR");
			m = accept(firstMsg(withTime(818, eight), 803));
			vendorDr(m, GmmsStatus.REJECTED);
			d = dr(818, m.getMsgID(), 5000);
			check(d != null && out(804, m.getMsgID(), 1) == null, "RE-11b other failure code: DR to the customer, no reroute");
			m = accept(firstMsg(withTime(818, eight + 3600 * 1000L), 803));
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			d = dr(818, m.getMsgID(), 5000);
			check(d != null && out(804, m.getMsgID(), 1) == null, "RE-11c past RerouteDRDelayTimeInSec (1800 s): DR to the customer, no reroute");

			// ---- Relay=a;b: only a is loaded ----
			GmmsMessage rb = first(819, "18667991226", 805);
			check(rb != null, "RT-05a Relay=V805;V806: goes to the first");
			fail(rb, GmmsStatus.COMMUNICATION_ERROR);
			d = dr(819, rb.getMsgID(), 5000);
			check(d != null && out(806, rb.getMsgID(), 1) == null, "RT-05b the part after ; is not a failover: failure DR, nothing on the second");

			// ---- no server online for a DR ----
			GmmsMessage nos = first(817, "18667991226", 801);
			GmmsMessage rep = new GmmsMessage(nos);
			rep.setMsgID(nos.getMsgID() + "N");
			rep.setInMsgID("IN-" + rep.getMsgID());
			rep.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			rep.setStatus(GmmsStatus.DELIVERED);
			check(q.produceMqmReportMessage(rep), "DR-06a DR for a customer with no server online");
			check(sqlSeen("INSERT", "RDQ", rep.getMsgID(), 1), "DR-06b nowhere to send it: kept in RDQ");
			check(drs(817, rep.getMsgID()) == 0, "DR-06c nothing in a DR stream");
			jedis.sadd("system:server:nodes", "1");
			jedis.set("module:status:server:MultiSmppServer1:1", "ONLINE");
			GmmsMessage again = new GmmsMessage(rep);
			again.setOriginalQueue(MessageStoreManager.RDQ);
			again.setRowId(5);
			check(q.produceMqmReportMessage(again), "DR-06d server back, MQM tries again");
			check(dr(817, rep.getMsgID(), 8000) != null, "DR-06e the DR reaches the customer's stream");

			claimRaces();
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
