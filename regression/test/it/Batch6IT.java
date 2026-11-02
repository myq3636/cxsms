package it;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAutoClaimParams;
import redis.clients.jedis.resps.StreamEntry;

/** Core batch 6: redelivery dedup of vendor DRs and outbound results; not part of the repo. */
public class Batch6IT extends ItSupport {
	static StreamQueueManager q;
	static final String IDR = "stream:core:inbound-dr:8";
	static final String IDR_GROUP = "CoreInboundDRGroup";
	static final String RES_GROUP = "CoreResultGroup";

	static GmmsMessage original(String id) {
		GmmsMessage m = mt(id, 8888, 8, 0);
		m.setOutMsgID("OUT-" + id);
		return m;
	}

	static GmmsMessage vendorDr(String outId, String drId) {
		GmmsMessage dr = new GmmsMessage();
		dr.setMsgID(drId);
		dr.setOutMsgID(outId);
		dr.setRSsID(8);
		dr.setOSsID(-1);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setStatus(GmmsStatus.DELIVERED);
		return dr;
	}

	/** DRs produced to customer 8888 for an original msgId */
	static int drsOut(String msgId) {
		return inStreams("stream:dr:pending:8888:*", msgId).size();
	}

	/** put a vendor DR into its stream and read it like the consumer (delivery 1) */
	static GmmsMessage inboundDr(String id) {
		GmmsMessage o = original(id);
		jedis.set(o.getOutMsgID(), SerializableHandler.convertGmmsMessage2RedisMessage(o));
		q.produceInboundDeliveryReport(vendorDr(o.getOutMsgID(), "8_" + id));
		q.createGroup(IDR, IDR_GROUP, false);
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(IDR, StreamEntryID.UNRECEIVED_ENTRY);
		for (GmmsMessage m : q.consumeBatch(IDR_GROUP, "core-inbound-dr-1", 10, query, false)) {
			if (("8_" + id).equals(m.getMsgID())) return m;
		}
		throw new IllegalStateException("not read: " + id);
	}

	/** what auto-claim hands over again: the same pending entry */
	static GmmsMessage reclaim(String stream, String group, String id) {
		Map.Entry<StreamEntryID, List<StreamEntry>> c = jedis.xautoclaim(stream, group, "core-x", 0,
				new StreamEntryID(0, 0), XAutoClaimParams.xAutoClaimParams().count(100));
		for (StreamEntry e : c.getValue()) {
			if (e.getID().toString().equals(id)) {
				GmmsMessage g = SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				g.setRedisStreamID(id);
				g.setRedisStreamMessageType(g.getMessageType());
				return g;
			}
		}
		throw new IllegalStateException("not pending: " + id);
	}

	static void consumeDr(GmmsMessage m) throws Exception {
		Method pm = InboundDRStreamConsumer.class.getDeclaredMethod("processMessage", String.class, GmmsMessage.class);
		pm.setAccessible(true);
		pm.invoke(InboundDRStreamConsumer.getInstance(), IDR, m);
	}

	static void consumeResult(String stream, GmmsMessage m) throws Exception {
		Method pm = ResultStreamConsumer.class.getDeclaredMethod("processResult", String.class, GmmsMessage.class);
		pm.setAccessible(true);
		pm.invoke(ResultStreamConsumer.getInstance(), stream, m);
	}

	/** first attempt in its own thread, stopped either before _done (crash) or after _done (ack lost) */
	static void firstAttempt(final String stream, final GmmsMessage m, final boolean writeDone, final boolean result)
			throws Exception {
		final Exception[] err = new Exception[1];
		Thread t = new Thread() {
			public void run() {
				try {
					StreamAttempt.beginEntry(stream, m, RedisClient.getInstance().getReportMqRedis(), "crashed:1");
					if (result) {
						InternalCoreEngineSession.getReactiveInstance().handleOutboundResultInternal(m);
					} else {
						MessageStoreManager.withInlineDrDispatch(new Callable<Boolean>() {
							public Boolean call() {
								return InternalCoreEngineSession.getReactiveInstance().handleInboundDRInternal(m, null);
							}
						});
					}
					if (writeDone) {   // finish() up to the ack: _done + after-done actions, then the ack is lost
						StreamAttempt a = StreamAttempt.current();
						Method flush = StreamAttempt.class.getDeclaredMethod("flush", boolean.class, boolean.class);
						flush.setAccessible(true);
						flush.invoke(a, false, true);
						Field f = StreamAttempt.class.getDeclaredField("afterDone");
						f.setAccessible(true);
						for (Object r : (List<?>) f.get(a)) ((Runnable) r).run();
					}
				} catch (Exception e) {
					err[0] = e;
				}
			}
		};
		t.start();
		t.join();
		if (err[0] != null) throw err[0];
	}

	public static void main(String[] args) throws Exception {
		boot();
		q = StreamQueueManager.getInstance();
		props.setProperty("CoreMtLeaseMs", "800");

		// ---- B6-01 vendor DR: done, ack lost -> redelivery only acks ----
		GmmsMessage a = inboundDr("B601");
		String aid = a.getRedisStreamID();
		firstAttempt(IDR, a, true, false);
		check(drsOut("B601") == 1 && !jedis.exists("OUT-B601"), "B6-01a first attempt: DR to the customer, lookup deleted after _done");
		check(waitFor(() -> cdrCount("B601", "OUT_DELI_REP") == 1, 5000), "B6-01b first attempt: 1 OUT_DELI_REP CDR");
		Thread.sleep(900);
		consumeDr(reclaim(IDR, IDR_GROUP, aid));
		Thread.sleep(800);
		check(drsOut("B601") == 1 && cdrCount("B601", "OUT_DELI_REP") == 1, "B6-01c redelivery: no second DR, no second CDR");
		check(logLine("8_B601", "redis miss") == null, "B6-01d redelivery did not fall back to DB/SDQ (lookup miss)");
		check(jedis.xpending(IDR, IDR_GROUP).getTotal() == 0 && !jedis.exists(StreamAttempt.entryMarkerKey(IDR, aid)),
				"B6-01e redelivered entry acked, marker removed");

		// ---- B6-02 vendor DR: crash before _done -> redelivery takes the same path, repeats nothing ----
		GmmsMessage b = inboundDr("B602");
		String bid = b.getRedisStreamID();
		firstAttempt(IDR, b, false, false);
		check(drsOut("B602") == 1 && jedis.exists("OUT-B602"), "B6-02a crash after the DR was produced: lookup still there (deleted only after _done)");
		Thread.sleep(900);
		consumeDr(reclaim(IDR, IDR_GROUP, bid));
		check(waitFor(() -> !jedis.exists("OUT-B602"), 3000) && drsOut("B602") == 1,
				"B6-02b redelivery finds the original again, produces no second DR, then deletes the lookup");
		Thread.sleep(800);
		check(cdrCount("B602", "OUT_DELI_REP") == 1, "B6-02c no second OUT_DELI_REP CDR");
		check(jedis.xpending(IDR, IDR_GROUP).getTotal() == 0, "B6-02d acked");

		// ---- B6-03 switch off: old behaviour, the redelivery falls back (lookup already deleted) ----
		props.setProperty("CoreDrResultIdempotency", "false");
		GmmsMessage c = inboundDr("B603");
		String cid = c.getRedisStreamID();
		final GmmsMessage cm = c;
		MessageStoreManager.withInlineDrDispatch(new Callable<Boolean>() {
			public Boolean call() {
				return InternalCoreEngineSession.getReactiveInstance().handleInboundDRInternal(cm, null);
			}
		});   // processed, never acked
		consumeDr(reclaim(IDR, IDR_GROUP, cid));
		check(logLine("8_B603", "not in redis, looking up the DB") != null,
				"B6-03 switch off: redelivery finds no lookup and falls back to DB/SDQ (the old problem)");
		props.remove("CoreDrResultIdempotency");
		jedis.del(IDR);

		// ---- B6-04 result: failed submit, crash before _done / ack lost -> one CDR, one failure DR ----
		for (String mode : new String[] { "crash", "acklost" }) {
			GmmsMessage r = original("B604" + mode);
			r.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			r.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			q.produceResult(r);
			String rk = q.resultStreamKey(r);
			q.createGroup(rk, RES_GROUP, false);
			Map<String, StreamEntryID> rq = new HashMap<String, StreamEntryID>();
			rq.put(rk, StreamEntryID.UNRECEIVED_ENTRY);
			GmmsMessage got = q.consumeBatch(RES_GROUP, "core-result-1", 10, rq, false).get(0);
			String rid = got.getRedisStreamID();
			firstAttempt(rk, got, "acklost".equals(mode), true);
			final String id = "B604" + mode;
			check(waitFor(() -> cdrCount(id, "OUT_SUBMIT") == 1, 5000) && drsOut(id) == 1,
					"B6-04a " + mode + ": first attempt wrote OUT_SUBMIT CDR and the failure DR");
			Thread.sleep(900);
			consumeResult(rk, reclaim(rk, RES_GROUP, rid));
			Thread.sleep(800);
			check(cdrCount(id, "OUT_SUBMIT") == 1 && drsOut(id) == 1,
					"B6-04b " + mode + ": redelivered result wrote no second CDR and no second failure DR");
			check(jedis.xpending(rk, RES_GROUP).getTotal() == 0, "B6-04c " + mode + ": acked");
		}

		// ---- B6-05 regular retries are new entries: each one processed (customer DR failed twice) ----
		for (int i = 0; i < 2; i++) {
			GmmsMessage d = original("B605");
			d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
			d.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
			q.produceResult(d);
			String dk = q.resultStreamKey(d);
			q.createGroup(dk, RES_GROUP, false);
			Map<String, StreamEntryID> dq = new HashMap<String, StreamEntryID>();
			dq.put(dk, StreamEntryID.UNRECEIVED_ENTRY);
			for (GmmsMessage g : q.consumeBatch(RES_GROUP, "core-result-1", 10, dq, false)) consumeResult(dk, g);
		}
		check(waitFor(() -> cdrCount("B605", "IN_DELI_REP") == 2, 5000),
				"B6-05 two DR_resp results of the same message (retry) -> two IN_DELI_REP CDRs, both processed");

		// ---- B6-06 DB queue insert: once per entry, again for another entry ----
		GmmsMessage e = original("B606");
		e.setRedisStreamID("1-1");
		RedisClient rc = RedisClient.getInstance();
		StreamAttempt.beginEntry("stream:it:dbq", e, rc.getReportMqRedis(), "it:1");
		msm.insertMessageToDB(e, "RMQ");
		StreamAttempt.abandon();
		StreamAttempt.beginEntry("stream:it:dbq", e, rc.getReportMqRedis(), "it:1");
		msm.insertMessageToDB(e, "RMQ");
		StreamAttempt.abandon();
		check(logLine("B606", "Redelivered, already inserted into RMQ") != null, "B6-06a same entry redelivered: RMQ insert skipped");
		GmmsMessage e2 = original("B606b");
		e2.setRedisStreamID("2-1");
		StreamAttempt.beginEntry("stream:it:dbq", e2, rc.getReportMqRedis(), "it:1");
		msm.insertMessageToDB(e2, "RMQ");
		StreamAttempt.abandon();
		check(logLine("B606b", "Redelivered, already inserted") == null, "B6-06b another entry inserts normally");

		// ---- B6-07 reroute already done by an earlier attempt: not sent again ----
		GmmsMessage f = original("B607");
		f.setRedisStreamID("3-1");
		jedis.hset(StreamAttempt.entryMarkerKey("stream:it:rr", "3-1"), "x:reroute:0", "1");
		StreamAttempt.beginEntry("stream:it:rr", f, rc.getReportMqRedis(), "it:1");
		boolean rerouted = msm.rerouteOutMsg(f);
		StreamAttempt.abandon();
		check(rerouted && logLine("B607", "Redelivered, already rerouted") != null, "B6-07 reroute recorded earlier: reported rerouted, not dispatched again");

		// ---- B6-08 two consumers on one entry: the second backs off ----
		props.setProperty("CoreMtLeaseMs", "60000");
		GmmsMessage h = inboundDr("B608");
		final GmmsMessage hm = h;
		Thread holder = new Thread() {
			public void run() {
				StreamAttempt.beginEntry(IDR, hm, RedisClient.getInstance().getReportMqRedis(), "other:1");
			}
		};
		holder.start();
		holder.join();
		consumeDr(reclaim(IDR, IDR_GROUP, h.getRedisStreamID()));
		check(drsOut("B608") == 0 && jedis.xpending(IDR, IDR_GROUP).getTotal() == 1,
				"B6-08 entry held by another consumer: not processed, left pending");
		jedis.del(IDR);
		props.setProperty("CoreMtLeaseMs", "800");

		// ---- B6-09 performance: 800 vendor DRs, switch off vs on ----
		InboundDRStreamConsumer.getInstance().start();
		long[] ms = new long[4];
		for (int round = 0; round < 4; round++) {   // off, on, off, on: the first pair warms up the JVM
			props.setProperty("CoreDrResultIdempotency", round % 2 == 0 ? "false" : "true");
			String tag = (round % 2 == 0 ? "PFOFF" : "PFON") + round + "_";
			for (int i = 0; i < 800; i++) {
				GmmsMessage o = original(tag + i);
				jedis.set(o.getOutMsgID(), SerializableHandler.convertGmmsMessage2RedisMessage(o));
			}
			long t0 = System.currentTimeMillis();
			for (int i = 0; i < 800; i++) {
				GmmsMessage dr = vendorDr("OUT-" + tag + i, "8_" + tag + i);
				dr.setRSsID(i % 2 == 0 ? 8 : 9);
				q.produceInboundDeliveryReport(dr);
			}
			waitFor(() -> streamLen("stream:core:inbound-dr:*") == 0, 60000);
			ms[round] = System.currentTimeMillis() - t0;
		}
		props.remove("CoreDrResultIdempotency");
		System.out.println("     800 vendor DRs: warm-up off " + ms[0] + " / on " + ms[1] + " ms; measured off " + ms[2]
				+ " ms, on " + ms[3] + " ms");
		check(ms[2] > 0 && ms[3] > 0, "B6-09 800 DRs processed with the switch off (" + ms[2] + " ms) and on (" + ms[3] + " ms)");

		exit();
	}
}
