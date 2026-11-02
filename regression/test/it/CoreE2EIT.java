package it;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/**
 * Full Core paths on the local Redis (not part of the repo): real MTStreamConsumer, routing
 * (conf/routing), outbound produce, outbox scanner and ResultStreamConsumer.
 */
public class CoreE2EIT extends ItSupport {
	static final String ROUTED_8 = "stream:mt:routed:8:*";
	static final String CONSUMER = "core-mt-router-1";

	static String pendingKey(GmmsMessage m) {
		return "stream:mt:pending:" + m.getOSsID() + ":" + StreamQueueManager.getInstance().calculateShardId(m);
	}

	/** read the MT back from stream:mt:pending with the Core group, as the consumer would */
	static GmmsMessage readPending(GmmsMessage in) {
		StreamQueueManager q = StreamQueueManager.getInstance();
		String key = pendingKey(in);
		q.createGroup(key, "CoreMTGroup", true);
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(key, StreamEntryID.UNRECEIVED_ENTRY);
		for (GmmsMessage m : q.consumeBatch("CoreMTGroup", CONSUMER, 100, query, true)) {
			if (in.getMsgID().equals(m.getMsgID())) return m;
		}
		throw new IllegalStateException("not read back: " + in.getMsgID());
	}

	static void consumerProcess(String streamKey, GmmsMessage msg) throws Exception {
		Method pm = MTStreamConsumer.class.getDeclaredMethod("processMessage", String.class, GmmsMessage.class);
		pm.setAccessible(true);
		pm.invoke(MTStreamConsumer.getInstance(), streamKey, msg);
	}

	public static void main(String[] args) throws Exception {
		boot();
		StreamQueueManager q = StreamQueueManager.getInstance();
		props.setProperty("CoreOutboundUnreadStallMs", "4000");
		props.setProperty("CoreOutboundRespTimeoutMs", "5000");
		props.setProperty("CoreOutboundOutboxRecheckMs", "1000");
		props.setProperty("CoreMtLeaseMs", "1000");

		// ---- E2E-02: crash between produce and ack, redelivered: nothing repeated ----
		final GmmsMessage m2 = readPending(produced(q, mt("8888_E2E02", 8888, -1, 0)));
		Thread crashed = new Thread() {
			public void run() {
				StreamAttempt.begin(m2, "crashed:1");
				InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(m2);
				// process exits here: no ack, lease left behind
			}
		};
		crashed.start();
		crashed.join();
		check(inStreams(ROUTED_8, "8888_E2E02").size() == 1, "E2E-02a first attempt routed the MT to rssid 8 (1 entry)");
		check(waitFor(() -> cdrCount("8888_E2E02", "IN_SUBMIT") == 1, 5000), "E2E-02b first attempt wrote 1 IN_SUBMIT CDR");
		check("1".equals(jedis.get("Duplicate:8613800000000")), "E2E-02c first attempt counted the recipient (1)");
		check(jedis.xpending(pendingKey(m2), "CoreMTGroup").getTotal() == 1, "E2E-02d MT still pending (not acked)");
		Thread.sleep(1200); // lease of the crashed owner expires
		GmmsMessage redelivered = readPendingAgain(m2);
		consumerProcess(pendingKey(m2), redelivered);
		check(inStreams(ROUTED_8, "8888_E2E02").size() == 1, "E2E-02e redelivery produced nothing new (still 1 routed entry)");
		Thread.sleep(1500);
		check(cdrCount("8888_E2E02", "IN_SUBMIT") == 1, "E2E-02f redelivery wrote no second IN_SUBMIT CDR");
		check("1".equals(jedis.get("Duplicate:8613800000000")), "E2E-02g redelivery did not count the recipient again");
		check(jedis.xlen(pendingKey(m2)) == 0 && !jedis.exists(StreamAttempt.markerKey(m2)),
				"E2E-02h redelivery acked the MT and removed the marker");

		// ---- E2E-03: CoreMtIdempotency=false keeps the old behaviour ----
		props.setProperty("CoreMtIdempotency", "false");
		GmmsMessage m3 = readPending(produced(q, mt("8888_E2E03", 8888, -1, 0)));
		consumerProcess(pendingKey(m3), m3);
		check(!jedis.exists(StreamAttempt.markerKey(m3)) && inStreams(ROUTED_8, "8888_E2E03").size() == 1,
				"E2E-03a flag off: processed without a marker");
		consumerProcess(pendingKey(m3), m3); // same message delivered again
		check(inStreams(ROUTED_8, "8888_E2E03").size() == 2, "E2E-03b flag off: a redelivery produces again (old behaviour)");
		props.remove("CoreMtIdempotency");

		// ---- E2E-01: normal MT through the running consumer ----
		ResultStreamConsumer.getInstance().start();
		MTStreamConsumer.getInstance().start();
		GmmsMessage m1 = mt("8888_E2E01", 8888, -1, 0);
		q.produceSubmitMessage(m1);
		check(waitFor(() -> inStreams(ROUTED_8, "8888_E2E01").size() == 1, 5000), "E2E-01a MT routed to stream:mt:routed:8:*");
		check(jedis.exists("core:outbox:submit:8888_E2E01:0:0"), "E2E-01b outbox core:outbox:submit:8888_E2E01:0:0 written");
		check(waitFor(() -> jedis.xlen(pendingKey(m1)) == 0 && !jedis.exists(StreamAttempt.markerKey(m1)), 5000),
				"E2E-01c MT acked, marker removed");
		check(waitFor(() -> cdrCount("8888_E2E01", "IN_SUBMIT") == 1, 5000), "E2E-01d exactly 1 IN_SUBMIT CDR");

		// ---- E2E-05: the real result arrives first, the outbox stops watching ----
		GmmsMessage resp = inStreams(ROUTED_8, "8888_E2E01").get(0);
		resp.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		resp.setStatus(GmmsStatus.SUCCESS);
		resp.setOutMsgID("OUT-E2E01");
		q.produceResult(resp);
		check(waitFor(() -> !jedis.exists("core:outbox:submit:8888_E2E01:0:0") && cdrCount("8888_E2E01", "OUT_SUBMIT") == 1, 8000),
				"E2E-05a submit_resp claims the outbox, OUT_SUBMIT CDR written once");
		check(waitFor(() -> jedis.exists("OUT-E2E01"), 5000), "E2E-05b DR lookup cached under outMsgID");

		// ---- E2E-04: no client: unread MTs pulled back, timeout result processed by Core ----
		check(waitFor(() -> inStreams(ROUTED_8, "8888_E2E02").isEmpty()
				&& !jedis.exists("core:outbox:submit:8888_E2E02:0:0"), 12000),
				"E2E-04a unread MT pulled back from routed after the stall time, outbox claimed by the timeout result");
		check(waitFor(() -> cdrCount("8888_E2E02", "OUT_SUBMIT") == 1, 5000),
				"E2E-04b timeout result processed by ResultStreamConsumer (OUT_SUBMIT CDR, status 2500)");
		check(waitFor(() -> streamLen("stream:dr:pending:8888:MultiSmppServer1:*") >= 1, 5000),
				"E2E-04c failure DR for the customer produced to stream:dr:pending:8888:MultiSmppServer1");

		// ---- E2E-06: a late submit_resp for an attempt that already timed out is dropped ----
		GmmsMessage late = mt("8888_E2E02", 8888, 8, 0);
		late.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		late.setStatus(GmmsStatus.SUCCESS);
		q.produceResult(late);
		check(waitFor(() -> logLine("8888_E2E02", "[RESULT-DROPPED]") != null, 8000),
				"E2E-06a late submit_resp dropped as STALE");
		Thread.sleep(1500);
		check(cdrCount("8888_E2E02", "OUT_SUBMIT") == 1, "E2E-06b no second OUT_SUBMIT CDR for the attempt");
		check(streamLen("stream:core:results*") == 0, "E2E-06c late result acked (result streams empty)");

		// ---- B1-11: putMsg with an unknown o-customer does not throw ----
		GmmsMessage orphan = mt("9999_B111", 9999, 9998, 0);
		boolean put;
		try {
			put = MessageProcessorHandler.getInstance().putMsg(orphan);
		} catch (Throwable t) {
			put = false;
		}
		check(put, "B1-11 MessageProcessorHandler.putMsg with an unknown o-customer is queued, no NPE");

		exit();
	}

	static GmmsMessage produced(StreamQueueManager q, GmmsMessage m) {
		q.produceSubmitMessage(m);
		return m;
	}

	/** what auto-claim would hand over: the same pending entry, re-read */
	static GmmsMessage readPendingAgain(GmmsMessage m) {
		String key = pendingKey(m);
		Map.Entry<StreamEntryID, List<redis.clients.jedis.resps.StreamEntry>> claimed = jedis.xautoclaim(key,
				"CoreMTGroup", CONSUMER, 0, new StreamEntryID(0, 0),
				redis.clients.jedis.params.XAutoClaimParams.xAutoClaimParams().count(10));
		for (redis.clients.jedis.resps.StreamEntry e : claimed.getValue()) {
			GmmsMessage g = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
			if (g != null && m.getMsgID().equals(g.getMsgID())) {
				g.setRedisStreamID(e.getID().toString());
				return g;
			}
		}
		throw new IllegalStateException("not claimed: " + m.getMsgID());
	}
}
