package it;

import java.lang.reflect.Field;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.CoreOutboundOutboxManager;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

/** Local integration test of Core batch 3 (MT idempotency, outbox, timeouts); not part of the repo. */
public class Batch3IT {
	static int failures = 0;
	static Jedis jedis;
	static final String CLIENT_GROUP = "ClientOutboundSubmitGroup";
	static final Map<String, String> PENDING_STREAM = new HashMap<String, String>();

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	interface Cond { boolean ok() throws Exception; }

	static boolean waitFor(Cond c, long ms) throws Exception {
		long end = System.currentTimeMillis() + ms;
		while (System.currentTimeMillis() < end) {
			if (c.ok()) return true;
			Thread.sleep(100);
		}
		return c.ok();
	}

	static GmmsMessage mt(String msgId, int rssid, int split) {
		GmmsMessage m = new GmmsMessage();
		m.setMsgID(msgId);
		m.setInMsgID("IN-" + msgId);
		m.setOSsID(8888);
		m.setRSsID(rssid);
		m.setOA2P(56);
		m.setRA2P(56);
		m.setCurrentA2P(56);
		m.setSenderAddress("18667991226");
		m.setRecipientAddress("8613800000000");
		m.setTextContent("code 123456");
		m.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		m.setDeliveryReport(true);
		m.setSplitStatus(split);
		m.setDateIn(new Date());
		m.setTimeStamp(new Date());
		m.setExpiryDate(new Date(System.currentTimeMillis() + 3600 * 1000L));
		return m;
	}

	static String routedKey(int rssid) {
		return "stream:mt:routed:" + rssid + ":" + StreamQueueManager.getInstance().calculateOutboundShardId(mt("x", rssid, 0));
	}

	static long routedLen(int rssid) {
		long n = 0;
		for (String k : jedis.keys("stream:mt:routed:" + rssid + ":*")) n += jedis.xlen(k);
		return n;
	}

	/** results produced for msgId: list of status codes */
	static java.util.List<Integer> results(String msgId) {
		java.util.List<Integer> codes = new java.util.ArrayList<Integer>();
		// legacy stream:core:results and the sharded stream:core:results:{ssid}:{shard} (batch 5)
		for (String key : jedis.keys("stream:core:results*")) {
			for (StreamEntry e : jedis.xrange(key, (StreamEntryID) null, (StreamEntryID) null)) {
				GmmsMessage m = SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && msgId.equals(m.getMsgID())) codes.add(m.getStatusCode());
			}
		}
		return codes;
	}

	/** put an MT into stream:mt:pending and read it with the Core group, as MTStreamConsumer does */
	static GmmsMessage pendingMt(String msgId) {
		StreamQueueManager q = StreamQueueManager.getInstance();
		GmmsMessage in = mt(msgId, 8, 0);
		q.produceSubmitMessage(in);
		String key = "stream:mt:pending:8888:" + q.calculateShardId(in);
		q.createGroup(key, "CoreMTGroup", true);
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(key, StreamEntryID.UNRECEIVED_ENTRY);
		for (GmmsMessage m : q.consumeBatch("CoreMTGroup", "core-mt-it", 100, query, true)) {
			if (msgId.equals(m.getMsgID())) {
				PENDING_STREAM.put(msgId, key);
				return m;
			}
		}
		throw new IllegalStateException("MT not read back: " + msgId);
	}

	static String pendingStream(GmmsMessage m) {
		return PENDING_STREAM.get(m.getMsgID());
	}

	public static void main(String[] args) throws Exception {
		String home = System.getProperty("a2p_home");
		jedis = new Jedis("127.0.0.1", 16379);
		jedis.flushAll();
		GmmsUtility gu = GmmsUtility.getInstance();
		Field lf = GmmsUtility.class.getDeclaredField("log");
		lf.setAccessible(true);
		if (lf.get(null) == null) lf.set(null, com.king.framework.SystemLogger.getSystemLogger(GmmsUtility.class));
		gu.initUtility(home + "Gmms/GmmsConfig.properties");
		gu.initRedisClient("M");
		gu.initCoreShardAssignment();
		gu.initCDRManager();
		MessageStoreManager msm = new MessageStoreManager();
		Field dmf = com.king.db.DataControl.class.getDeclaredField("dataManagers");
		dmf.setAccessible(true);
		@SuppressWarnings("unchecked")
		java.util.Hashtable<Object, Object> dms = (java.util.Hashtable<Object, Object>) dmf.get(null);
		dms.put(MessageStoreManager.class.getName(), msm);
		dms.put(com.king.db.DBLockConnection.class.getName(), new com.king.db.DBLockConnection());
		Field inf = com.king.db.DataControl.class.getDeclaredField("initialized");
		inf.setAccessible(true);
		inf.set(null, Boolean.TRUE);
		gu.initDBManager(com.king.db.DatabaseStatus.MASTER_USED);
		Field pf = GmmsUtility.class.getDeclaredField("properties");
		pf.setAccessible(true);
		Properties props = (Properties) pf.get(gu);
		props.setProperty("CoreOutboundUnreadStallMs", "1500");
		props.setProperty("CoreOutboundRespTimeoutMs", "2500");
		props.setProperty("CoreOutboundOutboxHardTimeoutMs", "6000");
		props.setProperty("CoreOutboundOutboxRecheckMs", "1000");
		StreamQueueManager q = StreamQueueManager.getInstance();

		// ---- A. redelivery after a failed attempt repeats no side effect ----
		GmmsMessage a = pendingMt("8888_A");
		String marker = StreamAttempt.markerKey(a);
		check(StreamAttempt.begin(a, "nodeA:1") == StreamAttempt.Start.RUN, "A: first attempt claims the MT");
		check(StreamAttempt.once("cdr", "IN_SUBMIT:0:8"), "A: first attempt writes the IN_SUBMIT CDR");
		check(StreamAttempt.countOnce("Duplicate:A", 86400) == 1, "A: first attempt counts the recipient (1)");
		GmmsMessage outA = mt("8888_A", 8, 0);
		check(q.produceOutboundMessage(outA), "A: part 0 produced to stream:mt:routed");
		String idA = outA.getRedisStreamID();
		check(routedLen(8) == 1 && idA.equals(jedis.hget(marker, "p:0")), "A: marker records part 0 = " + idA);
		check("1".equals(jedis.hget(marker, "cdr#1:IN_SUBMIT:0:8")) && jedis.hexists(marker, "cnt#1:Duplicate:A"),
				"A: CDR and counter flags written with the part (same Lua)");
		check(jedis.exists("core:outbox:submit:8888_A:0:0") && jedis.zscore("zset:core:outbox:submit:timeout", "core:outbox:submit:8888_A:0:0") != null,
				"A: outbox core:outbox:submit:8888_A:0:0 written with its timeout entry");
		StreamAttempt.abandon();
		check(!jedis.hexists(marker, "_lease"), "A: failed attempt releases its lease, keeps its flags");

		check(StreamAttempt.begin(a, "nodeB:7") == StreamAttempt.Start.RUN, "A: redelivery claims the MT again");
		check(!StreamAttempt.once("cdr", "IN_SUBMIT:0:8"), "A: redelivery skips the CDR");
		check(StreamAttempt.countOnce("Duplicate:A", 86400) == 1 && "1".equals(jedis.get("Duplicate:A")),
				"A: redelivery reads the counter, does not increment it");
		GmmsMessage outA2 = mt("8888_A", 8, 0);
		check(q.produceOutboundMessage(outA2) && routedLen(8) == 1 && idA.equals(outA2.getRedisStreamID()),
				"A: redelivery does not produce part 0 again (same id, routed length 1)");
		check(StreamAttempt.once("dr", "10000:0"), "A: a side effect not done before still runs");
		boolean acked = StreamAttempt.finish(pendingStream(a), "CoreMTGroup", a.getRedisStreamID());
		check(acked && !jedis.exists(marker), "A: finish acks the MT and deletes the marker");
		check(jedis.xlen(pendingStream(a)) == 0, "A: pending entry deleted (XACK+XDEL)");

		// ---- B. lease: a second consumer backs off while the first is working ----
		final GmmsMessage b = pendingMt("8888_B");
		props.setProperty("CoreMtLeaseMs", "1500");
		Thread t = new Thread() {
			public void run() {
				StreamAttempt.begin(b, "nodeA:2");  // never finishes: holds the lease
			}
		};
		t.start();
		t.join();
		check(StreamAttempt.begin(b, "nodeB:9") == StreamAttempt.Start.BUSY, "B: second consumer sees a fresh lease -> BUSY");
		Thread.sleep(1700);
		check(StreamAttempt.begin(b, "nodeB:9") == StreamAttempt.Start.RUN, "B: expired lease (owner died) -> taken over");
		check(StreamAttempt.finish(pendingStream(b), "CoreMTGroup", b.getRedisStreamID()), "B: taken-over MT finished");

		// ---- C. split parts get their own outbox entries ----
		for (int part = 1; part <= 3; part++) {
			q.produceOutboundMessage(mt("8888_C", 8, part));
		}
		check(jedis.exists("core:outbox:submit:8888_C:1:0") && jedis.exists("core:outbox:submit:8888_C:2:0")
				&& jedis.exists("core:outbox:submit:8888_C:3:0"), "C: 3 parts -> 3 outbox entries (no overwrite)");

		// ---- D. exactly one result per attempt ----
		CoreOutboundOutboxManager ob = CoreOutboundOutboxManager.getInstance();
		GmmsMessage r1 = mt("8888_C", 8, 2);
		r1.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		r1.setRedisStreamID("1-1");
		check(ob.claimResult(r1) == CoreOutboundOutboxManager.Claim.FIRST && !jedis.exists("core:outbox:submit:8888_C:2:0"),
				"D: first result of part 2 claims the outbox");
		check(jedis.exists("core:outbox:submit:8888_C:1:0"), "D: part 1 outbox untouched");
		r1.setRedisStreamID("2-1");
		check(ob.claimResult(r1) == CoreOutboundOutboxManager.Claim.STALE, "D: second result (e.g. late resp after timeout) -> STALE");
		r1.setRedisStreamID("1-1");
		check(ob.claimResult(r1) == CoreOutboundOutboxManager.Claim.FIRST, "D: retry of the same result entry -> FIRST");
		GmmsMessage retry = mt("8888_C", 8, 2);
		retry.setRetriedNumber(1);
		q.produceOutboundMessage(retry);
		check(jedis.exists("core:outbox:submit:8888_C:2:1"), "D: a resend (retriedNumber 1) gets a new outbox entry");
		GmmsMessage unknown = mt("8888_ZZ", 8, 0);
		unknown.setRedisStreamID("3-1");
		check(ob.claimResult(unknown) == CoreOutboundOutboxManager.Claim.UNTRACKED, "D: result without outbox -> UNTRACKED");

		// ---- E1. unread and the Client group does not exist: pulled back, 2500 ----
		GmmsMessage e1 = mt("8888_E1", 101, 0);
		q.produceOutboundMessage(e1);
		check(waitFor(() -> routedLen(101) == 0 && results("8888_E1").contains(2500), 8000),
				"E1: unread with no client -> entry deleted from routed, COMMUNICATION_ERROR result " + results("8888_E1"));

		// ---- E2. sent by the client (acked) but no result: 2700 after resp timeout ----
		GmmsMessage e2 = mt("8888_E2", 102, 0);
		q.produceOutboundMessage(e2);
		String k2 = routedKey(102);
		jedis.xgroupCreate(k2, CLIENT_GROUP, new StreamEntryID(0, 0), true);
		Map<String, StreamEntryID> rq = new HashMap<String, StreamEntryID>();
		rq.put(k2, StreamEntryID.UNRECEIVED_ENTRY);
		List<Map.Entry<String, List<StreamEntry>>> read = jedis.xreadGroup(CLIENT_GROUP, "client-1",
				XReadGroupParams.xReadGroupParams().count(1), rq);
		StreamEntryID id2 = read.get(0).getValue().get(0).getID();
		jedis.xack(k2, CLIENT_GROUP, id2);
		jedis.xdel(k2, id2);
		long sentAt = System.currentTimeMillis();
		check(waitFor(() -> results("8888_E2").contains(2700), 8000) && System.currentTimeMillis() - sentAt >= 1500,
				"E2: client acked, no result -> SUBMIT_RESP_ERROR after the resp timeout " + results("8888_E2"));

		// ---- E3. read but not acked: waits for the hard timeout ----
		GmmsMessage e3 = mt("8888_E3", 103, 0);
		long e3At = System.currentTimeMillis();
		q.produceOutboundMessage(e3);
		String k3 = routedKey(103);
		jedis.xgroupCreate(k3, CLIENT_GROUP, new StreamEntryID(0, 0), true);
		Map<String, StreamEntryID> rq3 = new HashMap<String, StreamEntryID>();
		rq3.put(k3, StreamEntryID.UNRECEIVED_ENTRY);
		jedis.xreadGroup(CLIENT_GROUP, "client-1", XReadGroupParams.xReadGroupParams().count(1), rq3);
		Thread.sleep(3500);
		check(results("8888_E3").isEmpty(), "E3: in client PEL, no result before the hard timeout");
		check(waitFor(() -> results("8888_E3").contains(2700), 6000) && System.currentTimeMillis() - e3At >= 6000,
				"E3: in client PEL -> SUBMIT_RESP_ERROR at the hard timeout " + results("8888_E3"));

		// ---- E4. queued behind a client that is reading: not pulled back; pulled once it stalls ----
		final String k4 = routedKey(104);
		for (int i = 0; i < 6; i++) {
			q.produceOutboundMessage(mt("8888_E4_" + i, 104, 0));
		}
		jedis.xgroupCreate(k4, CLIENT_GROUP, new StreamEntryID(0, 0), true);
		final Jedis reader = new Jedis("127.0.0.1", 16379);
		Thread client = new Thread() {
			public void run() {
				Map<String, StreamEntryID> q4 = new HashMap<String, StreamEntryID>();
				q4.put(k4, StreamEntryID.UNRECEIVED_ENTRY);
				for (int i = 0; i < 3; i++) {   // reads 3 of 6, one every 700 ms, then the client "dies"
					List<Map.Entry<String, List<StreamEntry>>> r = reader.xreadGroup(CLIENT_GROUP, "client-1",
							XReadGroupParams.xReadGroupParams().count(1), q4);
					StreamEntryID id = r.get(0).getValue().get(0).getID();
					reader.xack(k4, CLIENT_GROUP, id);
					reader.xdel(k4, id);
					try { Thread.sleep(700); } catch (InterruptedException e) { return; }
				}
			}
		};
		client.start();
		Thread.sleep(1900);
		check(results("8888_E4_5").isEmpty() && jedis.xlen(k4) >= 3,
				"E4: last entry queued behind a reading client is not pulled back (" + jedis.xlen(k4) + " left)");
		client.join();
		check(waitFor(() -> results("8888_E4_5").contains(2500) && jedis.xlen(k4) == 0, 8000),
				"E4: client stopped reading -> the unread entries are pulled back with COMMUNICATION_ERROR");

 		// ---- F. the real MTStreamConsumer runs the attempt end to end ----
		GmmsMessage f = mt("7777_F", 8, 0);
		f.setOSsID(7777); // unknown customer: CDR + done, no routing needed
		q.produceSubmitMessage(f);
		final String fKey = "stream:mt:pending:7777:" + q.calculateShardId(f);
		com.king.gmms.messagequeue.MTStreamConsumer.getInstance().start();
		check(waitFor(() -> jedis.exists(fKey) && jedis.xlen(fKey) == 0 && !jedis.exists("core:mt:7777_F"), 10000),
				"F: MTStreamConsumer processed, acked and removed the marker");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
