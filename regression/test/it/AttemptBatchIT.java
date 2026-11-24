package it;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;

/**
 * StreamAttempt by batch (CoreStreamAttemptBatch, docs/StreamAttempt按批处理设计.md): one Lua claims a batch, one acks it,
 * the flags and _done of each entry stay in memory until the ack unless they guard a message written out
 * (CoreStreamAttemptBatchFlagsInMemory, docs/Redis消息操作次数统计.md); RUN, BUSY and DONE in one batch, a marker twice in
 * a batch, the lease renewed after half of it, a crash in the middle of a batch, and the same DRs with the switch off.
 */
public class AttemptBatchIT extends ItSupport {
	static StreamQueueManager q;
	static final String IDR = "stream:core:inbound-dr:8";
	static final String IDR_GROUP = "CoreInboundDRGroup";

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

	static int drsOut(String msgId) {
		return inStreams("stream:dr:pending:8888:*", msgId).size();
	}

	/** vendor DRs for these ids in the stream (with their DR lookup entries), read like the consumer does */
	static List<GmmsMessage> readDrs(String... ids) {
		for (String id : ids) {
			GmmsMessage o = original(id);
			jedis.set(com.king.gmms.messagequeue.DrLookup.key(o.getOutMsgID()), SerializableHandler.convertGmmsMessage2RedisMessage(o));
			q.produceInboundDeliveryReport(vendorDr(o.getOutMsgID(), "8_" + id));
		}
		q.createGroup(IDR, IDR_GROUP, false);
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(IDR, StreamEntryID.UNRECEIVED_ENTRY);
		return q.consumeBatch(IDR_GROUP, "core-inbound-dr-1", 100, query, false);
	}

	static boolean process(GmmsMessage m) throws Exception {
		return MessageStoreManager.withInlineDrDispatch(new Callable<Boolean>() {
			public Boolean call() {
				return com.king.gmms.processor.CoreMessageProcessor.getInstance().handleInboundDRInternal(m);
			}
		});
	}

	static long lua() {
		Map<String, Long> c = new HashMap<String, Long>();
		for (String l : jedis.info("commandstats").split("\r?\n")) {
			if (l.startsWith("cmdstat_eval:") || l.startsWith("cmdstat_evalsha:")) {
				c.put(l.substring(8, l.indexOf(':')), Long.parseLong(l.replaceAll("^[^:]+:calls=(\\d+),.*", "$1")));
			}
		}
		long n = 0;
		for (long v : c.values()) n += v;
		return n;
	}

	static long cmd(String name) {
		for (String l : jedis.info("commandstats").split("\r?\n")) {
			if (l.startsWith("cmdstat_" + name + ":")) return Long.parseLong(l.replaceAll("^[^:]+:calls=(\\d+),.*", "$1"));
		}
		return 0;
	}

	static void consumeDr(GmmsMessage m) throws Exception {
		Method pm = InboundDRStreamConsumer.class.getDeclaredMethod("processMessage", String.class, GmmsMessage.class);
		pm.setAccessible(true);
		pm.invoke(InboundDRStreamConsumer.getInstance(), IDR, m);
	}

	public static void main(String[] args) throws Exception {
		boot();
		q = StreamQueueManager.getInstance();
		props.setProperty("CoreInboundDRShardEnable", "false");   // these checks read the stream by its name
		RedisConnection report = RedisClient.getInstance().getReportMqRedis();

		// ---- AB-01 RUN, BUSY and DONE in one batch: one Lua claims, one acks ----
		List<GmmsMessage> l = readDrs("AB1A", "AB1B", "AB1C");
		check(l.size() == 3, "AB-01a 3 vendor DRs read");
		GmmsMessage a = l.get(0), b = l.get(1), c = l.get(2);
		jedis.hset(StreamAttempt.entryMarkerKey(IDR, a.getRedisStreamID()), "_done", "1");          // finished earlier
		jedis.hset(StreamAttempt.entryMarkerKey(IDR, b.getRedisStreamID()), "_lease", "other:1|" + System.currentTimeMillis());
		jedis.configResetStat();
		StreamAttempt.Batch batch = StreamAttempt.Batch.claim(IDR, IDR_GROUP, l, report, true, "it:1");
		check(lua() < 3, "AB-01b 3 entries claimed with fewer Lua calls than one each: " + lua());
		check(batch.begin(a) == StreamAttempt.Start.DONE && StreamAttempt.current() == null, "AB-01c an entry with _done: DONE, nothing bound");
		batch.done(a);
		check(batch.begin(b) == StreamAttempt.Start.BUSY && StreamAttempt.current() == null, "AB-01d an entry with a fresh foreign lease: BUSY");
		check(batch.begin(c) == StreamAttempt.Start.RUN && StreamAttempt.current() != null, "AB-01e a new entry: RUN, bound to the thread");
		check(process(c), "AB-01f processed (DR to the customer)");
		long hsetBefore = cmd("hset");
		batch.finished(c);
		String markerC = StreamAttempt.entryMarkerKey(IDR, c.getRedisStreamID());
		check(!jedis.hexists(markerC, "_done") && StreamAttempt.current() == null && cmd("hset") == hsetBefore,
				"AB-01g finished: its flags and _done stay in memory, no HSET");
		check(jedis.exists("drl:OUT-AB1C"), "AB-01h the after-done action (DR lookup entry deleted) waits for the ack");
		final List<String> acked = new ArrayList<String>();
		batch.ackAll(new StreamAttempt.AckListener() {
			public void acked(GmmsMessage m, String id, boolean ok) {
				if (ok) acked.add(id);
			}
		});
		check(acked.size() == 2 && !jedis.exists("drl:OUT-AB1C"),
				"AB-01i DONE and finished acked together: " + acked.size() + " acked, then the DR lookup entry deleted");
		check(!jedis.exists(markerC) && !jedis.exists(StreamAttempt.entryMarkerKey(IDR, a.getRedisStreamID())),
				"AB-01j their markers deleted with the ack");
		check(jedis.xpending(IDR, IDR_GROUP).getTotal() == 1 && jedis.exists(StreamAttempt.entryMarkerKey(IDR, b.getRedisStreamID())),
				"AB-01k the BUSY entry stays pending with its marker");
		check(drsOut("AB1C") == 1 && drsOut("AB1A") == 0, "AB-01l one DR out, for the new entry only");
		jedis.del(IDR, StreamAttempt.entryMarkerKey(IDR, b.getRedisStreamID()));

		// ---- AB-02 a marker twice in a batch (MTs: by msgId) ----
		GmmsMessage m1 = mt("AB2", 8888, 8, 0), m2 = mt("AB2", 8888, 8, 0), m3 = mt("AB2X", 8888, 8, 0);
		m1.setRedisStreamID("1-1");
		m2.setRedisStreamID("1-2");
		m3.setRedisStreamID("1-3");
		List<GmmsMessage> mts = new ArrayList<GmmsMessage>();
		mts.add(m1); mts.add(m2); mts.add(m3);
		StreamAttempt.Batch mb = StreamAttempt.Batch.claim("stream:mt:pending:8888:0", "CoreMTGroup", mts,
				RedisClient.getInstance().getSubmitMqRedis(), false, "it:1");
		check(mb.covers(m1) && !mb.covers(m2) && mb.covers(m3),
				"AB-02 the second MT with the same msgId is not in the batch claim (processed on its own after the ack)");
		RedisClient.getInstance().getSubmitMqRedis().evalScript("return redis.call('DEL', KEYS[1], KEYS[2])",
				java.util.Arrays.asList(StreamAttempt.markerKey(m1), StreamAttempt.markerKey(m3)), new ArrayList<String>());

		// ---- AB-03 after half the lease the entry is claimed again on its own ----
		props.setProperty("CoreMtLeaseMs", "400");
		List<GmmsMessage> l3 = readDrs("AB3");
		StreamAttempt.Batch b3 = StreamAttempt.Batch.claim(IDR, IDR_GROUP, l3, report, true, "it:1");
		String marker3 = StreamAttempt.entryMarkerKey(IDR, l3.get(0).getRedisStreamID());
		long leased = Long.parseLong(jedis.hget(marker3, "_lease").split("\\|")[1]);
		Thread.sleep(260);
		check(b3.begin(l3.get(0)) == StreamAttempt.Start.RUN, "AB-03a RUN after 260 ms of a 400 ms lease");
		long renewed = Long.parseLong(jedis.hget(marker3, "_lease").split("\\|")[1]);
		check(renewed - leased >= 250, "AB-03b the lease renewed when the entry starts: " + (renewed - leased) + " ms later");
		check(process(l3.get(0)), "AB-03c processed");
		b3.finished(l3.get(0));
		b3.ackAll(new StreamAttempt.AckListener() { public void acked(GmmsMessage m, String id, boolean ok) { } });
		props.remove("CoreMtLeaseMs");

		// ---- AB-04 a crash in the middle of a batch: flags in memory, then with the switch off (nothing repeated) ----
		for (int round = 0; round < 2; round++) {
			String x = round == 0 ? "AB4A" : "AB4C", y = round == 0 ? "AB4B" : "AB4D";
			if (round == 1) props.setProperty("CoreStreamAttemptBatchFlagsInMemory", "false");
			List<GmmsMessage> l4 = readDrs(x, y);
			StreamAttempt.Batch b4 = StreamAttempt.Batch.claim(IDR, IDR_GROUP, l4, report, true, "crashed:1");
			b4.begin(l4.get(0));
			process(l4.get(0));
			b4.finished(l4.get(0));        // done, then the Core dies: no ack, the second entry never started
			check(drsOut(x) == 1 && jedis.xpending(IDR, IDR_GROUP).getTotal() == 2,
					"AB-04a" + round + " crash after the first entry: both pending, one DR out");
			props.setProperty("CoreMtLeaseMs", "1");                        // the dead Core's lease is old
			Thread.sleep(20);
			for (GmmsMessage m : new ArrayList<GmmsMessage>(l4)) consumeDr(m);  // redelivered one by one (auto-claim)
			props.remove("CoreMtLeaseMs");
			check(drsOut(x) == 1 && drsOut(y) == 1,
					"AB-04b" + round + " redelivered: the DR to the customer not produced again (its p: flag is written by the produce), the other once");
			int cdrs = cdrCount(x, "OUT_DELI_REP");
			// flags in memory: the CDR flag still came before the crash, written with the DR produce (pendingFlags)
			check(jedis.xpending(IDR, IDR_GROUP).getTotal() == 0 && cdrs == 1 && cdrCount(y, "OUT_DELI_REP") == 1,
					"AB-04c" + round + " both acked, one OUT_DELI_REP CDR each" + (round == 0 ? " (the CDR flag went with the DR produce)" : " (switch off)"));
		}
		props.remove("CoreStreamAttemptBatchFlagsInMemory");

		// ---- AB-07 flags that guard a message written out are written at once: a produce to another Redis (p:), a delivery ----
		String[][] guards = { { "AB7P", "p:" }, { "AB7D", "delivery#" } };
		for (String[] g : guards) {
			List<GmmsMessage> l7 = readDrs(g[0]);
			StreamAttempt.Batch b7 = StreamAttempt.Batch.claim(IDR, IDR_GROUP, l7, report, true, "it:1");
			b7.begin(l7.get(0));
			if ("p:".equals(g[1])) {
				StreamAttempt.producedElsewhere(StreamAttempt.partField(0), "9-9");
			} else {
				StreamAttempt.once("delivery", "0");
			}
			b7.finished(l7.get(0));
			String marker7 = StreamAttempt.entryMarkerKey(IDR, l7.get(0).getRedisStreamID());
			boolean flag = false;
			for (String f : jedis.hkeys(marker7)) flag |= f.startsWith(g[1]);
			check(flag && jedis.hexists(marker7, "_done"), "AB-07 " + g[1] + " flag: written with _done when finished, before the ack");
			b7.ackAll(new StreamAttempt.AckListener() { public void acked(GmmsMessage m, String id, boolean ok) { } });
		}

		// ---- AB-08 a batch older than half the lease: the flags are written when an entry is finished ----
		props.setProperty("CoreMtLeaseMs", "400");
		List<GmmsMessage> l8 = readDrs("AB8");
		StreamAttempt.Batch b8 = StreamAttempt.Batch.claim(IDR, IDR_GROUP, l8, report, true, "it:1");
		Thread.sleep(260);
		b8.begin(l8.get(0));
		process(l8.get(0));
		b8.finished(l8.get(0));
		check(jedis.hexists(StreamAttempt.entryMarkerKey(IDR, l8.get(0).getRedisStreamID()), "_done"),
				"AB-08 finished 260 ms into a 400 ms lease: _done written at once");
		b8.ackAll(new StreamAttempt.AckListener() { public void acked(GmmsMessage m, String id, boolean ok) { } });
		props.remove("CoreMtLeaseMs");

		// ---- AB-06 processing changes the entry id on the message (a produce sets the new entry's id): acked by the id read ----
		List<GmmsMessage> l6 = readDrs("AB6");
		GmmsMessage m6 = l6.get(0);
		String read6 = m6.getRedisStreamID();
		StreamAttempt.Batch b6 = StreamAttempt.Batch.claim(IDR, IDR_GROUP, l6, report, true, "it:1");
		b6.begin(m6);
		process(m6);
		m6.setRedisStreamID("1-1");                    // as the MT written to stream:mt:routed gets the routed entry's id
		b6.finished(m6);
		final String[] ackedId = new String[1];
		b6.ackAll(new StreamAttempt.AckListener() {
			public void acked(GmmsMessage m, String id, boolean ok) {
				if (ok) ackedId[0] = id;
			}
		});
		check(read6.equals(ackedId[0]) && jedis.xpending(IDR, IDR_GROUP).getTotal() == 0
				&& !jedis.exists(StreamAttempt.entryMarkerKey(IDR, read6)),
				"AB-06 the id changed while processing: acked by the id read (" + ackedId[0] + "), nothing pending, marker gone");

		// ---- AB-05 the consumer: 30 DRs with batches, then 30 with the switch off ----
		long[] luaPer = new long[2];
		for (int round = 0; round < 2; round++) {
			if (round == 1) props.setProperty("CoreStreamAttemptBatch", "false");
			String p = round == 0 ? "AB5B" : "AB5S";
			for (int i = 0; i < 30; i++) {
				GmmsMessage o = original(p + i);
				jedis.set(com.king.gmms.messagequeue.DrLookup.key(o.getOutMsgID()), SerializableHandler.convertGmmsMessage2RedisMessage(o));
			}
			jedis.configResetStat();
			for (int i = 0; i < 30; i++) q.produceInboundDeliveryReport(vendorDr("OUT-" + p + i, "8_" + p + i));
			if (round == 0) InboundDRStreamConsumer.getInstance().start();
			final String pf = p;
			boolean all = waitFor(() -> { for (int i = 0; i < 30; i++) if (drsOut(pf + i) != 1) return false; return true; }, 20000);
			waitFor(() -> jedis.xlen(IDR) == 0, 5000);
			luaPer[round] = lua();
			check(all && jedis.xlen(IDR) == 0 && jedis.keys("core:att:" + IDR + ":*").isEmpty(),
					"AB-05" + (round == 0 ? "a batches" : "b switch off") + ": 30 DRs out once each, all acked, no marker left");
		}
		props.remove("CoreStreamAttemptBatch");
		check(luaPer[1] - luaPer[0] >= 30, "AB-05c batches need fewer Lua calls for the same 30 DRs: " + luaPer[0] + " vs " + luaPer[1] + " one by one");
		InboundDRStreamConsumer.getInstance().stop();
		exit();
	}
}
