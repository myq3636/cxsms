package it;

import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.EarlyDrQueue;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;

/**
 * Vendor DRs that come before their submit result (EarlyDr.*, docs/DR早到重试与outbox应答超时改进设计.md 4): with T the
 * response timeout of the DR's channel (channel 8, BufferTimeout set to 2 s here) and t0 the time the Client received
 * it, Core queues one due at t0+T/2; the MQM job looks for the original itself and writes back only the matched ones;
 * one not matched is due again at t0+T and dropped after it, never stored in SDQ; one that comes after t0+T is dropped
 * at once. DRs and originals as AttemptBatchIT (stream:core:inbound-dr:8).
 */
public class EarlyDrIT extends ItSupport {
	static final String IDR = AttemptBatchIT.IDR, GROUP = AttemptBatchIT.IDR_GROUP;
	static final long T = 2000L;
	/** the outMsgIds the WDQ lookup of the MQM job finds */
	static final Set<String> IN_WDQ = new HashSet<String>();

	/** the vendor DRs now in the inbound DR stream, read like the consumer does */
	static List<GmmsMessage> read() {
		StreamQueueManager q = StreamQueueManager.getInstance();
		q.createGroup(IDR, GROUP, false);
		Map<String, StreamEntryID> query = new HashMap<>();
		query.put(IDR, StreamEntryID.UNRECEIVED_ENTRY);
		List<GmmsMessage> l = q.consumeBatch(GROUP, "core-inbound-dr-1", 100, query, false);
		return l == null ? new ArrayList<GmmsMessage>() : l;
	}

	static void ack(GmmsMessage m) {
		jedis.xack(IDR, GROUP, new StreamEntryID(m.getRedisStreamID()));
		jedis.xdel(IDR, new StreamEntryID(m.getRedisStreamID()));
	}

	/** a vendor DR the Client received receivedAgoMs ago, through the inbound DR stream */
	static GmmsMessage earlyDr(String id, long receivedAgoMs) {
		GmmsMessage dr = AttemptBatchIT.vendorDr("OUT-" + id, "8_" + id);
		dr.setInTimeMs(System.currentTimeMillis() - receivedAgoMs);
		StreamQueueManager.getInstance().produceInboundDeliveryReport(dr);
		List<GmmsMessage> l = read();
		if (l.size() != 1) throw new IllegalStateException("not read: " + id + " " + l.size());
		return l.get(0);
	}

	/** Core handles it (no original yet) and the entry is acked */
	static void handled(GmmsMessage d, String what) throws Exception {
		check(AttemptBatchIT.process(d), what + " handled by Core (the entry can be acked)");
		ack(d);
	}

	static String entry(String id) {
		Set<String> k = jedis.keys(EarlyDrQueue.PREFIX + "OUT-" + id + ":*");
		return k.isEmpty() ? null : k.iterator().next();
	}

	static void original(String id) {
		GmmsMessage o = AttemptBatchIT.original(id);
		jedis.set(o.getOutMsgID(), SerializableHandler.convertGmmsMessage2RedisMessage(o));
	}

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		}
		exit();
	}

	static void run() throws Exception {
		// the DB lookup by outMsgId finds nothing, as with a DB (here there is none: it would throw); the MQM job's WDQ
		// lookup finds the ids in WDQ
		msmFactory = () -> new com.king.message.gmms.MessageStoreManager() {
			@Override
			public GmmsMessage getGmmsMessageByOutMsgID(String outMsgID) {
				return null;
			}

			@Override
			public Set<String> existingOutMsgIDs(Collection<String> outMsgIDs) {
				Set<String> s = new HashSet<String>(outMsgIDs);
				s.retainAll(IN_WDQ);
				return s;
			}
		};
		boot();
		AttemptBatchIT.q = StreamQueueManager.getInstance();
		props.setProperty("CoreInboundDRShardEnable", "false");   // these checks read the stream by its name
		A2PCustomerInfo ch8 = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(8);
		int bt8 = ch8.getBufferTimeout();
		ch8.setBufferTimeout((int) T);
		check(EarlyDrQueue.waitMs(AttemptBatchIT.vendorDr("OUT-X", "8_X")) == T, "ED-00 T = the response timeout of channel 8");

		// ---- ED-01 no original yet: queued for t0+T/2, with its end t0+T; not in SDQ, no DR to the customer ----
		GmmsMessage d1 = earlyDr("ED1", 0);
		long t0 = d1.getInTimeMs();
		handled(d1, "ED-01a");
		String k1 = entry("ED1");
		Double s1 = k1 == null ? null : jedis.zscore(EarlyDrQueue.READY_ZSET, k1);
		check(k1 != null && s1 != null && s1.longValue() == t0 + T / 2 && String.valueOf(t0 + T).equals(jedis.hget(k1, "end"))
				&& jedis.hget(k1, "body") != null && jedis.hget(k1, "tries") == null && jedis.pttl(k1) > T,
				"ED-01b queued: due " + (s1 == null ? null : s1.longValue() - t0) + " ms after t0, end t0+T, with a TTL");
		check(AttemptBatchIT.drsOut("ED1") == 0, "ED-01c no DR to the customer yet");

		// ---- ED-02 not due: nothing; at t0+T/2 still no original: due again at t0+T, not written back ----
		check(EarlyDrQueue.dispatchDue() == 0, "ED-02a nothing claimed before t0+T/2");
		Thread.sleep(Math.max(0, t0 + T / 2 + 50 - System.currentTimeMillis()));
		int c2 = EarlyDrQueue.dispatchDue();
		Double s2 = jedis.zscore(EarlyDrQueue.READY_ZSET, k1);
		check(c2 == 1 && s2 != null && s2.longValue() == t0 + T && read().isEmpty() && jedis.zcard(EarlyDrQueue.PROCESSING_ZSET) == 0,
				"ED-02b first try without an original: due again at t0+T, nothing written back to Core: " + c2);

		// ---- ED-03 the original comes: the second try writes it back, Core matches it ----
		original("ED1");
		Thread.sleep(Math.max(0, t0 + T + 50 - System.currentTimeMillis()));
		int c3 = EarlyDrQueue.dispatchDue();
		List<GmmsMessage> back = read();
		check(c3 == 1 && back.size() == 1 && "OUT-ED1".equals(back.get(0).getOutMsgID()) && back.get(0).getInTimeMs() == t0,
				"ED-03a matched on the second try: written back to " + IDR + " with its t0: " + back.size());
		check(entry("ED1") == null && jedis.zcard(EarlyDrQueue.READY_ZSET) == 0 && jedis.zcard(EarlyDrQueue.PROCESSING_ZSET) == 0,
				"ED-03b the entry deleted");
		check(!back.isEmpty() && AttemptBatchIT.process(back.get(0)) && AttemptBatchIT.drsOut("ED1") == 1,
				"ED-03c Core matches it: one DR to the customer");
		if (!back.isEmpty()) ack(back.get(0));

		// ---- ED-04 no original ever: (after t0+T/2 at the first lookup) due at t0+T, then dropped ----
		GmmsMessage d4 = earlyDr("ED4", T / 2 + 200);
		long t4 = d4.getInTimeMs();
		handled(d4, "ED-04a");
		String k4 = entry("ED4");
		Double s4 = k4 == null ? null : jedis.zscore(EarlyDrQueue.READY_ZSET, k4);
		check(s4 != null && s4.longValue() == t4 + T, "ED-04b first lookup after t0+T/2: due at t0+T only");
		Thread.sleep(Math.max(0, t4 + T + 50 - System.currentTimeMillis()));
		int c4 = EarlyDrQueue.dispatchDue();
		check(c4 == 1 && entry("ED4") == null && read().isEmpty() && jedis.zcard(EarlyDrQueue.READY_ZSET) == 0
				&& jedis.zcard(EarlyDrQueue.PROCESSING_ZSET) == 0 && AttemptBatchIT.drsOut("ED4") == 0,
				"ED-04c still no original at t0+T: dropped, not written back, no DR to the customer");

		// ---- ED-05 the DR comes after t0+T (a backlog): dropped at the first lookup, never queued ----
		GmmsMessage d5 = earlyDr("ED5", T + 500);
		handled(d5, "ED-05a");
		check(entry("ED5") == null && jedis.zcard(EarlyDrQueue.READY_ZSET) == 0, "ED-05b past t0+T: dropped at once, not queued");

		// ---- ED-06 a redelivery of a queued DR: one entry, its due time kept ----
		GmmsMessage d6 = AttemptBatchIT.vendorDr("OUT-ED6", "8_ED6");
		d6.setInTimeMs(System.currentTimeMillis());
		EarlyDrQueue.Offer o1 = EarlyDrQueue.offer(d6), o2 = EarlyDrQueue.offer(d6);
		String k6 = entry("ED6");
		check(o1 == EarlyDrQueue.Offer.QUEUED && o2 == EarlyDrQueue.Offer.QUEUED && k6 != null
				&& jedis.zscore(EarlyDrQueue.READY_ZSET, k6).longValue() == d6.getInTimeMs() + T / 2 && jedis.zcard(EarlyDrQueue.READY_ZSET) == 1,
				"ED-06 offered twice: one entry, due t0+T/2");

		// ---- ED-07 a claim left by a stopped MQM comes back ----
		jedis.zrem(EarlyDrQueue.READY_ZSET, k6);
		jedis.zadd(EarlyDrQueue.PROCESSING_ZSET, System.currentTimeMillis() - 1, k6);   // claimed, then the MQM stopped
		original("ED6");
		int c7 = EarlyDrQueue.dispatchDue();
		List<GmmsMessage> back7 = read();
		check(c7 == 1 && back7.size() == 1 && jedis.zcard(EarlyDrQueue.PROCESSING_ZSET) == 0 && entry("ED6") == null,
				"ED-07 the run-out claim is put back, the original found, written back: " + c7 + ", " + back7.size());
		for (GmmsMessage m : back7) ack(m);

		// ---- ED-08 the original is in WDQ (not in Redis): found by the MQM job's WDQ lookup ----
		GmmsMessage d8 = earlyDr("ED8", T / 2 + 200);
		handled(d8, "ED-08a");
		IN_WDQ.add("OUT-ED8");
		jedis.zadd(EarlyDrQueue.READY_ZSET, System.currentTimeMillis() - 1, entry("ED8"));   // due now
		int c8 = EarlyDrQueue.dispatchDue();
		List<GmmsMessage> back8 = read();
		check(c8 == 1 && back8.size() == 1 && "OUT-ED8".equals(back8.get(0).getOutMsgID()) && entry("ED8") == null,
				"ED-08b original in WDQ: written back: " + c8 + ", " + back8.size());
		for (GmmsMessage m : back8) ack(m);

		// ---- ED-09 an entry of before (tries, no end): one more lookup, then dropped ----
		jedis.hset(EarlyDrQueue.PREFIX + "OUT-ED9:10000:8_ED9", "body",
				SerializableHandler.convertGmmsMessage2RedisMessage(AttemptBatchIT.vendorDr("OUT-ED9", "8_ED9")));
		jedis.hset(EarlyDrQueue.PREFIX + "OUT-ED9:10000:8_ED9", "tries", "3");
		jedis.zadd(EarlyDrQueue.READY_ZSET, System.currentTimeMillis() - 1, EarlyDrQueue.PREFIX + "OUT-ED9:10000:8_ED9");
		int c9 = EarlyDrQueue.dispatchDue();
		check(c9 == 1 && entry("ED9") == null && read().isEmpty() && jedis.zcard(EarlyDrQueue.READY_ZSET) == 0,
				"ED-09 an entry without its end: dropped after one lookup");

		// ---- ED-10 switched off: never queued ----
		props.setProperty("EarlyDr.Enable", "false");
		GmmsMessage d10 = earlyDr("ED10", 0);
		handled(d10, "ED-10a");
		check(jedis.zcard(EarlyDrQueue.READY_ZSET) == 0 && entry("ED10") == null,
				"ED-10b EarlyDr.Enable=false: stored as before, nothing queued");
		props.remove("EarlyDr.Enable");
		ch8.setBufferTimeout(bt8);
	}
}
