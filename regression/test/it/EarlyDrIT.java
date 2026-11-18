package it;

import java.util.*;

import com.king.gmms.messagequeue.EarlyDrQueue;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;

/**
 * Vendor DRs that come before their submit result (EarlyDr.*, docs/5000TPS多主测试慢的环节分析.md 7.3): Core puts one in
 * the Redis delay queue instead of SDQ; the MQM job writes the due ones back to the inbound DR stream, where Core
 * matches them once the original is there; a claim left by a stopped MQM comes back; after EarlyDr.MaxTries the old
 * way; switched off, never queued. DRs and originals as AttemptBatchIT (stream:core:inbound-dr:8).
 */
public class EarlyDrIT extends ItSupport {
	static final String IDR = AttemptBatchIT.IDR, GROUP = AttemptBatchIT.IDR_GROUP;

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

	static GmmsMessage earlyDr(String id) {
		StreamQueueManager.getInstance().produceInboundDeliveryReport(AttemptBatchIT.vendorDr("OUT-" + id, "8_" + id));
		List<GmmsMessage> l = read();
		if (l.size() != 1) throw new IllegalStateException("not read: " + id + " " + l.size());
		return l.get(0);
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
		// the DB lookup by outMsgId finds nothing, as with a DB (here there is none: it would throw)
		msmFactory = () -> new com.king.message.gmms.MessageStoreManager() {
			@Override
			public GmmsMessage getGmmsMessageByOutMsgID(String outMsgID) {
				return null;
			}
		};
		boot();
		AttemptBatchIT.q = StreamQueueManager.getInstance();
		props.setProperty("CoreInboundDRShardEnable", "false");   // these checks read the stream by its name
		props.setProperty("EarlyDr.DelayMs", "200");

		// ---- ED-01 no original yet: in the delay queue, not in SDQ ----
		GmmsMessage d1 = earlyDr("ED1");
		check(AttemptBatchIT.process(d1), "ED-01a the DR is handled (the entry can be acked)");
		ack(d1);
		List<String> ready = jedis.zrange(EarlyDrQueue.READY_ZSET, 0, -1);
		String key = ready.isEmpty() ? null : ready.iterator().next();
		check(ready.size() == 1 && key.startsWith(EarlyDrQueue.PREFIX + "OUT-ED1:") && "1".equals(jedis.hget(key, "tries"))
				&& jedis.hget(key, "body") != null, "ED-01b in the delay queue, try 1: " + ready);
		check(AttemptBatchIT.drsOut("ED1") == 0, "ED-01c no DR to the customer yet");

		// ---- ED-02 the original comes; the MQM job writes the due DR back, Core matches it ----
		GmmsMessage o1 = AttemptBatchIT.original("ED1");
		jedis.set(o1.getOutMsgID(), SerializableHandler.convertGmmsMessage2RedisMessage(o1));
		int notDue = EarlyDrQueue.dispatchDue();
		Thread.sleep(250);
		int due = EarlyDrQueue.dispatchDue();
		check(notDue == 0 && due == 1, "ED-02a written back only once due: " + notDue + " then " + due);
		List<GmmsMessage> back = read();
		check(back.size() == 1 && "OUT-ED1".equals(back.get(0).getOutMsgID()), "ED-02b back in " + IDR + ": " + back.size());
		check(jedis.zcard(EarlyDrQueue.PROCESSING_ZSET) == 0 && jedis.zcard(EarlyDrQueue.READY_ZSET) == 0 && jedis.hget(key, "body") == null,
				"ED-02c nothing claimed or due any more, the body dropped");
		check(!back.isEmpty() && AttemptBatchIT.process(back.get(0)) && AttemptBatchIT.drsOut("ED1") == 1,
				"ED-02d matched now: one DR to the customer");
		if (!back.isEmpty()) ack(back.get(0));

		// ---- ED-03 a claim left by a stopped MQM comes back ----
		GmmsMessage d3 = earlyDr("ED3");
		AttemptBatchIT.process(d3);
		ack(d3);
		String k3 = jedis.zrange(EarlyDrQueue.READY_ZSET, 0, -1).iterator().next();
		jedis.zrem(EarlyDrQueue.READY_ZSET, k3);
		jedis.zadd(EarlyDrQueue.PROCESSING_ZSET, System.currentTimeMillis() - 1, k3);   // claimed, then the MQM stopped
		int again = EarlyDrQueue.dispatchDue();
		List<GmmsMessage> back3 = read();
		check(again == 1 && back3.size() == 1 && jedis.zcard(EarlyDrQueue.PROCESSING_ZSET) == 0,
				"ED-03 the run-out claim is put back and written back: " + again + ", " + back3.size());
		for (GmmsMessage m : back3) ack(m);

		// ---- ED-04 after EarlyDr.MaxTries the old way ----
		props.setProperty("EarlyDr.MaxTries", "2");
		GmmsMessage d4 = AttemptBatchIT.vendorDr("OUT-ED4", "8_ED4");
		boolean t1 = EarlyDrQueue.offer(d4), t2 = EarlyDrQueue.offer(d4), t3 = EarlyDrQueue.offer(d4);
		check(t1 && t2 && !t3 && jedis.keys(EarlyDrQueue.PREFIX + "OUT-ED4:*").isEmpty(),
				"ED-04 queued twice, the third try goes the old way and the entry is gone: " + t1 + "," + t2 + "," + t3);
		props.remove("EarlyDr.MaxTries");
		jedis.del(EarlyDrQueue.READY_ZSET);

		// ---- ED-05 switched off: never queued ----
		props.setProperty("EarlyDr.Enable", "false");
		GmmsMessage d5 = earlyDr("ED5");
		AttemptBatchIT.process(d5);
		ack(d5);
		check(jedis.zcard(EarlyDrQueue.READY_ZSET) == 0 && jedis.keys(EarlyDrQueue.PREFIX + "OUT-ED5:*").isEmpty(),
				"ED-05 EarlyDr.Enable=false: stored as before, nothing queued");
		props.remove("EarlyDr.Enable");
	}
}
