package it;

import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.DrLookup;
import com.king.gmms.messagequeue.EarlyDrQueue;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;

/**
 * The DR lookup in the state Redis or on the report masters (DrLookupStore, DrLookupLegacyRead,
 * docs/DR匹配数据迁移设计.md): where it is written, Core's match and delete, the reads across a switch, the early DR offer
 * that finds its lookup, and the MQM job moving the old ones to WDQ. DRs and originals as EarlyDrIT (channel 8, customer
 * 8888, stream:core:inbound-dr:8). Here one Redis plays state and report: the places differ by their keys.
 */
public class DrLookupIT extends ItSupport {
	/** what the MQM job stored in WDQ */
	static final List<GmmsMessage> wdq = Collections.synchronizedList(new ArrayList<GmmsMessage>());
	static volatile boolean wdqDown = false;

	static GmmsMessage original(String id, long dateInAgoMs) {
		GmmsMessage o = AttemptBatchIT.original(id);
		o.setDateIn(new Date(System.currentTimeMillis() - dateInAgoMs));
		return o;
	}

	static String body(GmmsMessage o) {
		return SerializableHandler.convertGmmsMessage2RedisMessage(o);
	}

	/** a final vendor DR for this original through the inbound DR stream, handled by Core and acked */
	static boolean drHandled(String id) throws Exception {
		GmmsMessage dr = AttemptBatchIT.vendorDr("OUT-" + id, "8_" + id);
		dr.setInTimeMs(System.currentTimeMillis());
		StreamQueueManager.getInstance().produceInboundDeliveryReport(dr);
		List<GmmsMessage> l = EarlyDrIT.read();
		if (l.size() != 1) throw new IllegalStateException("not read: " + id + " " + l.size());
		boolean ok = AttemptBatchIT.process(l.get(0));
		EarlyDrIT.ack(l.get(0));
		return ok;
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
		msmFactory = () -> new com.king.message.gmms.MessageStoreManager() {
			@Override
			public GmmsMessage getGmmsMessageByOutMsgID(String outMsgID) {
				return null;
			}

			@Override
			public Set<String> existingOutMsgIDs(Collection<String> outMsgIDs) {
				return new HashSet<String>();
			}

			@Override
			public boolean insertMessageToDB(GmmsMessage message, String table) {
				if (wdqDown) return false;
				wdq.add(message);
				return true;
			}
		};
		boot();
		AttemptBatchIT.q = StreamQueueManager.getInstance();
		props.setProperty("CoreInboundDRShardEnable", "false");   // the checks read the stream by its name
		GmmsUtility u = GmmsUtility.getInstance();

		// ---- where it is written ----
		props.setProperty("DrLookupStore", "state");
		GmmsMessage s1 = original("DL1", 0);
		check(DrLookup.put(s1, "OUT-DL1", body(s1), 600) && jedis.exists("OUT-DL1") && jedis.hexists(u.getRedisDateIn(s1), "OUT-DL1")
				&& !jedis.exists("drl:OUT-DL1"), "DL-01 DrLookupStore=state: {outMsgId} and DR{seconds} on the state Redis, as before");
		props.setProperty("DrLookupStore", "report");
		GmmsMessage r2 = original("DL2", 5000);
		check(DrLookup.put(r2, "OUT-DL2", body(r2), 600) && jedis.exists("drl:OUT-DL2") && !jedis.exists("OUT-DL2"),
				"DL-02a DrLookupStore=report: drl:{outMsgId}, not {outMsgId}");
		Double score = jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL2");
		long ttl = jedis.ttl("drl:OUT-DL2");
		check(score != null && score.longValue() == r2.getDateIn().getTime() && ttl > 590 && ttl <= 600,
				"DL-02b in drl:due with its dateIn (" + score + "), TTL " + ttl + " s");

		// ---- Core's match and delete ----
		check(drHandled("DL2") && AttemptBatchIT.drsOut("DL2") == 1, "DL-03a report: the vendor DR is matched, the customer's DR written");
		check(!jedis.exists("drl:OUT-DL2") && jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL2") == null,
				"DL-03b report: after the final DR the lookup and its drl:due member are deleted");
		check(drHandled("DL1") && AttemptBatchIT.drsOut("DL1") == 1 && !jedis.exists("OUT-DL1") && !jedis.hexists(u.getRedisDateIn(s1), "OUT-DL1"),
				"DL-04 report, written before the switch (state): found by DrLookupLegacyRead, its key and DR{seconds} field deleted");
		props.setProperty("DrLookupStore", "state");
		GmmsMessage r5 = original("DL5", 0);
		props.setProperty("DrLookupStore", "report");
		DrLookup.put(r5, "OUT-DL5", body(r5), 600);
		props.setProperty("DrLookupStore", "state");
		check(drHandled("DL5") && AttemptBatchIT.drsOut("DL5") == 1 && !jedis.exists("drl:OUT-DL5") && jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL5") == null,
				"DL-05 back to state: one written on report is still found and deleted there (rollback)");

		// ---- DrLookupLegacyRead=false ----
		props.setProperty("DrLookupStore", "report");
		props.setProperty("DrLookupLegacyRead", "false");
		GmmsMessage s6 = original("DL6", 0);
		jedis.set("OUT-DL6", body(s6));
		check(drHandled("DL6") && AttemptBatchIT.drsOut("DL6") == 0 && EarlyDrIT.entry("DL6") != null,
				"DL-06 DrLookupLegacyRead=false: one in the old place is not looked for, the DR goes to the early DR queue");
		props.remove("DrLookupLegacyRead");
		jedis.del("OUT-DL6");
		for (String k : jedis.keys(EarlyDrQueue.PREFIX + "OUT-DL6:*")) { jedis.zrem(EarlyDrQueue.READY_ZSET, k); jedis.del(k); }

		// ---- the early DR offer finds its lookup ----
		GmmsMessage r7 = original("DL7", 0);
		DrLookup.put(r7, "OUT-DL7", body(r7), 600);
		GmmsMessage dr7 = AttemptBatchIT.vendorDr("OUT-DL7", "8_DL7");
		dr7.setInTimeMs(System.currentTimeMillis());
		check(EarlyDrQueue.offer(dr7) == EarlyDrQueue.Offer.FOUND && EarlyDrIT.entry("DL7") == null,
				"DL-07a offer of a DR whose lookup is there (written meanwhile): FOUND, not queued");
		GmmsMessage dr8 = AttemptBatchIT.vendorDr("OUT-DL8", "8_DL8");
		dr8.setInTimeMs(System.currentTimeMillis());
		check(EarlyDrQueue.offer(dr8) == EarlyDrQueue.Offer.QUEUED && EarlyDrIT.entry("DL8") != null,
				"DL-07b without its lookup: queued as before");
		for (String k : jedis.keys(EarlyDrQueue.PREFIX + "OUT-DL8:*")) { jedis.zrem(EarlyDrQueue.READY_ZSET, k); jedis.del(k); }
		DrLookup.delete("OUT-DL7", DrLookup.find("OUT-DL7"), r7);

		// ---- the MQM job: the old ones to WDQ ----
		long interval = 30 * 60 * 1000L;
		GmmsMessage old9 = original("DL9", interval + 60000L), young10 = original("DL10", 60000L);
		DrLookup.put(old9, "OUT-DL9", body(old9), 3600);
		DrLookup.put(young10, "OUT-DL10", body(young10), 3600);
		int moved = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		check(moved == 1 && wdq.size() == 1 && "OUT-DL9".equals(wdq.get(0).getOutMsgID()) && !jedis.exists("drl:OUT-DL9")
				&& jedis.zcard(DrLookup.PROCESSING_ZSET) == 0 && jedis.exists("drl:OUT-DL10") && jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL10") != null,
				"DL-08 MQM: the one older than the interval moved to WDQ and deleted, the young one stays");
		GmmsMessage old11 = original("DL11", interval + 60000L);
		DrLookup.put(old11, "OUT-DL11", body(old11), 3600);
		wdqDown = true;
		props.setProperty("DrLookup.ClaimTimeoutMs", "1");   // a claim of 1 ms: due again on the next round
		int m2 = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		check(m2 == 0 && jedis.exists("drl:OUT-DL11") && jedis.zscore(DrLookup.PROCESSING_ZSET, "drl:OUT-DL11") != null,
				"DL-09a WDQ down: kept, claimed");
		wdqDown = false;
		Thread.sleep(20);
		int m3 = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		props.remove("DrLookup.ClaimTimeoutMs");
		check(m3 == 1 && !jedis.exists("drl:OUT-DL11") && jedis.zcard(DrLookup.PROCESSING_ZSET) == 0,
				"DL-09b the claim ran out: moved on a later round");
		DrLookup.delete("OUT-DL10", DrLookup.find("OUT-DL10"), young10);
		check(jedis.zcard(DrLookup.DUE_ZSET) == 0, "DL-10 nothing left in drl:due");
		props.remove("DrLookupStore");
	}
}
