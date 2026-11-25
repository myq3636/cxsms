package it;

import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.DrLookup;
import com.king.gmms.messagequeue.EarlyDrQueue;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;

/**
 * The DR lookup on the report masters (docs/DR匹配数据迁移设计.md): where it is written, Core's match and delete, the
 * early DR offer that finds its lookup, and the MQM job moving the old ones to WDQ. DRs and originals as EarlyDrIT (channel 8, customer
 * 8888, stream:core:inbound-dr:8). Here one Redis plays every role.
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
		props.setProperty("Redis_RetrieveRedisInterval", "30");

		// ---- where it is written ----
		GmmsMessage r2 = original("DL2", 5000);
		check(DrLookup.put(r2, "OUT-DL2", body(r2), 600) && jedis.exists("drl:OUT-DL2") && !jedis.exists("OUT-DL2"),
				"DL-01a drl:{outMsgId} (not {outMsgId} of the state Redis)");
		Double score = jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL2");
		long ttl = jedis.ttl("drl:OUT-DL2");
		long due2 = r2.getDateIn().getTime() + 30 * 60000L;   // its expiry (1 h) is later than Redis_RetrieveRedisInterval
		check(score != null && score.longValue() == due2 && Math.abs(ttl - ((due2 - System.currentTimeMillis()) / 1000 + 600)) <= 5,
				"DL-01b in drl:due:at with dateIn + Redis_RetrieveRedisInterval (" + score + "), TTL 10 min after that (" + ttl + " s, not the 600 asked)");
		check(DrLookup.ttl("OUT-DL2") > 0 && DrLookup.ttl("OUT-NONE") == -2L, "DL-01c DrLookup.ttl (used by the fake DR timeout)");

		// ---- Core's match and delete ----
		check(drHandled("DL2") && AttemptBatchIT.drsOut("DL2") == 1, "DL-02a the vendor DR is matched, the customer's DR written");
		check(!jedis.exists("drl:OUT-DL2") && jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL2") == null,
				"DL-02b after the final DR the lookup and its drl:due member are deleted");

		// ---- the early DR offer finds its lookup ----
		GmmsMessage r7 = original("DL7", 0);
		DrLookup.put(r7, "OUT-DL7", body(r7), 600);
		GmmsMessage dr7 = AttemptBatchIT.vendorDr("OUT-DL7", "8_DL7");
		dr7.setInTimeMs(System.currentTimeMillis());
		check(EarlyDrQueue.offer(dr7) == EarlyDrQueue.Offer.FOUND && EarlyDrIT.entry("DL7") == null,
				"DL-03a offer of a DR whose lookup is there (written meanwhile): FOUND, not queued");
		GmmsMessage dr8 = AttemptBatchIT.vendorDr("OUT-DL8", "8_DL8");
		dr8.setInTimeMs(System.currentTimeMillis());
		check(EarlyDrQueue.offer(dr8) == EarlyDrQueue.Offer.QUEUED && EarlyDrIT.entry("DL8") != null,
				"DL-03b without its lookup: queued as before");
		for (String k : jedis.keys(EarlyDrQueue.PREFIX + "OUT-DL8:*")) { jedis.zrem(EarlyDrQueue.READY_ZSET, k); jedis.del(k); }
		DrLookup.delete("OUT-DL7");

		// ---- the MQM job: the old ones to WDQ ----
		long interval = 30 * 60 * 1000L;
		GmmsMessage old9 = original("DL9", interval + 60000L), young10 = original("DL10", 60000L);
		DrLookup.put(old9, "OUT-DL9", body(old9), 3600);
		DrLookup.put(young10, "OUT-DL10", body(young10), 3600);
		int moved = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		check(moved == 1 && wdq.size() == 1 && "OUT-DL9".equals(wdq.get(0).getOutMsgID()) && !jedis.exists("drl:OUT-DL9")
				&& jedis.zcard(DrLookup.PROCESSING_ZSET) == 0 && jedis.exists("drl:OUT-DL10") && jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL10") != null,
				"DL-04 MQM: the one older than the interval moved to WDQ and deleted, the young one stays");
		GmmsMessage old11 = original("DL11", interval + 60000L);
		DrLookup.put(old11, "OUT-DL11", body(old11), 3600);
		wdqDown = true;
		props.setProperty("DrLookup.ClaimTimeoutMs", "1");   // a claim of 1 ms: due again on the next round
		int m2 = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		check(m2 == 0 && jedis.exists("drl:OUT-DL11") && jedis.zscore(DrLookup.PROCESSING_ZSET, "drl:OUT-DL11") != null,
				"DL-05a WDQ down: kept, claimed");
		wdqDown = false;
		Thread.sleep(20);
		int m3 = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		props.remove("DrLookup.ClaimTimeoutMs");
		check(m3 == 1 && !jedis.exists("drl:OUT-DL11") && jedis.zcard(DrLookup.PROCESSING_ZSET) == 0,
				"DL-05b the claim ran out: moved on a later round");
		DrLookup.delete("OUT-DL10");
		check(jedis.zcard(DrLookup.DUE_ZSET) == 0, "DL-06 nothing left in drl:due:at");

		// ---- an expiry before Redis_RetrieveRedisInterval: to WDQ at its expiry (the expired DR in time) ----
		long gmt = u.getGMTTime().getTime();
		GmmsMessage s12 = original("DL12", 0);
		s12.setExpiryDate(new Date(gmt + 15 * 60000L));   // GMT wall clock, as the gateway keeps it: 15 min left
		DrLookup.put(s12, "OUT-DL12", body(s12), 3600);
		Double sc12 = jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL12");
		long left12 = sc12 == null ? -1 : (sc12.longValue() - System.currentTimeMillis()) / 60000;
		check(left12 >= 14 && left12 <= 15 && jedis.ttl("drl:OUT-DL12") >= 3590,
				"DL-07 15 min left: due at its expiry (" + left12 + " min, not 30), TTL kept (" + jedis.ttl("drl:OUT-DL12") + " s)");
		GmmsMessage s13 = original("DL13", 60000L);
		s13.setExpiryDate(new Date(gmt - 1000L));   // expired: due now
		DrLookup.put(s13, "OUT-DL13", body(s13), 3600);
		wdq.clear();
		int m4 = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		check(m4 == 1 && wdq.size() == 1 && "OUT-DL13".equals(wdq.get(0).getOutMsgID()) && jedis.exists("drl:OUT-DL12"),
				"DL-08 MQM: the expired one moved to WDQ 1 min after its dateIn (not 30), the one with 15 min left stays");
		GmmsMessage l14 = original("DL14", 0);
		l14.setExpiryDate(new Date(gmt + 15 * 60000L));   // 15 min left, Redis_RetrieveRedisInterval 1000 as on the test server
		props.setProperty("Redis_RetrieveRedisInterval", "1000");
		DrLookup.put(l14, "OUT-DL14", body(l14), 3600);
		Double sc14 = jedis.zscore(DrLookup.DUE_ZSET, "drl:OUT-DL14");
		long ttl14 = jedis.ttl("drl:OUT-DL14");
		check(sc14 != null && Math.abs(sc14.longValue() - System.currentTimeMillis() - 15 * 60000L) < 70000 && ttl14 >= 3590,
				"DL-09 Redis_RetrieveRedisInterval 1000 min, 15 min left: due at its expiry, the lookup lives past it (TTL " + ttl14 + " s): it expired in Redis unmoved before");
		props.setProperty("Redis_RetrieveRedisInterval", "30");
		DrLookup.delete("OUT-DL12");
		DrLookup.delete("OUT-DL14");

		// ---- lookups written before (drl:due, score dateIn): moved as before, deleted from it too ----
		GmmsMessage o15 = original("DL15", interval + 60000L), y16 = original("DL16", 60000L);
		jedis.setex("drl:OUT-DL15", 3600, body(o15));
		jedis.zadd(DrLookup.LEGACY_DUE_ZSET, o15.getDateIn().getTime(), "drl:OUT-DL15");
		jedis.setex("drl:OUT-DL16", 3600, body(y16));
		jedis.zadd(DrLookup.LEGACY_DUE_ZSET, y16.getDateIn().getTime(), "drl:OUT-DL16");
		wdq.clear();
		int m5 = DrLookup.moveDue(interval, m -> u.getMessageStoreManager().insertMessageToDB(m, "WDQ"));
		check(m5 == 1 && wdq.size() == 1 && "OUT-DL15".equals(wdq.get(0).getOutMsgID()) && jedis.zscore(DrLookup.LEGACY_DUE_ZSET, "drl:OUT-DL16") != null,
				"DL-10a drl:due (before the upgrade): older than the interval moved, the young one stays (no burst at the upgrade)");
		DrLookup.delete("OUT-DL16");
		check(!jedis.exists("drl:OUT-DL16") && jedis.zcard(DrLookup.LEGACY_DUE_ZSET) == 0 && jedis.zcard(DrLookup.PROCESSING_ZSET) == 0,
				"DL-10b its final DR deletes it from drl:due too");
	}
}
