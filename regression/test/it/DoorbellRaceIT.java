package it;

import java.util.Random;

import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/**
 * Stale-doorbell race (not part of the repo): MTs produced one by one while the consumer keeps
 * draining the stream and cleaning its doorbell. None may be left unread.
 */
public class DoorbellRaceIT extends ItSupport {
	public static void main(String[] args) throws Exception {
		boot();
		StreamQueueManager q = StreamQueueManager.getInstance();
		MTStreamConsumer.getInstance().start();
		Random rnd = new Random(42);
		int n = Integer.getInteger("race.count", 300);
		String key = null;
		for (int i = 0; i < n; i++) {
			GmmsMessage m = mt("7777_RACE" + i, 7777, -1, 0); // unknown customer: fast CDR-only path
			m.setSenderAddress("100");
			m.setRecipientAddress("8613900000000");
			q.produceSubmitMessage(m);
			key = "stream:mt:pending:7777:" + q.calculateShardId(m);
			Thread.sleep(rnd.nextInt(15));
		}
		final String k = key;
		boolean drained = waitFor(() -> jedis.xlen(k) == 0, 15000);
		long left = jedis.xlen(k);
		boolean doorbell = jedis.zscore("zset:mt:pending:active", k) != null;
		// RACE-02: the interleaving itself, step by step
		String rk = "stream:mt:pending:7777:race2";
		String bell = "zset:it:race:active"; // not watched by any consumer
		jedis.zadd(bell, 1, rk);                                   // stale doorbell, stream empty
		long seenLen = q.streamLength(rk, true);                   // dispatcher: XLEN = 0
		java.util.Map<String, String> f = new java.util.HashMap<String, String>();
		f.put("body", "x");
		jedis.xadd(rk, redis.clients.jedis.StreamEntryID.NEW_ENTRY, f); // producer: XADD ...
		jedis.zadd(bell, 2, rk);                                   // ... and rings the doorbell
		java.lang.reflect.Method m = StreamQueueManager.class.getDeclaredMethod("removeDoorbellIfEmpty",
				String.class, String.class, boolean.class);
		m.setAccessible(true);
		boolean removed = (Boolean) m.invoke(q, bell, rk, true);  // dispatcher: remove the stale doorbell
		check(seenLen == 0 && !removed && jedis.zscore(bell, rk) != null,
				"RACE-02 entry produced between XLEN and removal keeps its doorbell (old code: ZREM removed it)");
		jedis.del(rk);
		check((Boolean) m.invoke(q, bell, rk, true) && jedis.zscore(bell, rk) == null,
				"RACE-03 doorbell of a really empty stream is removed");
		check(drained, "RACE-01 " + n + " MTs produced into a draining stream: all consumed (left " + left
				+ ", doorbell present=" + doorbell + ")");
		exit();
	}
}
