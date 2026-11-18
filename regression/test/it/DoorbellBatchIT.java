package it;

import java.util.*;

import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/**
 * Doorbells popped by batch (CoreMTDispatchBatch, docs/5000TPS多主测试慢的环节分析.md 10): at most max streams, in the
 * doorbell's order (the longest waiting first); a stream with no entries only loses its doorbell; the next call gets
 * the rest; a popped stream written again rings again.
 */
public class DoorbellBatchIT extends ItSupport {
	static final String ZSET = StreamQueueManager.ZSET_MT_PENDING_ACTIVE;

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
		boot();
		StreamQueueManager q = StreamQueueManager.getInstance();
		jedis.del(ZSET);
		// 6 streams with an MT each, rung in this order, and one rung but empty
		List<String> streams = new ArrayList<>();
		for (int i = 0; i < 6; i++) {
			String key = "stream:mt:pending:" + (8800 + i) + ":0";
			jedis.xadd(key, redis.clients.jedis.StreamEntryID.NEW_ENTRY, Collections.singletonMap("body", "x"));
			jedis.zadd(ZSET, 1000 + i, key);
			streams.add(key);
		}
		String empty = "stream:mt:pending:8899:0";
		jedis.zadd(ZSET, 999, empty);   // rung first, its entries already read and deleted

		List<String> first = q.popResponsibleDoorbells(ZSET, true, 4);
		check(first.equals(streams.subList(0, 4)), "DB-01 four popped, in the doorbell's order: " + first);
		check(jedis.zscore(ZSET, empty) == null, "DB-02 the empty stream only lost its doorbell");
		List<String> rest = q.popResponsibleDoorbells(ZSET, true, 4);
		check(rest.equals(streams.subList(4, 6)) && jedis.zcard(ZSET) == 0, "DB-03 the next call gets the other two, the doorbell is empty: " + rest);

		GmmsMessage m = mt("8800_DB04", 8800, 8, 0);
		q.produceSubmitMessage(m);
		Set<String> rung = new HashSet<>(jedis.zrange(ZSET, 0, -1));
		check(rung.size() == 1 && rung.iterator().next().startsWith("stream:mt:pending:8800:"), "DB-04 written again: rings again " + rung);
		for (String k : streams) jedis.del(k);
	}
}
