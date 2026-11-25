package it;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;

import redis.clients.jedis.StreamEntryID;

/**
 * The backlog monitor's doorbell_missing (StreamQueueManager.scanBacklogPattern): a stream with entries and no doorbell
 * is mostly one being processed (its consumer took the doorbell); it counts as missing only when it had none at the
 * check before either and its first entry did not change (nobody read it). Before 2026-11-26 every stream being
 * processed at a check counted (10-20 a minute at 1000 TPS, PF009).
 */
public class BacklogDoorbellIT extends ItSupport {
	static StreamQueueManager q;
	static Method scan;
	static String gauge;

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		}
		exit();
	}

	static long check() throws Exception {
		scan.invoke(q, StreamQueueManager.STR_MT_PENDING_PREFIX + "*", "CoreMTGroup", StreamQueueManager.ZSET_MT_PENDING_ACTIVE, true, "server_to_core");
		return MetricsCollector.getInstance().getGauge(gauge);
	}

	static void run() throws Exception {
		boot();
		q = StreamQueueManager.getInstance();
		props.setProperty("RedisStreamMonitor.FullScanEveryCycles", "1");   // every check scans for streams without a doorbell
		scan = StreamQueueManager.class.getDeclaredMethod("scanBacklogPattern", String.class, String.class, String.class, boolean.class, String.class);
		scan.setAccessible(true);
		gauge = MetricsNames.gauge("redis.stream.server_to_core", "doorbell_missing");
		int shard = GmmsUtility.getInstance().getMyShards().iterator().next();
		final String s = StreamQueueManager.STR_MT_PENDING_PREFIX + "9599:" + shard;
		RedisConnection mq = RedisClient.getInstance().mq(s, true);
		final Map<String, String> body = new HashMap<String, String>();
		body.put("m", "1");
		mq.runPipeline(pl -> {
			pl.del(s);
			pl.zrem(StreamQueueManager.ZSET_MT_PENDING_ACTIVE, s);
			pl.xadd(s, StreamEntryID.NEW_ENTRY, body);
			pl.sadd(StreamQueueManager.STREAM_INDEX_KEY, s);   // as the producer's Lua: the monitor finds streams by the index
		});

		check(check() == 0, "BD-01 an entry without a doorbell, first seen: not missing (being processed)");
		check(check() == 1, "BD-02 the same first entry, still no doorbell at the next check: missing");
		check(check() == 1, "BD-03 and the check after");

		// a consumer read the first entry and acked it, another came: progress, not missing
		mq.runPipeline(pl -> {
			pl.del(s);
			pl.xadd(s, StreamEntryID.NEW_ENTRY, body);
		});
		check(check() == 0, "BD-04 the first entry changed (read meanwhile): not missing");
		check(check() == 1, "BD-05 then unchanged again: missing");

		mq.runPipeline(pl -> pl.zadd(StreamQueueManager.ZSET_MT_PENDING_ACTIVE, System.currentTimeMillis(), s));
		check(check() == 0, "BD-06 its doorbell rung: not missing");
		mq.runPipeline(pl -> pl.zrem(StreamQueueManager.ZSET_MT_PENDING_ACTIVE, s));
		check(check() == 0, "BD-07 the doorbell taken again: first sighting, not missing");

		mq.runPipeline(pl -> {
			pl.del(s);
			pl.srem(StreamQueueManager.STREAM_INDEX_KEY, s);
		});
		check(check() == 0, "BD-08 emptied: not missing");
		check(check() == 0, "BD-09 and gone from the watched streams");
	}
}
