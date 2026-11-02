package it;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/** Core batch 5: sharded outbound result streams; not part of the repo. */
public class Batch5IT extends ItSupport {
	static StreamQueueManager q;

	static GmmsMessage submitResp(String msgId, int rssid) {
		GmmsMessage m = mt(msgId, 8888, rssid, 0);
		m.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		m.setStatus(GmmsStatus.SUCCESS);
		m.setOutMsgID("OUT-" + msgId);
		return m;
	}

	static GmmsMessage drResp(String msgId, int ossid) {
		GmmsMessage m = mt(msgId, ossid, 8, 0);
		m.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
		m.setStatus(GmmsStatus.DELIVERED);
		return m;
	}

	static long results() {
		return streamLen("stream:core:results*");
	}

	/** distinct threads that acked result entries of the given msgId prefix */
	static Set<String> ackThreads(String prefix) throws Exception {
		Set<String> threads = new HashSet<String>();
		for (String l : Files.readAllLines(new File(home + "log/it.log").toPath())) {
			if (l.contains("STREAM-GET") && l.contains("flow=result->core") && l.contains(" " + prefix)) {
				threads.add(l.substring(l.indexOf('[') + 1, l.indexOf(']')));
			}
		}
		return threads;
	}

	public static void main(String[] args) throws Exception {
		boot();
		q = StreamQueueManager.getInstance();

		// ---- R5-01 key format ----
		String k1 = q.resultStreamKey(submitResp("8888_K1", 8));
		check(k1.matches("stream:core:results:8:[01]"), "R5-01a submit_resp -> stream:core:results:{rssid}:{shard} (" + k1 + ")");
		String k2 = q.resultStreamKey(drResp("8888_K2", 8888));
		check(k2.matches("stream:core:results:8888:[01]"), "R5-01b DR_resp -> stream:core:results:{ossid}:{shard} (" + k2 + ")");
		GmmsMessage noR = submitResp("8888_K3", -1);
		check(q.resultStreamKey(noR).startsWith("stream:core:results:8888:"), "R5-01c rssid missing -> ossid used");
		GmmsMessage p1 = submitResp("8888_SPLIT", 8);
		p1.setSplitStatus(1);
		GmmsMessage p3 = submitResp("8888_SPLIT", 8);
		p3.setSplitStatus(3);
		check(q.resultStreamKey(p1).equals(q.resultStreamKey(p3)), "R5-01d parts of a split message share one stream");
		Set<String> shards = new HashSet<String>();
		for (int i = 0; i < 50; i++) shards.add(q.resultStreamKey(submitResp("8888_S" + i, 8)));
		check(shards.size() == 2, "R5-01e results of one channel spread over TotalShards=2 streams " + shards);
		GmmsMessage noId = submitResp(null, 8);
		noId.setInMsgID("IN-X");
		check(q.resultStreamKey(noId).startsWith("stream:core:results:8:"), "R5-01f no msgId -> inMsgID used for the shard");

		// ---- R5-02 switch ----
		props.setProperty("CoreResultStreamSharded", "false");
		check("stream:core:results".equals(q.resultStreamKey(submitResp("8888_K4", 8))), "R5-02 CoreResultStreamSharded=false -> legacy stream:core:results");
		props.remove("CoreResultStreamSharded");

		// ---- R5-06 consumer start re-rings existing sharded streams ----
		for (int i = 0; i < 5; i++) q.produceResult(submitResp("8888_RR" + i, 8));
		jedis.del("zset:core:results:active");   // doorbells lost (e.g. Redis restart)
		check(results() == 5, "R5-06a 5 results in sharded streams, doorbells removed");
		ResultStreamConsumer.getInstance().start();
		check(waitFor(() -> results() == 0, 8000), "R5-06b consumer start finds them by SCAN and processes them");
		check(waitFor(() -> cdrCount("8888_RR4", "OUT_SUBMIT") == 1, 5000), "R5-06c and processes them normally (OUT_SUBMIT CDR)");

		// ---- R5-03 legacy and sharded consumed side by side ----
		props.setProperty("CoreResultStreamSharded", "false");
		for (int i = 0; i < 3; i++) q.produceResult(submitResp("8888_LEG" + i, 8));
		props.remove("CoreResultStreamSharded");
		for (int i = 0; i < 3; i++) q.produceResult(submitResp("8888_NEW" + i, 9));
		q.produceResult(drResp("7001_DRR", 7001));
		check(waitFor(() -> results() == 0, 8000), "R5-03a legacy, sharded submit and DR results all consumed");
		check(waitFor(() -> cdrCount("8888_LEG2", "OUT_SUBMIT") == 1 && cdrCount("8888_NEW2", "OUT_SUBMIT") == 1, 5000),
				"R5-03b both kinds processed (OUT_SUBMIT CDR each)");
		check(logLine("8888_NEW0", "stream=stream:core:results:9:") != null && logLine("8888_NEW0", "flow=result->core") != null,
				"R5-09 sharded stream traced as flow=result->core");

		// ---- R5-04 parallelism: legacy vs sharded, 1200 results each ----
		props.setProperty("CoreResultStreamSharded", "false");
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < 1200; i++) q.produceResult(submitResp("8888_PL" + i, i % 2 == 0 ? 8 : 9));
		waitFor(() -> results() == 0, 60000);
		long legacyMs = System.currentTimeMillis() - t0;
		props.remove("CoreResultStreamSharded");
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < 600; i++) q.produceResult(submitResp("8888_PS" + i, i % 2 == 0 ? 8 : 9));
		for (int i = 0; i < 600; i++) q.produceResult(drResp("8888_PD" + i, i % 2 == 0 ? 8888 : 7001));
		waitFor(() -> results() == 0, 60000);
		long shardedMs = System.currentTimeMillis() - t1;
		Set<String> legacyThreads = ackThreads("8888_PL");
		Set<String> shardedThreads = ackThreads("8888_PS");
		shardedThreads.addAll(ackThreads("8888_PD"));
		shardedThreads.addAll(ackThreads("7001_PD"));
		System.out.println("     legacy: " + legacyMs + " ms, threads " + legacyThreads.size()
				+ "; sharded: " + shardedMs + " ms, threads " + shardedThreads.size());
		check(shardedThreads.size() >= 3 && shardedThreads.size() > legacyThreads.size(),
				"R5-04a sharded results processed by " + shardedThreads.size() + " threads (legacy " + legacyThreads.size() + ")");
		check(shardedMs < legacyMs, "R5-04b 1200 sharded results in " + shardedMs + " ms < legacy " + legacyMs + " ms");

		// ---- R5-05 fairness: a channel with a backlog does not hold back another ssid ----
		for (int i = 0; i < 3000; i++) q.produceResult(submitResp("8888_BIG" + i, 8));
		q.produceResult(drResp("7001_SMALL", 7001));
		final String smallKey = q.resultStreamKey(drResp("7001_SMALL", 7001));
		long t2 = System.currentTimeMillis();
		check(waitFor(() -> !jedis.exists(smallKey) || jedis.xlen(smallKey) == 0, 5000), "R5-05a small ssid result processed");
		long smallMs = System.currentTimeMillis() - t2;
		long bigLeft = streamLen("stream:core:results:8:*");
		check(bigLeft > 0, "R5-05b after " + smallMs + " ms while channel 8 still had " + bigLeft + " results queued");
		check(waitFor(() -> results() == 0, 60000), "R5-05c backlog drained");

		// ---- R5-07 auto-claim covers sharded streams ----
		props.setProperty("RedisStreamAutoClaimEnable", "true");
		props.setProperty("RedisStreamAutoClaimIdleMs", "300");
		props.setProperty("RedisStreamAutoClaimIntervalMs", "700");
		ResultStreamConsumer.getInstance().stop();
		GmmsMessage lost = submitResp("8888_LOST", 9);
		q.produceResult(lost);
		String lk = q.resultStreamKey(lost);
		q.createGroup(lk, "CoreResultGroup", false);
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(lk, StreamEntryID.UNRECEIVED_ENTRY);
		q.consumeBatch("CoreResultGroup", "dead-core", 10, query, false);   // read by a Core that died
		jedis.del("zset:core:results:active");
		ResultStreamConsumer.getInstance().start();
		check(waitFor(() -> cdrCount("8888_LOST", "OUT_SUBMIT") == 1 && jedis.xlen(lk) == 0, 10000),
				"R5-07 result left pending by a dead Core on a sharded stream: recovered by auto-claim");

		exit();
	}
}
