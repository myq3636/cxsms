package it;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.king.framework.lifecycle.event.ShutdownEvent;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.ha.ShardAssignmentChecker;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.CDRAsynWriter;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/** Core batch 4: dead letter, SCAN, consumer fairness, shard check, CDR drain; not part of the repo. */
public class Batch4IT extends ItSupport {
	static StreamQueueManager q;

	public static void main(String[] args) throws Exception {
		boot();
		q = StreamQueueManager.getInstance();
		deadLetter();
		deadLetterRealConsumer();
		scan();
		fairness();
		shardCheck();
		cdrDrain();
		exit();
	}

	static List<StreamEntry> dlq(String key) {
		if (!jedis.exists(key)) return new ArrayList<StreamEntry>();
		return jedis.xrange(key, (StreamEntryID) null, (StreamEntryID) null);
	}

	// ---- A. max deliveries -> dead letter ----
	static void deadLetter() throws Exception {
		props.setProperty("RedisStreamMaxDeliveries", "3");
		GmmsMessage m = mt("7777_DLQ1", 7777, -1, 0);
		q.produceSubmitMessage(m);
		String key = "stream:mt:pending:7777:" + q.calculateShardId(m);
		q.createGroup(key, "CoreMTGroup", true);
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(key, StreamEntryID.UNRECEIVED_ENTRY);
		q.consumeBatch("CoreMTGroup", "dead-1", 10, query, true);               // delivery 1, never acked
		List<GmmsMessage> r2 = q.recoverPending(key, "CoreMTGroup", "c2", 0, 10, true); // delivery 2
		List<GmmsMessage> r3 = q.recoverPending(key, "CoreMTGroup", "c2", 0, 10, true); // delivery 3
		check(r2.size() == 1 && r3.size() == 1 && "7777_DLQ1".equals(r3.get(0).getMsgID()),
				"DLQ-01 below the limit the pending MT is claimed again (deliveries 2 and 3)");
		List<GmmsMessage> r4 = q.recoverPending(key, "CoreMTGroup", "c2", 0, 10, true);
		List<StreamEntry> d = dlq("stream:dlq:submit");
		check(r4.isEmpty() && d.size() == 1, "DLQ-02 at 3 deliveries: not claimed, moved to stream:dlq:submit");
		Map<String, String> f = d.isEmpty() ? new HashMap<String, String>() : d.get(0).getFields();
		check("max_deliveries".equals(f.get("reason")) && key.equals(f.get("sourceStream"))
				&& "3".equals(f.get("deliveries")) && "CoreMTGroup".equals(f.get("sourceGroup")) && f.get("body") != null
				&& "7777_DLQ1".equals(com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(f.get("body")).getMsgID()),
				"DLQ-03 dead letter keeps reason, source stream/group, deliveries and the original body " + f.keySet());
		check(jedis.xlen(key) == 0 && jedis.xpending(key, "CoreMTGroup").getTotal() == 0,
				"DLQ-04 removed from the source stream and its PEL");
		check(waitFor(() -> logLine("", "[STREAM-DLQ] moved to stream:dlq:submit after 3 deliveries. msgId=7777_DLQ1") != null, 3000),
				"DLQ-05 ERROR log [STREAM-DLQ] with msgId");

		props.setProperty("RedisStreamMaxDeliveries", "0");
		GmmsMessage u = mt("7777_DLQ2", 7777, -1, 0);
		u.setRecipientAddress("8613811112222");
		q.produceSubmitMessage(u);
		String key2 = "stream:mt:pending:7777:" + q.calculateShardId(u);
		q.createGroup(key2, "CoreMTGroup", true);
		Map<String, StreamEntryID> q2 = new HashMap<String, StreamEntryID>();
		q2.put(key2, StreamEntryID.UNRECEIVED_ENTRY);
		q.consumeBatch("CoreMTGroup", "dead-1", 10, q2, true);
		int claimed = 0;
		for (int i = 0; i < 6; i++) {
			for (GmmsMessage g : q.recoverPending(key2, "CoreMTGroup", "c2", 0, 10, true)) {
				if ("7777_DLQ2".equals(g.getMsgID())) claimed++;
			}
		}
		check(claimed == 6 && dlq("stream:dlq:submit").size() == 1, "DLQ-06 RedisStreamMaxDeliveries=0: no limit (old behaviour)");
		jedis.del(key2);
		props.setProperty("RedisStreamMaxDeliveries", "3");
	}

	// ---- A. a DR that always fails, through the real consumer and auto-claim runner ----
	static void deadLetterRealConsumer() throws Exception {
		GmmsMessage orig = mt("8888_DLQ3", 8888, 8, 0);
		orig.setOutMsgID("OUT-DLQ3");
		jedis.set("OUT-DLQ3", com.king.redis.SerializableHandler.convertGmmsMessage2RedisMessage(orig));
		Field msmField = InternalCoreEngineSession.class.getDeclaredField("msgStoreManager");
		msmField.setAccessible(true);
		Object real = msmField.get(InternalCoreEngineSession.getReactiveInstance());
		msmField.set(InternalCoreEngineSession.getReactiveInstance(), null); // every attempt fails
		props.setProperty("RedisStreamAutoClaimEnable", "true");
		props.setProperty("RedisStreamAutoClaimIdleMs", "300");
		props.setProperty("RedisStreamAutoClaimIntervalMs", "700");
		GmmsMessage dr = new GmmsMessage();
		dr.setMsgID("8_DLQ3");
		dr.setOutMsgID("OUT-DLQ3");
		dr.setRSsID(8);
		dr.setOSsID(-1);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setStatus(GmmsStatus.DELIVERED);
		q.produceInboundDeliveryReport(dr);
		InboundDRStreamConsumer.getInstance().start();
		boolean moved = waitFor(() -> dlq("stream:dlq:report").size() == 1, 15000);
		check(moved && jedis.xlen("stream:core:inbound-dr:8") == 0,
				"DLQ-07 inbound DR failing every time: after 3 deliveries moved to stream:dlq:report by the auto-claim runner");
		check(streamLen("stream:dr:pending:*") == 0, "DLQ-08 no DR produced for it (no failure DR)");
		msmField.set(InternalCoreEngineSession.getReactiveInstance(), real);
		props.setProperty("RedisStreamAutoClaimEnable", "false");
	}

	// ---- B. SCAN instead of KEYS ----
	static void scan() throws Exception {
		Pipeline p = jedis.pipelined();
		Map<String, String> body = new HashMap<String, String>();
		body.put("body", "x");
		for (int i = 0; i < 20000; i++) {
			p.xadd("stream:mt:pending:" + (100000 + i) + ":0", StreamEntryID.NEW_ENTRY, body);
		}
		p.sync();
		jedis.configResetStat();
		List<String> found = q.listResponsibleStreams("stream:mt:pending:1*", true);
		String stats = jedis.info("commandstats");
		check(found.size() >= 20000, "SCAN-01 listResponsibleStreams finds all 20000 streams (" + found.size() + ")");
		check(!stats.contains("cmdstat_keys:") && stats.contains("cmdstat_scan:"),
				"SCAN-02 listed with SCAN, no KEYS command issued");
		Pipeline d = jedis.pipelined();
		for (int i = 0; i < 20000; i++) {
			d.del("stream:mt:pending:" + (100000 + i) + ":0");
		}
		d.sync();
	}

	// ---- C. fairness: a stream with a backlog does not keep the only worker ----
	static void fairness() throws Exception {
		props.setProperty("RedisStreamConsumer.CoreMT.WorkerCorePoolSize", "1");
		props.setProperty("RedisStreamConsumer.CoreMT.WorkerMaxPoolSize", "1");
		props.setProperty("RedisStreamConsumer.CoreMT.WorkerQueueSize", "20");
		props.setProperty("RedisStreamConsumer.CoreMT.BatchSize", "50");
		props.setProperty("CoreStreamTaskMaxBatches", "2");
		String big = null;
		for (int i = 0; i < 1500; i++) {
			GmmsMessage m = mt("7781_BIG" + i, 7781, -1, 0);
			q.produceSubmitMessage(m);
			big = "stream:mt:pending:7781:" + q.calculateShardId(m);
		}
		GmmsMessage small = mt("7782_SMALL", 7782, -1, 0);
		q.produceSubmitMessage(small);
		final String bigKey = big;
		final String smallKey = "stream:mt:pending:7782:" + q.calculateShardId(small);
		long t0 = System.currentTimeMillis();
		MTStreamConsumer.getInstance().start();
		check(waitFor(() -> jedis.xlen(smallKey) == 0, 20000), "FAIR-01 small stream processed");
		long smallAt = System.currentTimeMillis() - t0;
		long bigLeft = jedis.xlen(bigKey);
		check(bigLeft > 300, "FAIR-02 small stream served after " + smallAt + " ms while the big one still had "
				+ bigLeft + " of 1500 (one worker shared, task gives the stream back after 2 batches)");
		check(waitFor(() -> jedis.xlen(bigKey) == 0, 60000), "FAIR-03 big stream drained afterwards (re-rung after each budget)");

		// pool of 1 worker + queue 20, 60 streams at once: rejected submissions are handed back, all processed
		List<String> keys = new ArrayList<String>();
		for (int i = 0; i < 60; i++) {
			GmmsMessage m = mt("78" + (10 + i) + "_REJ", 7810 + i, -1, 0);
			q.produceSubmitMessage(m);
			keys.add("stream:mt:pending:" + (7810 + i) + ":" + q.calculateShardId(m));
		}
		final List<String> ks = keys;
		check(waitFor(() -> {
			for (String k : ks) if (jedis.xlen(k) > 0) return false;
			return true;
		}, 30000), "FAIR-04 pool saturated (1 worker, queue 20, 60 streams): every stream processed");
		check(logLine("", "MT Dispatcher loop error") == null, "FAIR-05 dispatcher never failed while the pool was full");

		// guaranteed saturation: 30 streams x 100 MTs, 1 worker, queue 20 -> submissions are rejected;
		// no MT may be processed on the dispatcher thread (old CallerRunsPolicy did exactly that)
		MTStreamConsumer.getInstance().stop();   // queue all 30 backlogs first, then start: guaranteed saturation
		Thread.sleep(500);
		List<String> sat = new ArrayList<String>();
		for (int s = 0; s < 30; s++) {
			for (int i = 0; i < 100; i++) {
				GmmsMessage m = mt("79" + (10 + s) + "_SAT" + i, 7910 + s, -1, 0);
				q.produceSubmitMessage(m);
				if (i == 0) sat.add("stream:mt:pending:" + (7910 + s) + ":" + q.calculateShardId(m));
			}
		}
		MTStreamConsumer.getInstance().start();
		Thread.sleep(300);
		String dispatcher = null;
		for (Map.Entry<Thread, StackTraceElement[]> t : Thread.getAllStackTraces().entrySet()) {
			for (StackTraceElement e : t.getValue()) {
				if (e.getClassName().endsWith("MTStreamConsumer") && "dispatcherLoop".equals(e.getMethodName())) {
					dispatcher = t.getKey().getName();
				}
			}
		}
		final List<String> satKeys = sat;
		check(waitFor(() -> {
			for (String k : satKeys) if (jedis.xlen(k) > 0) return false;
			return true;
		}, 90000), "FAIR-06a saturated pool: 3000 MTs in 30 streams all processed");
		int onDispatcher = 0;
		for (String l : java.nio.file.Files.readAllLines(new java.io.File(home + "log/it.log").toPath())) {
			// one "customer not found" line per processed MT (unknown customers 7910..7939)
			if (l.contains("[" + dispatcher + "]") && l.contains("customer not found for OSsID=79")) onDispatcher++;
		}
		check(dispatcher != null && onDispatcher == 0,
				"FAIR-06b no MT processed on the dispatcher thread " + dispatcher + " (" + onDispatcher + ")");
	}

	// ---- D. shard assignment check ----
	static void shardCheck() throws Exception {
		jedis.setex("system:shards:core:CoreEngine1:1", 60, "16|0,1,2,3,4,5,6,7");
		jedis.setex("system:shards:core:CoreEngine2:2", 60, "16|6,7,8,9,10,11,12,13,14,15");
		jedis.setex("system:shards:client:Client1:1", 60, "16|0,1,2,3,4,5,6,7");
		jedis.setex("system:shards:server:MultiSmppServer1:1", 60, "1|0");
		List<String> p = ShardAssignmentChecker.check(GmmsUtility.getInstance().getRedisClient());
		String all = p.toString();
		check(all.contains("ERROR TotalShards differs") && all.contains("server:MultiSmppServer1:1=1"),
				"SHARD-01 server with TotalShards 1 vs 16 elsewhere -> ERROR");
		check(all.contains("WARN core shards owned by more than one node: [6=") && all.contains("7=["),
				"SHARD-02 core shards 6,7 owned twice -> WARN");
		check(all.contains("ERROR client shards owned by no node (never consumed): [8, 9, 10, 11, 12, 13, 14, 15]"),
				"SHARD-03 client shards 8..15 owned by nobody -> ERROR");
		check(logLine("", "[SHARD-CHECK]") != null, "SHARD-04 problems logged with [SHARD-CHECK]");
		jedis.setex("system:shards:core:CoreEngine2:2", 60, "16|8,9,10,11,12,13,14,15");
		jedis.setex("system:shards:client:Client2:2", 60, "16|8,9,10,11,12,13,14,15");
		jedis.setex("system:shards:server:MultiSmppServer1:1", 60, "16|0");
		jedis.del("system:shards:core:CoreEngine1:1");
		jedis.setex("system:shards:core:CoreEngine1:1", 60, "16|0,1,2,3,4,5,6,7");
		List<String> ok = ShardAssignmentChecker.check(GmmsUtility.getInstance().getRedisClient());
		check(ok.isEmpty(), "SHARD-05 consistent configuration -> no problem " + ok);
		jedis.del("system:shards:core:CoreEngine1:1", "system:shards:core:CoreEngine2:2", "system:shards:client:Client1:1",
				"system:shards:client:Client2:2", "system:shards:server:MultiSmppServer1:1");
		ShardAssignmentChecker.publish(GmmsUtility.getInstance(), "core", "CoreEngineIT", "9", 30);
		String own = jedis.get("system:shards:core:CoreEngineIT:9");
		check("2|0,1".equals(own) && jedis.ttl("system:shards:core:CoreEngineIT:9") > 0,
				"SHARD-06 a node publishes TotalShards|its shards with a TTL (" + own + ")");
	}

	// ---- E. CDRs still queued at shutdown are written ----
	static void cdrDrain() throws Exception {
		CDRAsynWriter w = CDRAsynWriter.getInstance();
		Field wf = CDRAsynWriter.class.getDeclaredField("cdrWriter");
		wf.setAccessible(true);
		Object writerThread = wf.get(w);
		Method stop = writerThread.getClass().getDeclaredMethod("stop");
		stop.setAccessible(true);
		stop.invoke(writerThread);   // background writer stops: CDRs stay queued
		Thread.sleep(800);
		for (int i = 0; i < 50; i++) {
			w.recoreCDR("0,IN_SUBMIT,DRAINTEST,\"DRAIN_" + i + "\"\n");
		}
		Field qf = CDRAsynWriter.class.getDeclaredField("cdrQueue");
		qf.setAccessible(true);
		int queued = ((java.util.concurrent.BlockingQueue<?>) qf.get(w)).size();
		check(queued == 50, "CDR-01 50 CDRs queued, background writer stopped (" + queued + ")");
		w.OnEvent(new ShutdownEvent());
		int written = 0;
		for (int i = 0; i < 50; i++) {
			if (cdrCount("DRAIN_" + i, "IN_SUBMIT") == 1) written++;
		}
		check(written == 50 && ((java.util.concurrent.BlockingQueue<?>) qf.get(w)).isEmpty(),
				"CDR-02 shutdown event writes all 50 queued CDRs to the file (" + written + ")");
	}
}
