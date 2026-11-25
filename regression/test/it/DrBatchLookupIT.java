package it;

import java.util.*;

import com.king.gmms.ha.SmppServerSessionRegistry;
import com.king.gmms.messagequeue.DrLookup;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * Fewer Redis round trips a vendor DR (docs/DR消费减少Redis往返设计.md): the DR lookups of a batch read with one MGET and
 * deleted with one Lua (CoreInboundDrBatchLookup), a customer's sessions on a Server read in one Lua
 * (SmppServerSessionRegistryLuaCheck) and once a batch (SmppServerSessionRegistryBatchMemo), the customer DRs of a
 * batch written with one Lua before the ack (CoreInboundDrBatchProduce). DRs and originals as
 * AttemptBatchIT (channel 8, customer 8888, stream:core:inbound-dr:8); one Redis plays every role.
 */
public class DrBatchLookupIT extends ItSupport {
	static final int OSSID = 9977;
	static final String M1 = "MultiSmppServer1", M2 = "MultiSmppServer2";

	static void bind(String module, String sessionKey) {
		long now = System.currentTimeMillis();
		jedis.setex("smpp:server:session:" + OSSID + ":" + sessionKey, 660,
				"role=smpp-server;module=" + module + ";nodeId=1;ossid=" + OSSID + ";expireAt=" + (now + 660000) + ";");
		jedis.zadd("smpp:server:active:" + OSSID, now, sessionKey);
		jedis.zadd("smpp:server:active:" + OSSID + ":" + module, now, sessionKey);
		jedis.zadd("smpp:server:module:" + OSSID, now, module);
	}

	static boolean active(String module) {
		return SmppServerSessionRegistry.getInstance().hasActiveSessionInModule(OSSID, module);
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
		boot();
		AttemptBatchIT.q = StreamQueueManager.getInstance();
		props.setProperty("CoreInboundDRShardEnable", "false");   // the checks read the stream by its name
		RedisConnection report = RedisClient.getInstance().getReportMqRedis();

		// ---- 3.1 the lookups of a batch read with one MGET ----
		check(DrLookup.batchEnabled(), "DB-00 CoreInboundDrBatchLookup on by default");
		jedis.set(DrLookup.key("OUT-DB1A"), "A");
		jedis.set(DrLookup.key("OUT-DB1B"), "B");
		jedis.configResetStat();
		DrLookup.beginBatch(Arrays.asList("OUT-DB1A", "OUT-DB1B", "OUT-DB1C", null));
		check(AttemptBatchIT.cmd("mget") == 1 && AttemptBatchIT.cmd("get") == 0, "DB-01a 3 lookups read with one MGET, no GET: "
				+ AttemptBatchIT.cmd("mget") + " MGET, " + AttemptBatchIT.cmd("get") + " GET");
		jedis.del(DrLookup.key("OUT-DB1A"));
		check("A".equals(DrLookup.get("OUT-DB1A")) && AttemptBatchIT.cmd("get") == 0, "DB-01b get uses the one read at the start");
		check(DrLookup.get("OUT-DB1A") == null && AttemptBatchIT.cmd("get") == 1,
				"DB-01c used once: a second get reads Redis (gone there)");
		jedis.set(DrLookup.key("OUT-DB1C"), "C");   // an early DR's lookup written after the start of the batch
		check("C".equals(DrLookup.get("OUT-DB1C")), "DB-01d not found at the start: read again, the one written meanwhile found");
		DrLookup.endBatch();
		check("B".equals(DrLookup.get("OUT-DB1B")), "DB-01e after the batch: get reads Redis as before");
		jedis.del(DrLookup.key("OUT-DB1B"), DrLookup.key("OUT-DB1C"));

		// ---- 3.2 the deletes of a batch: recorded, one Lua at the end ----
		GmmsMessage o2 = AttemptBatchIT.original("DB2A"), o3 = AttemptBatchIT.original("DB2B");
		DrLookup.put(o2, "OUT-DB2A", "A", 600);
		DrLookup.put(o3, "OUT-DB2B", "B", 600);
		DrLookup.beginBatch(Arrays.asList("OUT-DB2A", "OUT-DB2B"));
		DrLookup.delete("OUT-DB2A");
		DrLookup.delete("OUT-DB2B");
		DrLookup.delete("OUT-DB2B");
		check(jedis.exists(DrLookup.key("OUT-DB2A")) && jedis.exists(DrLookup.key("OUT-DB2B")),
				"DB-02a delete in a batch: still in Redis until the end of the batch");
		check(DrLookup.get("OUT-DB2A") == null && DrLookup.get("OUT-DB2B") == null,
				"DB-02b deleted in this batch: get finds none, as after a delete (a second final DR of the same MT)");
		long lua = AttemptBatchIT.lua();
		DrLookup.endBatch();
		check(AttemptBatchIT.lua() - lua == 1 && !jedis.exists(DrLookup.key("OUT-DB2A")) && !jedis.exists(DrLookup.key("OUT-DB2B"))
				&& jedis.zscore(DrLookup.DUE_ZSET, DrLookup.key("OUT-DB2A")) == null
				&& jedis.zscore(DrLookup.DUE_ZSET, DrLookup.key("OUT-DB2B")) == null,
				"DB-02c end of the batch: both lookups and their drl:due members deleted with one Lua ("
						+ (AttemptBatchIT.lua() - lua) + ")");
		DrLookup.put(o2, "OUT-DB2A", "A", 600);
		DrLookup.delete("OUT-DB2A");
		check(!jedis.exists(DrLookup.key("OUT-DB2A")), "DB-02d no batch on the thread: deleted at once as before");

		// ---- the consumer's order: claim, read ahead, process, ack, delete ----
		List<GmmsMessage> l = AttemptBatchIT.readDrs("DB3A", "DB3B", "DB3C");
		check(l.size() == 3, "DB-03a 3 vendor DRs read");
		StreamAttempt.Batch b = StreamAttempt.Batch.claim(AttemptBatchIT.IDR, AttemptBatchIT.IDR_GROUP, l, report, true, "it:1");
		List<String> outs = new ArrayList<String>();
		for (GmmsMessage m : l) outs.add(m.getOutMsgID());
		jedis.configResetStat();
		DrLookup.beginBatch(outs);
		SmppServerSessionRegistry.getInstance().beginBatchMemo();
		boolean ok = true;
		for (GmmsMessage m : l) {
			ok &= b.begin(m) == StreamAttempt.Start.RUN && AttemptBatchIT.process(m);
			b.finished(m);
		}
		check(ok && AttemptBatchIT.cmd("mget") == 1, "DB-03b processed with the lookups read by one MGET");
		check(jedis.exists(DrLookup.key("OUT-DB3A")), "DB-03c the deletes wait for the ack and the end of the batch");
		final List<String> acked = new ArrayList<String>();
		b.ackAll((m, id, a) -> { if (a) acked.add(id); });
		SmppServerSessionRegistry.getInstance().endBatchMemo();
		DrLookup.endBatch();
		check(acked.size() == 3 && !jedis.exists(DrLookup.key("OUT-DB3A")) && !jedis.exists(DrLookup.key("OUT-DB3B"))
				&& !jedis.exists(DrLookup.key("OUT-DB3C")), "DB-03d acked, then the 3 lookups deleted");
		check(AttemptBatchIT.drsOut("DB3A") == 1 && AttemptBatchIT.drsOut("DB3B") == 1 && AttemptBatchIT.drsOut("DB3C") == 1,
				"DB-03e each customer gets its DR once");

		// ---- 3.4 the customer DRs of a batch written with one Lua before the ack ----
		check(StreamQueueManager.drBatchProduceEnabled(), "DB-07 CoreInboundDrBatchProduce on by default");
		l = AttemptBatchIT.readDrs("DB7A", "DB7B", "DB7C");
		b = StreamAttempt.Batch.claim(AttemptBatchIT.IDR, AttemptBatchIT.IDR_GROUP, l, report, true, "it:1");
		StreamQueueManager.beginDrBatch();
		ok = true;
		for (GmmsMessage m : l) {
			ok &= b.begin(m) == StreamAttempt.Start.RUN;
			StreamQueueManager.beginDrEntry(m.getRedisStreamID());
			ok &= AttemptBatchIT.process(m);
			if (m == l.get(2)) {   // not processed: left pending, its DR not written
				StreamAttempt.abandon();
				StreamQueueManager.discardDrEntry();
			} else {
				StreamQueueManager.commitDrEntry();
				b.finished(m);
			}
		}
		check(ok && AttemptBatchIT.drsOut("DB7A") == 0 && AttemptBatchIT.drsOut("DB7B") == 0,
				"DB-07a processed: the customer DRs wait for the end of the batch");
		lua = AttemptBatchIT.lua();
		AttemptBatchIT.q.flushDrBatch();
		check(AttemptBatchIT.lua() - lua == 1 && AttemptBatchIT.drsOut("DB7A") == 1 && AttemptBatchIT.drsOut("DB7B") == 1,
				"DB-07b both written with one Lua (" + (AttemptBatchIT.lua() - lua) + ")");
		check(AttemptBatchIT.drsOut("DB7C") == 0, "DB-07c an entry not processed: its DR not written");
		final Set<String> ackedIds = new HashSet<String>();
		b.ackAll((m, id, a) -> { if (a) ackedIds.add(id); });
		StreamQueueManager.endDrBatch(ackedIds);
		check(ackedIds.size() == 2, "DB-07d the 2 processed acked");

		// a crash after the write, before the ack: the redelivery writes no second DR
		l = AttemptBatchIT.readDrs("DB8A");
		b = StreamAttempt.Batch.claim(AttemptBatchIT.IDR, AttemptBatchIT.IDR_GROUP, l, report, true, "it:1");
		StreamQueueManager.beginDrBatch();
		b.begin(l.get(0));
		StreamQueueManager.beginDrEntry(l.get(0).getRedisStreamID());
		AttemptBatchIT.process(l.get(0));
		StreamQueueManager.commitDrEntry();
		b.finished(l.get(0));
		AttemptBatchIT.q.flushDrBatch();
		StreamQueueManager.endDrBatch(new HashSet<String>());   // no ack
		check(AttemptBatchIT.drsOut("DB8A") == 1, "DB-08a written once");
		jedis.set(DrLookup.key("OUT-DB8A"), SerializableHandler.convertGmmsMessage2RedisMessage(AttemptBatchIT.original("DB8A")));
		b = StreamAttempt.Batch.claim(AttemptBatchIT.IDR, AttemptBatchIT.IDR_GROUP, l, report, true, "it:1");
		StreamQueueManager.beginDrBatch();
		boolean run = b.begin(l.get(0)) == StreamAttempt.Start.RUN;
		StreamQueueManager.beginDrEntry(l.get(0).getRedisStreamID());
		run &= AttemptBatchIT.process(l.get(0));
		StreamQueueManager.commitDrEntry();
		b.finished(l.get(0));
		AttemptBatchIT.q.flushDrBatch();
		final Set<String> acked8 = new HashSet<String>();
		b.ackAll((m, id, a) -> { if (a) acked8.add(id); });
		StreamQueueManager.endDrBatch(acked8);
		check(run && AttemptBatchIT.drsOut("DB8A") == 1 && acked8.size() == 1,
				"DB-08b redelivered and processed again: still one DR (the marker has it), then acked");

		// the switch off: written at once, as before
		props.setProperty("CoreInboundDrBatchProduce", "false");
		check(!StreamQueueManager.drBatchProduceEnabled(), "DB-09a CoreInboundDrBatchProduce=false: off");
		props.remove("CoreInboundDrBatchProduce");
		l = AttemptBatchIT.readDrs("DB9A");
		b = StreamAttempt.Batch.claim(AttemptBatchIT.IDR, AttemptBatchIT.IDR_GROUP, l, report, true, "it:1");
		b.begin(l.get(0));
		AttemptBatchIT.process(l.get(0));   // no batch begun on the thread
		check(AttemptBatchIT.drsOut("DB9A") == 1, "DB-09b no batch on the thread: the DR written at once");
		b.finished(l.get(0));
		b.ackAll((m, id, a) -> { });

		// ---- the switch off ----
		props.setProperty("CoreInboundDrBatchLookup", "false");
		check(!DrLookup.batchEnabled(), "DB-04 CoreInboundDrBatchLookup=false: the consumer reads and deletes one by one");
		props.remove("CoreInboundDrBatchLookup");

		// ---- 3.3 a the sessions in one Lua, the same results ----
		bind(M1, "s1");
		bind(M1, "s2");
		bind(M2, "s3");
		jedis.zadd("smpp:server:active:" + OSSID + ":" + M1, System.currentTimeMillis(), "gone");   // detail expired
		jedis.configResetStat();
		boolean a1 = active(M1);
		check(a1 && AttemptBatchIT.lua() == 1 && AttemptBatchIT.cmd("zrange") == 1,
				"DB-05a bound on MultiSmppServer1: true, one Lua (the ZRANGE and the GETs inside): " + AttemptBatchIT.lua() + " Lua");
		check(jedis.zscore("smpp:server:active:" + OSSID + ":" + M1, "gone") == null && jedis.zscore("smpp:server:active:" + OSSID, "gone") == null,
				"DB-05b a member without its detail removed, as before");
		check(!active("MultiSmppServer3"), "DB-05c no session on a module: false");
		jedis.del("smpp:server:session:" + OSSID + ":s3");
		check(!active(M2) && new HashSet<String>(jedis.zrange("smpp:server:module:" + OSSID, 0, -1)).equals(Collections.singleton(M1)),
				"DB-05d its only session expired: false, the module removed from the customer's modules as before");
		bind(M2, "s4");
		jedis.setex("smpp:server:session:" + OSSID + ":s4", 660, "role=smpp-server;module=" + M1 + ";nodeId=1;ossid=" + OSSID + ";");
		check(!active(M2), "DB-05e a session whose detail names another module: not this module's");
		props.setProperty("SmppServerSessionRegistryLuaCheck", "false");
		jedis.configResetStat();
		check(active(M1) && AttemptBatchIT.lua() == 0 && AttemptBatchIT.cmd("zrange") == 1 && !active(M2),
				"DB-05f SmppServerSessionRegistryLuaCheck=false: a ZRANGE and a GET each, the same results");
		props.remove("SmppServerSessionRegistryLuaCheck");

		// ---- 3.3 b once a batch ----
		SmppServerSessionRegistry reg = SmppServerSessionRegistry.getInstance();
		reg.beginBatchMemo();
		jedis.configResetStat();
		boolean first = active(M1), second = active(M1);
		check(first && second && AttemptBatchIT.lua() == 1, "DB-06a in a batch: the same customer and Server looked up once ("
				+ AttemptBatchIT.lua() + " Lua for 2)");
		check(!active(M2) && AttemptBatchIT.lua() == 2, "DB-06b another Server of the same customer: looked up on its own");
		jedis.del("smpp:server:session:" + OSSID + ":s1", "smpp:server:session:" + OSSID + ":s2");
		check(active(M1), "DB-06c its sessions gone in the middle of the batch: the batch keeps what it saw");
		reg.endBatchMemo();
		check(!active(M1), "DB-06d next batch: looked up again, none");
		bind(M1, "s5");
		props.setProperty("SmppServerSessionRegistryBatchMemo", "false");
		reg.beginBatchMemo();
		jedis.configResetStat();
		boolean x1 = active(M1), x2 = active(M1);
		reg.endBatchMemo();
		check(x1 && x2 && AttemptBatchIT.lua() == 2, "DB-06e SmppServerSessionRegistryBatchMemo=false: every DR looks up ("
				+ AttemptBatchIT.lua() + " Lua for 2)");
		props.remove("SmppServerSessionRegistryBatchMemo");
	}
}
