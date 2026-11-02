package it;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

/**
 * Local integration test (not part of the repo): real Core initialisation against a real
 * Redis, with this program playing the Client module (results, vendor DRs).
 */
public class CoreDrFlowIT {
	static int failures = 0;
	static String home;
	static Jedis jedis;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	interface Cond { boolean ok() throws Exception; }

	static boolean waitFor(Cond c, long ms) throws Exception {
		long end = System.currentTimeMillis() + ms;
		while (System.currentTimeMillis() < end) {
			if (c.ok()) return true;
			Thread.sleep(50);
		}
		return c.ok();
	}

	static long drPendingCount() {
		long n = 0;
		for (String k : jedis.keys("stream:dr:pending:8888:MultiSmppServer1:*")) n += jedis.xlen(k);
		return n;
	}

	/** log lines that mention msgId and contain text */
	static String logLine(String msgId, String text) throws Exception {
		for (String l : Files.readAllLines(new File(home + "log/it.log").toPath())) {
			if (l.contains(msgId) && l.contains(text)) return l;
		}
		return null;
	}

	static String thread(String line) {
		if (line == null) return null;
		int a = line.indexOf('['), b = line.indexOf(']', a);
		return line.substring(a + 1, b);
	}

	static GmmsMessage original(String msgId, String outMsgId) {
		GmmsMessage m = new GmmsMessage();
		m.setMsgID(msgId);
		m.setInMsgID("IN-" + msgId);
		m.setOutMsgID(outMsgId);
		m.setOSsID(8888);
		m.setRSsID(8);
		m.setOA2P(56);
		m.setRA2P(56);
		m.setCurrentA2P(56);
		m.setSenderAddress("18667991226");
		m.setRecipientAddress("8613800000000");
		m.setTextContent("Your verification code is 1872031");
		m.setDeliveryReport(true);
		m.setDateIn(new Date());
		m.setTimeStamp(new Date());
		m.setExpiryDate(new Date(System.currentTimeMillis() + 24 * 3600 * 1000L));
		m.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", "MultiSmppServer1"), "conn1"));
		return m;
	}

	static GmmsMessage vendorDr(String outMsgId, String drId) {
		GmmsMessage dr = new GmmsMessage();
		dr.setMsgID(drId);
		dr.setOutMsgID(outMsgId);
		dr.setRSsID(8);
		dr.setOSsID(-1);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setStatus(GmmsStatus.DELIVERED);
		return dr;
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		jedis = new Jedis("127.0.0.1", 16379);
		jedis.flushAll();

		GmmsUtility gu = GmmsUtility.getInstance();
		// GmmsUtility only creates its logger inside initUtility; an early error there would NPE
		Field lf = GmmsUtility.class.getDeclaredField("log");
		lf.setAccessible(true);
		if (lf.get(null) == null) {
			lf.set(null, com.king.framework.SystemLogger.getSystemLogger(GmmsUtility.class));
		}
		gu.initUtility(home + "Gmms/GmmsConfig.properties");
		gu.initRedisClient("M");
		gu.initCoreShardAssignment();
		gu.initCDRManager();
		// No database here: register the real MessageStoreManager without a data source.
		// Only paths that never reach the DB (Redis lookup, streams, CDR files) are exercised.
		MessageStoreManager msm = new MessageStoreManager();
		Field dmf = com.king.db.DataControl.class.getDeclaredField("dataManagers");
		dmf.setAccessible(true);
		@SuppressWarnings("unchecked")
		java.util.Hashtable<Object, Object> dms = (java.util.Hashtable<Object, Object>) dmf.get(null);
		dms.put(MessageStoreManager.class.getName(), msm);
		dms.put(com.king.db.DBLockConnection.class.getName(), new com.king.db.DBLockConnection());
		Field inf = com.king.db.DataControl.class.getDeclaredField("initialized");
		inf.setAccessible(true);
		inf.set(null, Boolean.TRUE);
		gu.initDBManager(com.king.db.DatabaseStatus.MASTER_USED);
		Properties props = (Properties) fieldOf(gu, "properties");
		StreamQueueManager q = StreamQueueManager.getInstance();
		check(gu.getCustomerManager().getCustomerBySSID(8888) != null
				&& gu.getCustomerManager().getCurrentA2P() == 56, "Core loaded cm.cfg (8888, 8, current A2P 56)");

		// ---- 1. recipient counter (real Redis Lua) ----
		RedisClient redis = gu.getRedisClient();
		check(redis.incrWithExpire("Duplicate:it1", 86400) == 1 && jedis.ttl("Duplicate:it1") > 86000,
				"incrWithExpire: new counter gets the TTL");
		check(redis.incrWithExpire("Duplicate:it1", 86400) == 2 && jedis.ttl("Duplicate:it1") > 86000,
				"incrWithExpire: second increment keeps the TTL");
		jedis.set("Duplicate:stuck", "5");
		check(jedis.ttl("Duplicate:stuck") == -1, "(setup) counter without TTL, as left by the old two-step code");
		check(redis.incrWithExpire("Duplicate:stuck", 86400) == 6 && jedis.ttl("Duplicate:stuck") > 86000,
				"incrWithExpire repairs a counter that had no TTL");
		jedis.set("Duplicate:short", "1");
		jedis.expire("Duplicate:short", 100);
		redis.incrWithExpire("Duplicate:short", 86400);
		check(jedis.ttl("Duplicate:short") <= 100, "incrWithExpire leaves an existing TTL alone");

		// ---- 2. submit result -> Core writes the DR lookup (real ResultStreamConsumer) ----
		ResultStreamConsumer.getInstance().start();
		InboundDRStreamConsumer.getInstance().start();
		GmmsMessage resp = original("8888_IT1", "OUT-IT1");
		resp.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		resp.setStatus(GmmsStatus.SUCCESS);
		check(q.produceResult(resp), "(client) submit_resp written to stream:core:results");
		check(waitFor(() -> jedis.exists("OUT-IT1"), 10000), "Core result consumer cached the original under outMsgID");

		// ---- 3. vendor DR -> Core forwards it inline, acks only afterwards ----
		check(q.produceInboundDeliveryReport(vendorDr("OUT-IT1", "8_DR1")), "(client) vendor DR written to stream:core:inbound-dr:8");
		check(waitFor(() -> drPendingCount() == 1, 10000), "Core produced the DR to stream:dr:pending:8888:MultiSmppServer1:*");
		check(waitFor(() -> !jedis.exists("OUT-IT1"), 5000), "DR lookup entry deleted after the DR was processed");
		check(waitFor(() -> jedis.xlen("stream:core:inbound-dr:8") == 0, 5000), "inbound DR acked (XACK+XDEL)");
		String sent = logLine("8888_IT1", "[DR-TO-SERVER]");
		System.out.println("     log: " + sent);
		check(sent != null && !thread(sent).startsWith("MessageProcessorThread"),
				"DR produced on the inbound-DR consumer thread, not the processor pool (" + thread(sent) + ")");

		// ---- 3b. processing fails (message store unavailable): lookup must survive ----
		GmmsMessage bad = original("8888_IT5", "OUT-IT5");
		bad.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		bad.setStatus(GmmsStatus.SUCCESS);
		q.produceResult(bad);
		check(waitFor(() -> jedis.exists("OUT-IT5"), 10000), "3b: lookup for OUT-IT5 cached");
		Field msmField = com.king.gmms.connectionpool.session.InternalCoreEngineSession.class.getDeclaredField("msgStoreManager");
		msmField.setAccessible(true);
		Object realMsm = msmField.get(com.king.gmms.connectionpool.session.InternalCoreEngineSession.getReactiveInstance());
		msmField.set(com.king.gmms.connectionpool.session.InternalCoreEngineSession.getReactiveInstance(), null);
		long pendingBefore = drPendingCount();
		q.produceInboundDeliveryReport(vendorDr("OUT-IT5", "8_DR5"));
		check(waitFor(() -> logLine("8_DR5", "STREAM-NACK") != null, 10000), "3b: failing DR is nacked (left pending for retry)");
		check(jedis.exists("OUT-IT5") && drPendingCount() == pendingBefore,
				"3b: lookup entry kept after the failure, no DR produced");
		msmField.set(com.king.gmms.connectionpool.session.InternalCoreEngineSession.getReactiveInstance(), realMsm);
		final GmmsMessage replay = vendorDr("OUT-IT5", "8_DR5");
		boolean retried = MessageStoreManager.withInlineDrDispatch(() ->
				com.king.gmms.connectionpool.session.InternalCoreEngineSession.getReactiveInstance()
						.handleInboundDRInternal(replay, null));
		check(retried && drPendingCount() == pendingBefore + 1, "3b: retry finds the original again and forwards the DR");
		check(!jedis.exists("OUT-IT5"), "3b: lookup entry deleted after the successful retry");

		// ---- 3c. NPE fix: original without timeStamp (used to throw) is forwarded ----
		GmmsMessage noTs = original("8888_IT6", "OUT-IT6");
		noTs.setTimeStamp(null);
		noTs.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		noTs.setStatus(GmmsStatus.SUCCESS);
		q.produceResult(noTs);
		check(waitFor(() -> jedis.exists("OUT-IT6"), 10000), "NPE-01a lookup for OUT-IT6 cached");
		long before6 = drPendingCount();
		q.produceInboundDeliveryReport(vendorDr("OUT-IT6", "8_DR6"));
		check(waitFor(() -> drPendingCount() == before6 + 1 && !jedis.exists("OUT-IT6"), 10000),
				"NPE-01b DR of an original without timeStamp forwarded, lookup deleted (no NPE)");

		// ---- 4. CoreDrInlineDispatch=false: only scoped paths inline ----
		props.setProperty("CoreDrInlineDispatch", "false");
		GmmsMessage a = original("8888_IT2", "OUT-IT2");
		a.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		a.setStatus(GmmsStatus.DELIVERED);
		msm.sendDRMessage(a);
		check(waitFor(() -> logLine("8888_IT2", "[DR-TO-SERVER]") != null, 10000), "flag false: unscoped DR still sent");
		String t2 = thread(logLine("8888_IT2", "[DR-TO-SERVER]"));
		check(t2.startsWith("MessageProcessorThread"), "flag false: unscoped DR goes through the processor pool (" + t2 + ")");

		GmmsMessage rj = original("8888_IT3", "OUT-IT3");
		rj.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		rj.setStatus(GmmsStatus.REJECTED_SDBL);
		long before = drPendingCount();
		msm.sendDRMessageForServerReject(rj);
		check(drPendingCount() == before + 1, "flag false: reject DR is in the stream when the call returns (inline)");
		String t3 = thread(logLine("8888_IT3", "[DR-TO-SERVER]"));
		check("main".equals(t3), "flag false: reject DR produced on the calling thread (" + t3 + ")");

		// ---- 5. default (true): every DR inline ----
		props.remove("CoreDrInlineDispatch");
		GmmsMessage d = original("8888_IT4", "OUT-IT4");
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		before = drPendingCount();
		msm.sendDRMessage(d);
		check(drPendingCount() == before + 1, "default: unscoped DR is in the stream when the call returns");
		String t4 = thread(logLine("8888_IT4", "[DR-TO-SERVER]"));
		check("main".equals(t4), "default: unscoped DR produced on the calling thread (" + t4 + ")");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}

	static Object fieldOf(Object o, String name) throws Exception {
		Field f = o.getClass().getDeclaredField(name);
		f.setAccessible(true);
		return f.get(o);
	}
}
