package it;

import java.util.HashMap;
import java.util.Map;

import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.smpp.pending.SmppPendingDrManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Server batch 3: DR result and ack in one Lua, enquire_link every EnquireLinkTime; not part of the repo. */
public class ServerBatch3IT extends ItSupport {
	static final int PORT = 17779;

	static SmppTestClient client(int cust) throws Exception {
		SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, 5, null);
		if (!c.bind("c" + cust, "p" + cust)) throw new IllegalStateException("bind failed c" + cust);
		return c;
	}

	static GmmsMessage dr(String id, int ossid) {
		GmmsMessage d = mt(id, ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInMsgID("IN-" + id);
		return d;
	}

	static long pendingDr(int ossid) {
		long n = 0;
		for (String k : jedis.keys("stream:dr:pending:" + ossid + ":*")) {
			try { n += jedis.xpending(k, "ServerDrToCustomerGroup_1").getTotal(); } catch (Exception e) { }
		}
		return n;
	}

	/** Redis calls by command name since the last reset */
	static Map<String, Long> calls() {
		Map<String, Long> m = new HashMap<String, Long>();
		for (String line : jedis.info("commandstats").split("\r?\n")) {
			if (!line.startsWith("cmdstat_")) continue;
			String name = line.substring(8, line.indexOf(':'));
			String v = line.substring(line.indexOf("calls=") + 6);
			m.put(name, Long.parseLong(v.substring(0, v.indexOf(','))));
		}
		return m;
	}

	/** n DRs to a fast customer: EVAL calls per DR on the server side (result + ack) */
	static double evalsPerDr(String prefix, int ossid, int n) throws Exception {
		final SmppTestClient c = client(ossid);
		StreamQueueManager q = StreamQueueManager.getInstance();
		GmmsMessage[] drs = new GmmsMessage[n];
		for (int i = 0; i < n; i++) drs[i] = dr(prefix + i, ossid);
		jedis.configResetStat();
		for (GmmsMessage d : drs) q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
		waitFor(() -> c.delivers.get() >= n && pendingDr(ossid) == 0, 20000);
		Thread.sleep(500);
		Map<String, Long> m = calls();
		c.close();
		long evals = m.containsKey("eval") ? m.get("eval") : 0;
		return (evals - n) / (double) n;      // minus the n produces of the test itself
	}

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		}
		exit();
	}

	static void setMode(int ssid, String mode) throws Exception {
		Object c = com.king.gmms.GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
		for (Class<?> k = c.getClass(); k != null; k = k.getSuperclass()) {
			try {
				java.lang.reflect.Field f = k.getDeclaredField("smppDRPendingMode");
				f.setAccessible(true);
				f.set(c, mode);
				return;
			} catch (NoSuchFieldException e) { }
		}
		throw new IllegalStateException("no smppDRPendingMode");
	}
	static boolean noKey(String seq) { return jedis.keys("smpp:pending:dr:*:itpm:" + seq).isEmpty(); }

	/** pending DR manager and the customer's mode: Redis only when something was written there */
	static void pendingModes() throws Exception {
		final int ssid = 38;
		SmppPendingDrManager mgr = new SmppPendingDrManager("MultiSmppServer1:" + ssid + ":itpm", 100, 60000L, null);

		setMode(ssid, "memory");
		jedis.configResetStat();
		for (int i = 0; i < 50; i++) { mgr.put("m" + i, dr("PMM" + i, ssid)); mgr.remove("m" + i); }
		Map<String, Long> c = calls();
		long evals = c.containsKey("eval") ? c.get("eval") : 0;
		check(evals < 5, "PM-01 memory: 50 put and remove, no Redis call per DR: " + evals + " EVAL");
		check(jedis.keys("smpp:pending:dr:*:itpm:*").isEmpty(), "PM-02 memory: no pending key in Redis");

		for (String mode : new String[] { "redis", "hybrid" }) {
			setMode(ssid, mode);
			mgr.put(mode, dr("PM" + mode, ssid));
			check(!noKey(mode), "PM-03 " + mode + ": pending key written");
			mgr.remove(mode);
			check(noKey(mode), "PM-04 " + mode + ": pending key deleted on the response");
		}

		setMode(ssid, "redis");
		mgr.put("sw", dr("PMSW", ssid));
		setMode(ssid, "memory");
		check(!noKey("sw"), "PM-05a written in redis mode, customer switched to memory: key still there");
		mgr.remove("sw");
		check(noKey("sw"), "PM-05b its response still deletes the key");
		mgr.put("sw2", dr("PMSW2", ssid));
		setMode(ssid, "redis");
		mgr.remove("sw2");
		mgr.put("sw3", dr("PMSW3", ssid));
		check(noKey("sw2") && !noKey("sw3"), "PM-06 switched from memory to redis: removing the old one is harmless, the next one is written");
		mgr.remove("sw3");

		// a key left by a redis-mode process, customer now on memory, same sequence in use: the scanner answers it
		setMode(ssid, "memory");
		GmmsMessage left = dr("PMLEFT", ssid);
		String zset = "zset:smpp:pending:dr:timeout:MultiSmppServer1:1:" + ssid;
		String key = "smpp:pending:dr:MultiSmppServer1:1:" + ssid + ":MultiSmppServer1:" + ssid + ":itpm:left";
		com.king.redis.RedisClient.getInstance().setPendingMessage(key, com.king.redis.SerializableHandler.convertGmmsMessage2RedisMessage(left),
				60, zset, System.currentTimeMillis() + 2000L, "set:smpp:pending:dr:timeout:zsets:MultiSmppServer1:1");
		mgr.put("left", dr("PMNEW", ssid));
		mgr.remove("left");
		check(jedis.exists(key), "PM-07a memory: an unrelated leftover key is not deleted by the response");
		check(waitFor(() -> !jedis.exists(key) && !inStreams("stream:core:results:" + ssid + ":*", "PMLEFT").isEmpty(), 8000), "PM-07b the scanner reports its timeout result, nothing lost");
		mgr.shutdown(false, "it");

		// the real session in redis mode
		setMode(ssid, "redis");
		final SmppTestClient c38 = client(ssid);
		StreamQueueManager q = StreamQueueManager.getInstance();
		for (int i = 0; i < 20; i++) { GmmsMessage d = dr("PMR" + i, ssid); q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d)); }
		check(waitFor(() -> c38.delivers.get() >= 20 && pendingDr(ssid) == 0, 15000), "PM-08a redis mode through a session: 20 DRs delivered and acked");
		check(waitFor(() -> jedis.keys("smpp:pending:dr:*:" + ssid + ":*").isEmpty(), 5000), "PM-08b no pending key left");
		c38.close();
		setMode(ssid, "memory");
	}

	static void run() throws Exception {
		// their startup writes to Redis would be counted as Lua calls per DR
		java.nio.file.Path cfg = java.nio.file.Paths.get(System.getProperty("a2p_home") + "Gmms/GmmsConfig.properties");
		java.nio.file.Files.write(cfg, (new String(java.nio.file.Files.readAllBytes(cfg), "UTF-8") + "\nConfigFilePublishEnable=false\nGmmsConfigPublishEnable=false\n").getBytes("UTF-8"));
		boot();
		jedis.set("thcon", "1");
		props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
		MultiSmppServer server = new MultiSmppServer();
		server.startService();
		Thread.sleep(1500);
		StreamQueueManager q = StreamQueueManager.getInstance();

		// ---- S3-01 result + ack in one Lua ----
		double now = evalsPerDr("S301A", 37, 200);
		props.setProperty("ServerDrAckOnResp", "false");
		double sendAck = evalsPerDr("S301B", 38, 200);
		props.remove("ServerDrAckOnResp");
		check(sendAck - now > 0.9,
				String.format("S3-01a Redis Lua calls per DR on the server: %.2f with result+ack in one vs %.2f before (one round trip less)", now, sendAck));
		check(now < 1.2, String.format("S3-01c memory pending: only the result+ack Lua per DR: %.2f", now));
		int combined = 0, separate = 0;
		for (String l : java.nio.file.Files.readAllLines(new java.io.File(home + "log/it.log").toPath())) {
			if (!l.contains("S301A") || !l.contains("[STREAM-ACK]")) continue;
			if (l.contains("with_result=")) combined++; else separate++;
		}
		check(combined + separate == 200 && combined >= 180,
				"S3-01b every DR acked once: " + combined + " in the result's Lua, " + separate
						+ " separately (the customer answered before the DR worker registered the entry)");

		// ---- S3-02 timeout result is written and acked together ----
		final SmppTestClient silent = client(31);         // BufferTimeout 3 s
		silent.ackDeliver = false;
		GmmsMessage t = dr("S302T", 31);
		q.produceDeliveryReportToQueue(t, q.getDrPendingQueue(t));
		check(waitFor(() -> silent.delivers.get() == 1, 5000) && pendingDr(31) == 1, "S3-02a sent, no response: entry pending");
		check(waitFor(() -> pendingDr(31) == 0, 8000) && inStreams("stream:core:results*", "S302T").size() == 1,
				"S3-02b timeout: one failure result written and the entry acked");
		silent.close();

		// ---- S3-03 result write fails: nothing acked; the retry writes and acks ----
		final SmppTestClient c3 = client(31);
		GmmsMessage f = dr("S303F", 31);
		GmmsMessage dummy = dr("S303F", 31);
		dummy.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
		q.produceResult(dummy);                            // the result stream now has 1 entry
		props.setProperty("RedisStreamBackPressureEnable", "true");
		props.setProperty("RedisStreamMonitor.BlockXLEN", "1");   // any non-empty stream is "full"
		q.produceDeliveryReportToQueue(f, q.getDrPendingQueue(f));
		check(waitFor(() -> c3.delivers.get() == 1, 5000), "S3-03a DR delivered, customer answered");
		Thread.sleep(1500);
		check(pendingDr(31) == 1 && inStreams("stream:core:results*", "S303F").size() == 1,
				"S3-03b result write blocked: entry not acked, no result written");
		props.remove("RedisStreamBackPressureEnable");
		props.remove("RedisStreamMonitor.BlockXLEN");
		check(waitFor(() -> pendingDr(31) == 0, 8000) && inStreams("stream:core:results*", "S303F").size() == 2,
				"S3-03c the pending manager's retry writes the result and acks the entry");
		c3.close();

		// ---- S3-04 enquire_link every EnquireLinkTime ----
		final SmppTestClient e = client(32);              // EnquireLinkTime 1 s, FailEnquireLinkTime 10 s
		Thread.sleep(5500);
		long pings = e.enquiresFromServer.get();
		check(pings >= 3 && !e.isClosedByServer(), "S3-04 answered pings: " + pings
				+ " enquire_link in 5.5 s (every EnquireLinkTime; before: one per FailEnquireLinkTime = at most 1)");
		e.close();

		final SmppTestClient mute = client(36);           // EnquireLinkTime 1, FailEnquireLinkTime 2, FailureNum 3
		mute.answerEnquire = false;
		long t0 = System.currentTimeMillis();
		boolean closed = waitFor(() -> mute.isClosedByServer(), 15000);
		long ms = System.currentTimeMillis() - t0;
		check(closed && mute.enquiresFromServer.get() >= 3,
				"S3-05 pings not answered: closed after " + ms + " ms and " + mute.enquiresFromServer.get() + " enquire_link (EnquireLinkFailureNum 3)");

		// ---- S3-06 first DRs of a new session from two shard streams at once ----
		int rounds = 10, per = 20;
		long missBefore = 0;
		for (String l : java.nio.file.Files.readAllLines(new java.io.File(home + "log/it.log").toPath())) {
			if (l.contains("Can't find the original message")) missBefore++;
		}
		long delivered = 0;
		for (int r = 0; r < rounds; r++) {
			final SmppTestClient c = client(39);
			GmmsMessage[] batch = new GmmsMessage[per];
			for (int i = 0; i < per; i++) batch[i] = dr("S306R" + r + "_" + i, 39);
			for (GmmsMessage d : batch) q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
			final int want = per;
			waitFor(() -> c.delivers.get() >= want && pendingDr(39) == 0, 10000);
			delivered += c.delivers.get();
			c.close();
			Thread.sleep(300);
		}
		long missAfter = 0;
		for (String l : java.nio.file.Files.readAllLines(new java.io.File(home + "log/it.log").toPath())) {
			if (l.contains("Can't find the original message")) missAfter++;
		}
		java.util.Set<String> shards = new java.util.HashSet<String>();
		for (String k : jedis.keys("stream:dr:pending:39:*")) shards.add(k);
		check(missAfter == missBefore && delivered == (long) rounds * per && pendingDr(39) == 0,
				"S3-06 " + rounds + " new sessions, first DRs from " + shards.size() + " shard streams at once: "
						+ delivered + " delivered, every response matched (" + (missAfter - missBefore) + " unmatched), all acked");
		pendingModes();
		server.stopService();
	}
}
