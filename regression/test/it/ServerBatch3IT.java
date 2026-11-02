package it;

import java.util.HashMap;
import java.util.Map;

import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
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

	static void run() throws Exception {
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
				String.format("S3-01a Redis Lua calls per DR on the server (incl. stream wake-ups): %.2f with result+ack in one vs %.2f before (one round trip less)", now, sendAck));
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
		server.stopService();
	}
}
