package it;

import java.util.List;

import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.processor.CsmExpirySweeper;
import com.king.gmms.processor.CsmStoreResult;
import com.king.gmms.processor.DBBackupHandler;
import com.king.gmms.processor.RedisCsmReassembler;
import com.king.gmms.routing.SyncSubmitRouter;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Core batch 7: inline DB fallback, CSM redis mode by default, Core expiry sweeper; not part of the repo. */
public class Batch7IT extends ItSupport {
	static int rcpt = 0;

	static String key(String ref, int total, String recipient) {
		return "csm:8888:18667991226:" + recipient + ":" + ref + ":" + total;
	}

	static GmmsMessage seg(String id, String ref, int total, int i, String recipient) {
		GmmsMessage s = mt(id, 8888, 0, 0);
		s.setRecipientAddress(recipient);
		s.setSarMsgRefNum(ref);
		s.setSarTotalSegments(total);
		s.setSarSegmentSeqNum(i);
		s.setTextContent("part" + i + " ");
		return s;
	}

	/** what MTStreamConsumer does for one entry: attempt, process, finish (ack) or abandon */
	static boolean process(GmmsMessage m, String owner) {
		StreamAttempt.begin(m, owner);
		boolean ok = false;
		try {
			ok = InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(m);
		} finally {
			if (ok) {
				StreamAttempt.finish("stream:it:none", "g", "0-1");   // ack + delete the marker
			} else {
				StreamAttempt.abandon();
			}
		}
		return ok;
	}

	/** first attempt of an MT in its own thread that dies without finishing (crash before ack) */
	static void crashed(final Runnable r) throws Exception {
		Thread t = new Thread(r);
		t.start();
		t.join();
	}

	static long routed() {
		return streamLen("stream:mt:routed:8:*");
	}

	static int drs(String msgId) {
		return inStreams("stream:dr:pending:8888:*", msgId).size();
	}

	static String newRecipient() {
		return "86139" + String.format("%06d", ++rcpt);
	}

	public static void main(String[] args) throws Exception {
		boot();
		props.setProperty("CoreMtLeaseMs", "800");
		final GmmsUtility gu = GmmsUtility.getInstance();

		// ================= A. DB fallback inline =================
		GmmsMessage a = mt("B7A1", 8888, 0, 0);
		a.setRecipientAddress("4412345678");               // no route for 8888 -> RouteFailed
		check(process(a, "core:1"), "B7-A1a route failed: processed (true -> ack)");
		check(cdrCount("B7A1", "OUT_SUBMIT") == 1 && drs("B7A1") == 1,
				"B7-A1b CDR and failure DR already written when processing returns (not left in a pool)");
		check(logLine("B7A1", "DB backup already queued") == null, "B7-A1c not handed to the DBBackupHandler pool");

		// ack lost: processed and the attempt's flags written (finish up to the ack), then the ack fails
		final GmmsMessage a2 = mt("B7A2", 8888, 0, 0);
		a2.setRecipientAddress("4412345678");
		crashed(new Runnable() {
			public void run() {
				try {
					StreamAttempt.begin(a2, "crashed:1");
					InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(a2);
					java.lang.reflect.Method flush = StreamAttempt.class.getDeclaredMethod("flush", boolean.class, boolean.class);
					flush.setAccessible(true);
					flush.invoke(StreamAttempt.current(), false, false);
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		});
		Thread.sleep(900);
		GmmsMessage a2r = mt("B7A2", 8888, 0, 0);
		a2r.setRecipientAddress("4412345678");
		check(process(a2r, "core:2"), "B7-A2a redelivery after the ack was lost: processed");
		Thread.sleep(500);
		check(cdrCount("B7A2", "OUT_SUBMIT") == 1 && drs("B7A2") == 1,
				"B7-A2b redelivery: no second OUT_SUBMIT CDR, no second failure DR");

		// crash in the few ms between the failure DR and finish writing the flags (DR on reportMq,
		// MT marker on submitMq: not one Lua) -> the DR is sent again: at-least-once, never lost
		final GmmsMessage a2c = mt("B7A2C", 8888, 0, 0);
		a2c.setRecipientAddress("4412345678");
		crashed(new Runnable() {
			public void run() {
				StreamAttempt.begin(a2c, "crashed:1");
				InternalCoreEngineSession.getReactiveInstance().processSubmitFromStream(a2c);
			}
		});
		Thread.sleep(900);
		GmmsMessage a2cr = mt("B7A2C", 8888, 0, 0);
		a2cr.setRecipientAddress("4412345678");
		process(a2cr, "core:2");
		check(drs("B7A2C") == 2, "B7-A2c crash before the flags were written: failure DR sent again (at-least-once, not lost)");

		// failed DB write: retryable failure with UDH -> synchronous RMQ insert, no data source -> fails
		final GmmsMessage a3 = mt("B7A3", 8888, 8, 0);
		a3.setStatus(GmmsStatus.COMMUNICATION_ERROR);
		a3.setUdh(new byte[] { 0x00 });
		StreamAttempt.begin(a3, "core:3");
		boolean done3 = DBBackupHandler.getInstance().processInline(a3);
		String marker3 = StreamAttempt.markerKey(a3);
		StreamAttempt.abandon();
		boolean triedRmq = logLine("B7A3", "INSERT INTO RMQ") != null;
		check(triedRmq && !done3, "B7-A3a DB write failed (RMQ insert) -> fallback not completed -> left pending");
		boolean dbqFlag = false, cdrFlag = false;
		for (String f : jedis.hkeys(marker3)) {
			dbqFlag |= f.startsWith("dbq:RMQ");
			cdrFlag |= f.startsWith("cdr#");
		}
		check(!dbqFlag && cdrFlag, "B7-A3b marker keeps the CDR flag, drops the failed RMQ insert flag (retried next time)");
		GmmsMessage a3r = mt("B7A3", 8888, 8, 0);
		a3r.setStatus(GmmsStatus.COMMUNICATION_ERROR);   // DB back: no UDH -> async SQL writer accepts it
		StreamAttempt.begin(a3r, "core:3");
		boolean done3r = DBBackupHandler.getInstance().processInline(a3r);
		StreamAttempt.abandon();
		check(done3r && logLine("B7A3", "already inserted into RMQ") == null && cdrCount("B7A3", "OUT_SUBMIT") == 1,
				"B7-A3c retry inserts into RMQ, CDR not written again");

		props.setProperty("CoreDbFallbackInline", "false");
		GmmsMessage a4 = mt("B7A4", 8888, 0, 0);
		a4.setRecipientAddress("4412345678");
		check(process(a4, "core:4") && waitFor(() -> drs("B7A4") == 1, 5000),
				"B7-A4 switch off: handed to the pool, the failure DR still arrives (asynchronously)");
		props.remove("CoreDbFallbackInline");

		// ================= B. CSM redis mode =================
		check(RedisCsmReassembler.enabled(), "B7-B0 CsmReassembleMode not configured -> redis mode");

		String r1 = newRecipient();
		long before = routed();
		boolean ok1 = process(seg("B7B1_1", "11", 3, 1, r1), "core:1");
		boolean ok2 = process(seg("B7B1_2", "11", 3, 2, r1), "core:1");
		check(ok1 && ok2 && routed() == before && jedis.hlen(key("11", 3, r1)) == 2
				&& jedis.zscore("csm:index", key("11", 3, r1)) != null,
				"B7-B1a segments 1, 2: stored in Redis (hash + index), acked, nothing sent");
		check(process(seg("B7B1_3", "11", 3, 3, r1), "core:1") && routed() == before + 1,
				"B7-B1b segment 3 completes the group: one assembled message sent");
		String k1 = key("11", 3, r1);
		check(!jedis.exists(k1) && !jedis.exists(k1.replace("csm:", "csm:ids:")) && !jedis.exists(k1.replace("csm:", "csm:asm:"))
				&& jedis.zscore("csm:index", k1) == null && jedis.hlen(k1.replace("csm:", "csm:done:")) == 3,
				"B7-B1c finished: hash, ids, lease and index entry removed, done record has the 3 msgIds");
		List<GmmsMessage> sent = inStreams("stream:mt:routed:8:*", "B7B1_1");
		check(sent.size() == 1 && sent.get(0).getTextContent().contains("part1") && sent.get(0).getTextContent().contains("part3"),
				"B7-B1d assembled content has all parts: " + (sent.isEmpty() ? "-" : sent.get(0).getTextContent()));

		String r2 = newRecipient();
		before = routed();
		process(seg("B7B2_3", "22", 3, 3, r2), "core:1");
		process(seg("B7B2_1", "22", 3, 1, r2), "core:1");
		process(seg("B7B2_2", "22", 3, 2, r2), "core:1");
		check(routed() == before + 1 && inStreams("stream:mt:routed:8:*", "B7B2_1").size() == 1,
				"B7-B2 out of order (3, 1, 2): sent once after the last one");

		before = routed();
		check(process(seg("B7B1_2", "11", 3, 2, r1), "core:5"), "B7-B3a segment 2 redelivered after the group was sent: acked");
		check(routed() == before && !jedis.exists(k1) && jedis.zscore("csm:index", k1) == null,
				"B7-B3b no new group (would expire into a wrong DR), nothing sent again");

		check(process(seg("B7B4_1", "11", 3, 1, r1), "core:1") && jedis.hlen(k1) == 1 && !jedis.exists(k1.replace("csm:", "csm:done:")),
				"B7-B4 new message reusing the same reference (new msgId): starts a new group, not swallowed");
		jedis.del(k1, k1.replace("csm:", "csm:ids:"));
		jedis.zrem("csm:index", k1);

		// B5: another consumer is sending the complete group -> BUSY, left pending; later DONE
		final String r5 = newRecipient();
		final RedisCsmReassembler re = RedisCsmReassembler.getInstance();
		process(seg("B7B5_1", "55", 2, 1, r5), "core:1");
		crashed(new Runnable() {
			public void run() {
				re.storeSegment(seg("B7B5_2", "55", 2, 2, r5), "other:9");   // takes the send lease, still working
			}
		});
		before = routed();
		check(!process(seg("B7B5_1", "55", 2, 1, r5), "core:1"),
				"B7-B5a redelivered segment while another consumer holds the send lease: BUSY, not acked");
		check(routed() == before, "B7-B5b nothing sent twice");

		// B6: the sender crashed after producing, before finishing; the last segment is redelivered
		final String r6 = newRecipient();
		process(seg("B7B6_1", "66", 2, 1, r6), "core:1");
		before = routed();
		crashed(new Runnable() {
			public void run() {
				GmmsMessage last = seg("B7B6_2", "66", 2, 2, r6);
				StreamAttempt.begin(last, "crashed:2");
				CsmStoreResult r = re.storeSegment(last, "crashed:2");
				SyncSubmitRouter.getInstance().routeSubmitSync(r.getAssembled());   // produced, then dies
			}
		});
		check(routed() == before + 1 && jedis.exists(key("66", 2, r6)), "B7-B6a crash after sending: group still in Redis");
		check(!process(seg("B7B6_2", "66", 2, 2, r6), "core:6"), "B7-B6b redelivery within the lease: BUSY");
		Thread.sleep(900);
		check(process(seg("B7B6_2", "66", 2, 2, r6), "core:6"), "B7-B6c redelivery after the lease: processed");
		check(routed() == before + 1 && !jedis.exists(key("66", 2, r6)),
				"B7-B6d not sent a second time (part marker of the attempt), group finished");

		// ================= C. expiry sweeper =================
		gu.setCsmIntegrityCacheTimeout(1000);
		String rc = newRecipient();
		process(seg("B7C1_1", "C1", 3, 1, rc), "core:1");
		process(seg("B7C1_2", "C1", 3, 2, rc), "core:1");
		final String kc = key("C1", 3, rc);
		check(CsmExpirySweeper.sweepOnce(200, 300000, CsmExpirySweeper.INLINE_DR) == 0 && jedis.exists(kc),
				"B7-C0 not due yet: untouched");
		Thread.sleep(1100);
		check(CsmExpirySweeper.sweepOnce(200, 300000, CsmExpirySweeper.INLINE_DR) == 1, "B7-C1a due incomplete group expired");
		List<GmmsMessage> exp = inStreams("stream:dr:pending:8888:*", "B7C1_1");
		boolean statusOk = exp.size() == 2;
		for (GmmsMessage d : exp) {
			statusOk &= d.getStatusCode() == GmmsStatus.EXPIRED.getCode() || d.getStatusCode() == GmmsStatus.UNDELIVERABLE.getCode();
		}
		check(statusOk, "B7-C1b one expired DR per received segment (2), status EXPIRED/UNDELIVERABLE, written before finishing");
		check(!jedis.exists(kc) && jedis.zscore("csm:index", kc) == null && jedis.exists(kc.replace("csm:", "csm:done:")),
				"B7-C1c group finished, done record kept");
		check(process(seg("B7C1_1", "C1", 3, 1, rc), "core:7") && !jedis.exists(kc),
				"B7-C2 late redelivery of an expired segment: acked, no new group");

		String rn = newRecipient();
		GmmsMessage noDr = seg("B7C3_1", "C3", 2, 1, rn);
		noDr.setDeliveryReport(false);
		process(noDr, "core:1");
		Thread.sleep(1100);
		CsmExpirySweeper.sweepOnce(200, 300000, CsmExpirySweeper.INLINE_DR);
		check(drs("B7C3_1") == 0 && !jedis.exists(key("C3", 2, rn)), "B7-C3 no DR requested: finished without a DR");

		// complete but not finished (sender crashed after producing, nobody redelivered)
		String rk = newRecipient();
		process(seg("B7C4_1", "C4", 2, 1, rk), "core:1");
		re.storeSegment(seg("B7C4_2", "C4", 2, 2, rk), "crashed:3");
		Thread.sleep(1100);
		CsmExpirySweeper.sweepOnce(200, 300000, CsmExpirySweeper.INLINE_DR);
		check(drs("B7C4_1") == 0 && !jedis.exists(key("C4", 2, rk)), "B7-C4 complete group at expiry: finished without a DR");

		// claim lease: a second sweeper does not see a claimed group until the lease is over
		String rl = newRecipient();
		process(seg("B7C5_1", "C5", 2, 1, rl), "core:1");
		Thread.sleep(1100);
		final int[] seen = new int[2];
		CsmExpirySweeper.DrSink failing = new CsmExpirySweeper.DrSink() {
			public boolean send(GmmsMessage dr) {
				seen[0]++;
				return false;          // e.g. Redis down: not sent
			}
		};
		CsmExpirySweeper.sweepOnce(200, 1000, failing);
		check(seen[0] == 1 && jedis.exists(key("C5", 2, rl)), "B7-C5a DR not sent: group kept");
		CsmExpirySweeper.sweepOnce(200, 1000, failing);
		check(seen[0] == 1, "B7-C5b claimed group invisible to another sweeper during the lease");
		Thread.sleep(1100);
		CsmExpirySweeper.sweepOnce(200, 1000, CsmExpirySweeper.INLINE_DR);
		check(drs("B7C5_1") == 1 && !jedis.exists(key("C5", 2, rl)), "B7-C5c after the lease another sweep sends it and finishes");

		// sweeper thread and its switch
		props.setProperty("CoreCsmExpiryIntervalSec", "1");
		props.setProperty("CoreCsmExpirySweep", "false");
		CsmExpirySweeper.getInstance().start();
		String rt = newRecipient();
		process(seg("B7C6_1", "C6", 2, 1, rt), "core:1");
		Thread.sleep(3000);
		check(drs("B7C6_1") == 0 && jedis.exists(key("C6", 2, rt)), "B7-C6a CoreCsmExpirySweep=false: not swept");
		props.setProperty("CoreCsmExpirySweep", "true");
		check(waitFor(() -> drs("B7C6_1") == 1, 4000), "B7-C6b switched on (hot): the Core sweeper thread expires it");
		CsmExpirySweeper.getInstance().stop();
		gu.setCsmIntegrityCacheTimeout(3600000);

		// legacy switch still selectable
		props.setProperty("CsmReassembleMode", "legacy");
		check(!RedisCsmReassembler.enabled(), "B7-D1 CsmReassembleMode=legacy selects the old mode");
		props.remove("CsmReassembleMode");

		// ================= P. throughput of the redis mode =================
		int n = 1000;
		long t0 = System.currentTimeMillis();
		for (int i = 0; i < n; i++) {
			String r = newRecipient();
			for (int p = 1; p <= 3; p++) {
				process(seg("B7P" + i + "_" + p, "P" + (i % 200), 3, p, r), "core:1");
			}
		}
		long ms = System.currentTimeMillis() - t0;
		check(jedis.keys("csm:8888:*:P*").isEmpty(), "B7-P1 " + n + " long SMS x 3 segments all completed and finished");
		System.out.println("CHECK B7-P2 " + (3 * n) + " segments in " + ms + " ms (" + (3000L * n / Math.max(1, ms)) + " segments/s, one thread, full MT path)");
		t0 = System.currentTimeMillis();
		for (int i = 0; i < 3 * n; i++) {
			GmmsMessage m = mt("B7S" + i, 8888, 0, 0);
			m.setRecipientAddress(newRecipient());
			process(m, "core:1");
		}
		long base = System.currentTimeMillis() - t0;
		System.out.println("CHECK B7-P3 baseline: " + (3 * n) + " normal (non-CSM) MTs in " + base + " ms on the same path");
		exit();
	}
}
