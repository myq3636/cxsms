package it;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.messagequeue.ChannelReleaseDispatcher;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/** Channel rate limit (Core hold + release) and the NPE fixes, on the local Redis; not part of the repo. */
public class ChannelRateIT extends ItSupport {
	static final String HOLD9 = "stream:core:hold:9";
	static final String ROUTED9 = "stream:mt:routed:9:*";
	static final String CM = "conf/cm.cfg";

	static GmmsMessage mt9(String id) {
		GmmsMessage m = mt(id, 7001, 9, 0);
		return m;
	}

	static void setTps(int tps, int maxSeconds) throws Exception {
		File f = new File(home + CM);
		String s = new String(Files.readAllBytes(f.toPath()), "UTF-8");
		s = s.replaceAll("(\\[VENDOR9\\][^\\[]*?SMSOptionCoreProcessorThrottlingNum: )\\d+", "$1" + tps);
		s = s.replaceAll("(\\[VENDOR9\\][^\\[]*?CoreHoldMaxSeconds: )\\d+", "$1" + maxSeconds);
		Files.write(f.toPath(), s.getBytes("UTF-8"));
		Method m = A2PCustomerManager.class.getDeclaredMethod("loadCustomers");
		m.setAccessible(true);
		m.invoke(GmmsUtility.getInstance().getCustomerManager());
	}

	static int failedDrs(String prefix) {
		int n = 0;
		for (String k : jedis.keys("stream:dr:pending:7001:*")) {
			for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (StreamEntryID) null, (StreamEntryID) null)) {
				GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && m.getMsgID() != null && m.getMsgID().startsWith(prefix)) n++;
			}
		}
		return n;
	}

	public static void main(String[] args) throws Exception {
		String cfg = new String(Files.readAllBytes(new File(System.getProperty("a2p_home") + CM).toPath()), "UTF-8");
		try {
			run();
		} finally {
			Files.write(new File(home + CM).toPath(), cfg.getBytes("UTF-8"));
		}
		exit();
	}

	static void run() throws Exception {
		boot();
		StreamQueueManager q = StreamQueueManager.getInstance();
		ChannelReleaseDispatcher d = ChannelReleaseDispatcher.getInstance();
		props.setProperty("CoreOutboundUnreadStallMs", "600000");   // no Client: keep released MTs in routed
		props.setProperty("CoreHoldRecoverIntervalMs", "1000");
		props.setProperty("CoreHoldRecoverIdleMs", "1500");
		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();

		// ---- CR-01 config ----
		check(ctm.getCustomerBySSID(9).getCoreProcessorThrottlingNum() == 2 && ctm.getCustomerBySSID(9).getCoreHoldMaxSeconds() == 5
				&& ctm.getCustomerBySSID(8).getCoreProcessorThrottlingNum() == 0 && ctm.getCustomerBySSID(8).getCoreHoldMaxSeconds() == 21600,
				"CR-01 SMSOptionCoreProcessorThrottlingNum/CoreHoldMaxSeconds parsed (VENDOR9 2/5, VENDOR8 default 0/21600)");

		// ---- CR-02 unlimited channel is not held ----
		q.produceOutboundMessage(mt("8888_CR02", 8888, 8, 0));
		check(inStreams("stream:mt:routed:8:*", "8888_CR02").size() == 1 && !jedis.exists("stream:core:hold:8"),
				"CR-02 channel without SMSOptionCoreProcessorThrottlingNum: produced to routed directly, no hold stream");

		// ---- CR-03 limited channel: MT goes to the hold stream, not to routed ----
		for (int i = 0; i < 10; i++) {
			check(q.produceOutboundMessage(mt9("7001_R" + i)), "CR-03 held MT " + i + " accepted");
		}
		check(jedis.xlen(HOLD9) == 10 && streamLen(ROUTED9) == 0 && jedis.zscore("zset:core:hold:active", HOLD9) != null,
				"CR-03 10 MTs in stream:core:hold:9, nothing in routed, hold doorbell set");

		// ---- CR-04 cap = 2 TPS x 5 s = 10: the 11th is failed with Throttled ----
		check(q.produceOutboundMessage(mt9("7001_FULL")) && jedis.xlen(HOLD9) == 10,
				"CR-04a hold full: 11th MT not held");
		check(waitFor(() -> cdrLines("7001_FULL", "OUT_SUBMIT").size() == 1
				&& cdrLines("7001_FULL", "OUT_SUBMIT").get(0).contains(",2600,"), 5000),
				"CR-04b rejected MT: OUT_SUBMIT CDR with Throttled (2600)");
		check(waitFor(() -> failedDrs("7001_FULL") == 1, 5000), "CR-04c rejected MT: failure DR to the customer");

		// ---- CR-05 release rate 2 TPS (max hold raised to 60 s so none expires while measuring) ----
		setTps(2, 60);
		long t0 = System.currentTimeMillis();
		d.start();
		Thread.sleep(2000);
		long after2s = streamLen(ROUTED9);
		check(after2s >= 3 && after2s <= 6, "CR-05a released " + after2s + " in 2 s at 2 TPS (expect 3..6)");
		check(waitFor(() -> streamLen(ROUTED9) == 10, 8000), "CR-05b all 10 released");
		long took = System.currentTimeMillis() - t0;
		check(took >= 3500, "CR-05c 10 MTs at 2 TPS took " + took + " ms (>= 3.5 s)");
		check(jedis.xlen(HOLD9) == 0 && jedis.zscore("zset:core:hold:active", HOLD9) == null,
				"CR-05d hold stream emptied, doorbell removed");
		check(jedis.exists("core:outbox:submit:7001_R0:0:0") && jedis.exists("core:outbox:submit:7001_R9:0:0"),
				"CR-05e released MTs get their outbox at release");
		List<GmmsMessage> r0 = inStreams(ROUTED9, "7001_R0");
		check(r0.size() == 1 && "7001_R0".equals(r0.get(0).getMsgID()), "CR-05f released body is the original MT");

		// ---- CR-06 held longer than CoreHoldMaxSeconds: not sent, EXPIRED ----
		d.stop();
		setTps(2, 5);
		for (int i = 0; i < 3; i++) {
			q.produceOutboundMessage(mt9("7001_OLD" + i));
		}
		Thread.sleep(5500);
		d.start();
		check(waitFor(() -> jedis.xlen(HOLD9) == 0, 6000), "CR-06a held MTs past 5 s taken out of the hold stream");
		check(inStreams(ROUTED9, "7001_OLD0").isEmpty() && inStreams(ROUTED9, "7001_OLD2").isEmpty(),
				"CR-06b held too long: not released to routed");
		check(waitFor(() -> cdrLines("7001_OLD1", "OUT_SUBMIT").size() == 1
				&& cdrLines("7001_OLD1", "OUT_SUBMIT").get(0).contains(",10200,"), 5000),
				"CR-06c held too long: OUT_SUBMIT CDR with EXPIRED (10200)");
		check(waitFor(() -> failedDrs("7001_OLD") == 3, 5000), "CR-06d held too long: failure DR for each");

		// ---- CR-07 MT already expired when its turn comes ----
		GmmsMessage exp = mt9("7001_EXP");
		exp.setExpiryDate(new Date(GmmsUtility.getInstance().getGMTTime().getTime() - 1000));
		q.produceOutboundMessage(exp);
		check(waitFor(() -> cdrLines("7001_EXP", "OUT_SUBMIT").size() == 1, 5000) && inStreams(ROUTED9, "7001_EXP").isEmpty(),
				"CR-07 expired MT is failed at release, not sent");

		// ---- CR-08 hold write is idempotent inside an MT attempt ----
		d.stop();
		GmmsMessage in = mt9("7001_IDEM");
		q.produceSubmitMessage(in);
		String pk = "stream:mt:pending:7001:" + q.calculateShardId(in);
		q.createGroup(pk, "CoreMTGroup", true);
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(pk, StreamEntryID.UNRECEIVED_ENTRY);
		GmmsMessage got = q.consumeBatch("CoreMTGroup", "it", 10, query, true).get(0);
		StreamAttempt.begin(got, "it:1");
		q.produceOutboundMessage(mt9("7001_IDEM"));
		q.produceOutboundMessage(mt9("7001_IDEM"));
		StreamAttempt.finish(pk, "CoreMTGroup", got.getRedisStreamID());
		check(inStreams(HOLD9, "7001_IDEM").size() == 1, "CR-08 same part held twice in one MT attempt -> 1 hold entry");

		// ---- CR-09 two Cores releasing the same entry: only one moves it ----
		Map<String, StreamEntryID> hq = new HashMap<String, StreamEntryID>();
		hq.put(HOLD9, StreamEntryID.UNRECEIVED_ENTRY);
		q.createGroup(HOLD9, ChannelReleaseDispatcher.GROUP, true);
		GmmsMessage h = q.consumeBatch(ChannelReleaseDispatcher.GROUP, "core-A", 1, hq, true).get(0);
		GmmsMessage sameEntry = new GmmsMessage(h);   // what a second Core would hold after auto-claim
		sameEntry.setRedisStreamID(h.getRedisStreamID());
		String first = q.releaseHeld(HOLD9, ChannelReleaseDispatcher.GROUP, h);
		String second = q.releaseHeld(HOLD9, ChannelReleaseDispatcher.GROUP, sameEntry);
		check(first != null && second == null && inStreams(ROUTED9, "7001_IDEM").size() == 1,
				"CR-09 same hold entry released twice: second gets GONE, 1 routed entry");

		// ---- CR-10 entry read by a Core that died: recovered and released ----
		q.produceOutboundMessage(mt9("7001_LOST"));
		GmmsMessage lost = q.consumeBatch(ChannelReleaseDispatcher.GROUP, "core-dead", 1, hq, true).get(0);
		check("7001_LOST".equals(lost.getMsgID()) && jedis.xpending(HOLD9, ChannelReleaseDispatcher.GROUP).getTotal() >= 1,
				"CR-10a entry pending on a dead consumer");
		d.start();
		check(waitFor(() -> inStreams(ROUTED9, "7001_LOST").size() == 1, 8000), "CR-10b recovered by auto-claim and released");

		// ---- CR-11 hot reload: TPS 2 -> 10 ----
		d.stop();
		for (int i = 0; i < 10; i++) {
			q.produceOutboundMessage(mt9("7001_FAST" + i));
		}
		setTps(10, 60);
		check(ctm.getCustomerBySSID(9).getCoreProcessorThrottlingNum() == 10, "CR-11a reload: VENDOR9 now 10 TPS");
		long t1 = System.currentTimeMillis();
		d.start();
		check(waitFor(() -> inStreams(ROUTED9, "7001_FAST9").size() == 1 && jedis.xlen(HOLD9) == 0, 6000),
				"CR-11b 10 MTs released at the new rate in " + (System.currentTimeMillis() - t1) + " ms (< 2.5 s)");
		check(System.currentTimeMillis() - t1 < 2500, "CR-11c faster than 2 TPS would allow");

		// ---- CR-12 limit removed (0): new MTs bypass the hold, leftovers drained ----
		d.stop();
		q.produceOutboundMessage(mt9("7001_LEFT"));
		setTps(0, 60);
		q.produceOutboundMessage(mt9("7001_DIRECT"));
		check(inStreams(ROUTED9, "7001_DIRECT").size() == 1 && inStreams(HOLD9, "7001_DIRECT").isEmpty(),
				"CR-12a TPS 0: new MT produced to routed directly");
		d.start();
		check(waitFor(() -> inStreams(ROUTED9, "7001_LEFT").size() == 1 && jedis.xlen(HOLD9) == 0, 5000),
				"CR-12b TPS 0: MT left in the hold is drained");
		d.stop();

		// ---- CR-13 full Core path: MTStreamConsumer -> routing -> hold ----
		setTps(2, 5);
		GmmsMessage e2e = mt("7001_E2E", 7001, -1, 0);
		q.produceSubmitMessage(e2e);
		MTStreamConsumer.getInstance().start();
		check(waitFor(() -> inStreams(HOLD9, "7001_E2E").size() == 1, 5000),
				"CR-13a MT of customer 7001 routed to VENDOR9 and held");
		d.start();
		check(waitFor(() -> inStreams(ROUTED9, "7001_E2E").size() == 1, 5000), "CR-13b then released to routed");

		// ---- NPE-03 DR reroute check with an unknown channel no longer throws ----
		GmmsMessage dr = mt("8888_NPE3", 8888, 77, 0);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setStatus(GmmsStatus.UNDELIVERABLE);
		dr.setTimeStamp(null);
		boolean ok;
		try {
			msm.handleOutDeliveryReportResForInnerAck(dr);
			ok = true;
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			ok = false;
		}
		check(ok, "NPE-02a DR with unknown channel and no timeStamp handled without NPE");
		check(waitFor(() -> streamLen("stream:dr:pending:8888:*") >= 1, 5000), "NPE-02b and the DR is forwarded");
	}

}
