package it;

import java.nio.file.*;

import com.king.gmms.messagequeue.CoreMQMStreamConsumer;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.processor.DelayedInDRDispatcher;
import com.king.gmms.processor.FakeDrTimeoutScheduler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * Fake DRs (docs/假DR逻辑重新设计.md) as the real Core does them; this program plays the Client (submit results,
 * vendor DRs). Ways 1 to 4, failures that must stay failures, one final DR per MT, the old options.
 * CoreDrBizIT has the basic cases of ways 1 and 2 (ratio, delay, prefix).
 */
public class FakeDrIT extends CoreDrBizIT {
	static final String HK = "85212345678";

	static GmmsMessage firstTo(int ossid, String recipient, int rssid) throws Exception {
		GmmsMessage m = msg(ossid, "18667991226", recipient, "hello");
		q.produceSubmitMessage(m);
		return out(rssid, m.getMsgID(), 1);
	}
	static int wdq(String id) { return sqlOf("INSERT", "WDQ", id).size(); }
	static boolean itLog(String part) throws Exception {
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(part)) return true;
		return false;
	}
	static String st(GmmsMessage d) { return d == null ? "none" : String.valueOf(d.getStatusCode()); }

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder();
		cm.append(cust("V951", 951, ""));
		cm.append(cust("V952", 952, "SMSOptionSendDRByOutSubmit: true\n"));
		cm.append(cust("V953", 953, "SMSOptionSubmitRetryCode: 2500\nSMSOptionMTRetryPolicy: 1S\n"));
		cm.append(cust("C951", 951 + 50, "SMSOptionSendDRByInSubmitInCU: true\n"));
		cm.append(cust("C952", 952 + 50, "SMSOptionSendDRByInSubmitInCU: true\nSMSOptionFakeDRSuccessRatio: 0\n"));
		cm.append(cust("C953", 953 + 50, "SMSOptionDRSuccessRatio: 100\nSMSOptionDRSuccessRatioPrefix: 8613\n"));
		cm.append(cust("C955", 955 + 50, "SMSOptionFakeDRTimeoutSec: 3\n"));
		cm.append(cust("C956", 956 + 50, "SMSOptionFakeDRTimeoutSec: 3\nSMSOptionFakeDRTimeoutStatus: fail\n"));
		cm.append(cust("C957", 957 + 50, "SMSOptionFakeDRDisable: true\n"));
		cm.append(cust("C958", 958 + 50, ""));
		cm.append(cust("C959", 959 + 50, "SMSOptionSendFakeDRByInSubmitRecPrefixInCU: 8613\n"));
		cm.append(cust("C960", 960 + 50, "SMSOptionSendFakeDR: true\n"));
		appendCm(cm.toString());
		for (int c : new int[] { 1001, 1002, 1003, 1005, 1006, 1010 }) route("C" + (c - 50), c, "A2PMainRouting: (Priority=30,PhonePrefix=86;852,Relay=V951)");
		for (int c : new int[] { 1007, 1008 }) route("C" + (c - 50), c, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V952)");
		route("C959", 1009, "A2PMainRouting: (Priority=30,PhonePrefix=86;852,Relay=V953)");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\nAutoInDR.DispatcherIdleSleepMs=100\n").getBytes("UTF-8"));
		msmFactory = RecordingStore::new;
		try {
			start();
			q = StreamQueueManager.getInstance();
			ResultStreamConsumer.getInstance().start();
			InboundDRStreamConsumer.getInstance().start();
			CoreMQMStreamConsumer.getInstance().start();
			DelayedInDRDispatcher.getInstance().startDispatcher();
			FakeDrTimeoutScheduler.getInstance().start();
			Thread.sleep(800);

			// ---- failures stay failures ----
			GmmsMessage nr = msg(1001, "18667991226", "4412345678", "hello");
			q.produceSubmitMessage(nr);
			GmmsMessage d = dr(1001, nr.getMsgID(), 5000);
			check(d != null && d.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "FN-01a way 1 customer, MT not routed: failure DR, not a fake success: " + st(d));
			GmmsMessage m = firstTo(1001, DST, 951);
			d = dr(1001, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "FN-02a way 1, all recipients, ratio not configured: DELIVERED on routing: " + st(d));
			fail(m, GmmsStatus.UNDELIVERABLE);
			final String f1 = m.getMsgID();
			waitFor(() -> cdrCount(f1, "OUT_SUBMIT") >= 1, 5000);
			Thread.sleep(800);
			check(drs(1001, f1) == 1, "FN-02b then the vendor refuses: no second DR: " + drs(1001, f1));
			m = firstTo(1002, DST, 951);
			d = dr(1002, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "FN-04a way 1 with SMSOptionFakeDRSuccessRatio 0: the fake DR fails: " + st(d));

			// ---- DEF-C8 case: way 1 by prefix, recipient outside, retry code, last retry fails ----
			m = firstTo(1009, HK, 953);
			check(dr(1009, m.getMsgID(), 1500) == null, "FN-03a 852 outside the prefix 8613: no DR on routing");
			fail(m, GmmsStatus.COMMUNICATION_ERROR);
			d = dr(1009, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "FN-03b last retry failed: the customer gets the failure DR: " + st(d));
			m = firstTo(1009, DST, 953);
			check(dr(1009, m.getMsgID(), 5000) != null, "FN-03c 8613 inside: DR on routing");
			fail(m, GmmsStatus.COMMUNICATION_ERROR);
			final String f3 = m.getMsgID();
			waitFor(() -> cdrCount(f3, "OUT_SUBMIT") >= 1, 5000);
			Thread.sleep(800);
			check(drs(1009, f3) == 1, "FN-03d then the last retry fails: still one DR: " + drs(1009, f3));

			// ---- way 2: channel switch, customer opt-out; DEF-C9 case ----
			m = accept(firstTo(1007, DST, 952));
			check(dr(1007, m.getMsgID(), 1500) == null, "FN-05a customer SMSOptionFakeDRDisable: no fake DR from the channel");
			vendorDr(m, GmmsStatus.DELIVERED);
			check(dr(1007, m.getMsgID(), 5000) != null, "FN-05b the vendor's DR goes to it");
			m = accept(firstTo(1008, DST, 952));
			d = dr(1008, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "FN-05c channel SMSOptionSendDRByOutSubmit: fake DR after the accept: " + st(d));
			vendorDr(m, GmmsStatus.EXPIRED);
			final String f6 = m.getMsgID();
			waitFor(() -> cdrCount(f6, "OUT_DELI_REP") >= 1, 5000);
			Thread.sleep(800);
			check(drs(1008, f6) == 1 && wdq(f6) == 0, "FN-06 then vendor EXPIRED: no second DR, not kept in WDQ: DRs " + drs(1008, f6) + ", WDQ " + wdq(f6));

			// ---- way 3 ----
			m = accept(firstTo(1003, DST, 951));
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			d = dr(1003, m.getMsgID(), 5000);
			final String f7 = m.getMsgID();
			waitFor(() -> cdrCount(f7, "OUT_DELI_REP") >= 1, 3000);
			check(d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "FN-07a SMSOptionDRSuccessRatio 100, recipient in the prefix, vendor UNDELIV: DELIVERED to the customer: " + st(d));
			java.util.List<String> od = cdrLines(f7, "OUT_DELI_REP");
			check(!od.isEmpty() && od.get(0).contains("," + GmmsStatus.UNDELIVERABLE.getCode() + ","), "FN-07b OUT_DELI_REP keeps the vendor's status");
			m = accept(firstTo(1003, DST, 951));
			vendorDr(m, GmmsStatus.REJECTED);
			d = dr(1003, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "FN-07c vendor REJECTED: DELIVERED too (every status but DELIVERED): " + st(d));
			m = accept(firstTo(1003, HK, 951));
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			d = dr(1003, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() == GmmsStatus.UNDELIVERABLE.getCode(), "FN-07d recipient outside the prefix: the vendor's status: " + st(d));
			m = firstTo(1003, DST, 951);
			fail(m, GmmsStatus.UNDELIVERABLE);
			d = dr(1003, m.getMsgID(), 5000);
			check(d != null && d.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "FN-07e the vendor refuses the MT: failure DR (way 3 only rewrites vendor DRs): " + st(d));

			// ---- way 4 ----
			m = accept(firstTo(1005, DST, 951));
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			d = dr(1005, m.getMsgID(), 3000);
			check(d != null && d.getStatusCode() == GmmsStatus.UNDELIVERABLE.getCode(), "FN-08a timeout 3 s, the vendor's DR first: forwarded: " + st(d));
			final String f8 = m.getMsgID();
			Thread.sleep(4500);
			check(drs(1005, f8) == 1, "FN-08b the timeout passes: no second DR: " + drs(1005, f8));
			long t0 = System.currentTimeMillis();
			m = accept(firstTo(1005, DST, 951));
			final String f9 = m.getMsgID();
			d = dr(1005, f9, 8000);
			long took = System.currentTimeMillis() - t0;
			check(d != null && d.getStatusCode() == GmmsStatus.DELIVERED.getCode() && took >= 2500, "FN-09a no vendor DR: DELIVERED after the timeout: " + st(d) + " in " + took + " ms");
			vendorDr(m, GmmsStatus.UNDELIVERABLE);
			waitFor(() -> cdrCount(f9, "OUT_DELI_REP") >= 1, 5000);
			Thread.sleep(800);
			check(drs(1005, f9) == 1 && cdrCount(f9, "OUT_DELI_REP") >= 1, "FN-09b the vendor's DR later: only recorded: DRs " + drs(1005, f9));
			m = accept(firstTo(1006, DST, 951));
			d = dr(1006, m.getMsgID(), 8000);
			check(d != null && d.getStatusCode() != GmmsStatus.DELIVERED.getCode(), "FN-09c SMSOptionFakeDRTimeoutStatus fail: a failure DR after the timeout: " + st(d));
			check(jedis.zcard("zset:fakedr:timeout:0") + jedis.zcard("zset:fakedr:timeout:1") == 0, "FN-09d nothing left waiting");

			// ---- old options ----
			m = accept(firstTo(1010, DST, 951));
			check(dr(1010, m.getMsgID(), 1500) == null, "FN-14a customer SMSOptionSendFakeDR no longer used: no fake DR");
			check(itLog("SMSOptionSendFakeDR is no longer used"), "FN-14b WARN for it at load");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		exit();
	}
}
