package it;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import com.king.db.DbObject;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/**
 * The DR the gateway gives when the vendor gives none (MQM WDQ ExpiredMessageTask, MessageStoreManager.getExpiredMessage)
 * on a real MySQL: a temporary database (gmms_wdq_it) with a WDQ like gmms.WDQ. An MT waits in WDQ for its DR (moved
 * there from Redis after Redis_RetrieveRedisInterval, or at once when it expires within 10 minutes); once its expiry
 * date (GMT wall clock) is past and before the final expiry (MessageFinalExpireTime - MessageExpireTime later) the
 * customer gets EXPIRED (SmsOptionIsSupportExpiredDR: yes) or DELIVERED; the row is deleted, or, when the customer's
 * expiry is before the vendor's final expiry, kept as Retrieved (12000): a vendor DR coming later only writes its CDR
 * and deletes it, the customer gets no second DR. Before 2026-11-26 the WDQ check used the real now: an expiry kept as GMT wall clock looked the
 * time zone offset (8 h in Asia/Shanghai) earlier, a 5 minute validity got its DR at the first scan.
 *
 * Manual suite (manual-suites.txt): WDQ_DB_HOST, WDQ_DB_USER, WDQ_DB_PASSWORD (a MySQL with gmms.WDQ); without them SKIP.
 */
public class WdqExpiryIT extends CoreBizSupport {
	static final String DB = "gmms_wdq_it";
	static Connection admin;

	static int count(String where) throws Exception {
		try (Statement s = admin.createStatement(); ResultSet r = s.executeQuery("SELECT COUNT(*) FROM " + DB + ".WDQ WHERE " + where)) {
			r.next();
			return r.getInt(1);
		}
	}

	/** an MT the vendor accepted, waiting for its DR, its expiry minutes from now (GMT wall clock, as the gateway keeps it) */
	static GmmsMessage waiting(String id, int ossid, long minutes) {
		GmmsMessage m = mt(id, ossid, 8, 0);
		m.setOutMsgID("OUT-" + id);
		m.setStatus(GmmsStatus.SUCCESS);
		m.setActionCode(1);
		m.setExpiryDate(new Date(GmmsUtility.getInstance().getGMTTime().getTime() + minutes * 60000L));
		return m;
	}

	static GmmsMessage find(List<GmmsMessage> l, String id) {
		if (l != null) for (GmmsMessage m : l) if (id.equals(m.getMsgID())) return m;
		return null;
	}

	public static void main(String[] args) throws Exception {
		String host = System.getenv("WDQ_DB_HOST"), user = System.getenv("WDQ_DB_USER"), pw = System.getenv("WDQ_DB_PASSWORD");
		if (host == null || user == null || pw == null) {
			System.out.println("SKIP WDQ_DB_HOST, WDQ_DB_USER, WDQ_DB_PASSWORD not set");
			System.out.println("ALL CHECKS PASSED");
			System.exit(0);
		}
		home = System.getProperty("a2p_home");
		String url = "jdbc:mysql://" + host + ":3306/" + DB + "?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai";
		admin = DriverManager.getConnection("jdbc:mysql://" + host + ":3306/?serverTimezone=Asia/Shanghai", user, pw);
		try (Statement s = admin.createStatement()) {
			s.execute("CREATE DATABASE IF NOT EXISTS " + DB);
			s.execute("DROP TABLE IF EXISTS " + DB + ".WDQ");
			s.execute("CREATE TABLE " + DB + ".WDQ LIKE gmms.WDQ");
		}
		appendCm(cust("E61", 61, "SmsOptionIsSupportExpiredDR: yes\n") + cust("E62", 62, ""));
		try {
			boot();
			// the message store on the temporary database
			Properties p = new Properties();
			p.setProperty("DS_gmms_Driver", "com.mysql.cj.jdbc.Driver");
			p.setProperty("DS_gmms_URL", url);
			p.setProperty("DS_gmms_Login", user);
			p.setProperty("DS_gmms_Password", pw);
			p.setProperty("DS_gmms_Dialect", "org.hibernate.dialect.MySQLDialect");
			p.setProperty("DS_gmms_MaxPoolSize", "4");
			p.setProperty("DS_gmms_MinPoolSize", "1");
			Field dcf = com.king.db.DataControl.class.getDeclaredField("dsConfigs");
			dcf.setAccessible(true);
			Hashtable<Object, Object> ds = new Hashtable<Object, Object>();
			ds.put("gmms", new DbObject(p, "gmms"));
			dcf.set(null, ds);
			msm.setDsName("gmms");
			Field af = MessageStoreManager.class.getDeclaredField("asyncSqlWriterDisabled");   // inserts at once
			af.setAccessible(true);
			af.set(null, Boolean.TRUE);
			props.setProperty("DescendingTime", "10");   // as the servers (the WDQ query of other A2Ps uses it, no default)
			int fin = GmmsUtility.getInstance().getFinalExpireTimeInMinute() - GmmsUtility.getInstance().getExpireTimeInMinute();
			System.out.println("INFO zone " + TimeZone.getDefault().getID() + ", final expiry " + fin + " min after the expiry");

			check(msm.insertMessageToDB(waiting("WA", 61, -1), MessageStoreManager.WDQ), "WQ-00 inserted into WDQ");
			msm.insertMessageToDB(waiting("WB", 61, 5), MessageStoreManager.WDQ);
			msm.insertMessageToDB(waiting("WC", 62, -1), MessageStoreManager.WDQ);
			msm.insertMessageToDB(waiting("WD", 61, -(fin + 10)), MessageStoreManager.WDQ);
			check(count("1=1") == 4, "WQ-01 4 MTs waiting: " + count("1=1"));

			ArrayList<GmmsMessage> l = msm.getExpiredMessage(MessageStoreManager.WDQ);
			GmmsMessage a = find(l, "WA"), b = find(l, "WB"), c = find(l, "WC"), d = find(l, "WD");
			check(a != null && a.getStatusCode() == GmmsStatus.EXPIRED.getCode() && GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(a.getMessageType()),
					"WQ-02 expired 1 min ago, customer SmsOptionIsSupportExpiredDR yes: a DR EXPIRED (10200): " + (a == null ? "none" : a.getMessageType() + " " + a.getStatusCode()));
			check(c != null && c.getStatusCode() == GmmsStatus.DELIVERED.getCode(),
					"WQ-03 expired, customer without SmsOptionIsSupportExpiredDR: a DR DELIVERED: " + (c == null ? "none" : String.valueOf(c.getStatusCode())));
			check(b == null, "WQ-04 expiry in 5 min: no DR yet (the real now made it 8 h past in +8)");
			check(d == null, "WQ-05 past the final expiry: no DR");
			check(a != null && a.getRSsID() == 8 && a.getOSsID() == 61 && "OUT-WA".equals(a.getOutMsgID()), "WQ-06 the DR is the MT's (ossid, rssid, outMsgId kept)");
			// the customer's expiry is before the vendor's final expiry: the row stays, marked Retrieved, for the vendor's DR
			check(count("MsgID IN ('WA','WC') AND StatusCode=12000 AND ActionCode=0") == 2 && count("MsgID IN ('WB','WD') AND StatusCode=0") == 2,
					"WQ-07 the DR'd rows marked Retrieved (12000, ActionCode 0), the others unchanged: "
							+ count("MsgID IN ('WA','WC') AND StatusCode=12000") + " " + count("MsgID IN ('WB','WD') AND StatusCode=0"));
			GmmsMessage late = msm.getGmmsMessageByOutMsgID("OUT-WA");
			check(late != null && GmmsStatus.RETRIEVED.getText().equalsIgnoreCase(late.getStatusText()), "WQ-08a the vendor's DR coming later finds the Retrieved MT: "
					+ (late == null ? "none" : late.getStatusText()));
			if (late != null) {
				late.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				late.setStatusCode(GmmsStatus.DELIVERED.getCode());
				msm.handleOutDeliveryReportReq(late);
				Thread.sleep(500);
				check(count("MsgID='WA'") == 0, "WQ-08b the late DR: its row deleted");
				check(inStreams("stream:dr:pending:61*", "WA").isEmpty() && inStreams("stream:mqm*", "WA").isEmpty(),
						"WQ-08c the late DR is not sent to the customer again (only its CDR)");
			}
			check(msm.getGmmsMessageByOutMsgID("OUT-WB") != null, "WQ-09 the waiting MT is still found by its DR");
			check(msm.getExpiredMessage(MessageStoreManager.WDQ) == null, "WQ-10 the next scan: nothing more");

			// five minutes later
			try (Statement s = admin.createStatement()) {
				s.executeUpdate("UPDATE " + DB + ".WDQ SET ExpiryDate = DATE_SUB(ExpiryDate, INTERVAL 6 MINUTE) WHERE MsgID='WB'");
			}
			l = msm.getExpiredMessage(MessageStoreManager.WDQ);
			b = find(l, "WB");
			check(b != null && b.getStatusCode() == GmmsStatus.EXPIRED.getCode(), "WQ-11 its 5 min passed: a DR EXPIRED: " + (b == null ? "none" : String.valueOf(b.getStatusCode())));

			// the vendor accepted an MT of a 5 minute validity: not in Redis, straight to WDQ (expires within 10 minutes)
			GmmsMessage e = waiting("WE", 61, 5);
			e.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
			msm.handleOutSubmitRes(e, false);
			check(count("MsgID='WE'") == 1, "WQ-12 accepted with 5 min left: in WDQ at once: " + count("MsgID='WE'"));
			check(find(msm.getExpiredMessage(MessageStoreManager.WDQ), "WE") == null, "WQ-13 and no DR before its 5 min");
			GmmsMessage f = waiting("WF", 61, 60);
			f.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
			msm.handleOutSubmitRes(f, false);
			check(count("MsgID='WF'") == 0, "WQ-14 accepted with 60 min left: waits in Redis, not WDQ (the real now gave -420 min: WDQ)");
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		} finally {
			try (Statement s = admin.createStatement()) {
				s.execute("DROP DATABASE IF EXISTS " + DB);
			} catch (Exception ignored) {
			}
		}
		exit();
	}
}
