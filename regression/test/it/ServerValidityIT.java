package it;

import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.message.gmms.GmmsMessage;

/**
 * The validity_period of a customer's submit_sm at the real server (Smpp.createGmmsMessage): with
 * SMSOptionParseValidityPeriod the absolute (YYMMDDhhmmsstnn+/-, nn quarters of an hour) or relative (YYMMDDhhmmss000R)
 * time is the expiry when within Min/MaxMessageExpireTimeFromCust (60..10080 min by default); else, and without the
 * option or a validity, the customer's ExpireTime (hours). The expiry date is kept as GMT wall clock (getGMTTime): the
 * parsed time was kept as the real instant, the time zone offset (8 h in Asia/Shanghai) later than the default and
 * every check of it. SMSOptionValidityPeriodNoRange: the validity is read (without SMSOptionParseValidityPeriod too) and
 * taken without the range check.
 */
public class ServerValidityIT extends ItSupport {
	static final int PORT = 17789;
	static int no = 0;

	static String cust(String name, int ssid, String user, String opts) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\n" + opts
				+ "IncomingConnection: (ConnName=" + name + "_c1,userName=" + user + ",password=pw" + ssid
				+ ",URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n";
	}

	/** the MT the server wrote to stream:mt:pending, by its text */
	static GmmsMessage find(int ssid, String text) throws Exception {
		long end = System.currentTimeMillis() + 5000;
		while (System.currentTimeMillis() < end) {
			for (String k : jedis.keys("stream:mt:pending:" + ssid + ":*"))
				for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
					GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
					if (m != null && text.equals(m.getTextContent())) return m;
				}
			Thread.sleep(50);
		}
		return null;
	}

	/** minutes from now (real) to the expiry the message carries, read as GMT wall clock */
	static long minutesLeft(SmppTestClient c, int ssid, String validity) throws Exception {
		String text = "VP" + (++no);
		c.submit("SENDER", "8613800000000", text, validity);
		GmmsMessage m = find(ssid, text);
		if (m == null) return Long.MIN_VALUE;
		if (m.getExpiryDate() == null) return Long.MAX_VALUE;
		long now = System.currentTimeMillis();
		long gmtNow = now - TimeZone.getDefault().getOffset(now);
		return Math.round((m.getExpiryDate().getTime() - gmtNow) / 60000.0);
	}

	/** an absolute validity for the instant ms from now, written in the zone of nn quarters of an hour and sign p */
	static String absolute(long ms, int nn, char p) {
		SimpleDateFormat f = new SimpleDateFormat("yyMMddHHmmss");
		f.setTimeZone(TimeZone.getTimeZone("GMT" + p + (nn / 4) + ":" + String.format("%02d", nn % 4 * 15)));
		return f.format(new Date(System.currentTimeMillis() + ms)) + "0" + String.format("%02d", nn) + p;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg");
		byte[] orig = Files.readAllBytes(cm);
		StringBuilder x = new StringBuilder(new String(orig, "UTF-8"));
		x.append(cust("V95", 95, "v95", "SMSOptionParseValidityPeriod: true\nExpireTime: 5\n"));
		x.append(cust("V96", 96, "v96", "ExpireTime: 5\n"));
		// only the new option: it reads the validity period too
		x.append(cust("V97", 97, "v97", "SMSOptionValidityPeriodNoRange: true\nExpireTime: 5\n"));
		Files.write(cm, x.toString().getBytes("UTF-8"));
		try {
			boot();
			jedis.set("thcon", "1");
			props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
			MultiSmppServer server = new MultiSmppServer();
			server.startService();
			Thread.sleep(1500);
			System.out.println("INFO JVM zone " + TimeZone.getDefault().getID());
			SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, 5, null);
			check(c.bind("v95", "pw95"), "VP-00 customer with SMSOptionParseValidityPeriod bound");
			long m;
			m = minutesLeft(c, 95, absolute(2 * 3600000L, 32, '+'));
			check(m == 120, "VP-01 absolute in GMT+8, 2 h from now: expiry in 120 min: " + m);
			m = minutesLeft(c, 95, absolute(2 * 3600000L, 22, '+'));
			check(m == 120, "VP-02 absolute in GMT+5:30 (nn 22): 120 min: " + m);
			m = minutesLeft(c, 95, absolute(2 * 3600000L, 20, '-'));
			check(m == 120, "VP-03 absolute in GMT-5 (nn 20, '-'): 120 min: " + m);
			m = minutesLeft(c, 95, "000000020000000R");
			check(m == 120, "VP-04 relative 2 h: 120 min: " + m);
			m = minutesLeft(c, 95, "000001120000000R");
			check(m == 36 * 60, "VP-05 relative 1 day 12 h (within MaxMessageExpireTimeFromCust): 2160 min: " + m);
			m = minutesLeft(c, 95, "000000003000000R");
			check(m == 300, "VP-06 relative 30 min, under MinMessageExpireTimeFromCust 60: the customer's ExpireTime 5 h: " + m);
			m = minutesLeft(c, 95, "000008000000000R");
			check(m == 300, "VP-07 relative 8 days, over MaxMessageExpireTimeFromCust (10080 min by default): ExpireTime 5 h: " + m);
			m = minutesLeft(c, 95, "");
			check(m == 300, "VP-08 no validity: ExpireTime 5 h: " + m);
			m = minutesLeft(c, 95, "000000000500000R");
			check(m == 300, "VP-06b relative 5 min, under MinMessageExpireTimeFromCust: ExpireTime 5 h: " + m);
			m = minutesLeft(c, 95, "not a time");
			check(m == 300, "VP-09 validity not understood: ExpireTime 5 h: " + m);
			c.close();
			SmppTestClient d = new SmppTestClient("127.0.0.1", PORT, 5, null);
			check(d.bind("v96", "pw96"), "VP-10a customer without SMSOptionParseValidityPeriod bound");
			m = minutesLeft(d, 96, "000000020000000R");
			check(m == 300, "VP-10b its validity is not read: ExpireTime 5 h: " + m);
			d.close();
			SmppTestClient e = new SmppTestClient("127.0.0.1", PORT, 5, null);
			check(e.bind("v97", "pw97"), "VP-11a customer with SMSOptionValidityPeriodNoRange (and no SMSOptionParseValidityPeriod) bound");
			m = minutesLeft(e, 97, "000000000500000R");
			check(m == 5, "VP-11b relative 5 min taken as it is, under MinMessageExpireTimeFromCust: 5 min: " + m);
			m = minutesLeft(e, 97, absolute(5 * 60000L, 32, '+'));
			check(m == 5, "VP-11c absolute 5 min from now: 5 min: " + m);
			m = minutesLeft(e, 97, "000008000000000R");
			check(m == 8 * 24 * 60, "VP-11d relative 8 days, over MaxMessageExpireTimeFromCust: taken, 11520 min: " + m);
			m = minutesLeft(e, 97, "");
			check(m == 300, "VP-11e no validity: ExpireTime 5 h: " + m);
			m = minutesLeft(e, 97, "not a time");
			check(m == 300, "VP-11f validity not understood: ExpireTime 5 h: " + m);
			e.close();
			server.stopService();
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		} finally {
			Files.write(cm, orig);
		}
		exit();
	}
}
