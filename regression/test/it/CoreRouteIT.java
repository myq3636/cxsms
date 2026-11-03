package it;

import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Core routing priority: number, sender, keyword, prefix, backup, default; proportional split; blackhole. */
public class CoreRouteIT extends CoreBizSupport {
	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder(cust("V401", 401, "") + cust("V402", 402, "") + cust("V403", 403, "") + cust("V404", 404, "") + cust("V405", 405, ""));
		cm.append(cust("R500", 500, "")).append(cust("R501", 501, "")).append(cust("R502", 502, "")).append(cust("R503", 503, "CustomerServiceType: SVC\n"));
		cm.append(cust("R504", 504, "BlackholePercent: (Percent=100)\n")).append(cust("R505", 505, "")).append(cust("BH", 490, "Role: blackhole\n")).append(cust("R506", 506, ""));
		appendCm(cm.toString());
		route("R500", 500, "A2PRoutingRelay: (PhonePrefix=86,Relay=V401);(PhonePrefix=8613,Relay=V402)",
				"A2PSenderRoutingRelay: (PhonePrefix=86,Sender=SENDA,Relay=V403)",
				"A2PContentKeywordRoutingRelay: (PhonePrefix=86,KeywordId=K1,Relay=V404)",
				"A2PBackupRoutingRelay: (PhonePrefix=852,Relay=V405)",
				"A2PNumberRoutingRelay: (Number=8613800000099,Relay=405,Priority=1)");
		route("R505", 505, "A2PRoutingRelay: (PhonePrefix=86,Relay=BH)");
		route("R506", 506, "A2PRoutingRelay: (PhonePrefix=86,Relay=V401)",
				"A2PNumberRoutingRelay: (Number=8613800000090;8613800000091,Relay=405,Priority=1);(Number=8613800000092,Relay=403,Priority=2);(Number=8613800000092,Relay=402,Priority=1);(Number=8613800000093,Relay=999,Priority=1);(Number=8613800000094,Relay=402:50,403:50,Priority=1)");
		route("R501", 501, "A2PRoutingRelay: (PhonePrefix=86,Relay=V401:20,V402:80)");
		route("R502", 502, "A2PRoutingRelay: (PhonePrefix=86,Relay=V401)");
		route("R504", 504, "A2PRoutingRelay: (PhonePrefix=86,Relay=V401)");
		Files_writeDefault();
		conf("contentTemplate.cfg", "[K1]\nEnglish: your otp .*\n");
		java.nio.file.Path cfg = java.nio.file.Paths.get(home + "Gmms/GmmsConfig.properties");
		java.nio.file.Files.write(cfg, (new String(java.nio.file.Files.readAllBytes(cfg), "UTF-8") + "\nBlackholeSsid=490\n").getBytes("UTF-8"));
		try {
			start();
			Thread.sleep(800);
			Out o;
			o = send(500, "18667991226", "8618600000000", "plain"); check(o.routedTo(401), "RT-01a prefix 86: " + o);
			o = send(500, "18667991226", DST, "plain"); check(o.routedTo(402), "RT-01b longest prefix 8613 wins: " + o);
			o = send(500, "SENDA", DST, "plain"); check(o.routedTo(403), "RT-02 sender routing beats the prefix: " + o);
			o = send(500, "18667991226", DST, "your otp 1234"); check(o.routedTo(404), "RT-03a keyword routing beats the prefix: " + o);
			o = send(500, "SENDA", DST, "your otp 1234"); check(o.routedTo(403), "RT-03b sender routing beats keyword routing: " + o);
			o = send(500, "18667991226", "85212345678", "plain"); check(o.routedTo(405), "RT-06 no main route for 852: backup route: " + o);
			o = send(500, "18667991226", "8613800000099", "plain"); check(o.routedTo(405), "RT-07 number routing beats the prefix: " + o);
			o = send(500, "SENDA", "8613800000099", "plain"); check(o.routedTo(405), "RT-07b number routing beats sender routing: " + o);
			o = send(506, "18667991226", "8613800000090", "plain"); check(o.routedTo(405), "RT-07c several numbers in one group, first: " + o);
			o = send(506, "18667991226", "8613800000091", "plain"); check(o.routedTo(405), "RT-07d several numbers in one group, second: " + o);
			o = send(506, "18667991226", "8613800000092", "plain"); check(o.routedTo(402), "RT-07e same number twice: the lowest Priority wins: " + o);
			o = send(506, "18667991226", "8613800000089", "plain"); check(o.routedTo(401), "RT-07f number not listed: prefix routing: " + o);
			o = send(502, "18667991226", "8613800000090", "plain"); check(o.routedTo(401), "RT-07g number routing is per customer: another customer uses its prefix route: " + o);
			o = send(506, "18667991226", "8613800000093", "plain"); check(o.routedTo(401), "RT-07h ssid of no channel: falls back to prefix routing: " + o);
			java.util.Set<Integer> mix = new java.util.HashSet<>();
			for (int i = 0; i < 30; i++) { o = send(506, "18667991226", "8613800000094", "m" + i); mix.add(o.rssid); }
			check(mix.equals(new java.util.HashSet<>(java.util.Arrays.asList(402, 403))), "RT-07i split on one number, 402:50,403:50: both used: " + mix);
			o = send(502, "18667991226", "85212345678", "plain"); check(o.rssid == -1 && o.dr != null && o.acked, "RT-13 no route: failure DR, acked: " + o);
			o = send(503); check(o.routedTo(403), "RT-12 customer without a route: default route of its service type: " + o);
			int v1 = 0, v2 = 0, other = 0;
			for (int i = 0; i < 300; i++) { GmmsMessage m = msg(501, "18667991226", DST, "m" + i); com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceSubmitMessage(m); }
			final int total = 300; waitFor(() -> count(401, "501_") + count(402, "501_") >= total, 60000);
			v1 = count(401, "501_"); v2 = count(402, "501_");
			check(v1 + v2 == 300 && v1 >= 30 && v1 <= 90, "RT-04 split 20:80 over 300 MTs: " + v1 + "/" + v2);
			o = send(505); final String bh = o.id;
			check(o.rssid == -1 && logLine(bh, "[CDR] type=OUT_SUBMIT status=0") != null && logLine(bh, "[CDR] type=OUT_DELI_REP status=10000") != null, "RT-16a route to a Role=blackhole channel: not sent out, CDRs as if sent and delivered: " + o);
			o = send(504); check(o.routedTo(401), "RT-16b BlackholePercent=100: first MT of a timer window still goes out (number == 1): " + o);
			o = send(504); check(o.rssid == -1 && o.inCdr == 1, "RT-16c BlackholePercent=100: next MT of the window is not sent out: " + o);
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
	static int count(int rssid, String prefix) throws Exception {
		int n = 0;
		for (String k : jedis.keys("stream:mt:routed:" + rssid + ":*"))
			for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
				GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && m.getMsgID().startsWith(prefix)) n++;
			}
		return n;
	}
	static void Files_writeDefault() throws Exception {
		java.nio.file.Files.write(java.nio.file.Paths.get(home + "conf/routing/SVC_default_routingInfo.cfg"), "A2PRoutingRelay: (PhonePrefix=86,Relay=V403)\n".getBytes("UTF-8"));
	}
}
