package it;

import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.*;

import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/**
 * The routing cases the other routing suites leave out, on the real Core with an ENUM / IR21 stand-in (EnumMock):
 * OP the operator layer from an ENUM lookup (operator, country, -2, its backup) and the operator layer over the prefixes;
 * EN the lookup itself: by prefix only, the global URL, a failure / error / timeout routed by the prefixes, concurrent
 * lookups; IR IR21 over DNS (the Core runs dig); SR the second routing; RR Relay=V1,V2,V3 in turn; EX ExcludeKeywordId;
 * SY vendorRoutingReplacement.cfg reloaded with RELOAD_SYSTEM_REPLACE; BH BlackholePercent by the channel (R_SSID) and
 * by ratio; DW the delivery window of the channel. A channel of the operator, number or second layer keeps the operator
 * in roperator (setRSsID sets it to the channel; the old routers set it back, the routing by rules did not before
 * 2026-11-27): the CDR's operator column, the operator backup on a reroute; the channel's settings go by rssid.
 */
public class RoutingExtraIT extends CoreRetryIT {
	static final String URL = "http://127.0.0.1:15201/mnp/realenumquery2";
	static final String DNS = "127.0.0.1:15354";
	static EnumMock mock;

	static String opRoutes(int ssid) {
		return "CCBSSID: " + ssid + "\n"
				+ "A2POPRouting: (Priority=10,Opid=860001,Relay=V931);(Priority=10,Opid=860000,Relay=V932);(Priority=10,Opid=-2,Relay=V933);(Priority=10,Opid=860003,Relay=V999)\n"
				+ "A2POPBackupRouting: (Priority=10,Opid=860003,Relay=V936);(Priority=10,Opid=860001,Relay=V939)\n"
				+ "A2PMainRouting: (Priority=10,PhonePrefix=8613800,Relay=V934);(Priority=20,PhonePrefix=86,Relay=V935)\n"
				+ "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V940)\n";
	}
	static void routeFile(String name, String text) throws Exception {
		Files.write(Paths.get(home + "conf/routing/" + name + "_routingInfo.cfg"), text.getBytes("UTF-8"));
	}
	static int to(int ossid, String recipient) throws Exception { return send(ossid, "18667991226", recipient, "hello").rssid; }
	static int to(int ossid, String recipient, String text) throws Exception { return send(ossid, "18667991226", recipient, text).rssid; }
	static void writeOld(Path p, String text) throws Exception {
		Files.write(p, text.getBytes("UTF-8"));
		Files.setLastModifiedTime(p, FileTime.fromMillis(System.currentTimeMillis() - 5000));
	}
	static void control(String cmd) throws Exception {
		jedis.publish("channel:gmms:control", cmd);
		Thread.sleep(2000);
	}
	static boolean digInstalled() {
		for (String d : System.getenv("PATH").split(":")) if (Files.isExecutable(Paths.get(d, "dig"))) return true;
		return false;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		mock = EnumMock.start("127.0.0.1:15201", DNS, null);
		mock.setRules(Arrays.asList(
				"8613800 86,1,460,00,ChinaMobile",
				"8613801 86,2,460,01,ChinaUnicom",
				"8613803 86,3,460,03,ChinaTelecom",
				"8613811 852,5,454,05,HK5",
				"8613820 86,1,460,00,ChinaMobile delay=100",
				"8613821 86,2,460,01,ChinaUnicom delay=100",
				"8613902 fail", "8613903 86,1,460,00,ChinaMobile delay=35000", "8613904 http500", "8613905 badjson", "8613906 empty"));
		StringBuilder cm = new StringBuilder();
		for (int s = 931; s <= 954; s++) cm.append(cust("V" + s, s, s == 931 ? "SMSOptionSubmitRetryCode: 2500\nSMSOptionMTRetryPolicy: 1S,1S\n" : s == 950 ? window() : ""));
		cm.append(cust("BH", 490, "Role: blackhole\n"));
		String enumOn = "SMSOptionEnumQueryRecPrefix: 86\nSMSOptionEnumRoutingURL: " + URL + "\n";
		cm.append(cust("C970", 970, ""));
		cm.append(cust("C971", 971, enumOn));
		cm.append(cust("C972", 972, "SMSOptionEnumQueryRecPrefix: 852\nSMSOptionEnumRoutingURL: " + URL + "\n"));
		cm.append(cust("C973", 973, "SMSOptionEnumQueryRecPrefix: 86\n"));   // the global mnpqueryurl
		cm.append(cust("C974", 974, "SMSOptionEnumQueryRecPrefix: 86\nSMSOptionIR21EnumQueryRecPrefix: 86\nSMSOptionEnumIR21RoutingURL: 127.0.0.1 -p 15354\n"));
		String sec = "smsOptionNeedSupportSecondRouting: true\nsmsOptionNeedSupportSecondRoutingExpiredTime: 3\n";
		cm.append(cust("C975", 975, sec + "SMSOptionSecondRoutingRecPrefix: 86\n"));
		cm.append(cust("C976", 976, sec + "SMSOptionSecondRoutingRecPrefix: all\n"));
		cm.append(cust("C977", 977, ""));
		cm.append(cust("C984", 984, sec + "SMSOptionSecondRoutingRecPrefix: all\n"));
		cm.append(cust("C978", 978, "")).append(cust("C979", 979, "")).append(cust("C980", 980, ""));
		cm.append(cust("C981", 981, enumOn + "BlackholePercent: (Percent=0);(R_SSID=947,Percent=100)\n"));
		cm.append(cust("C983", 983, enumOn));
		cm.append(cust("C982", 982, "BlackholePercent: (Percent=30)\n"));
		appendCm(cm.toString());
		for (int s : new int[] { 970, 971, 972, 973, 974 }) routeFile("C" + s, opRoutes(s));
		String secRoutes = "A2PSecRouting: (Priority=10,PhonePrefix=86,Relay=V937)\nA2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V938);(Priority=10,PhonePrefix=852,Relay=V938)\n";
		for (int s : new int[] { 975, 976, 977 }) routeFile("C" + s, "CCBSSID: " + s + "\n" + secRoutes);
		// the A2PSecRouting groups: 86 before all, whatever the Priority
		route("C984", 984, "A2PSecRouting: (Priority=10,PhonePrefix=all,Relay=V952);(Priority=20,PhonePrefix=86,Relay=V953)",
				"A2PMainRouting: (Priority=10,PhonePrefix=86;44,Relay=V954)");
		route("C978", 978, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V941,V942,V943)");
		route("C979", 979, "A2PMainRouting: (Priority=10,PhonePrefix=86,ExcludeKeywordId=K7,Relay=V944);(Priority=20,PhonePrefix=86,Relay=V945)");
		route("C980", 980, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V946)");
		route("C981", 981, "A2POPRouting: (Priority=10,Opid=860001,Relay=V947);(Priority=10,Opid=860000,Relay=V948)");
		route("C983", 983, "A2POPRouting: (Priority=10,Opid=860001,Relay=V950);(Priority=10,Opid=860000,Relay=V951)");
		route("C982", 982, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V949)");
		conf("contentTemplate.cfg", "[K7]\nEnglish: promo .*\n");
		Path replace = Paths.get(home + "conf/vendorRoutingReplacement.cfg");
		writeOld(replace, "86:999:1:2\n");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nBlackholeSsid=490\n").getBytes("UTF-8"));
		try {
			start();
			q = StreamQueueManager.getInstance();
			com.king.gmms.messagequeue.ResultStreamConsumer.getInstance().start();
			props.setProperty("mnpqueryurl", URL);
			Thread.sleep(800);
			int r;

			// ---- OP: the operator layer from the ENUM lookup ----
			r = to(971, "8613800000001");
			List<GmmsMessage> op1 = new ArrayList<GmmsMessage>();
			op1.addAll(byRecipient("stream:mt:routed:931:*", "8613800000001"));
			check(!op1.isEmpty() && op1.get(0).getRoperator() == 860001, "OP-00 routed by the operator layer: roperator stays the operator 860001 (not the channel 931): "
					+ (op1.isEmpty() ? "none" : op1.get(0).getRoperator()));
			check(r == 931, "OP-01 operator 860001 (86 + operator 1): its own rule V931, over the longer prefix 8613800 (V934): " + r);
			r = to(971, "8613801000001"); check(r == 932, "OP-02 operator 860002 without a rule: the country rule 860000, V932: " + r);
			r = to(971, "8613811000001"); check(r == 933, "OP-03 operator 8520005 (no rule, no 8520000): Opid=-2, V933: " + r);
			r = to(971, "8613803000001"); check(r == 936, "OP-04 operator rule to a channel not in cm.cfg: the operator backup A2POPBackupRouting, V936: " + r);
			check(mock.httpQueries("8613800000001") == 1, "OP-05 one lookup for the MT: " + mock.httpQueries("8613800000001"));
			// the operator channel fails: the operator layer's backup 1 (A2POPBackupRouting 860001 -> V939), not the prefixes' (V940)
			GmmsMessage op6 = msg(971, "18667991226", "8613800000007", "op6");
			q.produceSubmitMessage(op6);
			GmmsMessage op6out = out(931, op6.getMsgID(), 1);
			if (op6out != null) fail(op6out, com.king.message.gmms.GmmsStatus.COMMUNICATION_ERROR);
			waitFor(() -> routedTo(op6) != 931, 8000);
			waitFor(() -> !inStreams("stream:mt:routed:939:*", op6.getMsgID()).isEmpty() || !inStreams("stream:mt:routed:940:*", op6.getMsgID()).isEmpty(), 8000);
			String where = !inStreams("stream:mt:routed:939:*", op6.getMsgID()).isEmpty() ? "V939" : !inStreams("stream:mt:routed:940:*", op6.getMsgID()).isEmpty() ? "V940" : "none";
			check(op6out != null && "V939".equals(where), "OP-06 operator channel V931 fails (2500): the operator layer's backup 1 V939: " + where
					+ " (queries " + mock.httpQueries("8613800000007") + ")");
			final String op6id = op6.getMsgID();
			waitFor(() -> { try { return !cdrLines(op6id, "OUT_SUBMIT").isEmpty(); } catch (Exception e) { return false; } }, 5000);
			List<String> cdr6 = cdrLines(op6id, "OUT_SUBMIT");
			String col = op6out == null ? "" : "," + op6out.getOoperator() + ",1,";
			check(!cdr6.isEmpty() && cdr6.get(0).contains(col), "OP-07 the OUT_SUBMIT CDR of V931: operator column 1 (860001 % 10000): " + (cdr6.isEmpty() ? "no CDR" : cdr6.get(0)));

			// ---- EN: the lookup ----
			r = to(970, "8613800000002"); check(r == 934 && mock.httpQueries("8613800000002") == 0, "EN-01 no SMSOptionEnumQueryRecPrefix: no lookup, the prefixes (V934): " + r);
			r = to(972, "8613800000003"); check(r == 934 && mock.httpQueries("8613800000003") == 0, "EN-02 SMSOptionEnumQueryRecPrefix 852, to 86: no lookup: " + r);
			r = to(973, "8613800000004"); check(r == 931 && mock.httpQueries("8613800000004") == 1, "EN-03 no SMSOptionEnumRoutingURL: the global mnpqueryurl, V931: " + r);
			String[][] bad = { { "8613902000001", "errmsg FAIL" }, { "8613904000001", "HTTP 500" }, { "8613905000001", "not JSON" }, { "8613906000001", "SUCCESS without data" }, { "8613990000001", "number not found" } };
			boolean firstBad = true;
			for (String[] b : bad) {
				r = to(971, b[0]);
				if (firstBad) {
					firstBad = false;
					List<GmmsMessage> nm = new ArrayList<GmmsMessage>();
					nm.addAll(byRecipient("stream:mt:routed:935:*", b[0]));
					check(!nm.isEmpty() && nm.get(0).getRoperator() == 935, "OP-08 routed by the prefix layer: roperator is the channel 935, as before: " + (nm.isEmpty() ? "none" : nm.get(0).getRoperator()));
				}
				check(r == 935 && mock.httpQueries(b[0]) == 1, "EN-04 " + b[1] + ": no operator, the prefixes (86, V935), the MT not lost: " + r);
			}
			// concurrent lookups (one DNSClient, one HttpSession for every Core thread): every MT its own answer
			List<GmmsMessage> burst = new ArrayList<GmmsMessage>();
			for (int i = 0; i < 40; i++) {
				GmmsMessage m = msg(971, "18667991226", (i % 2 == 0 ? "8613820" : "8613821") + String.format("%06d", i), "c" + i);
				burst.add(m);
				q.produceSubmitMessage(m);
			}
			waitFor(() -> count(931, burst) + count(932, burst) + count(-1, burst) >= 40, 30000);
			int wrong = 0, lost = 0;
			for (GmmsMessage m : burst) {
				int want = m.getRecipientAddress().startsWith("8613820") ? 931 : 932;
				int got = routedTo(m);
				if (got == -1) lost++; else if (got != want) { wrong++; if (wrong < 4) System.out.println("INFO EN-05 " + m.getRecipientAddress() + " routed to " + got + ", not " + want); }
			}
			check(wrong == 0 && lost == 0, "EN-05 40 MTs at once, two operators, lookups of 100 ms: each by its own answer: " + wrong + " wrong, " + lost + " not routed");
			// a lookup that does not answer: the Core waits EnumQueryTimeoutMs (5 s by default), then routes by the prefixes
			GmmsMessage slow = msg(971, "18667991226", "8613903000001", "slow");
			long t0 = System.currentTimeMillis();
			q.produceSubmitMessage(slow);
			GmmsMessage quick = msg(971, "18667991226", "8613800000005", "quick");
			q.produceSubmitMessage(quick);
			boolean quickRouted = waitFor(() -> routedTo(quick) == 931, 5000);
			long quickMs = System.currentTimeMillis() - t0;
			boolean slowRouted = waitFor(() -> routedTo(slow) != -1, 20000);
			long slowMs = System.currentTimeMillis() - t0;
			System.out.println("INFO EN-06 lookup answering after 35 s: routed after " + slowMs + " ms to " + routedTo(slow) + "; an MT sent with it routed after " + quickMs + " ms");
			check(slowRouted && routedTo(slow) == 935 && slowMs >= 4500 && slowMs < 9000, "EN-06 lookup timeout (EnumQueryTimeoutMs 5000 by default): routed by the prefixes after " + slowMs + " ms: " + routedTo(slow));
			props.setProperty("EnumQueryTimeoutMs", "2000");   // as a hot reload of GmmsConfig puts it
			GmmsMessage slow2 = msg(971, "18667991226", "8613903000002", "slow2");
			long t2 = System.currentTimeMillis();
			q.produceSubmitMessage(slow2);
			boolean slow2Routed = waitFor(() -> routedTo(slow2) != -1, 20000);
			long slow2Ms = System.currentTimeMillis() - t2;
			props.remove("EnumQueryTimeoutMs");
			check(slow2Routed && routedTo(slow2) == 935 && slow2Ms >= 1500 && slow2Ms < 5000, "EN-06b EnumQueryTimeoutMs 2000 (changed at run time): routed after " + slow2Ms + " ms");
			check(quickRouted, "EN-07 another MT during that lookup is not held by it: " + quickMs + " ms");

			// ---- IR: IR21 over DNS ----
			javax.naming.directory.InitialDirContext dctx = new javax.naming.directory.InitialDirContext(new Hashtable<String, String>(
					Collections.singletonMap("java.naming.provider.url", "dns://" + DNS)) {{ put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory"); }});
			Object naptr = dctx.getAttributes("1.0.0.0.0.0.0.0.8.3.1.6.8.e164.arpa.", new String[] { "NAPTR" }).get("NAPTR");
			check(naptr != null && naptr.toString().contains("@cc86.opid1.mnc00.mcc460"), "IR-01 the stand-in answers NAPTR as the IR21 server: " + naptr);
			boolean dig = digInstalled();
			r = to(974, "8613800000006");
			System.out.println("INFO IR-02 dig installed: " + dig + ", DNS lookups: " + mock.dnsQueries("8613800000006") + ", routed to " + r);
			if (dig) check(r == 931 && mock.dnsQueries("8613800000006") >= 1, "IR-02 IR21 by dig: operator 860001, V931: " + r);
			else check(r == 934, "IR-02 no dig on this host: IR21 fails, routed by the prefixes (V934), the MT not lost: " + r);
			if (dig) {
				r = to(974, "8613801000006"); check(r == 932, "IR-03 IR21 by dig, operator 860002: the country rule, V932: " + r);
				GmmsMessage irSlow = msg(974, "18667991226", "8613903000003", "irslow");
				long t3 = System.currentTimeMillis();
				q.produceSubmitMessage(irSlow);
				boolean irRouted = waitFor(() -> routedTo(irSlow) != -1, 25000);
				long irMs = System.currentTimeMillis() - t3;
				check(irRouted && routedTo(irSlow) == 935 && irMs >= 4500 && irMs < 9000, "IR-04 IR21 answering after 35 s: dig gives up after EnumQueryTimeoutMs (one try), routed by the prefixes after " + irMs + " ms: " + routedTo(irSlow));
				r = to(974, "8613990000006"); check(r == 935, "IR-05 IR21 number not found (NXDOMAIN): routed by the prefixes, V935: " + r);
			}

			// ---- SR: second routing ----
			r = to(975, "8613700000001"); check(r == 938, "SR-01 first MT to a number: the main route V938: " + r);
			r = to(975, "8613700000001"); check(r == 937, "SR-02 again within 3 s: A2PSecRouting, V937: " + r);
			r = to(975, "8613700000002"); check(r == 938, "SR-03 another number, first: V938: " + r);
			r = to(975, "85212345678"); int r2 = to(975, "85212345678"); check(r == 938 && r2 == 938, "SR-04 852 not in SMSOptionSecondRoutingRecPrefix: main route both times: " + r + "," + r2);
			Thread.sleep(3500);
			r = to(975, "8613700000001"); check(r == 938, "SR-05 after smsOptionNeedSupportSecondRoutingExpiredTime (3 s): the main route again: " + r);
			r = to(976, "8613700000003"); r2 = to(976, "8613700000003"); check(r == 938 && r2 == 937, "SR-06 SMSOptionSecondRoutingRecPrefix all: second time V937: " + r + "," + r2);
			r = to(977, "8613700000004"); r2 = to(977, "8613700000004"); check(r == 938 && r2 == 938, "SR-07 smsOptionNeedSupportSecondRouting off: A2PSecRouting not used: " + r + "," + r2);
			r = to(984, "8613700000005"); r2 = to(984, "8613700000005");
			check(r == 954 && r2 == 953, "SR-08a A2PSecRouting all (P10) and 86 (P20), to 86 again: the 86 group V953, not all: " + r + "," + r2);
			r = to(984, "447700900001"); r2 = to(984, "447700900001");
			check(r == 954 && r2 == 952, "SR-08b to 44 again (no 44 group): the all group V952: " + r + "," + r2);

			// ---- RR: Relay=V941,V942,V943 in turn ----
			List<Integer> seq = new ArrayList<Integer>();
			for (int i = 0; i < 6; i++) seq.add(to(978, "8613800001" + i));
			check(new HashSet<Integer>(seq).equals(new HashSet<Integer>(Arrays.asList(941, 942, 943))) && seq.get(0).equals(seq.get(3)) && seq.get(1).equals(seq.get(4)) && seq.get(2).equals(seq.get(5)),
					"RR-01 Relay=V941,V942,V943: in turn, each twice in 6: " + seq);

			// ---- EX: ExcludeKeywordId ----
			r = to(979, "8613800002001", "hello"); check(r == 944, "EX-01 content not of K7: the rule with ExcludeKeywordId=K7, V944: " + r);
			r = to(979, "8613800002002", "promo 50 off"); check(r == 945, "EX-02 content of K7 (promo .*): excluded, the next rule V945: " + r);

			// ---- SY: RELOAD_SYSTEM_REPLACE ----
			r = to(980, "8613800003001"); check(r == 946, "SY-01 no replacement for 946: V946: " + r);
			writeOld(replace, "86:980:946:947\n86:-1:946:948\n");
			control("RELOAD_SYSTEM_REPLACE");
			r = to(980, "8613800003002"); check(r == 947, "SY-02 after RELOAD_SYSTEM_REPLACE, a customer rule and a -1 rule: the customer's wins, V947: " + r);
			writeOld(replace, "86:-1:946:948\n");
			control("RELOAD_SYSTEM_REPLACE");
			r = to(980, "8613800003003"); check(r == 948, "SY-03 only the -1 rule: V948: " + r);
			writeOld(replace, "86:999:1:2\n");
			control("RELOAD_SYSTEM_REPLACE");
			r = to(980, "8613800003004"); check(r == 946, "SY-04 the rule removed: V946 again: " + r);

			// ---- BH: BlackholePercent by operator (the ENUM answer) and by ratio ----
			Out b1 = send(981, "18667991226", "8613801004001", "bh"); check(b1.routedTo(948), "BH-01 the first MT of the timer window is sent (operator 860002 -> V948): " + b1);
			Out b2 = send(981, "18667991226", "8613800004002", "bh"); check(b2.rssid == -1 && b2.inCdr == 1, "BH-02 operator 860001 -> V947 (R_SSID=947,Percent=100, by the channel): blackholed: " + b2);
			Out b3 = send(981, "18667991226", "8613801004003", "bh"); check(b3.routedTo(948), "BH-03 operator 860002 -> V948 (Percent=0): sent: " + b3);
			Out b4 = send(981, "18667991226", "8613800004004", "bh"); check(b4.rssid == -1, "BH-04 operator 860001 -> V947 again: blackholed: " + b4);

			// ---- DW: the delivery window of the channel, for a channel of the operator layer ----
			GmmsMessage dw = msg(983, "18667991226", "8613800006001", "dw");
			q.produceSubmitMessage(dw);
			boolean held = waitFor(() -> logLine(dw.getMsgID(), "scheduleDeliveryTime update to next day") != null, 6000);
			check(held && routedTo(dw) == -1, "DW-01 operator 860001 -> V950, outside V950's delivery window (" + window().replace("\n", " ") + "GMT): held for its window, not sent: " + routedTo(dw));
			r = to(983, "8613801006002"); check(r == 951, "DW-02 operator 860002 -> V951 (no window): sent: " + r);
			send(982, "18667991226", "8613800005000", "first");   // the first of the window is never blackholed
			List<GmmsMessage> many = new ArrayList<GmmsMessage>();
			for (int i = 0; i < 400; i++) { GmmsMessage m = msg(982, "18667991226", "86138000051" + String.format("%02d", i % 100), "r" + i); many.add(m); q.produceSubmitMessage(m); }
			int sent = -1;
			for (long end = System.currentTimeMillis() + 60000; System.currentTimeMillis() < end;) {   // until it stops growing
				Thread.sleep(3000);
				int now = count(949, many);
				if (now == sent && now > 0) break;
				sent = now;
			}
			double ratio = (400 - sent) / 400.0;
			check(ratio >= 0.22 && ratio <= 0.38, "BH-05 BlackholePercent 30 over 400 MTs: " + String.format("%.1f%%", ratio * 100) + " blackholed (" + sent + " sent)");
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		} finally {
			mock.stop();
		}
		exit();
	}

	/** a delivery window of an hour, 2 hours from now (GMT, the default SMSOptionTimeZone): now is outside it */
	static String window() {
		java.text.SimpleDateFormat f = new java.text.SimpleDateFormat("HH:mm");
		f.setTimeZone(TimeZone.getTimeZone("GMT"));
		long now = System.currentTimeMillis();
		return "SMSOptionDeliveryStartTime: " + f.format(new Date(now + 2 * 3600000L)) + "\nSMSOptionDeliveryEndTime: " + f.format(new Date(now + 3 * 3600000L)) + "\n";
	}

	/** the MTs to this recipient in the streams of the pattern, as read back from Redis */
	static List<GmmsMessage> byRecipient(String pattern, String recipient) {
		List<GmmsMessage> out = new ArrayList<GmmsMessage>();
		for (String k : jedis.keys(pattern))
			for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
				GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null && recipient.equals(m.getRecipientAddress())) out.add(m);
			}
		return out;
	}

	/** the channel an MT was routed to, -1 none (yet) */
	static int routedTo(GmmsMessage m) {
		for (String k : jedis.keys("stream:mt:routed:*")) if (!inStreams(k, m.getMsgID()).isEmpty()) return Integer.parseInt(k.split(":")[3]);
		return -1;
	}
	/** how many of these MTs are in the outbound streams of rssid (-1: in the customer's DR stream) */
	static int count(int rssid, List<GmmsMessage> ms) {
		Set<String> ids = new HashSet<String>();
		String pattern = rssid == -1 ? "stream:dr:pending:*" : "stream:mt:routed:" + rssid + ":*";
		for (String k : jedis.keys(pattern))
			for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
				GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (m != null) ids.add(m.getMsgID());
			}
		int n = 0;
		for (GmmsMessage m : ms) if (ids.contains(m.getMsgID())) n++;
		return n;
	}
}
