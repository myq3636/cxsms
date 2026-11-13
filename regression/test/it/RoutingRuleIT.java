package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.domain.A2PCustomerConfig;
import com.king.gmms.messagequeue.CoreMQMStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.routing.rule.RuleSet;
import com.king.message.gmms.CDR;
import com.king.message.gmms.CDRManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.SerializableHandler;

/**
 * The MT routing by rules (docs/路由重构设计.md, case list docs/路由重构测试用例清单.md) on the real Core: layers,
 * prefixes and priorities, exclusions, default route, backup levels on a failed submit, the retry code, the retry on
 * the main route once the levels are used up, the system routing replacement, CDR MsgID; the rule files themselves.
 */
public class RoutingRuleIT extends CoreRetryIT {
	static String name(int ssid) { return "V" + ssid; }

	/** a routing file parsed as the Core reads it; null and the message in err when it is refused */
	static RuleSet parse(String text, StringBuilder err) throws Exception {
		File f = File.createTempFile("RRT", "_routingInfo.cfg");
		Files.write(f.toPath(), text.getBytes("UTF-8"));
		try {
			A2PCustomerConfig block = A2PCustomerConfig.parseRoutingFile(f, "RRT");
			return RuleSet.parse(block, 9999, n -> n.startsWith("V") ? Integer.parseInt(n.substring(1)) : -1);
		} catch (Exception e) {
			err.append(e.getMessage());
			return null;
		} finally {
			f.delete();
		}
	}
	static GmmsMessage resendFromRmq(GmmsMessage last, String id) {
		String ins = sqlOf("INSERT", "RMQ", id).get(sqlOf("INSERT", "RMQ", id).size() - 1);
		GmmsMessage back = fromRmq(last, retried(ins));
		back.setRSsID(Integer.parseInt(ins.replaceAll("(?s).*RSsID=(\\d+).*", "$1")));
		java.util.regex.Matcher m = java.util.regex.Pattern.compile("RoutingLevel=(\\d+)").matcher(ins);
		if (m.find()) back.setRoutingLevel(Integer.parseInt(m.group(1)));
		return back;
	}
	static int at(GmmsMessage m) { return m == null ? -1 : m.getRSsID(); }

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");

		// ---- the rule files (unit) ----
		StringBuilder err = new StringBuilder();
		RuleSet ok = parse("CCBSSID: 9999\nA2PMainRouting: (Priority=10,PhonePrefix=86;852,Sender=BANK;OTP,ExcludeKeywordId=3,Relay=V1:70,V2:30);(Priority=50,PhonePrefix=86,Relay=V3)\n"
				+ "A2PBackupRouting: (Priority=10,PhonePrefix=all,Relay=V4)\nA2PBackup10Routing: (Priority=10,PhonePrefix=86,Relay=V5)\n"
				+ "A2POPRouting: (Priority=10,Opid=860001,Relay=V6)\nA2PNumberRouting: (Priority=1,Number=8613800000000,Relay=V7)\n"
				+ "A2PRoutingRelay: (PhonePrefix=86,Relay=V8)\nA2PBackup11Routing: (Priority=1,PhonePrefix=86,Relay=V9)\n", err);
		check(ok != null && ok.size() == 6, "RR-A01 a file with every layer and level 1/10 is read: " + (ok == null ? err : ok.size() + " rules"));
		check(ok != null && ok.warnings().stream().anyMatch(w -> w.startsWith("A2PRoutingRelay")) && ok.warnings().stream().anyMatch(w -> w.startsWith("A2PBackup11Routing")),
				"RR-A15 old name and level 11: warned, not used: " + (ok == null ? "" : ok.warnings()));
		String[][] bad = {
				{ "A-05 no Priority", "A2PMainRouting: (PhonePrefix=86,Relay=V1)" },
				{ "A-06 Priority 0", "A2PMainRouting: (Priority=0,PhonePrefix=86,Relay=V1)" },
				{ "A-06 Priority 101", "A2PMainRouting: (Priority=101,PhonePrefix=86,Relay=V1)" },
				{ "A-07 no Relay", "A2PMainRouting: (Priority=10,PhonePrefix=86)" },
				{ "A-08 same Priority, same prefix", "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V1);(Priority=10,PhonePrefix=86,Sender=BANK,Relay=V2)" },
				{ "A-11 Sender and ExcludeSender", "A2PMainRouting: (Priority=10,PhonePrefix=86,Sender=BANK,ExcludeSender=bank,Relay=V1)" },
				{ "A-12 ExcludePhonePrefix not under PhonePrefix", "A2PMainRouting: (Priority=10,PhonePrefix=86,ExcludePhonePrefix=852,Relay=V1)" },
				{ "number rule without Number", "A2PNumberRouting: (Priority=10,Relay=V1)" },
		};
		for (String[] b : bad) {
			StringBuilder e = new StringBuilder();
			check(parse("CCBSSID: 9999\n" + b[1] + "\n", e) == null && e.length() > 0, "RR-" + b[0] + ": refused (" + e + ")");
		}
		check(parse("CCBSSID: 9999\nA2PMainRouting: (Priority=1,PhonePrefix=7,Relay=V1);(Priority=1,PhonePrefix=77,Relay=V2)\nA2PBackupRouting: (Priority=1,PhonePrefix=7,Relay=V3)\n", new StringBuilder()) != null,
				"RR-A09 same Priority in another prefix or level: fine");

		// ---- CDR MsgID (2.10, unit) ----
		GmmsMessage c = mt("CDRM1", 951, 911, 0);
		CDR cdr = new CDR(c);
		cdr.setCdrType(CDR.TYPE_OUT_SUBMIT);
		cdr.setRoutingSsIDs(",911,");
		String m1 = CDRManager.cdrMsgId(cdr);
		cdr.setRoutingSsIDs(",911,,913,"); cdr.setRSsID(913);
		String m2 = CDRManager.cdrMsgId(cdr);
		cdr.setRSsID(911);
		String m3 = CDRManager.cdrMsgId(cdr);
		cdr.setRSsID(913); cdr.setCdrType(CDR.TYPE_IN_DELI_REP);
		String m4 = CDRManager.cdrMsgId(cdr);
		check("CDRM1".equals(m1) && "CDRM1-913".equals(m2) && "CDRM1".equals(m3) && "CDRM1".equals(m4),
				"RR-P01 MsgID: one channel " + m1 + ", backup " + m2 + ", main again (retry) " + m3 + ", IN_DELI_REP " + m4);

		// ---- the Core ----
		StringBuilder cm = new StringBuilder();
		String retry = "SMSOptionSubmitRetryCode: 2500\nSMSOptionMTRetryPolicy: 1S,1S,1S\n";
		for (int ssid = 901; ssid <= 924; ssid++) cm.append(cust(name(ssid), ssid, ssid == 917 ? ""
				: ssid == 918 ? retry + "SMSSpecialSupportContentLengthByCountry: (prefix=86,value=50::deny)\n" : retry));
		cm.append(cust("C951", 951, "")).append(cust("C952", 952, "CustomerServiceType: SVC9\n")).append(cust("C954", 954, ""));
		cm.append(cust("C955", 955, "")).append(cust("C956", 956, "")).append(cust("C957", 957, "SMSOptionMaxReroute: 1\n"));
		cm.append(cust("C958", 958, "")).append(cust("C959", 959, ""));
		cm.append(cust("C961", 961, "")).append(cust("C962", 962, "")).append(cust("C963", 963, ""));
		appendCm(cm.toString());
		route("C951", 951, "A2PMainRouting: (Priority=1,PhonePrefix=7,Relay=V901);(Priority=2,PhonePrefix=77,Sender=BANK,Relay=V902);(Priority=5,PhonePrefix=77,Relay=V903)"
				+ ";(Priority=9,PhonePrefix=all,Relay=V909);(Priority=10,PhonePrefix=86,ExcludeSender=BANK;OTP,Relay=V904);(Priority=20,PhonePrefix=86,Relay=V905)"
				+ ";(Priority=10,PhonePrefix=8613,ExcludeSender=PAY,Relay=V906);(Priority=10,PhonePrefix=852,ExcludePhonePrefix=8529,Relay=V907)",
				"A2PNumberRouting: (Priority=1,Number=8613800000077,Relay=V908);(Priority=1,Number=8613800000078,Relay=V999)");
		java.nio.file.Files.write(Paths.get(home + "conf/routing/SVC9_default_routingInfo.cfg"), "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V910)\n".getBytes("UTF-8"));
		route("C954", 954, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)",
				"A2PBackupRouting: (Priority=10,PhonePrefix=86,Sender=BANK,Relay=V912);(Priority=20,PhonePrefix=86,Relay=V913)",
				"A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V914:50,V915:50)",
				"A2PBackup3Routing: (Priority=10,PhonePrefix=all,Relay=V916)");
		route("C955", 955, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V917)", "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V913)");
		route("C956", 956, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)", "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V917)",
				"A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V913)");
		route("C957", 957, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)", "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V912)",
				"A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V913)");
		route("C958", 958, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V921)", "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V921)",
				"A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V923)");
		route("C959", 959, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)", "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V999)",
				"A2PBackup2Routing: (Priority=10,PhonePrefix=86,Relay=V913)");
		route("C961", 961, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V918)", "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V919)");
		route("C962", 962, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V999)", "A2PBackupRouting: (Priority=10,PhonePrefix=86,Relay=V919)");
		route("C963", 963, "A2PMainRouting: (Priority=10,PhonePrefix=86,Relay=V911)", "A2PBackupRouting: (Priority=10,PhonePrefix=852,Relay=V919)",
				"A2PRoutingRelay: (PhonePrefix=86,Relay=V901)");
		conf("vendorRoutingReplacement.cfg", "86:958:921:922\n");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\nDB.RoutingLevelColumn=true\n").getBytes("UTF-8"));
		msmFactory = RecordingStore::new;
		try {
			start();
			q = StreamQueueManager.getInstance();
			ResultStreamConsumer.getInstance().start();
			CoreMQMStreamConsumer.getInstance().start();
			Thread.sleep(800);
			GmmsMessage s = mt("SER1", 951, 913, 0);
			s.setRoutingLevel(2); s.setRoutingRule("op|main:2:86:10");
			GmmsMessage sb = SerializableHandler.convertRedisMssage2GmmsMessage(SerializableHandler.convertGmmsMessage2RedisMessage(s));
			check(sb.getRoutingLevel() == 2 && "op|main:2:86:10".equals(sb.getRoutingRule()), "RR-N01 routingLevel, routingRule in the stream entry (fields 62, 63)");

			// ---- main route: groups longest first, rules by Priority, exclusions, number layer, default ----
			Out o;
			o = send(951, "BANK", "77012345678", "x"); check(o.routedTo(902), "RR-E01 77, BANK: Priority 2 in 77: " + o);
			o = send(951, "OTP", "77012345678", "x"); check(o.routedTo(903), "RR-E02 77, OTP: Priority 5 in 77: " + o);
			o = send(951, "bank", "77012345678", "x"); check(o.routedTo(902), "RR-F02 Sender is not case sensitive: " + o);
			o = send(951, "OTP", "70012345678", "x"); check(o.routedTo(901), "RR-E03 70: the shorter prefix 7: " + o);
			o = send(951, "OTP", "44012345678", "x"); check(o.routedTo(909), "RR-E04 44: all: " + o);
			o = send(951, "PAY", "8620000000000", "x"); check(o.routedTo(904), "RR-G01a 86, PAY (not excluded): " + o);
			o = send(951, "BANK", "8620000000000", "x"); check(o.routedTo(905), "RR-G01b 86, BANK excluded: the next rule of 86: " + o);
			o = send(951, "OTP", "8613800000001", "x"); check(o.routedTo(906), "RR-G02a 8613, OTP: " + o);
			o = send(951, "PAY", "8613800000001", "x"); check(o.routedTo(904), "RR-G02b 8613, PAY excluded: back to the shorter prefix 86: " + o);
			o = send(951, "OTP", "85291234567", "x"); check(o.routedTo(909), "RR-G07a 8529 excluded from 852: all: " + o);
			o = send(951, "OTP", "85212345678", "x"); check(o.routedTo(907), "RR-G07b 852: " + o);
			o = send(951, "BANK", "8613800000077", "x"); check(o.routedTo(908), "RR-B02 the number layer before the prefixes: " + o);
			o = send(951, "OTP", "8613800000078", "x"); check(o.rssid == -1 && o.dr != null, "RR-I01 rule to a channel not in cm.cfg, no backup route: not routed, no other main rule: " + o);
			// ---- the rules in use, in Redis (docs/路由配置说明.md 7.1) ----
			com.king.gmms.GmmsUtility.getInstance().getCustomerManager().loadRoutingInfoToRedis("CoreEngine1");
			Map<String, String> r951 = jedis.hgetAll("routingInfoCoreEngine1.Rules:C951");
			com.alibaba.fastjson.JSONObject e77 = r951.containsKey("main:0:77:2") ? com.alibaba.fastjson.JSON.parseObject(r951.get("main:0:77:2")) : null;
			check("CCBSSID=951".equals(r951.get("_file")) && e77 != null && "902".equals(e77.getString("Relay")) && "bank".equals(e77.getString("Sender"))
					&& e77.getString("Source").startsWith("C951 A2PMainRouting"), "RR-Q09a Rules:C951: _file, a rule by layer:level:group:priority, Relay as ssid: " + r951.get("main:0:77:2"));
			check(r951.containsKey("main:0:86;852:10") == false && r951.containsKey("number:0:8613800000078:1")
					&& "-1".equals(com.alibaba.fastjson.JSON.parseObject(r951.get("number:0:8613800000078:1")).getString("Relay")),
					"RR-Q09b a channel not in cm.cfg is -1: " + r951.get("number:0:8613800000078:1"));
			Map<String, String> r954 = jedis.hgetAll("routingInfoCoreEngine1.Rules:C954");
			check(r954.containsKey("main:2:86:10") && "914:50,915:50".equals(com.alibaba.fastjson.JSON.parseObject(r954.get("main:2:86:10")).getString("Relay")),
					"RR-Q09c weights: " + r954.get("main:2:86:10"));
			Map<String, String> r963 = jedis.hgetAll("routingInfoCoreEngine1.Rules:C963");
			check(String.valueOf(r963.get("_warnings")).contains("A2PRoutingRelay") && !r963.containsKey("main:0:86:30"), "RR-Q09d _warnings: " + r963.get("_warnings"));
			check("default of service type SVC9".equals(jedis.hget("routingInfoCoreEngine1.Rules:SVC9_default", "_file")), "RR-Q09e default file: " + jedis.hget("routingInfoCoreEngine1.Rules:SVC9_default", "_file"));

			// ---- no usable channel in the main route: the backup levels at routing time ----
			o = send(961, "OTP", DST, "short"); check(o.routedTo(918), "RR-I02a content allowed by the main channel: main route: " + o);
			GmmsMessage longOne = msg(961, "OTP", DST, "this text is longer than the fifty bytes channel V918 takes");
			o = send(longOne); check(o.routedTo(919), "RR-I02b content too long for the main channel: backup 1 at routing time: " + o);
			List<GmmsMessage> lo = inStreams("stream:mt:routed:919:*", o.id);
			check(!lo.isEmpty() && lo.get(0).getRoutingLevel() == 1, "RR-I02c it is at backup level 1: " + (lo.isEmpty() ? "" : lo.get(0).getRoutingLevel()));
			o = send(962, "OTP", DST, "x"); check(o.routedTo(919), "RR-I01b main rule to a channel not in cm.cfg: backup 1: " + o);
			o = send(963, "OTP", "85212345678", "x"); check(o.routedTo(919), "RR-I06b no main rule for 852: backup 1: " + o);
			o = send(952, "OTP", DST, "x"); check(o.routedTo(910), "RR-D-default no own file: default route of the service type: " + o);
			o = send(952, "OTP", "44012345678", "x"); check(o.rssid == -1 && o.dr != null, "RR-I06 no main and no backup rule anywhere: not routed: " + o);

			// ---- backup levels on a failed submit (2.4, 2.9) ----
			GmmsMessage f = first(954, "OTP", 911);
			final String fid = f.getMsgID();
			check(f.getRoutingLevel() == 0 && f.getRoutingRule() != null && f.getRoutingRule().startsWith("main|main:0:86:10"), "RR-Q05 main route: level 0, rule " + (f == null ? "" : f.getRoutingRule()));
			fail(f, GmmsStatus.COMMUNICATION_ERROR);
			GmmsMessage b1 = out(913, fid, 1);
			check(b1 != null && b1.getRoutingLevel() == 1 && dr(954, fid, 300) == null, "RR-J01 code 2500 in the retry code: backup 1, Priority 20 (OTP): level " + (b1 == null ? "" : b1.getRoutingLevel()));
			fail(b1, GmmsStatus.COMMUNICATION_ERROR);
			waitFor(() -> !inStreams("stream:mt:routed:914:*", fid).isEmpty() || !inStreams("stream:mt:routed:915:*", fid).isEmpty(), 8000);
			GmmsMessage b2 = !inStreams("stream:mt:routed:914:*", fid).isEmpty() ? out(914, fid, 1) : out(915, fid, 1);
			check(b2 != null && b2.getRoutingLevel() == 2, "RR-J03 backup 2 (V914 or V915): " + at(b2));
			fail(b2, GmmsStatus.COMMUNICATION_ERROR);
			GmmsMessage b3 = out(916, fid, 1);
			int other = at(b2) == 914 ? 915 : 914;
			check(b3 != null && b3.getRoutingLevel() == 3 && inStreams("stream:mt:routed:" + other + ":*", fid).isEmpty(),
					"RR-J04 backup 3, the other channel of level 2 not tried: " + at(b3));
			fail(b3, GmmsStatus.COMMUNICATION_ERROR);
			check(sqlSeen("INSERT", "RMQ", fid, 1), "RR-K05a levels used up: retry (RMQ)");
			String ins = sqlOf("INSERT", "RMQ", fid).get(0);
			check(ins.contains("RSsID=911") && ins.contains("RoutingLevel=3"), "RR-K05b the retry is on the main route, the level kept: " + ins.replaceAll("(?s).*(RSsID=\\d+).*(RoutingLevel=\\d+).*", "$1 $2"));
			check(q.produceMqmSubmitMessage(resendFromRmq(b3, fid)), "RR-K05c MQM sends it back");
			GmmsMessage again = out(911, fid, 2);
			check(again != null, "RR-K05d sent again on the main route V911");
			if (again != null) {
				fail(again, GmmsStatus.COMMUNICATION_ERROR);
				// it came from RMQ: the next retry updates its row there
				check(sqlSeen("UPDATE", "RMQ", fid, 1) && inStreams("stream:mt:routed:913:*", fid).size() == 1
						&& logLine(fid, "no backup level after level 3 (rssid=911") != null,
						"RR-K06 the main route fails again: no backup again, next retry (RMQ updated)");
			}
			check(waitFor(() -> cdrCount(fid, "OUT_SUBMIT") >= 2 && cdrCount(fid + "-913", "OUT_SUBMIT") == 1 && cdrCount(fid + "-916", "OUT_SUBMIT") == 1, 5000),
					"RR-P07 CDR MsgID: main route " + fid + " (first try and retry), backups " + fid + "-913 ... " + fid + "-916: "
					+ cdrCount(fid, "OUT_SUBMIT") + "/" + cdrCount(fid + "-913", "OUT_SUBMIT") + "/" + cdrCount(fid + "-916", "OUT_SUBMIT"));

			GmmsMessage g = first(954, "BANK", 911);
			fail(g, GmmsStatus.COMMUNICATION_ERROR);
			check(out(912, g.getMsgID(), 1) != null, "RR-J02 backup 1 by its rules: BANK, Priority 10");

			// ---- the retry code decides (2.9) ----
			GmmsMessage k = first(955, "OTP", 917);
			fail(k, GmmsStatus.COMMUNICATION_ERROR);
			check(dr(955, k.getMsgID(), 5000) != null && inStreams("stream:mt:routed:913:*", k.getMsgID()).isEmpty(), "RR-K04 channel without SMSOptionSubmitRetryCode: no reroute, failure DR");
			GmmsMessage k2 = first(956, "OTP", 911);
			fail(k2, GmmsStatus.COMMUNICATION_ERROR);
			GmmsMessage k2b = out(917, k2.getMsgID(), 1);
			if (k2b != null) fail(k2b, GmmsStatus.COMMUNICATION_ERROR);
			check(k2b != null && dr(956, k2.getMsgID(), 5000) != null && inStreams("stream:mt:routed:913:*", k2.getMsgID()).isEmpty(),
					"RR-K02 backup 1 fails with a code not in its retry code: no backup 2, failure DR");
			GmmsMessage k3 = first(954, "OTP", 911);
			fail(k3, GmmsStatus.SUBMIT_RESP_ERROR);
			check(dr(954, k3.getMsgID(), 5000) != null && inStreams("stream:mt:routed:913:*", k3.getMsgID()).isEmpty(), "RR-K01 a code not in the retry code: failure DR, no backup");

			// ---- SMSOptionMaxReroute, replacement, unusable backup ----
			GmmsMessage x = first(957, "OTP", 911);
			fail(x, GmmsStatus.COMMUNICATION_ERROR);
			GmmsMessage xb = out(912, x.getMsgID(), 1);
			if (xb != null) fail(xb, GmmsStatus.COMMUNICATION_ERROR);
			check(xb != null && sqlSeen("INSERT", "RMQ", x.getMsgID(), 1) && inStreams("stream:mt:routed:913:*", x.getMsgID()).isEmpty()
					&& sqlOf("INSERT", "RMQ", x.getMsgID()).get(0).contains("RSsID=911"), "RR-J15 SMSOptionMaxReroute=1: one reroute, then the main route retried");
			GmmsMessage y = first(958, "OTP", 922);
			check(y != null, "RR-M01 main route V921 replaced by V922 (vendorRoutingReplacement.cfg)");
			if (y != null) {
				fail(y, GmmsStatus.COMMUNICATION_ERROR);
				check(out(923, y.getMsgID(), 1) != null && inStreams("stream:mt:routed:922:*", y.getMsgID()).size() == 1,
						"RR-M03 backup 1 V921 is V922 again, already used: backup 2 V923");
			}
			GmmsMessage z = first(959, "OTP", 911);
			fail(z, GmmsStatus.COMMUNICATION_ERROR);
			check(out(913, z.getMsgID(), 1) != null, "RR-J09 backup 1 to a channel not in cm.cfg: backup 2");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
