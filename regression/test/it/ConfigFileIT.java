package it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/** Configuration file content written to Redis for the portal (not part of the repo). */
public class ConfigFileIT extends ItSupport {
	static final String MOD = "config:files:CoreEngine1:1";

	static String ver(String name) {
		return jedis.hget(MOD, name + ".version");
	}

	static String base(String name) {
		return "config:file:" + name + ":" + ver(name);
	}

	static void control(String cmd) {
		jedis.publish("channel:gmms:control", cmd);
	}

	public static void main(String[] args) throws Exception {
		Path cm = null, op = null, rule = null;
		byte[] cmOrig = null, opOrig = null, ruleOrig = null;
		try {
			boot();
			cm = Paths.get(home + "conf/cm.cfg");
			op = Paths.get(home + "conf/OperatorMode.properties");
			rule = Paths.get(home + "conf/recipientRuleFile.cfg");
			cmOrig = Files.readAllBytes(cm);
			opOrig = Files.readAllBytes(op);
			ruleOrig = Files.readAllBytes(rule);
			run(cm, op, rule, cmOrig);
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		} finally {
			if (cmOrig != null) Files.write(cm, cmOrig);
			if (opOrig != null) Files.write(op, opOrig);
			if (ruleOrig != null) Files.write(rule, ruleOrig);
		}
		exit();
	}

	static void run(Path cm, Path op, Path rule, byte[] cmOrig) throws Exception {
		// ---- F-01 startup: cm.cfg written once Redis is up (the customers load before Redis) ----
		check(waitFor(() -> ver("cm") != null && jedis.exists(base("cm") + ":meta"), 15000),
				"F-01a cm.cfg content written at startup: " + MOD + " cm.version=" + ver("cm"));
		String v1 = ver("cm");
		String b = base("cm");
		JSONObject vendor9 = JSON.parseObject(jedis.hget(b, "VENDOR9"));
		check(vendor9 != null && "9".equals(vendor9.getString("CCBSSID")) && "VENDOR9".equals(jedis.hget(b + ":ssid", "9")),
				"F-01b HGET " + b + " VENDOR9 = " + vendor9 + "; ssid 9 -> VENDOR9");
		long blocks = 0;
		for (String l : new String(cmOrig, "UTF-8").split("\n")) if (l.trim().startsWith("[")) blocks++;
		check(jedis.hlen(b) == blocks && String.valueOf(blocks).equals(jedis.hget(b + ":meta", "entries"))
				&& "blocks".equals(jedis.hget(b + ":meta", "format")),
				"F-01c one entry per customer block (" + jedis.hlen(b) + " of " + blocks + "), meta format=blocks");
		String c31 = jedis.hget(b, "SMPPCUST31");
		check(c31 != null && c31.contains("password=******") && !c31.contains("p31"),
				"F-01d password inside IncomingConnection masked: " + c31.substring(c31.indexOf("IncomingConnection")));
		check("ok".equals(jedis.hget(MOD, "cm.result")) && jedis.ttl(b) > 6 * 24 * 3600 && jedis.sismember("config:effective:modules", "CoreEngine1:1"),
				"F-01e cm.result=ok, content TTL " + jedis.ttl(b) + " s (7 days), module listed");

		// ---- F-02 other files at startup ----
		long w0 = System.currentTimeMillis();
		waitFor(() -> ver("routing") != null && ver("phonePrefix") != null && ver("vendorTemplate") != null
				&& ver("systemVendorReplace") != null && ver("operatorMode") != null, 15000);
		System.out.println("INFO other files written " + (System.currentTimeMillis() - w0) + "ms after cm.cfg");
		check(ver("routing") != null && jedis.hget(base("routing"), "CUST8888") != null,
				"F-02a routing written (" + jedis.hlen(base("routing")) + " blocks)");
		check(ver("phonePrefix") != null && ver("vendorTemplate") != null && ver("systemVendorReplace") != null
				&& ver("operatorMode") != null,
				"F-02b senderReplacement / vendorTemplate / vendorRoutingReplacement / OperatorMode recorded (empty files: 0 entries)");

		// ---- F-03 cm reload: new version, old one deleted, a failed customer kept out ----
		String text = new String(cmOrig, "UTF-8");
		Files.write(cm, (text + "\n[NEWCUST95]\nCCBSSID: 95\nA2PPlayerType: operator\nProtocol: SMPP\n"
				+ "\n[BADCUST96]\nCCBSSID: x96\nProtocol: SMPP\n").getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> ver("cm") != null && !v1.equals(ver("cm")), 15000), "F-03a cm reload: new version " + ver("cm"));
		String b2 = base("cm");
		check(waitFor(() -> jedis.exists(b2 + ":meta"), 5000) && jedis.hget(b2, "NEWCUST95") != null
				&& jedis.hget(b2, "BADCUST96") == null && "partial".equals(jedis.hget(MOD, "cm.result"))
				&& jedis.hget(MOD, "cm.failed").contains("BADCUST96"),
				"F-03b new customer in, failed customer not (cm.result=partial, cm.failed=" + jedis.hget(MOD, "cm.failed") + ")");
		check(!jedis.exists("config:file:cm:" + v1 + ":meta") && !jedis.exists("config:file:cm:" + v1),
				"F-03c old version deleted: no module uses it any more");

		// ---- F-04 a version still used by another module is kept ----
		jedis.hset("config:files:OtherCore:9", "cm.version", ver("cm"));
		jedis.sadd("config:effective:modules", "OtherCore:9");
		String v2 = ver("cm");
		Files.write(cm, cmOrig);
		control("RELOAD_CUSTOMER");
		check(waitFor(() -> !v2.equals(ver("cm")), 15000) && jedis.exists("config:file:cm:" + v2 + ":meta"),
				"F-04 another module still on " + v2 + ": not deleted");
		check(v1.equals(ver("cm")), "F-04b back to the original content: same version as at startup (" + v1 + ")");
		jedis.del("config:files:OtherCore:9");
		jedis.srem("config:effective:modules", "OtherCore:9");

		// ---- F-05 OperatorMode: written when the monitor sees the change ----
		String opv = ver("operatorMode");
		Files.write(op, "StoreMode=8888\nDRStoreMode=9\n".getBytes("UTF-8"));
		check(waitFor(() -> !opv.equals(ver("operatorMode")), 15000)
				&& "8888".equals(jedis.hget(base("operatorMode"), "StoreMode")),
				"F-05 OperatorMode.properties change written: StoreMode=" + jedis.hget(base("operatorMode"), "StoreMode")
						+ ", format=" + jedis.hget(base("operatorMode") + ":meta", "format"));

		// ---- F-06 recipientRule: file lines ----
		Files.write(rule, "[\n{\"cc\":\"86\",\"lens\":[11]}\n]\n".getBytes("UTF-8"));
		control("RELOAD_RECIPIENT_RULE");
		check(waitFor(() -> ver("recipientRule") != null && "{\"cc\":\"86\",\"lens\":[11]}".equals(jedis.hget(base("recipientRule"), "000002")), 15000),
				"F-06 recipientRuleFile.cfg written line by line: 000002 = " + jedis.hget(base("recipientRule"), "000002"));

		// ---- F-07 switch off ----
		props.setProperty("ConfigFilePublishEnable", "false");
		String v3 = ver("cm");
		Files.write(cm, (text + "\n[NEWCUST97]\nCCBSSID: 97\nProtocol: SMPP\n").getBytes("UTF-8"));
		control("RELOAD_CUSTOMER");
		Thread.sleep(3000);
		check(v3.equals(ver("cm")), "F-07 ConfigFilePublishEnable=false: reload not written");
		props.remove("ConfigFilePublishEnable");

		// ---- F-08 a cm.cfg that cannot be parsed at all ----
		Map<String, String> before = jedis.hgetAll(MOD);
		System.out.println("    | " + MOD + " = " + before);
	}
}
