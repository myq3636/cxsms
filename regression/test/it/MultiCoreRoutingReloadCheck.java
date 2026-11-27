package it;

import java.nio.file.*;
import java.util.*;

import redis.clients.jedis.Jedis;

/**
 * RELOAD_ROUTING on the running Cores of a test server (not the in-process Core of the other suites): a routing file
 * of a customer no cm.cfg has (ZZMCTEST, CCBSSID 99901) added, changed, broken, fixed, reloaded with -f and removed;
 * after each step every Core must report the same routing version and result (config:files:{module}:{node}) and the
 * same rules (routingInfo{module}.Rules:ZZMCTEST). Removing it is more than ReloadCustomerMaxRemovePercent of the
 * files on the test server: refused by every Core, then allowed for the clean-up by a hot reload of the GmmsConfig.
 *
 * Run on the server (the classes of the regression build):
 * REDISCLI_AUTH=... java -cp regression/.work/testall:jar/* -Dredis=172.19.4.38:8386 -Da2p=/usr/local/a2p/ \
 *   -Dcores=CoreEngine1:0,CoreEngine2:0,CoreEngine3:0 -Dch1=RTCH01:9621 -Dch2=RTCH02:9622 it.MultiCoreRoutingReloadCheck
 */
public class MultiCoreRoutingReloadCheck {
	static Jedis redis;
	static List<String> cores;
	static Path dir, file, cfg;
	static int failures = 0;
	static final String NAME = "ZZMCTEST";

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}
	static Map<String, String> status(String core) { return redis.hgetAll("config:files:" + core); }
	static String all(String field) {
		StringBuilder b = new StringBuilder();
		for (String c : cores) b.append(c.split(":")[0].replace("CoreEngine", "C")).append('=').append(status(c).get(field)).append(' ');
		return b.toString().trim();
	}
	static Set<String> distinct(String field) {
		Set<String> s = new HashSet<String>();
		for (String c : cores) s.add(String.valueOf(status(c).get(field)));
		return s;
	}
	static String rule(String core, String field) {
		return redis.hget("routingInfo" + core.split(":")[0] + ".Rules:" + NAME, field);
	}
	/** writes through a temporary file and a rename, as the config doc recommends */
	static void write(Path p, String text) throws Exception {
		Path tmp = p.resolveSibling("." + p.getFileName() + ".tmp");
		Files.write(tmp, text.getBytes("UTF-8"));
		Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		Thread.sleep(700);   // past ConfigFileStableCheckMs (500)
	}
	/** sends a reload command, returns when every Core's routing.loadedAt moved (or after 20 s) */
	static long reload(String cmd) throws Exception {
		Map<String, String> before = new HashMap<String, String>();
		for (String c : cores) before.put(c, String.valueOf(status(c).get("routing.loadedAt")) + String.valueOf(status(c).get("routing.result")) + status(c).get("routing.failReason"));
		long t0 = System.currentTimeMillis();
		redis.publish("channel:gmms:control", cmd);
		while (System.currentTimeMillis() - t0 < 20000) {
			boolean done = true;
			for (String c : cores) {
				String now = String.valueOf(status(c).get("routing.loadedAt")) + String.valueOf(status(c).get("routing.result")) + status(c).get("routing.failReason");
				if (now.equals(before.get(c))) done = false;
			}
			if (done) break;
			Thread.sleep(200);
		}
		Thread.sleep(500);
		return System.currentTimeMillis() - t0;
	}
	static String routes(String relay) {
		return "CCBSSID: 99901\nA2PMainRouting: (Priority=10,PhonePrefix=86,Relay=" + relay + ")\n";
	}

	public static void main(String[] a) throws Exception {
		String[] r = System.getProperty("redis", "172.19.4.38:8386").split(":");
		redis = new Jedis(r[0], Integer.parseInt(r[1]));
		String pw = System.getenv("REDISCLI_AUTH");
		if (pw != null && !pw.isEmpty()) redis.auth(pw);
		String home = System.getProperty("a2p", "/usr/local/a2p/");
		cores = Arrays.asList(System.getProperty("cores", "CoreEngine1:0,CoreEngine2:0,CoreEngine3:0").split(","));
		String[] ch1 = System.getProperty("ch1", "RTCH01:9621").split(":"), ch2 = System.getProperty("ch2", "RTCH02:9622").split(":");
		dir = Paths.get(home, "conf/routing");
		file = dir.resolve(NAME + "_routingInfo.cfg");
		cfg = Paths.get(home, "Gmms/GmmsConfig.properties");
		byte[] cfgOrig = Files.readAllBytes(cfg);
		Path cfgBackup = cfg.resolveSibling("GmmsConfig.properties.bak.multicore");
		Files.write(cfgBackup, cfgOrig);
		try {
			// ---- MC-00 the Cores before ----
			check(Files.notExists(file), "MC-00a no " + file.getFileName() + " before (a run left nothing)");
			String v0 = status(cores.get(0)).get("routing.version");
			check(distinct("routing.version").size() == 1 && distinct("routing.result").equals(Collections.singleton("ok")),
					"MC-00b " + cores.size() + " Cores, one routing version, ok: " + all("routing.version") + " / " + all("routing.result"));

			// ---- MC-01 a file added ----
			write(file, routes(ch1[0]));
			long ms = reload("RELOAD_ROUTING");
			String v1 = status(cores.get(0)).get("routing.version");
			check(distinct("routing.version").size() == 1 && !v0.equals(v1) && distinct("routing.result").equals(Collections.singleton("ok")),
					"MC-01a file added, RELOAD_ROUTING: every Core a new version, the same, ok (" + ms + " ms): " + all("routing.version"));
			boolean rules1 = true;
			for (String c : cores) rules1 &= rule(c, "main:0:86:10") != null && rule(c, "main:0:86:10").contains("\"Relay\":\"" + ch1[1] + "\"");
			check(rules1, "MC-01b every Core has the rule main:0:86:10 to " + ch1[0] + " (" + ch1[1] + ")");
			System.out.println("INFO loadedAt " + all("routing.loadedAt"));

			// ---- MC-02 the file changed ----
			write(file, routes(ch2[0]));
			ms = reload("RELOAD_ROUTING");
			String v2 = status(cores.get(0)).get("routing.version");
			boolean rules2 = true;
			for (String c : cores) rules2 &= rule(c, "main:0:86:10") != null && rule(c, "main:0:86:10").contains("\"Relay\":\"" + ch2[1] + "\"");
			check(distinct("routing.version").size() == 1 && !v2.equals(v1) && rules2, "MC-02 file changed to " + ch2[0] + ": every Core the new version and rule (" + ms + " ms)");

			// ---- MC-03 the file broken ----
			write(file, "CCBSSID: abc\n" + routes(ch1[0]).substring(routes(ch1[0]).indexOf('\n') + 1));
			ms = reload("RELOAD_ROUTING");
			boolean keeps = true;
			for (String c : cores) keeps &= rule(c, "main:0:86:10") != null && rule(c, "main:0:86:10").contains("\"Relay\":\"" + ch2[1] + "\"");
			boolean failedEach = true;
			for (String c : cores) failedEach &= String.valueOf(status(c).get("routing.failed")).startsWith(NAME + ":");
			check(distinct("routing.result").equals(Collections.singleton("partial")) && failedEach
					&& distinct("routing.version").equals(Collections.singleton(v2)) && keeps,
					"MC-03 file broken (CCBSSID abc): every Core partial, the file failed, its last rules kept (" + ch2[0] + "), version unchanged: " + all("routing.result")
					+ " | failed " + all("routing.failed") + " | version " + all("routing.version") + " (MC-02 " + v2 + ") | kept " + keeps);

			// ---- MC-04 fixed, MC-05 -f ----
			write(file, routes(ch2[0]));
			reload("RELOAD_ROUTING");
			check(distinct("routing.result").equals(Collections.singleton("ok")) && distinct("routing.version").equals(Collections.singleton(v2)),
					"MC-04 fixed (the content of MC-02): every Core ok, the version of MC-02 again: " + all("routing.result"));
			ms = reload("RELOAD_ROUTING:-f");
			check(distinct("routing.result").equals(Collections.singleton("ok")) && distinct("routing.version").equals(Collections.singleton(v2)),
					"MC-05 RELOAD_ROUTING:-f: every Core read every file again (" + ms + " ms), ok, same version");

			// ---- MC-06 removed: over ReloadCustomerMaxRemovePercent on this server ----
			Files.delete(file);
			Thread.sleep(700);
			reload("RELOAD_ROUTING");
			boolean still = true;
			for (String c : cores) still &= rule(c, "main:0:86:10") != null;
			check(distinct("routing.result").equals(Collections.singleton("failed")) && distinct("routing.version").equals(Collections.singleton(v2)) && still,
					"MC-06 file removed, 1 of 2 files (over 20%): every Core refused, routing kept: " + all("routing.result") + " (" + status(cores.get(0)).get("routing.failReason") + ")");

			// ---- MC-07 clean-up: ReloadCustomerMaxRemovePercent=100 by a GmmsConfig hot reload, then the removal ----
			Files.write(cfg, (new String(cfgOrig, "UTF-8") + "\nReloadCustomerMaxRemovePercent=100\n").getBytes("UTF-8"));
			Thread.sleep(700);
			redis.publish("channel:gmms:control", "RELOAD_GMMS_CONFIG");
			Thread.sleep(3000);
			reload("RELOAD_ROUTING");
			boolean gone = true;
			for (String c : cores) gone &= rule(c, "main:0:86:10") == null;
			check(distinct("routing.result").equals(Collections.singleton("ok")) && distinct("routing.version").equals(Collections.singleton(v0)) && gone,
					"MC-07 with ReloadCustomerMaxRemovePercent=100 (hot reload): every Core removed it, back to the version before (" + v0 + "): " + all("routing.version"));
		} finally {
			Files.deleteIfExists(file);
			Files.write(cfg, cfgOrig);
			Thread.sleep(700);
			redis.publish("channel:gmms:control", "RELOAD_GMMS_CONFIG");
			Thread.sleep(2000);
			check(Arrays.equals(Files.readAllBytes(cfg), cfgOrig), "MC-08 GmmsConfig.properties as before (backup " + cfgBackup.getFileName() + ")");
		}
		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
