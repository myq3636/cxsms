package it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;

/** RELOAD_ROUTING reads only the routing files that changed; a bad file fails alone; channel -> customers in Redis. */
public class IncrementalRoutingIT extends ItSupport {
	static final String MOD = "config:files:CoreEngine1:1";
	static Path dir, r8888, r7001;

	static A2PCustomerManager ctm() { return GmmsUtility.getInstance().getCustomerManager(); }
	static String files(String f) { return jedis.hget(MOD, f); }
	static Path file(String name) { return dir.resolve(name + "_routingInfo.cfg"); }
	static long logs(String text) throws Exception {
		long n = 0;
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(text)) n++;
		return n;
	}
	static void write(Path p, String text) throws Exception {
		Files.write(p, text.getBytes("UTF-8"));
		Files.setLastModifiedTime(p, FileTime.fromMillis(System.currentTimeMillis() - 5000 - (++tick)));
	}
	static int tick = 0;

	/** one RELOAD_ROUTING, returns when its completed log line appeared */
	static void reload(String cmd) throws Exception {
		long a = logs("Reload routingInfo completed") + logs("routing reload rejected") + logs("reload rejected, routing stays");
		jedis.publish("channel:gmms:control", cmd);
		waitFor(() -> {
			try {
				return logs("Reload routingInfo completed") + logs("routing reload rejected") + logs("reload rejected, routing stays") > a;
			} catch (Exception e) { return false; }
		}, 15000);
		Thread.sleep(500);
	}
	static void reload() throws Exception { reload("RELOAD_ROUTING"); }

	static String lastDone() throws Exception {
		String last = "";
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains("Reload routingInfo completed")) last = l;
		return last;
	}
	static JSONArray users(String channel) {
		String v = jedis.hget("config:file:routing:" + files("routing.version") + ":channel", channel);
		return v == null ? new JSONArray() : JSON.parseArray(v);
	}
	static JSONObject user(String channel, String customer) {
		for (Object o : users(channel)) if (customer.equals(((JSONObject) o).getString("customer"))) return (JSONObject) o;
		return null;
	}
	static JSONObject route(String channel, String customer) {
		String v = jedis.hget("config:file:routing:" + files("routing.version") + ":channelRoutes", channel + "|" + customer);
		return v == null ? null : JSON.parseArray(v).getJSONObject(0);
	}
	static String relay(int ssid) {
		com.king.message.gmms.GmmsMessage m = new com.king.message.gmms.GmmsMessage();
		m.setRecipientAddress("8613800000000");
		m.setOSsID(ssid);
		m.setRSsID(-1);
		return String.valueOf(ctm().getCustomerRoutingRelay(ssid, m));
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		dir = Paths.get(home + "conf/routing");
		r8888 = file("CUST8888"); r7001 = file("CUST7001");
		byte[] o8888 = Files.readAllBytes(r8888), o7001 = Files.readAllBytes(r7001);
		byte[] cmOrig = Files.readAllBytes(Paths.get(home + "conf/cm.cfg"));
		try {
			boot();
			waitFor(() -> "ok".equals(files("routing.result")), 15000);
			String v0 = files("routing.version");
			System.out.println("relay8888=" + relay(8888) + " relay7001=" + relay(7001));
			check(v0 != null && "".equals(String.valueOf(files("routing.failed"))), "IR-01a startup: routing published, nothing failed");
			JSONObject u = user("VENDOR8", "CUST8888");
			check(u != null && "8888".equals(u.getString("ssid")), "IR-01b channel index: VENDOR8 used by CUST8888 (" + u + ")");
			JSONObject route = route("VENDOR8", "CUST8888");
			check(u != null && u.getIntValue("routes") == 1 && route != null && "A2PRoutingRelay".equals(route.getString("type")) && "86".equals(route.getString("PhonePrefix")),
					"IR-01c channel index: route shows type and prefix");
			check(user("VENDOR9", "CUST7001") != null && user("VENDOR8", "CUST7001") == null, "IR-01d channel index: CUST7001 only under VENDOR9");

			// no change: nothing is read, tables stay the very same object
			java.lang.reflect.Field tf = A2PCustomerManager.class.getDeclaredField("routingTables");
			tf.setAccessible(true);
			Object t0 = tf.get(ctm());
			long noChange = logs("no routing file changed");
			reload();
			check(tf.get(ctm()) == t0 && logs("no routing file changed") == noChange + 1 && v0.equals(files("routing.version")),
					"IR-02 nothing changed: tables not rebuilt, version the same");

			// one file changed: only that one is loaded
			write(r8888, new String(o8888, "UTF-8").replace("Relay=VENDOR8", "Relay=VENDOR12"));
			reload();
			check(lastDone().contains("loaded=1,") && lastDone().contains("failed=0"), "IR-03a one file changed: loaded=1");
			check(relay(8888).startsWith("12") && relay(7001).startsWith("9"), "IR-03b the changed customer moved, the other did not (" + relay(8888) + "," + relay(7001) + ")");
			check(tf.get(ctm()) != t0 && !v0.equals(files("routing.version")), "IR-03c tables replaced, new version");
			check(user("VENDOR12", "CUST8888") != null && user("VENDOR8", "CUST8888") == null, "IR-03d channel index follows");
			String v1 = files("routing.version");
			check(!jedis.exists("config:file:routing:" + v0 + ":channel"), "IR-03e old version's channel index removed with it");

			// bad file: fails alone, keeps its routing
			String[][] bad = {
				{"empty", ""},
				{"no CCBSSID", "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR8)\n"},
				{"bad CCBSSID", "CCBSSID: abc\nA2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR8)\n"},
				{"no Relay", "CCBSSID: 8888\nA2PRoutingRelay: (PhonePrefix=86,Relayy=VENDOR8)\n"},
			};
			int i = 0;
			for (String[] b : bad) {
				i++;
				String relay7001 = i % 2 == 0 ? "VENDOR9" : "VENDOR12";
				write(r8888, b[1]);
				write(r7001, new String(o7001, "UTF-8").replace("Relay=VENDOR9", "Relay=" + relay7001));
				reload();
				check("partial".equals(files("routing.result")) && String.valueOf(files("routing.failed")).contains("CUST8888")
						&& relay(8888).startsWith("12") && relay(7001).startsWith(relay7001.substring(6)),
						"IR-04" + (char) ('a' + i - 1) + " " + b[0] + ": this file keeps its routing, the other file's change applies ("
								+ files("routing.failed") + ")");
			}
			write(r7001, new String(o7001, "UTF-8"));
			write(r8888, new String(o8888, "UTF-8").replace("Relay=VENDOR8", "Relay=VENDOR12"));
			reload();
			check("ok".equals(files("routing.result")) && "".equals(files("routing.failed")) && v1.equals(files("routing.version")),
					"IR-05 file fixed: result ok, failed cleared, content version as before");

			// a new bad file, then deleted
			Path extra = file("CUST9001");
			write(extra, "CCBSSID: x\n");
			reload();
			check("partial".equals(files("routing.result")) && String.valueOf(files("routing.failed")).contains("CUST9001") && v1.equals(files("routing.version")),
					"IR-06a new bad file: not loaded, the others unchanged");
			Files.delete(extra);
			reload();
			check("ok".equals(files("routing.result")) && "".equals(files("routing.failed")), "IR-06b it was deleted: failed cleared");

			// add files, remove one, remove too many
			for (int n = 1; n <= 8; n++) {
				write(file("CUSTX" + n), "CCBSSID: " + (7100 + n) + "\nA2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR9)\n");
			}
			reload();
			check(lastDone().contains("loaded=8,") && users("VENDOR9").size() == 9, "IR-07a 8 new files loaded, channel index has them");
			Files.delete(file("CUSTX8"));
			reload();
			check(lastDone().contains("removed=1") && user("VENDOR9", "CUSTX8") == null && "ok".equals(files("routing.result")), "IR-07b one file deleted: its routes are gone");
			String v2 = files("routing.version");
			for (int n = 1; n <= 5; n++) Files.delete(file("CUSTX" + n));
			reload();
			check("failed".equals(files("routing.result")) && v2.equals(files("routing.version")) && user("VENDOR9", "CUSTX1") != null,
					"IR-07c 5 of 9 deleted: rejected, nothing removed (" + files("routing.failReason") + ")");
			for (int n = 1; n <= 5; n++) write(file("CUSTX" + n), "CCBSSID: " + (7100 + n) + "\nA2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR9)\n");

			// file being written: fails alone, next reload picks it up
			final Path busy = file("CUSTX1");
			final AtomicBoolean go = new AtomicBoolean(true);
			Thread w = new Thread(() -> {
				int k = 0;
				while (go.get()) {
					try {
						Files.write(busy, ("CCBSSID: 7101\nA2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR9)\n#" + (k++) + "\n").getBytes("UTF-8"));
						Thread.sleep(50);
					} catch (Exception e) { }
				}
			});
			Thread.sleep(100);
			w.start();
			Thread.sleep(300);
			reload();
			check("partial".equals(files("routing.result")) && String.valueOf(files("routing.failed")).contains("CUSTX1"), "IR-08a file being written fails alone (" + files("routing.failed") + ")");
			go.set(false); w.join();
			Thread.sleep(800);
			reload();
			check("ok".equals(files("routing.result")), "IR-08b writing done: next reload ok");

			// channel index: mix and master/backup, other routing kinds
			write(file("CUSTX2"), "CCBSSID: 7102\nA2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR9:20,VENDOR12:80)\n"
					+ "A2PBackupRoutingRelay: (PhonePrefix=86,Relay=VENDOR8)\nA2PSenderRoutingRelay: (PhonePrefix=852,Sender=ABC,Relay=VENDOR31)\n");
			reload();
			JSONObject mix = user("VENDOR12", "CUSTX2");
			check(user("VENDOR9", "CUSTX2") != null && mix != null, "IR-09a mix relay: listed under both channels (" + mix + ")");
			check(user("VENDOR8", "CUSTX2") != null && "A2PBackupRoutingRelay".equals(route("VENDOR8", "CUSTX2").getString("type")),
					"IR-09b backup routing listed with its type");
			check(user("VENDOR31", "CUSTX2") != null, "IR-09c sender routing listed");

			// -f reloads everything
			reload("RELOAD_ROUTING:-f");
			check(lastDone().contains("action=-f") && lastDone().contains("loaded=" + lastDone().replaceAll(".*files=(\\d+).*", "$1") + ","), "IR-10 RELOAD_ROUTING:-f loads every file");

			// a version written before the channel index existed gets the index at its next publish
			String vc = files("routing.version");
			jedis.del("config:file:routing:" + vc + ":channel", "config:file:routing:" + vc + ":channelRoutes");
			reload("RELOAD_ROUTING:-f");
			waitFor(() -> jedis.exists("config:file:routing:" + vc + ":channel"), 5000);
			check(vc.equals(files("routing.version")) && user("VENDOR9", "CUSTX2") != null && route("VENDOR9", "CUSTX2") != null, "IR-12 same version without index (upgrade): index written");

			// a changed cm.cfg does not touch routing (only RELOAD_ROUTING does)
			String vb = files("routing.version");
			write(r8888, new String(o8888, "UTF-8"));
			long reads = logs("Start reload routingInfo");
			jedis.publish("channel:gmms:control", "RELOAD_CUSTOMER");
			Thread.sleep(2500);
			check(vb.equals(files("routing.version")) && logs("Start reload routingInfo") == reads && relay(8888).startsWith("12"), "IR-11 RELOAD_CUSTOMER: routing unchanged");
		} finally {
			Files.write(r8888, o8888); Files.write(r7001, o7001);
			Files.write(Paths.get(home + "conf/cm.cfg"), cmOrig);
			try (java.nio.file.DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "CUST[X9]*_routingInfo.cfg")) {
				for (Path p : ds) Files.delete(p);
			}
		}
		exit();
	}
}
