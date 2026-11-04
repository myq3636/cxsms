package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** A process stopped with kill: what only memory held is written, within the time, whatever hangs. Against kill -9 as the control. */
public class GracefulStopIT extends CoreFailoverIT {
	static class Run { int exit; long ms; String out; String home; }

	static Run run(String what, String sig, int n, String extraConfig) throws Exception {
		String h = home.replaceAll("/$", "") + "_S" + what + sig.replace("-", "") + "/";
		new ProcessBuilder("rm", "-rf", h).start().waitFor();
		new ProcessBuilder("cp", "-r", home, h).start().waitFor();
		Path c = Paths.get(h + "Gmms/GmmsConfig.properties");
		Files.write(c, (new String(Files.readAllBytes(c), "UTF-8") + "\nAsyncSqlMaxWaitMs=100000\nAsyncSqlBatchSize=100\n" + extraConfig + "\n").getBytes("UTF-8"));
		File out = new File(h + "node.out");
		ProcessBuilder b = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx512m", "-Dit.keepRedis=true", "-Da2p_home=" + h, "-Dmodule=CoreEngine1", "-DNodeID=1",
				"-Dlog4j.configurationFile=" + h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.StopNode", what, sig, String.valueOf(n));
		b.redirectErrorStream(true); b.redirectOutput(out);
		Process p = b.start();
		Run r = new Run();
		r.home = h;
		boolean gone = p.waitFor(90, TimeUnit.SECONDS);
		long end = System.currentTimeMillis();
		if (!gone) { p.destroyForcibly().waitFor(); r.exit = -1; } else r.exit = p.exitValue();
		r.out = new String(Files.readAllBytes(out.toPath()), "UTF-8");
		int i = r.out.indexOf("SIGNAL ");
		r.ms = i < 0 ? -1 : end - Long.parseLong(r.out.substring(i + 7, r.out.indexOf('\n', i)).trim());
		return r;
	}
	static int lines(String dir, String nameContains, String text) throws Exception {
		int n = 0;
		File[] files = new File(dir).listFiles();
		if (files == null) return 0;
		for (File f : files) {
			if (f.isDirectory()) { n += lines(f.getPath(), nameContains, text); continue; }
			if (!f.getName().contains(nameContains)) continue;
			for (String l : Files.readAllLines(f.toPath())) if (l.contains(text)) n++;
		}
		return n;
	}
	static int sql(Run r) throws Exception {
		Path p = Paths.get(r.home + "sql.out");
		return Files.exists(p) ? Files.readAllLines(p).size() : 0;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		appendCm(cust("V901", RSSID, "") + cust("R900", OSSID, ""));
		route("R900", OSSID, "A2PRoutingRelay: (PhonePrefix=86,Relay=V901)");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nRedisStreamAutoClaimEnable=true\nCoreMtLeaseMs=10000\nRedisStreamAutoClaimIdleMs=10000\nRedisStreamAutoClaimIntervalMs=1000\n").getBytes("UTF-8"));
		Process n1 = null, n2 = null;
		try {
			boot();

			// ---- asynchronous SQL ----
			Run r = run("sql", "-9", 5050, "");
			System.out.println("INFO kill -9: " + sql(r) + " of 5050 statements");
			check(sql(r) < 5050, "MS-03z control, kill -9: statements are lost: " + sql(r) + " of 5050");
			r = run("sql", "-TERM", 5050, "");
			System.out.println("INFO kill: exit in " + r.ms + " ms");
			check(sql(r) == 5050, "MS-03a kill: every statement written, also the batch a writer held: " + sql(r) + " of 5050");
			check(r.exit == 143 && r.ms < 4000, "MS-03b within 4 s, exit code 143: " + r.ms + " ms, " + r.exit);

			// ---- CDR ----
			r = run("cdr", "-9", 900, "");
			int killed = lines(r.home + "cdr", "asyn", "cdr-");
			r = run("cdr", "-TERM", 900, "");
			int termed = lines(r.home + "cdr", "asyn", "cdr-");
			System.out.println("INFO CDR kill -9 " + killed + ", kill " + termed);
			check(termed == 900, "MS-03c kill: every CDR of the queue written: " + termed + " of 900 (kill -9: " + killed + ")");

			// ---- SDQ ----
			r = run("sdq", "-TERM", 900, "");
			int sdq = lines(r.home + "queue", "", "INSERT INTO SDQ");
			check(sdq == 900, "MS-03d kill: every SDQ message written: " + sdq + " of 900");

			// ---- message id position ----
			r = run("id", "-TERM", 1000, "");
			String counter = r.out.substring(r.out.indexOf("COUNTER ") + 8, r.out.indexOf('\n', r.out.indexOf("COUNTER "))).trim();
			check(counter.equals(jedis.get("system:msgid:mark:CoreEngine1")), "MS-03e kill right after the ids: the position is saved: " + counter);

			// ---- whatever hangs ----
			r = run("stuck", "-TERM", 500, "");
			check(r.exit == 143 && r.ms >= 3500 && r.ms < 5000, "MS-05a database hanging: out at the 4 s limit, before the kill -9 of the stop script: " + r.ms + " ms, exit " + r.exit);
			check(r.out.contains("not finished: [SQL queue]"), "MS-05b it says what did not finish");
			r = run("stuck", "-TERM", 500, "ModuleStopMaxWaitSeconds=2");
			check(r.exit == 143 && r.ms >= 1500 && r.ms < 3000, "MS-05c ModuleStopMaxWaitSeconds=2: out in 2 s: " + r.ms + " ms");

			// ---- SHUTDOWN, then kill ----
			r = run("shutdown", "-TERM", 900, "");
			int cdr = lines(r.home + "cdr", "", "cdr-");
			int after = lines(r.home + "cdr", "", "after-");
			check(r.out.contains("STILL ALIVE") && r.exit == 143, "MS-04a SHUTDOWN@module does not end the process; kill does");
			check(cdr == 900, "MS-04b SHUTDOWN wrote the queue, kill wrote nothing twice: " + cdr + " of 900");
			check(after == 0, "MS-04c new CDRs after it go to the synchronous writer, not the closed queue: " + after);

			// ---- the launcher production uses ----
			String hl = home.replaceAll("/$", "") + "_Slauncher/";
			new ProcessBuilder("rm", "-rf", hl).start().waitFor();
			new ProcessBuilder("cp", "-r", home, hl).start().waitFor();
			Files.write(Paths.get(hl + "Gmms/GmmsConfig.properties"), (new String(Files.readAllBytes(Paths.get(hl + "Gmms/GmmsConfig.properties")), "UTF-8") + "\nMultiSmppServer1.Port=17790\n").getBytes("UTF-8"));
			Files.write(Paths.get(hl + "conf/a2p_modules.conf"), "1|CoreEngine1|core|x|0|core|10||\n1|MultiSmppServer1|smpp-server|x|0|server|40||\n".getBytes("UTF-8"));
			File lo = new File(hl + "launch.out");
			ProcessBuilder lb = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx256m", "-Da2p_home=" + hl, "-Dmodule=MultiSmppServer1", "-DNodeID=9",
					"-Dlog4j.configurationFile=" + hl + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.MessageIdLauncherNode", "start", "com.king.gmms.listener.smppserver.MultiSmppServer");
			lb.redirectErrorStream(true); lb.redirectOutput(lo);
			Process lp = lb.start();
			waitFor(() -> new String(Files.readAllBytes(lo.toPath()), "UTF-8").contains("Start service"), 30000);
			Thread.sleep(2000);
			long lt = System.currentTimeMillis();
			lp.destroy();
			boolean lgone = lp.waitFor(6, TimeUnit.SECONDS);
			long lms = System.currentTimeMillis() - lt;
			String lout = new String(Files.readAllBytes(lo.toPath()), "UTF-8");
			check(lgone && lms < 1500 && lp.exitValue() == 143, "MS-03f real GmmsLauncher, Server: kill, out in " + lms + " ms, as fast as before");
			check(lout.contains("[STOP] SIGTERM") && !lout.contains("not finished") && lout.contains("is successfully stopped"), "MS-03g every job finished, stop reading included");
			check(lout.substring(lout.indexOf("[STOP] SIGTERM")).matches("(?s)\\[STOP\\] SIGTERM: [1-9][0-9]* of [1-9][0-9]* jobs.*"), "MS-03h the module ran its jobs: " + lout.substring(lout.indexOf("[STOP] SIGTERM")).split("\n")[0]);

			// ---- under load, the other Core takes over ----
			jedis.set("system:core:shards:" + C1, "0");
			jedis.set("system:core:shards:" + C2, "1");
			n1 = start(C1, 1);
			n2 = start(C2, 2);
			List<String> load = new ArrayList<>();
			Thread sender = new Thread(() -> { try { load.addAll(send("L", 1500)); } catch (Exception e) { } });
			sender.start();
			Thread.sleep(400);
			long t0 = System.currentTimeMillis();
			n1.destroy();
			boolean gone = n1.waitFor(6, TimeUnit.SECONDS);
			long ms = System.currentTimeMillis() - t0;
			check(gone && ms < 4000, "MS-32a Core under load: kill, out in " + ms + " ms");
			sender.join();
			jedis.set("system:core:shards:" + C2, "0-1");
			jedis.publish("channel:gmms:control", "RELOAD_CORE_SHARDS@" + C2);
			allOnce(load, "MS-32b the other Core takes over, every MT once");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		finally {
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}
}
