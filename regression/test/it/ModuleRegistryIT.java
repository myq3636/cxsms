package it;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.ModuleRegistry;
import com.king.gmms.ha.ModuleStatusReporter;

/** The module registry: the real a2pctl, real processes started with the launcher of production, a module list in the format of production. */
public class ModuleRegistryIT extends ItSupport {
	static final String LIST =
			"# A2P module startup list.\n"
			+ "1|CoreEngine1|core|com.king.gmms.routing.DeliveryRouter|0|core|10||\n"
			+ "1|CoreEngine2|core|com.king.gmms.routing.DeliveryRouter|0|core|10||\n"
			+ "1|CoreEngine3|core|com.king.gmms.routing.DeliveryRouter|0|core|10||\n"
			+ "#1|CoreEngine4|core|com.king.gmms.routing.DeliveryRouter|0|core|10||\n\n"
			+ "1|MultiSmppClient1|smpp-client|com.king.gmms.client.MultiSmppClient|0|client|20||\n"
			+ "1|MultiSmppClient2|smpp-client|com.king.gmms.client.MultiSmppClient|0|client|20||\n"
			+ "1|CommonHttpClient1|http-client|com.king.gmms.client.CommonHttpClient|0|client|20||\n"
			+ "1|MsgQueueMonitor1|mqm|com.king.gmms.mqm.MsgQueueMonitor|0|mqm|30||\n"
			+ "1|MultiSmppServer1|smpp-server|com.king.gmms.listener.smppserver.MultiSmppServer|0|server|40||\n"
			+ "1|MultiSmppServer2|smpp-server|com.king.gmms.listener.smppserver.MultiSmppServer|0|server|40||\n"
			+ "1|CommonHttpServer1|http-server||0|httpserver|40|$A2P_HOME/bin/CommonHttpServer1.sh start|$A2P_HOME/bin/CommonHttpServer1.sh stop\n";
	static ModuleManager mm;
	static Path listFile;

	static String[] tool(String... args) throws Exception {
		List<String> c = new ArrayList<>(List.of(System.getProperty("java.home") + "/bin/java", "-Xmx256m", "-Da2p_home=" + home, "-Dlog4j.configurationFile=" + home + "conf/log4j2.xml",
				"-cp", System.getProperty("java.class.path"), "it.A2pCtlNode"));
		c.addAll(List.of(args));
		File out = File.createTempFile("a2pctl", ".out");
		ProcessBuilder b = new ProcessBuilder(c);
		b.redirectErrorStream(true); b.redirectOutput(out);
		Process p = b.start();
		if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); return new String[] { "-1", "timeout" }; }
		String text = new String(Files.readAllBytes(out.toPath()), "UTF-8");
		out.delete();
		return new String[] { String.valueOf(p.exitValue()), text };
	}
	static class Node { Process p; File out; String h;
		String text() throws Exception { return new String(Files.readAllBytes(out.toPath()), "UTF-8"); }
		boolean started() throws Exception { return text().contains("Start service successfully"); }
		void stop() throws Exception { p.destroy(); p.waitFor(8, TimeUnit.SECONDS); if (p.isAlive()) p.destroyForcibly(); }
		void kill9() throws Exception { p.destroyForcibly().waitFor(); }
	}
	/** a real module through GmmsLauncher; the file of this machine lists it */
	static Node start(String module, int node) throws Exception {
		Node n = new Node();
		n.h = home.replaceAll("/$", "") + "_R" + module + "_" + node + "/";
		new ProcessBuilder("rm", "-rf", n.h).start().waitFor();
		new ProcessBuilder("cp", "-r", home, n.h).start().waitFor();
		Path cfg = Paths.get(n.h + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\n" + module + ".Port=17790\n").getBytes("UTF-8"));
		Path list = Paths.get(n.h + "conf/a2p_modules.conf");
		Files.write(list, (new String(Files.readAllBytes(list), "UTF-8") + "1|" + module + "|smpp-server|com.king.gmms.listener.smppserver.MultiSmppServer|" + node + "|server|40||\n").getBytes("UTF-8"));
		n.out = new File(n.h + "launch.out");
		ProcessBuilder b = new ProcessBuilder(System.getProperty("java.home") + "/bin/java", "-Xmx256m", "-Da2p_home=" + n.h, "-Dmodule=" + module, "-DNodeID=" + node,
				"-Dlog4j.configurationFile=" + n.h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.MessageIdLauncherNode", "start", "com.king.gmms.listener.smppserver.MultiSmppServer");
		b.redirectErrorStream(true); b.redirectOutput(n.out);
		n.p = b.start();
		final Node x = n;
		waitFor(() -> !x.p.isAlive() || x.started(), 30000);
		return n;
	}
	static String state(String module) { return jedis.hget("system:module:" + module, "state"); }
	static void reload() { jedis.publish("channel:gmms:control", "RELOAD_MODULES"); }
	static void set(String module, String st) { ModuleRegistry.write(module, "state", st); }

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		listFile = Paths.get(home + "conf/a2p_modules.conf");
		Files.write(listFile, LIST.getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleStatusHeartbeatSeconds=1\nModuleStatusTTLSeconds=3\n").getBytes("UTF-8"));
		try {
			boot();
			mm = ModuleManager.getInstance();

			// ---- switched off, the default ----
			check(!ModuleRegistry.enabled() && jedis.keys("system:module*").isEmpty(), "RG-01a off by default: nothing in Redis");
			check(mm.getServerModules().size() == 3 && mm.getSessionNumberOfModule(4, "MultiSmppClient1") == 2, "RG-01b off: the lists of the file: " + mm.getServerModules());

			// ---- a2pctl init ----
			String[] t = tool("status");
			check("1".equals(t[0]) && t[1].contains("The registry is empty"), "RG-02a status before init says so");
			t = tool("init");
			check("0".equals(t[0]) && t[1].contains("10 modules added"), "RG-02b init takes the production list: " + t[1].trim().split("\n")[t[1].trim().split("\n").length - 1]);
			check("smpp-client".equals(jedis.hget("system:module:MultiSmppClient2", "role")) && "MultiSmppClient".equals(jedis.hget("system:module:MultiSmppClient2", "type"))
					&& "0".equals(jedis.hget("system:module:MultiSmppClient2", "nodeId")) && "ACTIVE".equals(state("MultiSmppClient2")), "RG-02c role, type, node, state");
			check(state("CoreEngine4") == null, "RG-02d a commented line is not taken");
			String v1 = jedis.get("system:modules:version");
			t = tool("init");
			check("1".equals(t[0]) && t[1].contains("already exists") && v1.equals(jedis.get("system:modules:version")), "RG-02e a second init changes nothing");
			t = tool("status");
			check("0".equals(t[0]) && t[1].contains("MultiSmppClient2") && t[1].contains("version " + v1), "RG-02f status lists them");
			Files.write(listFile, (LIST + "1|MultiSmppServer02|smpp-server|x|0|server|40||\n").getBytes("UTF-8"));
			t = tool("init", "--from-file");
			check("1".equals(t[0]) && t[1].contains("same number 2") && state("MultiSmppServer02") == null, "RG-03a same number in the list: refused, nothing written");
			Files.write(listFile, (LIST + "1|MultiSmppServer10|smpp-server|x|0|server|40||\n").getBytes("UTF-8"));
			t = tool("init", "--from-file");
			check("1".equals(t[0]) && t[1].contains("must be 1 to 9") && state("MultiSmppServer10") == null, "RG-03b number above 9: refused");
			Files.write(listFile, LIST.getBytes("UTF-8"));

			// ---- this process follows it ----
			props.setProperty("ModuleRegistryEnable", "true");
			GmmsUtility g = GmmsUtility.getInstance();
			ModuleRegistry.startup(g);
			// its node differs from the list: its record is corrected, which makes a new version
			check("1".equals(jedis.hget("system:module:CoreEngine1", "nodeId")) && String.valueOf(ModuleRegistry.version()).equals(jedis.get("system:modules:version")), "RG-04a loaded, its node corrected: version " + ModuleRegistry.version());
			check(mm.getServerModules().size() == 3 && mm.getSessionNumberOfModule(4, "MultiSmppClient1") == 2, "RG-04b same lists as the file");

			// ---- a state change, by command ----
			set("MultiSmppServer2", ModuleRegistry.DRAINING);
			reload();
			check(waitFor(() -> !mm.getServerModules().contains("MultiSmppServer2"), 3000), "RG-05a DRAINING, RELOAD_MODULES: out of the list: " + mm.getServerModules());
			boolean only = true;
			for (int i = 0; i < 30; i++) only &= "MultiSmppServer1".equals(mm.selectChannel("MultiSmppServer"));
			check(only, "RG-05b selectChannel does not choose it");
			check(mm.getConnectionInfo("MultiSmppServer2") != null && mm.getModuleNo("MultiSmppServer2") == 2, "RG-05c its connection info and number stay");
			set("MultiSmppServer2", ModuleRegistry.ACTIVE);
			reload();
			check(waitFor(() -> mm.getServerModules().contains("MultiSmppServer2"), 3000), "RG-05d back to ACTIVE: in the list");
			set("CoreEngine1", ModuleRegistry.DRAINING);
			set("CoreEngine2", ModuleRegistry.DRAINING);
			reload();
			check(waitFor(() -> !mm.getRouterModules().contains("CoreEngine2"), 3000) && mm.getRouterModules().contains("CoreEngine1"), "RG-05e a draining module still sees itself, the other is out: " + mm.getRouterModules());
			set("CoreEngine1", ModuleRegistry.ACTIVE);
			set("CoreEngine2", ModuleRegistry.ACTIVE);
			reload();
			waitFor(() -> mm.getRouterModules().contains("CoreEngine2"), 3000);

			// ---- no command at all: the heartbeat finds it; a module of another machine ----
			ModuleStatusReporter.start(g, "core", "CoreEngine1", g.getNodeId());
			ModuleRegistry.write("MultiSmppClient3", "type", "MultiSmppClient", "role", "smpp-client", "nodeId", "1", "state", ModuleRegistry.ACTIVE, "removedAt", "0");
			check(waitFor(() -> mm.getClientModules().contains("MultiSmppClient3"), 4000), "RG-06a no RELOAD_MODULES: the next heartbeat takes the change");
			check(mm.getConnectionInfo("MultiSmppClient3") != null && mm.getModuleNo("MultiSmppClient3") == 3, "RG-06b a module of another machine, not in this file: known, number 3");
			check(mm.getSessionNumberOfModule(6, "MultiSmppClient1") == 2 && mm.getSessionNumberOfModule(4, "MultiSmppClient1") == 2, "RG-06c connections: active modules of the cluster: 6/3, 4/3 rounded up");
			set("MultiSmppClient3", ModuleRegistry.REMOVED);
			check(waitFor(() -> mm.getSessionNumberOfModule(6, "MultiSmppClient1") == 3, 4000), "RG-06d removed: 6/2");
			check(waitFor(() -> String.valueOf(jedis.get("module:detail:core:CoreEngine1:" + g.getNodeId())).contains("registryVersion=" + ModuleRegistry.version()), 3000), "RG-07 the module reports the version it uses");
			int clientIndex = mm.getModuleIndex("MultiSmppClient2");
			set("MultiSmppClient1", ModuleRegistry.DRAINING);
			reload();
			waitFor(() -> !mm.getClientModules().contains("MultiSmppClient1"), 3000);
			check(clientIndex == 4 && mm.getModuleIndex("MultiSmppClient2") == 4, "RG-08 a module leaving the list does not move the v1 module number of the others: " + mm.getModuleIndex("MultiSmppClient2"));
			set("MultiSmppClient1", ModuleRegistry.ACTIVE);
			reload();
			waitFor(() -> mm.getClientModules().contains("MultiSmppClient1"), 3000);

			// ---- Redis lost it ----
			long kept = ModuleRegistry.version();
			jedis.set("system:modules:version", String.valueOf(kept - 1000));
			reload();
			Thread.sleep(500);
			check(ModuleRegistry.version() == kept, "RG-09a an older version is not taken");
			jedis.del(jedis.keys("system:module*").toArray(new String[0]));
			reload();
			Thread.sleep(1500);
			check(ModuleRegistry.version() == kept && mm.getServerModules().contains("MultiSmppServer1"), "RG-09b registry gone from Redis: the module goes on with what it has");
			t = tool("init", "--from-file");
			check("0".equals(t[0]) && t[1].contains("10 modules added"), "RG-09c init --from-file restores it");
			check(waitFor(() -> ModuleRegistry.version() > kept, 4000), "RG-09d and the module takes it, its version is newer: " + ModuleRegistry.version());

			// ---- the start script just starts a module ----
			Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nModuleRegistryEnable=true\n").getBytes("UTF-8"));
			Node n = start("MultiSmppServer3", 0);
			check(n.started(), "RG-10a a new module, started directly: it starts");
			check("ACTIVE".equals(state("MultiSmppServer3")) && "0".equals(jedis.hget("system:module:MultiSmppServer3", "nodeId")) && "smpp-server".equals(jedis.hget("system:module:MultiSmppServer3", "role")), "RG-10b it registered itself: ACTIVE");
			check(waitFor(() -> mm.getServerModules().contains("MultiSmppServer3"), 4000), "RG-10c the others see it");
			check(n.text().contains("Start service successfully") && new String(Files.readAllBytes(Paths.get(n.h + "log/it.log")), "UTF-8").contains("moduleNo=3"), "RG-10d message id number 3");
			check(jedis.exists("module:status:server:MultiSmppServer3:0"), "RG-11a heartbeat there");
			n.stop();
			check(!jedis.exists("module:status:server:MultiSmppServer3:0"), "RG-11b kill: the heartbeat is gone at once");
			Node moved = start("MultiSmppServer3", 1);
			check(moved.started(), "RG-11d so it can start right away on another machine (node 1)");
			check("1".equals(jedis.hget("system:module:MultiSmppServer3", "nodeId")), "RG-11e the registry has the new node");

			// ---- real conflicts ----
			Node dup = start("MultiSmppServer3", 2);
			check(!dup.p.isAlive() && dup.p.exitValue() == 255 && dup.text().contains("it is running on node 1"), "RG-12a the same name running on another node: refused: exit " + (dup.p.isAlive() ? "running" : String.valueOf(dup.p.exitValue())));
			if (dup.p.isAlive()) dup.kill9();
			moved.kill9();
			// as with the default TTL of 30 s: a JVM takes longer to start than the 3 s of this suite
			jedis.expire("module:status:server:MultiSmppServer3:1", 25L);
			Node early = start("MultiSmppServer3", 2);
			check(!early.p.isAlive() && early.p.exitValue() == 255, "RG-12b kill -9: the heartbeat is still there: refused");
			if (early.p.isAlive()) early.kill9();
			jedis.expire("module:status:server:MultiSmppServer3:1", 1L);
			check(waitFor(() -> !jedis.exists("module:status:server:MultiSmppServer3:1"), 8000), "RG-12c the heartbeat ends with its TTL");
			Node later = start("MultiSmppServer3", 2);
			check(later.started(), "RG-12d then it starts");
			later.stop();
			Node same = start("MultiSmppServer3", 2);
			check(same.started(), "RG-12e the same node starts again: no conflict");
			same.stop();
			Node num = start("MultiSmppServer03", 0);
			check(!num.p.isAlive() && num.p.exitValue() == 255 && num.text().contains("is the number of MultiSmppServer3"), "RG-13a another name with the same number: refused");
			if (num.p.isAlive()) num.kill9();
			check(state("MultiSmppServer03") == null, "RG-13b and not registered");
			Node ten = start("MultiSmppServer10", 0);
			check(!ten.p.isAlive() && ten.p.exitValue() == 255, "RG-13c number above 9: refused");
			if (ten.p.isAlive()) ten.kill9();

			// ---- removed, draining ----
			ModuleRegistry.write("MultiSmppServer3", "state", ModuleRegistry.REMOVED, "removedAt", String.valueOf(System.currentTimeMillis()));
			Node back = start("MultiSmppServer03", 0);
			check(!back.p.isAlive() && back.p.exitValue() == 255, "RG-14a the number of a module removed just now is kept");
			if (back.p.isAlive()) back.kill9();
			Node re = start("MultiSmppServer3", 0);
			check(re.started() && "ACTIVE".equals(state("MultiSmppServer3")) && "0".equals(jedis.hget("system:module:MultiSmppServer3", "removedAt")), "RG-14b a removed module started again: ACTIVE");
			re.stop();
			set("MultiSmppServer3", ModuleRegistry.DRAINING);
			Node dr = start("MultiSmppServer3", 0);
			check(dr.started() && "DRAINING".equals(state("MultiSmppServer3")), "RG-14c a draining module restarted: stays draining");
			dr.stop();
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		exit();
	}
}
