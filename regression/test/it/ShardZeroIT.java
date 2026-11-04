package it;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ShardAssignmentChecker;
import com.king.redis.RedisClient;

/** A Core without shards (shards "none") and the NodeID of the start command, on two real Core processes. */
public class ShardZeroIT extends CoreFailoverIT {
	/** nodeId 0: started without -DNodeID, as a custom start command may do */
	static Process node(String module, int nodeId, String h) throws Exception {
		new File(h + "node.out").delete();
		new File(h + "log/it.log").delete();
		List<String> c = new ArrayList<>(List.of(System.getProperty("java.home") + "/bin/java", "-Xmx512m", "-Dit.keepRedis=true", "-Da2p_home=" + h, "-Dmodule=" + module));
		if (nodeId > 0) c.add("-DNodeID=" + nodeId);
		c.addAll(List.of("-Dlog4j.configurationFile=" + h + "conf/log4j2.xml", "-cp", System.getProperty("java.class.path"), "it.ShardZeroNode"));
		ProcessBuilder b = new ProcessBuilder(c);
		b.redirectErrorStream(true);
		b.redirectOutput(new File(h + "node.out"));
		Process p = b.start();
		final String out = h + "node.out";
		if (!waitFor(() -> new File(out).exists() && new String(Files.readAllBytes(Paths.get(out))).contains("READY"), 60000)) throw new IllegalStateException(module + " did not start");
		Thread.sleep(800);
		return p;
	}
	static Process fresh(String module, int nodeId) throws Exception {
		String h = homeOf(module);
		new ProcessBuilder("rm", "-rf", h).start().waitFor();
		new ProcessBuilder("cp", "-r", home, h).start().waitFor();
		return node(module, nodeId, h);
	}
	static void reload(String module, String shards) throws Exception {
		if (shards == null) jedis.del("system:core:shards:" + module);
		else jedis.set("system:core:shards:" + module, shards);
		jedis.publish("channel:gmms:control", "RELOAD_CORE_SHARDS@" + module);
	}
	@SuppressWarnings("unchecked")
	static Set<Integer> validate(String value) throws Exception {
		Method m = GmmsUtility.class.getDeclaredMethod("validateShardSet", String.class, int.class, String.class);
		m.setAccessible(true);
		try { return (Set<Integer>) m.invoke(null, value, 2, "MyShards"); }
		catch (java.lang.reflect.InvocationTargetException e) { return null; }
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
			check(validate("none") != null && validate("none").isEmpty() && validate(" NONE ") != null, "MS-01a a hot reload accepts none");
			check(validate("") == null && validate("x") == null && validate("5") == null && validate("0-1").size() == 2, "MS-01b it still refuses an empty value and errors");

			jedis.set("system:core:shards:" + C1, "0");
			jedis.set("system:core:shards:" + C2, "1");
			n1 = fresh(C1, 5);
			n2 = fresh(C2, 6);

			// ---- the NodeID of the start command ----
			check(jedis.exists("module:status:core:" + C1 + ":5") && jedis.exists("module:status:core:" + C2 + ":6"), "MS-31a -DNodeID wins over the NodeID=1 of the file: nodes 5 and 6");
			check(logHas(C1, "Consumer: core-mt-router-5") && logHas(C2, "Consumer: core-mt-router-6"), "MS-31b consumer names differ by node");

			allOnce(send("A", 100), "MS-01c two Cores, one shard each");

			// ---- shards taken away ----
			reload(C1, "none");
			final String key = "system:shards:core:" + C1 + ":5";
			check(waitFor(() -> "2|".equals(jedis.get(key)), 3000), "MS-01d RELOAD_CORE_SHARDS: published at once, not with the heartbeat of 10 s: " + jedis.get(key));
			check(logHas(C1, "myShards=none"), "MS-01e the log says none");
			List<String> waiting = send("B", 100);
			Thread.sleep(3000);
			long left = backlog(0);
			check(left > 0 && backlog(1) == 0, "MS-01f shard 0 is not consumed, not falling back to shard 0: left " + left + ", shard 1 left " + backlog(1));

			// ---- an empty value is another thing: the fallback of today ----
			reload(C1, null);
			check(waitFor(() -> backlog(0) == 0, 20000), "MS-02 control: no value at all falls back to MyShards (0): shard 0 is consumed");
			allOnce(waiting, "MS-01g every MT once");

			// ---- moved to the other Core ----
			reload(C1, "none");
			reload(C2, "0-1");
			final String key2 = "system:shards:core:" + C2 + ":6";
			check(waitFor(() -> "2|".equals(jedis.get(key)) && "2|0,1".equals(jedis.get(key2)), 3000), "MS-01h both changes published: " + jedis.get(key) + " " + jedis.get(key2));
			List<String> moved = send("C", 100);
			allOnce(moved, "MS-01i Core 2 serves both shards");
			check(logHas(C2, OSSID + "_C0") && !logHas(C1, OSSID + "_C"), "MS-01j Core 2 logged them, the Core without shards touched none of them");
			List<String> problems = ShardAssignmentChecker.check(RedisClient.getInstance());
			check(problems.isEmpty(), "MS-01k [SHARD-CHECK]: a Core without shards is neither an overlap nor missing: " + problems);

			// ---- start with none in the file, no value in Redis ----
			kill(n1);
			jedis.del("system:core:shards:" + C1);
			Path c1 = Paths.get(homeOf(C1) + "Gmms/GmmsConfig.properties");
			Files.write(c1, (new String(Files.readAllBytes(c1), "UTF-8") + "\nCoreShard." + C1 + "=none\n").getBytes("UTF-8"));
			n1 = node(C1, 5, homeOf(C1));
			check(logHas(C1, "myShards=none") && "2|".equals(jedis.get(key)), "MS-01l CoreShard.x=none in the file: starts without shards: " + jedis.get(key));
			List<String> more = send("D", 50);
			allOnce(more, "MS-01m Core 2 still serves them");
			check(logHas(C2, OSSID + "_D0") && !logHas(C1, OSSID + "_D"), "MS-01n on Core 2, none of them on Core 1");

			// ---- entries pending under the old consumer name, the node number changed ----
			List<String> load = new ArrayList<>();
			Thread sender = new Thread(() -> { try { load.addAll(send("E", 1500)); } catch (Exception e) { } });
			sender.start();
			Thread.sleep(400);
			kill(n2);
			sender.join();
			check(backlog(0) + backlog(1) > 0, "MS-31c killed under load: entries left: " + (backlog(0) + backlog(1)));
			n2 = node(C2, 7, homeOf(C2));
			check(jedis.exists("module:status:core:" + C2 + ":7"), "MS-31d restarted with another node number");
			allOnce(load, "MS-31e entries of the old consumer name are taken over, every MT once");

			// ---- no -DNodeID ----
			kill(n1);
			n1 = node(C1, 0, homeOf(C1));
			check(jedis.exists("module:status:core:" + C1 + ":1"), "MS-31f without -DNodeID: the NodeID of the file");
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		finally {
			if (n1 != null) n1.destroyForcibly();
			if (n2 != null) n2.destroyForcibly();
		}
		exit();
	}
}
