package it;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import com.king.gmms.GmmsUtility;
import com.king.gmms.ha.ShardAssignmentChecker;
import com.king.redis.RedisClient;

/** Shard check grouped by stream family (SMPP vs HTTP clients) and the owners summary; not part of the repo. */
public class ShardGroupIT extends ItSupport {
	static String range(int a, int b) {
		StringBuilder sb = new StringBuilder();
		for (int i = a; i <= b; i++) { if (sb.length() > 0) sb.append(','); sb.append(i); }
		return sb.toString();
	}

	static void node(String group, String module, String shards) {
		jedis.setex("system:shards:" + group + ":" + module + ":0", 60, "16|" + shards);
	}

	static long logCount(String text) throws Exception {
		long n = 0;
		for (String l : Files.readAllLines(new File(home + "log/it.log").toPath())) if (l.contains(text)) n++;
		return n;
	}

	public static void main(String[] args) throws Exception {
		try {
			boot();
			RedisClient r = GmmsUtility.getInstance().getRedisClient();
			for (String k : jedis.keys("system:shards:*")) jedis.del(k);
			node("core", "CoreEngine1", range(0, 15));
			node("client", "MultiSmppClient1", range(0, 7));
			node("client", "MultiSmppClient2", range(8, 15));
			node("httpclient", "CommonHttpClient1", range(8, 15));
			node("httpclient", "CommonHttpClient2", range(0, 7));
			List<String> p = ShardAssignmentChecker.check(r);
			check(p.isEmpty(), "SG-01 SMPP clients 0-7/8-15 and HTTP clients 8-15/0-7: no overlap reported " + p);
			check(logCount("[SHARD-CHECK] shard owners: ") == 1
					&& logCount("client: client:MultiSmppClient1:0=0-7, client:MultiSmppClient2:0=8-15") == 1
					&& logCount("httpclient: httpclient:CommonHttpClient1:0=8-15, httpclient:CommonHttpClient2:0=0-7") == 1,
					"SG-02 owners summary logged with ranges per group");
			ShardAssignmentChecker.check(r);
			check(logCount("[SHARD-CHECK] shard owners: ") == 1, "SG-03 unchanged picture: summary not repeated");

			jedis.del("system:shards:httpclient:CommonHttpClient2:0");
			p = ShardAssignmentChecker.check(r);
			check(p.size() == 1 && p.get(0).startsWith("ERROR httpclient shards owned by no node") && p.get(0).contains("[0, 1, 2, 3, 4, 5, 6, 7]"),
					"SG-04 no HTTP client for 0-7: ERROR for httpclient (the SMPP client owning 0-7 no longer hides it) " + p);
			check(logCount("[SHARD-CHECK] shard owners: ") == 2, "SG-05 changed picture: summary logged again");

			node("httpclient", "CommonHttpClient2", range(0, 7));
			node("client", "MultiSmppClient3", "7,8");
			p = ShardAssignmentChecker.check(r);
			check(p.size() == 1 && p.get(0).startsWith("WARN client shards owned by more than one node: [7=")
					&& !p.get(0).contains("httpclient"),
					"SG-06 two SMPP clients owning 7 and 8: overlap WARN within the client group only " + p);
			jedis.del("system:shards:client:MultiSmppClient3:0");

			ShardAssignmentChecker.setShardGroup("httpclient");
			ShardAssignmentChecker.publish(GmmsUtility.getInstance(), "client", "CommonHttpClient9", "0", 60);
			check(jedis.exists("system:shards:httpclient:CommonHttpClient9:0") && !jedis.exists("system:shards:client:CommonHttpClient9:0"),
					"SG-07 an HTTP client publishes its shards under httpclient (module role stays client)");
			check("0-3,8-9".equals(range2("0,1,2,3,8,9")), "SG-08 range format 0,1,2,3,8,9 -> 0-3,8-9");
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		}
		exit();
	}

	static String range2(String csv) throws Exception {
		java.util.Set<Integer> s = new java.util.TreeSet<Integer>();
		for (String x : csv.split(",")) s.add(Integer.parseInt(x));
		java.lang.reflect.Method m = ShardAssignmentChecker.class.getDeclaredMethod("ranges", java.util.Set.class);
		m.setAccessible(true);
		return (String) m.invoke(null, s);
	}
}
