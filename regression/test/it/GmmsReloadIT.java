package it;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.king.gmms.GmmsUtility;
import com.king.message.gmms.CDRManager;
import com.king.gmms.messagequeue.StreamQueueManager;

/** GmmsConfig reload: validation by type, strict shards, removed keys, Redis copy (not part of the repo). */
public class GmmsReloadIT extends ItSupport {
	static Path cfg;
	static byte[] orig;
	static final String EFF = "config:effective:CoreEngine1:1";

	/** the original file plus extra lines (a line "-Key" removes Key); returns the reload result */
	static int reload(String... lines) throws Exception {
		String text = new String(orig, "ISO-8859-1");
		StringBuilder sb = new StringBuilder();
		for (String l : text.split("\n")) {
			boolean drop = false;
			for (String x : lines) {
				String k = x.startsWith("-") ? x.substring(1) : x.split("=")[0];
				if (l.trim().startsWith(k + "=")) drop = true;
			}
			if (!drop) sb.append(l).append("\n");
		}
		for (String x : lines) if (!x.startsWith("-")) sb.append(x).append("\n");
		Files.write(cfg, sb.toString().getBytes("ISO-8859-1"));
		return GmmsUtility.getInstance().reloadGmmsConfig("-a");
	}

	static String prop(String k) {
		return GmmsUtility.getInstance().getCommonProperty(k);
	}

	static long logCount(String text) throws Exception {
		long n = 0;
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(text)) n++;
		return n;
	}

	public static void main(String[] args) throws Exception {
		boot();
		cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		orig = Files.readAllBytes(cfg);
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		} finally {
			Files.write(cfg, orig);
		}
		exit();
	}

	static void run() throws Exception {
		GmmsUtility gu = GmmsUtility.getInstance();
		String shards = String.valueOf(gu.getMyShards());

		// ---- R-01 types: text, decimal, boolean keys reload ----
		check(reload("Metrics.LogLevel=INFO") == 0 && "INFO".equals(prop("Metrics.LogLevel")),
				"R-01a text key Metrics.LogLevel=INFO applied (before: whole reload failed)");
		check(reload("Metrics.LogLevel=INFO", "Metrics.MinTPS=0.5") == 0 && "0.5".equals(prop("Metrics.MinTPS")),
				"R-01b decimal key Metrics.MinTPS=0.5 applied");
		check(reload("DB.PoolStats.Enable=true", "MessageIdIncludeNodeId=true", "RedisStreamMaxDeliveries=7") == 0
				&& "true".equals(prop("DB.PoolStats.Enable")) && "true".equals(prop("MessageIdIncludeNodeId"))
				&& StreamQueueManager.getInstance().getMaxDeliveries() == 7,
				"R-01c boolean keys DB.PoolStats.Enable / MessageIdIncludeNodeId applied together with another key");
		Files.write(cfg, orig);
		gu.reloadGmmsConfig("-a");
		check(reload("CoreMtIdempotency=ture") == 1 && prop("CoreMtIdempotency") == null,
				"R-01d boolean typo 'ture' rejected (before: silently false)");
		check(reload("RedisStreamMaxDeliveries=7x") == 1 && StreamQueueManager.getInstance().getMaxDeliveries() == 10,
				"R-01e bad integer rejected");

		// ---- R-02 shards: a bad value rejects the reload ----
		check(reload("MyShards=0-3x") == 1 && shards.equals(String.valueOf(gu.getMyShards())),
				"R-02a MyShards=0-3x rejected, shards unchanged " + shards + " (before: accepted, shards became [0])");
		check(reload("MyShards=0-" + gu.getTotalShards()) == 1 && shards.equals(String.valueOf(gu.getMyShards())),
				"R-02b MyShards beyond TotalShards-1 rejected");
		check(reload("MyShards=") == 1, "R-02c empty MyShards rejected");
		check(reload("TotalShards=1") == 1 && gu.getTotalShards() > 1,
				"R-02d TotalShards lowered below the existing MyShards/CoreShard rejected");

		// ---- R-03 a key removed from the file goes back to its default ----
		check(reload("RedisStreamMaxDeliveries=7", "MessageExpireTime=100") == 0
				&& StreamQueueManager.getInstance().getMaxDeliveries() == 7 && gu.getExpireTimeInMinute() == 100,
				"R-03a set RedisStreamMaxDeliveries=7, MessageExpireTime=100");
		check(reload("-RedisStreamMaxDeliveries", "-MessageExpireTime") == 0 && prop("RedisStreamMaxDeliveries") == null
				&& StreamQueueManager.getInstance().getMaxDeliveries() == 10 && gu.getExpireTimeInMinute() == 1380,
				"R-03b removed from the file: back to defaults 10 and 1380 (before: kept 7 and 100)");

		// ---- R-04 startup-only keys reported as restart required ----
		long before = logCount("Restart required GmmsConfig: MaxCPUUsage");
		check(reload("MaxCPUUsage=70") == 0 && logCount("Restart required GmmsConfig: MaxCPUUsage") == before + 1,
				"R-04 MaxCPUUsage (read once at startup) logged as Restart required, not Applied");

		// ---- R-05 restart-required key with a bad value: reported now ----
		before = logCount("will fail at next restart: key=Redis_master_port");
		check(reload("Redis_master_port=16379x") == 0
				&& logCount("will fail at next restart: key=Redis_master_port") == before + 1,
				"R-05 Redis_master_port=16379x: ERROR 'will fail at next restart', reload not rejected");

		// ---- R-06 CDR file size reloads ----
		check(reload("CDRFileMaxSize=200") == 0 && CDRManager.getMaxFileSize() == 200 * 1024,
				"R-06 CDRFileMaxSize=200 applied to the CDR writer (" + CDRManager.getMaxFileSize() + " bytes)");

		// ---- R-07 effective config in Redis ----
		reload("MaxCPUUsage=70", "Redis_master_port=16379x");    // both still differ from what runs
		Map<String, String> h = jedis.hgetAll(EFF);
		check(jedis.sismember("config:effective:modules", "CoreEngine1:1"), "R-07a module listed in config:effective:modules");
		check("reload".equals(h.get("_meta.source")) && h.get("_meta.loadedAt") != null
				&& h.get("TotalShards") != null && h.get("TotalShards").equals(prop("TotalShards")),
				"R-07b " + EFF + " holds the effective values (TotalShards=" + h.get("TotalShards") + "), _meta.loadedAt="
						+ h.get("_meta.loadedAt"));
		boolean masked = true;
		for (Map.Entry<String, String> e : h.entrySet()) {
			String k = e.getKey().toLowerCase();
			if ((k.contains("password") || k.contains("pwd")) && e.getValue().length() > 0 && !"******".equals(e.getValue())) masked = false;
		}
		check(masked, "R-07c password values masked");
		String pending = h.get("_meta.pendingRestart");
		check(pending != null && pending.contains("MaxCPUUsage") && pending.contains("Redis_master_port"),
				"R-07d _meta.pendingRestart lists keys changed but not applied: " + pending);
		Files.write(cfg, orig);
		gu.reloadGmmsConfig("-a");
		check("".equals(jedis.hget(EFF, "_meta.pendingRestart")),
				"R-07d2 file reverted to the running values: _meta.pendingRestart empty again");
		reload("RedisStreamMaxDeliveries=8");
		check("8".equals(jedis.hget(EFF, "RedisStreamMaxDeliveries")) && "ok".equals(jedis.hget(EFF, "_meta.reloadResult")),
				"R-07e after a reload the new value is in Redis");
		reload("RedisStreamMaxDeliveries=bad");
		check("8".equals(jedis.hget(EFF, "RedisStreamMaxDeliveries")) && "failed".equals(jedis.hget(EFF, "_meta.reloadResult")),
				"R-07f failed reload: values unchanged in Redis, _meta.reloadResult=failed");
		reload("-RedisStreamMaxDeliveries");
		check(!jedis.hexists(EFF, "RedisStreamMaxDeliveries"), "R-07g key removed from the file is removed from Redis (default applies)");
		reload("GmmsConfigPublishEnable=false");
		String at = jedis.hget(EFF, "_meta.loadedAt");
		Thread.sleep(20);
		reload("GmmsConfigPublishEnable=false", "RedisStreamMaxDeliveries=9");
		check(at.equals(jedis.hget(EFF, "_meta.loadedAt")), "R-07h GmmsConfigPublishEnable=false: not written");
	}
}
