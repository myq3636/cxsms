package it;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.*;
import java.util.Properties;

import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;

/**
 * SWITCH_REDIS on the control channel (RedisSwitchListener, docs/流模式下系统管理和模块间连接代码清理分析.md 4.1): the
 * module moves to a side only when every Redis of it answers role:master, records it in ha/RedisStatus, keeps the
 * control channel (subscribed again on the new state Redis); at start the side whose state Redis is the master wins.
 * Here the IT Redis plays both sides once the slave addresses point at it.
 */
public class RedisSwitchIT extends ItSupport {
	static final String CHANNEL = "channel:gmms:control";

	static String statusFile() throws Exception {
		Path p = Paths.get(GmmsUtility.getInstance().getRedisFile());
		if (!Files.exists(p)) return null;
		Properties pr = new Properties();
		try (java.io.InputStream in = Files.newInputStream(p)) {
			pr.load(in);
		}
		return pr.getProperty("Used");
	}

	static boolean itLog(String part) throws Exception {
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(part)) return true;
		return false;
	}

	static void set(Object o, String name, Object v) throws Exception {
		Field f = RedisClient.class.getDeclaredField(name);
		f.setAccessible(true);
		f.set(o, v);
	}

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		}
		exit();
	}

	static void run() throws Exception {
		boot();
		RedisClient rc = RedisClient.getInstance();
		Files.deleteIfExists(Paths.get(GmmsUtility.getInstance().getRedisFile()));
		check(rc.isRedisHaFlag() && "M".equals(rc.detectMasterSide()), "RS-01 at start on the master side, its state Redis answers role:master");
		Thread.sleep(1500);   // the control subscriber subscribed

		// ---- the slave side is not a master: refused ----
		set(rc, "host1", "127.0.0.1");
		set(rc, "port1", 16399);   // nobody there
		jedis.publish(CHANNEL, "SWITCH_REDIS:slave");
		Thread.sleep(4000);
		check(rc.isRedisHaFlag() && itLog("not switched to the slave side") && statusFile() == null,
				"RS-02 SWITCH_REDIS:slave while the slave side is not a master: refused, still on the master side");

		// ---- the slave side answers master (here the same Redis): switched ----
		set(rc, "port1", 16379);
		String[] same = { "127.0.0.1:16379" };
		set(rc, "submitSlaves", same);
		set(rc, "reportSlaves", same);
		jedis.publish(CHANNEL, "SWITCH_REDIS:slave");
		check(waitFor(() -> !rc.isRedisHaFlag(), 8000) && "S".equals(statusFile()),
				"RS-03 SWITCH_REDIS:slave: every module Redis on the slave side, ha/RedisStatus Used=S: " + statusFile());
		check(rc.setString("rs:probe", "x") && "x".equals(rc.getString("rs:probe")), "RS-04 Redis used normally after the switch");
		jedis.publish(CHANNEL, "SWITCH_REDIS:master");
		check(waitFor(() -> rc.isRedisHaFlag(), 8000) && "M".equals(statusFile()),
				"RS-05 the control channel still heard after the switch (subscribed again on the new state Redis): back to the master side");
		jedis.publish(CHANNEL, "SWITCH_REDIS:master");
		Thread.sleep(1500);
		check(rc.isRedisHaFlag() && itLog("already on the master side"), "RS-06 a second SWITCH_REDIS:master: nothing to do");

		// ---- at start: the side whose state Redis is the master wins over ha/RedisStatus ----
		GmmsUtility.getInstance().writeRedisStatus("S");
		set(rc, "port1", 16399);
		Method m = GmmsUtility.class.getDeclaredMethod("startupRedisSide", String.class);
		m.setAccessible(true);
		check("M".equals(m.invoke(GmmsUtility.getInstance(), "M")), "RS-07 start: ha/RedisStatus says S, the master answers role:master: M");
		set(rc, "host", "127.0.0.1");
		set(rc, "port", 16398);   // the master side down too: the file decides
		check("S".equals(m.invoke(GmmsUtility.getInstance(), "M")), "RS-08 start: no state Redis answers master: the side of ha/RedisStatus (S)");
		set(rc, "port", 16379);
		Files.deleteIfExists(Paths.get(GmmsUtility.getInstance().getRedisFile()));
		jedis.del("rs:probe");
	}
}
