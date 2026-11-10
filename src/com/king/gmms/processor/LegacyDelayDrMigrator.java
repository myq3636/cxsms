package com.king.gmms.processor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * Delayed DRs written by an older version for MQM (SendDelayRedisDRThread, removed): key DELAYDR_{inMsgId},
 * ZSET delayDR_{ossid} (score = due time in seconds), SET delayDR. Core moves what is left to its own
 * delayed dispatch once after an upgrade; one Core at a time (lock), then nothing is left to do.
 */
public final class LegacyDelayDrMigrator {
	private static final SystemLogger log = SystemLogger.getSystemLogger(LegacyDelayDrMigrator.class);
	private static final String LOCK = "delayDR:migrate:lock";
	// KEYS: 1 zset; ARGV: 1 count. Takes up to count entries: member, score, body (nil when expired).
	private static final String TAKE_LUA =
			"local m = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1, 'WITHSCORES') "
			+ "local out = {} "
			+ "for i = 1, #m, 2 do "
			+ "  local body = redis.call('GET', m[i]) "
			+ "  redis.call('DEL', m[i]) redis.call('ZREM', KEYS[1], m[i]) "
			+ "  table.insert(out, m[i]) table.insert(out, m[i + 1]) table.insert(out, body or '') "
			+ "end "
			+ "return out";

	private LegacyDelayDrMigrator() {
	}

	/** runs on a thread of its own; quick when there is nothing to move */
	public static void startOnce() {
		Thread t = new Thread(new Runnable() {
			public void run() {
				try {
					migrate();
				} catch (Exception e) {
					log.warn("legacy delayed DR migration failed: {}", e.getMessage());
				}
			}
		}, "LegacyDelayDrMigrator");
		t.setDaemon(true);
		t.start();
	}

	static int migrate() {
		RedisConnection redis = GmmsUtility.getInstance().getRedisClient().getStateRedis();
		Set<String> zsets = redis.smembers("delayDR");
		if (zsets == null || zsets.isEmpty()) {
			return 0;
		}
		Object locked = redis.evalScript("return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', 600)",
				Collections.singletonList(LOCK), Collections.singletonList(String.valueOf(System.getProperty("module"))));
		if (locked == null) {
			return 0;
		}
		int moved = 0;
		long nowSec = System.currentTimeMillis() / 1000L;
		for (String zset : zsets) {
			while (true) {
				Object r = redis.evalScript(TAKE_LUA, Collections.singletonList(zset), Collections.singletonList("200"));
				List<?> l = r instanceof List ? (List<?>) r : Collections.emptyList();
				if (l.isEmpty()) {
					break;
				}
				for (int i = 0; i + 2 < l.size(); i += 3) {
					String body = String.valueOf(l.get(i + 2));
					GmmsMessage dr = body.isEmpty() ? null : SerializableHandler.convertRedisMssage2GmmsMessage(body);
					if (dr == null) {
						continue;
					}
					long due = (long) Double.parseDouble(String.valueOf(l.get(i + 1)));
					GmmsUtility.getInstance().getMessageStoreManager().insertMsgForDRDelay(dr, (int) Math.max(0L, due - nowSec));
					moved++;
				}
			}
			redis.evalScript("if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('SREM', KEYS[2], ARGV[1]) end return 1",
					Arrays.asList(zset, "delayDR"), Collections.singletonList(zset));
		}
		log.info("legacy delayed DRs moved to Core dispatch: {}", moved);
		return moved;
	}
}
