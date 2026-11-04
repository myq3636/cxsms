package com.king.gmms.ha;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.message.gmms.MessageIdGenerator;
import com.king.redis.RedisClient;

/**
 * Turns on the v2 message id format (MessageIdFormat=v2, see docs/消息ID防重复设计.md) when the Redis client
 * is up, before the module accepts traffic, and keeps the counter position in Redis: a restart continues
 * after it even when the counter ran ahead of the clock.
 */
public class MessageIdSetup {
	private static final SystemLogger log = SystemLogger.getSystemLogger(MessageIdSetup.class);
	private static final String MARK_PREFIX = "system:msgid:mark:";
	private static final String HEX_MARK_PREFIX = "system:msgid:hexmark:";
	private static final int MAX_MODULE_NO = 9;
	private static final long DEFAULT_COUNTER_PER_SECOND = 5000L;
	/** the mark is saved every second: on a restart, 2 s at 10000 TPS past it */
	private static final long RESTART_MARGIN = 20000L;
	private static final int MARK_TTL_SECONDS = 7 * 86400;
	private static final long WARN_INTERVAL_MS = 60000L;
	private static final long CYCLE_WINDOW_MS = 3600000L;

	private static boolean started = false;
	private static long lastSaved = -1L;
	private static long lastHexSaved = -1L;
	private static long lastSaveWarnAt = 0L;
	private static long lastAheadWarnAt = 0L;
	private static long windowCounter = -1L;
	private static long windowStart = 0L;

	private MessageIdSetup() {
	}

	/** {module}.MessageIdFormat first, then MessageIdFormat. */
	private static boolean isV2(GmmsUtility gmmsUtility, String module) {
		String format = gmmsUtility.getCommonProperty(module + ".MessageIdFormat",
				gmmsUtility.getCommonProperty("MessageIdFormat", "v2"));
		return !"v1".equalsIgnoreCase(format);
	}

	/** With v2 the id starts with the module number 1 to 9 (a leading 0 could be lost by customers): one further in the list must not start. A module not in the list stays on v1 (configure). */
	public static boolean validate() {
		String module = System.getProperty("module");
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
		if (module == null || !isV2(gmmsUtility, module)) {
			return true;
		}
		int moduleNo = ModuleManager.getInstance().getModuleNo(module);
		if (moduleNo > MAX_MODULE_NO) {
			String text = "[MSGID] MessageIdFormat=v2 needs a module number 1 to " + MAX_MODULE_NO + ", " + module
					+ " has " + moduleNo + " in the module list";
			log.fatal(text);
			System.out.println(text);
			return false;
		}
		return true;
	}

	public static synchronized void configure(GmmsUtility gmmsUtility) {
		String module = System.getProperty("module");
		if (started || module == null) {
			return;
		}
		try {
			if (!isV2(gmmsUtility, module)) {
				return;
			}
			ModuleManager moduleManager = ModuleManager.getInstance();
			int moduleNo = moduleManager.getModuleNo(module);
			if (moduleNo < 1 || moduleNo > MAX_MODULE_NO) {
				log.error("[MSGID] v2 format not used, ids of {} can repeat: its module number is {}", module, moduleNo);
				return;
			}
			long counterPerSecond = counterPerSecond(gmmsUtility);
			MessageIdGenerator.enableV2(moduleNo, counterPerSecond, moduleManager.isClientType(module),
					savedCounter(gmmsUtility, MARK_PREFIX, module), savedCounter(gmmsUtility, HEX_MARK_PREFIX, module));
			started = true;
			log.info("[MSGID] v2 format on. module={}, moduleNo={}, counterPerSecond={}, counter={}", module, moduleNo,
					counterPerSecond, MessageIdGenerator.v2Counter());
			startMarkThread(gmmsUtility, module);
		} catch (Exception e) {
			log.error("[MSGID] v2 format not enabled for " + module, e);
		}
	}

	private static long counterPerSecond(GmmsUtility gmmsUtility) {
		try {
			long value = Long.parseLong(gmmsUtility.getCommonProperty("MessageIdCounterPerSecond",
					String.valueOf(DEFAULT_COUNTER_PER_SECOND)).trim());
			return value > 0 ? value : DEFAULT_COUNTER_PER_SECOND;
		} catch (Exception e) {
			return DEFAULT_COUNTER_PER_SECOND;
		}
	}

	private static long savedCounter(GmmsUtility gmmsUtility, String prefix, String module) {
		try {
			String mark = gmmsUtility.getRedisClient().getString(prefix + module);
			if (mark != null) {
				return Long.parseLong(mark.trim()) + RESTART_MARGIN;
			}
			log.warn("[MSGID] no mark: {} has no saved position in {} (first start, or Redis not available); ids used just before a restart could be used again", module, prefix);
		} catch (Exception e) {
			log.warn("[MSGID] no mark: could not read the saved position of " + module, e);
		}
		return 0L;
	}

	private static void startMarkThread(final GmmsUtility gmmsUtility, final String module) {
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "MessageIdMark");
				t.setDaemon(true);
				return t;
			}
		});
		executor.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				try {
					tick(gmmsUtility, module);
				} catch (Throwable t) {
					log.warn("[MSGID] position check failed", t);
				}
			}
		}, 1L, 1L, TimeUnit.SECONDS);
	}

	private static void tick(GmmsUtility gmmsUtility, String module) {
		long now = System.currentTimeMillis();
		long counter = MessageIdGenerator.v2Counter();
		if (counter != lastSaved && save(gmmsUtility, MARK_PREFIX, module, counter, now)) {
			lastSaved = counter;
		}
		long hexCounter = MessageIdGenerator.v2HexCounter();
		if (hexCounter != lastHexSaved && save(gmmsUtility, HEX_MARK_PREFIX, module, hexCounter, now)) {
			lastHexSaved = hexCounter;
		}
		long aheadSeconds = MessageIdGenerator.v2AheadSeconds();
		if (aheadSeconds > longProperty(gmmsUtility, "MessageIdAheadWarnSeconds", 3600L)
				&& now - lastAheadWarnAt >= WARN_INTERVAL_MS) {
			lastAheadWarnAt = now;
			log.warn("[MSGID] ahead: the counter of {} is {} s ahead of the clock, the rate stays above MessageIdCounterPerSecond",
					module, aheadSeconds);
		}
		if (windowCounter < 0) {
			windowCounter = counter;
			windowStart = now;
		} else if (now - windowStart >= CYCLE_WINDOW_MS) {
			long expireMinutes = longProperty(gmmsUtility, "MessageExpireTime", 1380L);
			if (cycleShorterThan(counter - windowCounter, now - windowStart, expireMinutes)) {
				log.warn("[MSGID] cycle: at the rate of the last hour the ids of {} repeat sooner than MessageExpireTime ({} minutes)",
						module, expireMinutes);
			}
			windowCounter = counter;
			windowStart = now;
		}
	}

	private static boolean save(GmmsUtility gmmsUtility, String prefix, String module, long counter, long now) {
		boolean saved = false;
		try {
			RedisClient redis = gmmsUtility.getRedisClient();
			saved = redis != null && redis.setString(prefix + module, String.valueOf(counter), MARK_TTL_SECONDS);
		} catch (Exception e) {
			log.debug("[MSGID] save position failed", e);
		}
		if (!saved && now - lastSaveWarnAt >= WARN_INTERVAL_MS) {
			lastSaveWarnAt = now;
			log.warn("[MSGID] could not save the position of {}", module);
		}
		return saved;
	}

	/** Would ids repeat within the given minutes, going on at this rate? */
	public static boolean cycleShorterThan(long counterDelta, long elapsedMs, long minutes) {
		if (counterDelta <= 0 || elapsedMs <= 0) {
			return false;
		}
		double cycleMs = MessageIdGenerator.V2_COUNTER_SPACE * (double) elapsedMs / counterDelta;
		return cycleMs < minutes * 60000.0;
	}

	private static long longProperty(GmmsUtility gmmsUtility, String key, long defaultValue) {
		try {
			return Long.parseLong(gmmsUtility.getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
