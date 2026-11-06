package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.messagequeue.ChannelReleaseDispatcher;
import com.king.gmms.messagequeue.CoreMQMStreamConsumer;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.processor.CsmExpirySweeper;
import com.king.redis.RedisClient;

/**
 * Draining a module that is going away (registry state DRAINING, see docs/模块动态扩缩容设计.md section 8): it
 * takes no new work and finishes what it has, then reports done; the process is stopped by hand. Writing what
 * memory holds is left to the stop itself (GracefulStop). For a Core the shards are moved away by hand first:
 * results, inbound DRs and held MTs are read by every Core, so this Core stops reading them once it has no shard.
 */
public final class DrainController {
	private static final SystemLogger log = SystemLogger.getSystemLogger(DrainController.class);
	private static final String PROGRESS_PREFIX = "module:drain:";
	private static final int PROGRESS_TTL_SECONDS = 30;
	private static final long DEFAULT_MAX_SECONDS = 600L;
	private static final String PROGRESS_LUA =
			"redis.call('DEL', KEYS[1]) redis.call('HSET', KEYS[1], unpack(ARGV, 2)) "
			+ "redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])) return 1";

	public static final String WAIT_SHARDS = "WAIT_SHARDS";
	public static final String WAIT_INFLIGHT = "WAIT_INFLIGHT";
	public static final String DONE = "DONE";
	public static final String NOT_SUPPORTED = "NOT_SUPPORTED";

	private static Thread thread = null;
	private static volatile boolean draining = false;
	private static volatile boolean readersStopped = false;

	private DrainController() {
	}

	public static boolean isDraining() {
		return draining;
	}

	/** The registry state of this module, at start and on every change. */
	public static synchronized void onState(String state) {
		boolean wanted = ModuleRegistry.DRAINING.equals(state) || ModuleRegistry.DRAINED.equals(state);
		if (wanted && !draining) {
			begin();
		} else if (!wanted && draining) {
			cancel();
		}
	}

	private static void begin() {
		draining = true;
		final String module = System.getProperty("module");
		final long startedAt = System.currentTimeMillis();
		log.info("[DRAIN] {} starts draining", module);
		thread = new Thread("DrainController") {
			public void run() {
				long readersStoppedAt = 0L;
				while (draining) {
					try {
						String stage;
						List<String> detail = new ArrayList<String>();
						if (!ModuleManager.getInstance().isCoreEngine(module)) {
							stage = NOT_SUPPORTED;
						} else if (!GmmsUtility.getInstance().getMyShards().isEmpty()) {
							stage = WAIT_SHARDS;
							detail.addAll(Arrays.asList("shards", GmmsUtility.getInstance().formatShardSet(GmmsUtility.getInstance().getMyShards())));
						} else {
							stopCoreReaders();
							if (readersStoppedAt == 0L) {
								readersStoppedAt = System.currentTimeMillis();
							}
							int inflight = coreInFlight();
							boolean late = System.currentTimeMillis() - readersStoppedAt > maxSeconds() * 1000L;
							stage = inflight == 0 || late ? DONE : WAIT_INFLIGHT;
							detail.addAll(Arrays.asList("inflight", String.valueOf(inflight)));
							if (late && inflight > 0) {
								detail.addAll(Arrays.asList("leftToAutoClaim", String.valueOf(inflight)));
							}
						}
						report(module, stage, startedAt, detail);
						Thread.sleep(1000L);
					} catch (InterruptedException e) {
						return;
					} catch (Exception e) {
						log.warn("[DRAIN] progress failed", e);
					}
				}
			}
		};
		thread.setDaemon(true);
		thread.start();
	}

	private static void cancel() {
		draining = false;
		if (thread != null) {
			thread.interrupt();
			thread = null;
		}
		if (readersStopped) {
			ResultStreamConsumer.getInstance().start();
			InboundDRStreamConsumer.getInstance().start();
			ChannelReleaseDispatcher.getInstance().start();
			CsmExpirySweeper.getInstance().start();
			readersStopped = false;
		}
		try {
			RedisClient.getInstance().del(PROGRESS_PREFIX + System.getProperty("module"));
		} catch (Exception e) {
			log.warn("[DRAIN] progress not removed", e);
		}
		log.info("[DRAIN] {} cancelled, back to normal service", System.getProperty("module"));
	}

	/** Every second: a reader that started late (the module restarted while draining) is stopped again. */
	private static void stopCoreReaders() {
		boolean any = false;
		if (ResultStreamConsumer.getInstance().isRunning()) {
			ResultStreamConsumer.getInstance().stop();
			any = true;
		}
		if (InboundDRStreamConsumer.getInstance().isRunning()) {
			InboundDRStreamConsumer.getInstance().stop();
			any = true;
		}
		if (ChannelReleaseDispatcher.getInstance().isRunning()) {
			ChannelReleaseDispatcher.getInstance().stop();
			any = true;
		}
		if (CsmExpirySweeper.getInstance().isRunning()) {
			CsmExpirySweeper.getInstance().stop();
			any = true;
		}
		if (any) {
			readersStopped = true;
			log.info("[DRAIN] {} no shard left: stopped reading results, inbound DRs and held MTs", System.getProperty("module"));
		}
	}

	private static int coreInFlight() {
		return MTStreamConsumer.getInstance().inFlight() + ResultStreamConsumer.getInstance().inFlight()
				+ InboundDRStreamConsumer.getInstance().inFlight() + CoreMQMStreamConsumer.getInstance().inFlight();
	}

	private static void report(String module, String stage, long startedAt, List<String> detail) {
		List<String> args = new ArrayList<String>(Arrays.asList(String.valueOf(PROGRESS_TTL_SECONDS),
				"stage", stage, "startedAt", String.valueOf(startedAt), "done", String.valueOf(DONE.equals(stage)),
				"updatedAt", String.valueOf(System.currentTimeMillis())));
		args.addAll(detail);
		RedisClient.getInstance().getStateRedis().evalScript(PROGRESS_LUA, Arrays.asList(PROGRESS_PREFIX + module), args);
	}

	private static long maxSeconds() {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty("ModuleDrainMaxSeconds",
					String.valueOf(DEFAULT_MAX_SECONDS)).trim());
		} catch (Exception e) {
			return DEFAULT_MAX_SECONDS;
		}
	}
}
