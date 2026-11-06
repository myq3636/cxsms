package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.customerconnectionfactory.MultiSmppClientFactory;
import com.king.gmms.messagequeue.ChannelReleaseDispatcher;
import com.king.gmms.messagequeue.HttpClientStreamConsumer;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.smpp.pending.SmppPendingSubmitManager;
import com.king.gmms.messagequeue.CoreMQMStreamConsumer;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.processor.CsmExpirySweeper;
import com.king.redis.RedisClient;

/**
 * Draining a module that is going away (registry state DRAINING, see docs/模块动态扩缩容设计.md section 8): it
 * takes no new work and finishes what it has, then reports done; the process is stopped by hand. Writing what
 * memory holds is left to the stop itself (GracefulStop). The shards are moved away by hand first. A Core then
 * stops reading results, inbound DRs and held MTs, which every Core reads. An SMPP Client waits for the
 * responses of what it sent, keeps its vendor connections a while for the receipts on the way, then closes them.
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
	public static final String WAIT_WINDOW = "WAIT_WINDOW";
	public static final String WAIT_DR = "WAIT_DR";
	public static final String CLOSE_SESSIONS = "CLOSE_SESSIONS";
	private static final long DEFAULT_SESSION_INTERVAL_MS = 1000L;
	private static volatile boolean rejectConnections = false;
	public static final String DONE = "DONE";
	public static final String NOT_SUPPORTED = "NOT_SUPPORTED";

	private static Thread thread = null;
	private static volatile boolean draining = false;
	private static volatile boolean readersStopped = false;
	private static volatile boolean connectionsClosed = false;
	private static volatile int channelsClosed = 0;
	private static final long DEFAULT_DR_WAIT_SECONDS = 30L;

	private DrainController() {
	}

	public static boolean isDraining() {
		return draining;
	}

	/** A draining Server takes no new customer connection. */
	public static boolean rejectsConnections() {
		return rejectConnections;
	}

	/**
	 * A Core added by a2pctl add is JOINING until its shards are moved to it: it reads no results, inbound DRs and held
	 * MTs either (these streams are read by every Core whatever its shards), so a wrong configuration of a new Core
	 * touches no traffic. Read from ACTIVE on.
	 */
	private static volatile boolean joining = false;

	public static boolean holdsCoreReaders() {
		return joining;
	}

	/** The registry state of this module, at start and on every change. */
	public static synchronized void onState(String state) {
		boolean core = ModuleManager.getInstance().isCoreEngine(System.getProperty("module"));
		if (core && ModuleRegistry.JOINING.equals(state) && !joining) {
			joining = true;
			stopCoreReaders();
			readersStopped = false;
			log.info("[MODULE-REG] {} is JOINING: it reads results, inbound DRs and held MTs once ACTIVE", System.getProperty("module"));
		} else if (joining && !ModuleRegistry.JOINING.equals(state)) {
			joining = false;
			startCoreReaders();
			log.info("[MODULE-REG] {} is {}: reads results, inbound DRs and held MTs", System.getProperty("module"), state);
		}
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
				long noShardsAt = 0L;
				long drWaitFrom = 0L;
				long lastCloseAt = System.currentTimeMillis();
				long sessionsGoneAt = 0L;
				String type = ModuleManager.getInstance().getFullModuleType(module);
				while (draining) {
					try {
						String stage;
						List<String> detail = new ArrayList<String>();
						boolean core = ModuleManager.getInstance().isCoreEngine(module);
						boolean smppClient = "MultiSmppClient".equals(type);
						boolean httpClient = "CommonHttpClient".equals(type);
						boolean smppServer = "MultiSmppServer".equals(type);
						if (smppServer) {
							rejectConnections = true;
							List<com.king.gmms.connectionpool.session.MultiSmppSession> sessions =
									com.king.gmms.customerconnectionfactory.MultiSmppServerFactory.getInstance().liveServerSessions();
							if (!sessions.isEmpty()) {
								// one at a time: the customers bind again to the other Servers, not all at once
								if (System.currentTimeMillis() - lastCloseAt >= sessionIntervalMs()) {
									com.king.gmms.connectionpool.session.MultiSmppSession s = sessions.get(0);
									log.info("[DRAIN] closing customer session {}", s.getSessionName());
									s.stop();
									lastCloseAt = System.currentTimeMillis();
								}
								stage = CLOSE_SESSIONS;
								detail.addAll(Arrays.asList("sessions", String.valueOf(sessions.size())));
							} else {
								// DRs left for this module find no session: they go back to Core as 11005 and are sent again
								sessionsGoneAt = sessionsGoneAt == 0L ? System.currentTimeMillis() : sessionsGoneAt;
								int left = serverDrBacklog();
								boolean late = System.currentTimeMillis() - sessionsGoneAt > maxSeconds() * 1000L;
								stage = left == 0 || late ? DONE : WAIT_DR;
								detail.addAll(Arrays.asList("drLeft", String.valueOf(left)));
							}
						} else if (!core && !smppClient && !httpClient) {
							stage = NOT_SUPPORTED;
						} else if (!GmmsUtility.getInstance().getMyShards().isEmpty()) {
							stage = WAIT_SHARDS;
							detail.addAll(Arrays.asList("shards", GmmsUtility.getInstance().formatShardSet(GmmsUtility.getInstance().getMyShards())));
						} else if (httpClient) {
							int inflight = HttpClientStreamConsumer.getInstance().inFlight();
							noShardsAt = noShardsAt == 0L ? System.currentTimeMillis() : noShardsAt;
							boolean late = System.currentTimeMillis() - noShardsAt > maxSeconds() * 1000L;
							stage = inflight == 0 || late ? DONE : WAIT_INFLIGHT;
							detail.addAll(Arrays.asList("inflight", String.valueOf(inflight)));
						} else if (smppClient) {
							int inflight = OutboundStreamConsumer.getInstance().inFlight() + SmppPendingSubmitManager.totalPending();
							noShardsAt = noShardsAt == 0L ? System.currentTimeMillis() : noShardsAt;
							boolean late = System.currentTimeMillis() - noShardsAt > maxSeconds() * 1000L;
							detail.addAll(Arrays.asList("inflight", String.valueOf(inflight)));
							if (connectionsClosed) {
								stage = DONE;
								detail.addAll(Arrays.asList("channelsClosed", String.valueOf(channelsClosed)));
							} else if (inflight > 0 && !late) {
								stage = WAIT_WINDOW;
							} else {
								drWaitFrom = drWaitFrom == 0L ? System.currentTimeMillis() : drWaitFrom;
								long left = drWaitSeconds() * 1000L - (System.currentTimeMillis() - drWaitFrom);
								if (left > 0) {
									stage = WAIT_DR;
									detail.addAll(Arrays.asList("closeInMs", String.valueOf(left)));
								} else {
									channelsClosed = MultiSmppClientFactory.getInstance().closeAllForDrain();
									detail.addAll(Arrays.asList("channelsClosed", String.valueOf(channelsClosed)));
									connectionsClosed = true;
									stage = DONE;
								}
							}
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
		rejectConnections = false;
		if (thread != null) {
			thread.interrupt();
			thread = null;
		}
		if (readersStopped) {
			startCoreReaders();
			readersStopped = false;
		}
		if (connectionsClosed) {
			MultiSmppClientFactory.getInstance().allowConnectAgain();
			connectionsClosed = false;
			log.warn("[DRAIN] {}: its vendor connections were closed, restart the module to connect again", System.getProperty("module"));
		}
		try {
			RedisClient.getInstance().del(PROGRESS_PREFIX + System.getProperty("module"));
		} catch (Exception e) {
			log.warn("[DRAIN] progress not removed", e);
		}
		log.info("[DRAIN] {} cancelled, back to normal service", System.getProperty("module"));
	}

	/** the readers DeliveryRouter starts, unless the Core is JOINING */
	private static void startCoreReaders() {
		ResultStreamConsumer.getInstance().start();
		InboundDRStreamConsumer.getInstance().start();
		ChannelReleaseDispatcher.getInstance().start();
		CsmExpirySweeper.getInstance().start();
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

	private static int serverDrBacklog() {
		com.king.gmms.messagequeue.StreamQueueManager q = com.king.gmms.messagequeue.StreamQueueManager.getInstance();
		long n = com.king.gmms.smpp.pending.SmppPendingDrManager.totalPending();
		for (String stream : q.listResponsibleStreams(com.king.gmms.messagequeue.StreamQueueManager.STR_DR_PENDING_PREFIX + "*", false)) {
			n += q.streamLength(stream, false);
		}
		return (int) Math.min(Integer.MAX_VALUE, n);
	}

	private static long sessionIntervalMs() {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty("ModuleDrainSessionIntervalMs",
					String.valueOf(DEFAULT_SESSION_INTERVAL_MS)).trim());
		} catch (Exception e) {
			return DEFAULT_SESSION_INTERVAL_MS;
		}
	}

	private static long drWaitSeconds() {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty("ModuleDrainDrWaitSeconds",
					String.valueOf(DEFAULT_DR_WAIT_SECONDS)).trim());
		} catch (Exception e) {
			return DEFAULT_DR_WAIT_SECONDS;
		}
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
