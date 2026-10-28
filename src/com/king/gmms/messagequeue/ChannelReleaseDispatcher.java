package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.util.concurrent.RateLimiter;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/**
 * Releases MTs of rate limited channels (CoreOutboundTps > 0 in the channel's cm.cfg block).
 *
 * <p>Routing puts such an MT into stream:core:hold:{rssid} instead of its outbound stream; the
 * backlog stays in Core and the Client only ever sees what the channel can take. This thread
 * moves held MTs to their outbound stream at CoreOutboundTps per second, per Core node (local
 * RateLimiter, not shared). A full hold queue (CoreOutboundTps x CoreHoldMaxSeconds entries)
 * rejects new MTs with Throttled; an MT that expires or waits longer than CoreHoldMaxSeconds
 * is failed with EXPIRED instead of being sent. Both go through the normal failed-submit
 * handling (CDR, DR to the customer, retry policy).
 */
public class ChannelReleaseDispatcher {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ChannelReleaseDispatcher.class);
	private static final ChannelReleaseDispatcher instance = new ChannelReleaseDispatcher();

	public static final String GROUP = "CoreHoldGroup";
	private static final int MAX_PER_TICK = 100;

	private final ConcurrentHashMap<Integer, RateLimiter> limiters = new ConcurrentHashMap<Integer, RateLimiter>();
	private final Map<Integer, long[]> backlogCache = new ConcurrentHashMap<Integer, long[]>();
	private final Set<String> groupsCreated = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	/** hold streams with entries pending on this consumer (claimed, or a release that failed) */
	private final Set<String> ownPending = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
	private final String consumerName = "core-hold-" + System.getProperty("NodeID", "0");
	private volatile boolean running = false;
	private Thread thread;
	private long lastRecoverMs = 0L;

	public static ChannelReleaseDispatcher getInstance() {
		return instance;
	}

	public static String holdKey(int rssid) {
		return StreamQueueManager.STR_CORE_HOLD_PREFIX + rssid;
	}

	/** Whether MTs to this channel are held (CoreOutboundTps > 0). */
	public boolean holds(GmmsMessage msg) {
		return GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				&& channel(msg.getRSsID()) != null && channel(msg.getRSsID()).getCoreOutboundTps() > 0;
	}

	/**
	 * Hold an MT of a rate limited channel, or fail it with Throttled when the hold queue is full.
	 * Returns false only when the MT could not be stored (the caller falls back as for any produce).
	 */
	public boolean offer(GmmsMessage msg) {
		A2PCustomerInfo channel = channel(msg.getRSsID());
		long cap = (long) channel.getCoreOutboundTps() * channel.getCoreHoldMaxSeconds();
		long backlog = backlog(msg.getRSsID());
		if (backlog >= cap) {
			log.warn(msg, "Channel {} hold queue full ({} >= {}), MT failed with Throttled",
					msg.getRSsID(), backlog, cap);
			fail(msg, GmmsStatus.Throttled);
			return true;
		}
		boolean held = StreamQueueManager.getInstance().produceHold(holdKey(msg.getRSsID()), msg);
		if (held) {
			long[] cached = backlogCache.get(msg.getRSsID());
			if (cached != null) {
				cached[0]++;
			}
			log.info(msg, "MT held for channel {} (backlog ~{}, {} TPS per Core)", msg.getRSsID(), backlog + 1,
					channel.getCoreOutboundTps());
		}
		return held;
	}

	public synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		thread = new Thread(A2PThreadGroup.getInstance(), new Runnable() {
			public void run() {
				loop();
			}
		}, "ChannelReleaseDispatcher");
		thread.setDaemon(true);
		thread.start();
		log.info("Channel release dispatcher started. consumer={}", consumerName);
	}

	public synchronized void stop() {
		running = false;
		if (thread != null) {
			thread.interrupt();
			thread = null;
		}
	}

	private void loop() {
		while (running) {
			try {
				Set<String> keys = StreamQueueManager.getInstance().holdStreams();
				boolean recover = System.currentTimeMillis() - lastRecoverMs >= longProperty("CoreHoldRecoverIntervalMs", 30000L);
				for (String key : keys) {
					releaseFrom(key, recover);
				}
				if (recover) {
					lastRecoverMs = System.currentTimeMillis();
				}
				Thread.sleep(longProperty("CoreHoldReleaseTickMs", 100L));
			} catch (InterruptedException e) {
				return;
			} catch (Exception e) {
				log.error("Channel release loop error", e);
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ie) {
					return;
				}
			}
		}
	}

	private void releaseFrom(String holdKey, boolean recover) {
		int rssid;
		try {
			rssid = Integer.parseInt(holdKey.substring(StreamQueueManager.STR_CORE_HOLD_PREFIX.length()));
		} catch (NumberFormatException e) {
			return;
		}
		StreamQueueManager q = StreamQueueManager.getInstance();
		if (groupsCreated.add(holdKey)) {
			q.createGroup(holdKey, GROUP, true);
			// entries this consumer read before a restart are pending on it
			ownPending.add(holdKey);
		}
		if (recover) {
			// take over entries of a Core that died after reading them; released below at the normal rate
			if (!q.autoClaimBatch(holdKey, GROUP, consumerName,
					longProperty("CoreHoldRecoverIdleMs", 60000L), MAX_PER_TICK, true).isEmpty()) {
				ownPending.add(holdKey);
			}
		}
		A2PCustomerInfo channel = channel(rssid);
		int tps = channel == null ? 0 : channel.getCoreOutboundTps();
		int permits = 0;
		if (tps <= 0) {
			// limit removed (or channel gone): drain what is left
			permits = MAX_PER_TICK;
		} else {
			RateLimiter limiter = limiter(rssid, tps);
			while (permits < MAX_PER_TICK && limiter.tryAcquire()) {
				permits++;
			}
		}
		if (permits == 0) {
			return;
		}
		List<GmmsMessage> batch = new ArrayList<GmmsMessage>();
		if (ownPending.contains(holdKey)) {
			List<GmmsMessage> mine = read(q, holdKey, new StreamEntryID(0, 0), permits);
			batch.addAll(mine);
			if (mine.size() < permits) {
				ownPending.remove(holdKey);
			}
		}
		if (batch.size() < permits) {
			batch.addAll(read(q, holdKey, StreamEntryID.UNRECEIVED_ENTRY, permits - batch.size()));
		}
		for (GmmsMessage msg : batch) {
			releaseOne(holdKey, channel, msg);
		}
	}

	private List<GmmsMessage> read(StreamQueueManager q, String holdKey, StreamEntryID from, int count) {
		Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
		query.put(holdKey, from);
		List<GmmsMessage> list = q.consumeBatch(GROUP, consumerName, count, query, true);
		return list == null ? new ArrayList<GmmsMessage>() : list;
	}

	private void releaseOne(String holdKey, A2PCustomerInfo channel, GmmsMessage msg) {
		StreamQueueManager q = StreamQueueManager.getInstance();
		String holdId = msg.getRedisStreamID();
		try {
			long heldMs = System.currentTimeMillis() - idTime(holdId);
			int maxSeconds = channel == null ? 21600 : channel.getCoreHoldMaxSeconds();
			boolean expired = msg.getExpiryDate() != null
					&& msg.getExpiryDate().getTime() < GmmsUtility.getInstance().getGMTTime().getTime();
			if (expired || heldMs > maxSeconds * 1000L) {
				log.warn(msg, "Held MT not sent: {} (held {} ms, channel {})",
						expired ? "expired" : "held longer than " + maxSeconds + "s", heldMs, msg.getRSsID());
				fail(msg, GmmsStatus.EXPIRED);
				q.dropHeld(holdKey, GROUP, holdId);
				return;
			}
			String id = q.releaseHeld(holdKey, GROUP, msg);
			if (id == null) {
				log.info(msg, "Held MT already released by another Core. holdId={}", holdId);
			} else if (log.isDebugEnabled()) {
				log.debug(msg, "Held MT released to channel {} after {} ms, id={}", msg.getRSsID(), heldMs, id);
			}
		} catch (Exception e) {
			// stays pending on this consumer, retried next tick
			ownPending.add(holdKey);
			log.error(msg, "Release of held MT failed. holdKey=" + holdKey + ", holdId=" + holdId, e);
		}
	}

	/** Failed outbound submit: CDR, DR to the customer and retry policy as for a failed submit_resp. */
	private void fail(GmmsMessage msg, GmmsStatus status) {
		msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		msg.setStatus(status);
		GmmsUtility.getInstance().getMessageStoreManager().handleOutSubmitRes(msg, false);
	}

	/** Hold queue length, refreshed from Redis at most once a second. */
	private long backlog(int rssid) {
		long now = System.currentTimeMillis();
		long[] cached = backlogCache.get(rssid);
		if (cached == null || now - cached[1] >= 1000L) {
			cached = new long[] { StreamQueueManager.getInstance().streamLength(holdKey(rssid), true), now };
			backlogCache.put(rssid, cached);
		}
		return cached[0];
	}

	private RateLimiter limiter(int rssid, int tps) {
		RateLimiter limiter = limiters.get(rssid);
		if (limiter == null) {
			limiter = RateLimiter.create(tps);
			RateLimiter old = limiters.putIfAbsent(rssid, limiter);
			if (old != null) {
				limiter = old;
			}
		}
		if (limiter.getRate() != tps) {
			limiter.setRate(tps);
			log.info("Channel {} release rate set to {} TPS per Core", rssid, tps);
		}
		return limiter;
	}

	private static A2PCustomerInfo channel(int rssid) {
		return GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(rssid);
	}

	private static long idTime(String streamId) {
		try {
			return Long.parseLong(streamId.substring(0, streamId.indexOf('-')));
		} catch (Exception e) {
			return System.currentTimeMillis();
		}
	}

	private static long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
