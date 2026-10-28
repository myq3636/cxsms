package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.SerializableHandler;

/**
 * Expiry of CSM groups that never got all their segments (redis mode, {@link RedisCsmReassembler}).
 *
 * <p>Every Core runs it (CoreCsmExpirySweep, default true): each CoreCsmExpiryIntervalSec it claims
 * up to CoreCsmExpiryBatch groups due in csm:index, hiding them from other sweepers for
 * CoreCsmExpiryLeaseMs. For an incomplete group whose segments asked for a DR, the expired DR
 * (EXPIRED, or UNDELIVERABLE when the customer takes no expired DR) is produced inline, one per
 * received segment; only then the group is finished. A crash or a failed DR leaves the claim to
 * expire and the group is taken again (at-least-once: the DR may be sent twice, never lost).
 * A complete group (sent, finish not done) is finished without a DR.
 *
 * <p>The MQM CsmExpiryTask uses the same claim and finish ({@link #sweepOnce}), so both can run.
 */
public class CsmExpirySweeper {
	private static final SystemLogger log = SystemLogger.getSystemLogger(CsmExpirySweeper.class);
	private static final CsmExpirySweeper instance = new CsmExpirySweeper();

	/** Sends the expired DR of a group (one message carrying the InMsgIDs of all received segments). */
	public interface DrSink {
		/** true when the DR is sent and the group can be finished */
		boolean send(GmmsMessage expiredDr);
	}

	private volatile boolean running = false;
	private Thread thread;

	public static CsmExpirySweeper getInstance() {
		return instance;
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
		}, "CsmExpirySweeper");
		thread.setDaemon(true);
		thread.start();
		log.info("CSM expiry sweeper started.");
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
				Thread.sleep(Math.max(1, longProperty("CoreCsmExpiryIntervalSec", 60L)) * 1000L);
			} catch (InterruptedException e) {
				return;
			}
			if ("false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("CoreCsmExpirySweep", "true"))) {
				continue;
			}
			try {
				sweepOnce((int) longProperty("CoreCsmExpiryBatch", 200L),
						longProperty("CoreCsmExpiryLeaseMs", 300000L), INLINE_DR);
			} catch (Exception e) {
				log.warn("CSM expiry sweep failed", e);
			}
		}
	}

	/** Core: the DR is produced on this thread; a failed DB write (DR fallback) means not sent. */
	public static final DrSink INLINE_DR = new DrSink() {
		public boolean send(final GmmsMessage expiredDr) {
			final MessageStoreManager msm = GmmsUtility.getInstance().getMessageStoreManager();
			return MessageStoreManager.runTrackingDbWrites(new Runnable() {
				public void run() {
					try {
						MessageStoreManager.withInlineDrDispatch(new java.util.concurrent.Callable<Void>() {
							public Void call() {
								msm.sendDRMessage(expiredDr);
								return null;
							}
						});
					} catch (RuntimeException e) {
						throw e;
					} catch (Exception e) {
						throw new IllegalStateException(e);
					}
				}
			});
		}
	};

	/**
	 * Claim and handle the due groups once.
	 *
	 * @return number of incomplete groups expired
	 */
	public static int sweepOnce(int batch, long leaseMs, DrSink sink) {
		RedisCsmReassembler reassembler = RedisCsmReassembler.getInstance();
		List<String> due = reassembler.claimExpired(System.currentTimeMillis(), Math.max(1, batch), leaseMs);
		int expired = 0;
		for (String key : due) {
			try {
				List<GmmsMessage> segs = readSegments(key);
				if (segs.isEmpty()) {
					reassembler.finish(key);                // gone (TTL) or finished meanwhile
					continue;
				}
				int total = 0;
				for (GmmsMessage m : segs) {
					total = Math.max(total, m.getSarTotalSeqments());
				}
				if (total > 0 && segs.size() >= total) {
					log.info("CSM group complete at expiry (sent, not finished), finished without DR. key={}", key);
					reassembler.finish(key);
					continue;
				}
				GmmsMessage dr = buildExpiredDr(segs);
				if (dr != null && !sink.send(dr)) {
					log.warn("CSM expired DR not sent, the group is retried after the claim. key={}", key);
					continue;
				}
				reassembler.finish(key);
				expired++;
				log.info("CSM group expired with {}/{} segments, expired DR {}. key={}", segs.size(), total,
						dr == null ? "not requested" : "sent (" + dr.getStatusCode() + ")", key);
			} catch (Exception e) {
				log.warn("CSM expiry of {} failed, retried after the claim", key, e);
			}
		}
		return expired;
	}

	private static List<GmmsMessage> readSegments(String key) {
		List<?> flat = (List<?>) GmmsUtility.getInstance().getRedisClient().getStateRedis().evalScript(
				"return redis.call('HGETALL', KEYS[1])", Arrays.asList(key), Collections.<String>emptyList());
		List<GmmsMessage> segs = new ArrayList<GmmsMessage>();
		for (int i = 0; i + 1 < flat.size(); i += 2) {
			Object payload = flat.get(i + 1);
			GmmsMessage m = payload == null ? null : SerializableHandler.convertRedisMssage2GmmsMessage(String.valueOf(payload));
			if (m != null) {
				segs.add(m);
			}
		}
		Collections.sort(segs, new Comparator<GmmsMessage>() {
			public int compare(GmmsMessage a, GmmsMessage b) {
				return a.getSarSegmentSeqNum() - b.getSarSegmentSeqNum();
			}
		});
		return segs;
	}

	/**
	 * Expired DR from the received segments: InMsgIDs of all of them joined, split into one DR per
	 * segment when sent ({@link CsmUtility#splitCsmDr}). null when no segment asked for a DR.
	 */
	static GmmsMessage buildExpiredDr(List<GmmsMessage> segs) {
		boolean wantDr = false;
		StringBuilder inIds = new StringBuilder();
		for (GmmsMessage s : segs) {
			wantDr |= s.getDeliveryReport();
			if (inIds.length() > 0) {
				inIds.append(CsmUtility.CSM_ID_SEPERATOR);
			}
			inIds.append(s.getInMsgID());
		}
		if (!wantDr) {
			return null;
		}
		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		GmmsMessage dr = new GmmsMessage(segs.get(0));
		dr.setInMsgID(inIds.toString());
		dr.setDeliveryReport(true);
		dr.setStatus(ctm.isSupportExpiredDR(dr.getOSsID()) ? GmmsStatus.EXPIRED : GmmsStatus.UNDELIVERABLE);
		dr.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		dr.setRetriedNumber(0);
		dr.setNextRetryTime(null);
		dr.setDateIn(new Date());
		return dr;
	}

	private static long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
