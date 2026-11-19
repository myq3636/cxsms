package com.king.gmms.mqm;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.EarlyDrQueue;

/**
 * MQM job of the early vendor DRs ({@link EarlyDrQueue}): on the active MQM, every EarlyDr.IntervalMs the due ones are
 * looked up, the matched ones written back to stream:core:inbound-dr for Core, the others due again or dropped; while a
 * full batch was due, at once again. The counts are in the [VDR-EARLY-STATS] line of each minute.
 */
public class EarlyDrDispatcher implements Runnable {
	private static final SystemLogger log = SystemLogger.getSystemLogger(EarlyDrDispatcher.class);
	private volatile boolean running = false;
	private volatile Thread worker;

	public synchronized void start() {
		if (running) {
			return;
		}
		running = true;
		Thread t = new Thread(A2PThreadGroup.getInstance(), this, "EarlyDrDispatcher");
		t.setDaemon(true);
		worker = t;
		t.start();
		log.info("EarlyDrDispatcher started.");
	}

	public synchronized void stop() {
		running = false;
		Thread t = worker;
		if (t != null) {
			t.interrupt();
			worker = null;
		}
		log.info("EarlyDrDispatcher stopped.");
	}

	public void run() {
		while (running) {
			try {
				int claimed = 0;
				if (MqmActiveState.isActive() && EarlyDrQueue.enabled()) {
					claimed = EarlyDrQueue.dispatchDue();
				}
				if (claimed < batchSize()) {
					Thread.sleep(intervalMs());
				}
			} catch (InterruptedException e) {
				break;
			} catch (Exception e) {
				log.error("EarlyDrDispatcher loop error", e);
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ie) {
					break;
				}
			}
		}
	}

	private static long intervalMs() {
		try {
			return Math.max(10L, Long.parseLong(GmmsUtility.getInstance().getCommonProperty("EarlyDr.IntervalMs", "200").trim()));
		} catch (Exception e) {
			return 200L;
		}
	}

	private static int batchSize() {
		try {
			return Integer.parseInt(GmmsUtility.getInstance().getCommonProperty("EarlyDr.BatchSize", "500").trim());
		} catch (Exception e) {
			return 500;
		}
	}
}
