package com.king.gmms.messagequeue;

import com.king.gmms.GmmsUtility;

/**
 * How much one FetchAndProcessTask may do before it gives its stream back (re-ring and exit),
 * so a stream with a large backlog does not keep a worker while other streams wait:
 * {Core|Client}StreamTaskMaxBatches batches or {Core|Client}StreamTaskMaxMs, whichever comes first.
 */
final class StreamTaskBudget {
	private final int maxBatches;
	private final long deadline;
	private int batches;

	StreamTaskBudget() {
		this("Core");
	}

	/** prefix Core or Client: {prefix}StreamTaskMaxBatches / {prefix}StreamTaskMaxMs */
	StreamTaskBudget(String prefix) {
		this.maxBatches = (int) property(prefix + "StreamTaskMaxBatches", 10);
		this.deadline = System.currentTimeMillis() + property(prefix + "StreamTaskMaxMs", 2000);
	}

	/** Called after each processed batch: true when the task should stop and re-ring its stream. */
	boolean spent() {
		return ++batches >= maxBatches || System.currentTimeMillis() >= deadline;
	}

	private static long property(String key, long defaultValue) {
		try {
			long v = Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
			return v > 0 ? v : defaultValue;
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
