package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;

/**
 * Entries a consumer read but put off (send window full, no rate permit), per stream. They stay
 * pending for that consumer; after the delay {@code resume} is called for the stream, which
 * {@link #take}s them and processes them before reading the stream again. While a stream has
 * entries put off its consumer starts no other task for it.
 */
final class StreamDeferrals {
	interface Resume {
		void resume(String streamKey);
	}

	private final Map<String, List<GmmsMessage>> deferred = new HashMap<String, List<GmmsMessage>>();
	/** when the entries of a stream were put off, and why: for the defer.wait timer */
	private final Map<String, long[]> since = new HashMap<String, long[]>();
	private final Map<String, String> reasons = new HashMap<String, String>();
	/** {module}.defer.wait.{consumer}.{reason}; null: not measured */
	private volatile String waitMetric;

	/** Entries read and put off, not sent yet. */
	public int size() {
		synchronized (deferred) {
			int n = 0;
			for (List<GmmsMessage> l : deferred.values()) {
				n += l.size();
			}
			return n;
		}
	}
	private final ScheduledExecutorService timer;
	private final Resume resume;

	StreamDeferrals(ScheduledExecutorService timer, Resume resume) {
		this.timer = timer;
		this.resume = resume;
	}

	/** measure how long entries stay put off, as defer.wait.{consumer}.{reason} */
	void measureWait(String consumer) {
		this.waitMetric = MetricsNames.TIMER_DEFER_WAIT + "." + consumer;
	}

	boolean isDeferred(String streamKey) {
		synchronized (deferred) {
			return deferred.containsKey(streamKey);
		}
	}

	/** Put entries of a stream off for delayMs, merged with any already put off. */
	void defer(String streamKey, List<GmmsMessage> messages, long delayMs) {
		defer(streamKey, messages, delayMs, "other");
	}

	/** the same, with why (window_full, throttled, bind_throttled, ...) for the wait timer */
	void defer(String streamKey, List<GmmsMessage> messages, long delayMs, String reason) {
		boolean schedule;
		synchronized (deferred) {
			List<GmmsMessage> list = deferred.get(streamKey);
			schedule = list == null;
			if (schedule) {
				list = new ArrayList<GmmsMessage>();
				deferred.put(streamKey, list);
				if (waitMetric != null) {
					since.put(streamKey, new long[] { System.currentTimeMillis() });
					reasons.put(streamKey, reason == null ? "other" : reason);
				}
			}
			list.addAll(messages);
		}
		if (schedule) {
			schedule(streamKey, delayMs);
		}
	}

	/** Call resume again after delayMs, keeping the entries (e.g. the stream's task is still finishing). */
	void schedule(final String streamKey, long delayMs) {
		if (timer.isShutdown()) {
			return;   // stopping: the entries stay pending for auto-claim
		}
		timer.schedule(new Runnable() {
			public void run() {
				resume.resume(streamKey);
			}
		}, delayMs, TimeUnit.MILLISECONDS);
	}

	/** The entries put off for the stream, removed; null if none. */
	List<GmmsMessage> take(String streamKey) {
		long[] at;
		String reason;
		List<GmmsMessage> list;
		synchronized (deferred) {
			list = deferred.remove(streamKey);
			at = since.remove(streamKey);
			reason = reasons.remove(streamKey);
		}
		String metric = waitMetric;
		if (list != null && at != null && metric != null) {
			MetricsCollector.getInstance().recordMillis(MetricsNames.metric(metric, reason), System.currentTimeMillis() - at[0]);
		}
		return list;
	}
}
