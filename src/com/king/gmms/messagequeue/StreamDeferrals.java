package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
	private final ScheduledExecutorService timer;
	private final Resume resume;

	StreamDeferrals(ScheduledExecutorService timer, Resume resume) {
		this.timer = timer;
		this.resume = resume;
	}

	boolean isDeferred(String streamKey) {
		synchronized (deferred) {
			return deferred.containsKey(streamKey);
		}
	}

	/** Put entries of a stream off for delayMs, merged with any already put off. */
	void defer(String streamKey, List<GmmsMessage> messages, long delayMs) {
		boolean schedule;
		synchronized (deferred) {
			List<GmmsMessage> list = deferred.get(streamKey);
			schedule = list == null;
			if (schedule) {
				list = new ArrayList<GmmsMessage>();
				deferred.put(streamKey, list);
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
		synchronized (deferred) {
			return deferred.remove(streamKey);
		}
	}
}
