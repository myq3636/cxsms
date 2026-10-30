package com.king.gmms.util;

import java.util.concurrent.ConcurrentHashMap;

/**
 * For log lines that can repeat for every message (a fallback taken for every DR of a customer,
 * a channel that is not initialised): at most one line per key and interval, with the number of
 * occurrences since the previous line.
 *
 * <pre>
 * long n = LogRateLimiter.allow("dr-fallback:" + ossid, 600000L);
 * if (n > 0) log.warn("... ({} times in the last 10 min)", ..., n);
 * </pre>
 */
public final class LogRateLimiter {
	private static final ConcurrentHashMap<String, long[]> STATE = new ConcurrentHashMap<String, long[]>();
	private static final int MAX_KEYS = 10000;

	private LogRateLimiter() {
	}

	/**
	 * Count one occurrence of key; returns the occurrences to report (including this one) when a
	 * line may be written now, 0 when it is suppressed.
	 */
	public static long allow(String key, long intervalMs) {
		if (STATE.size() > MAX_KEYS) {
			STATE.clear();      // keys built from bad input must not grow without end
		}
		long[] s = STATE.computeIfAbsent(key, k -> new long[] {0L, 0L});   // {lastLoggedMs, suppressed}
		synchronized (s) {
			long now = System.currentTimeMillis();
			if (s[0] == 0L || now - s[0] >= intervalMs) {
				long n = s[1] + 1;
				s[0] = now;
				s[1] = 0L;
				return n;
			}
			s[1]++;
			return 0L;
		}
	}
}
