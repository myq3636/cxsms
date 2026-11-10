package com.king.gmms.throttle;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Rate of one SMPP bind (SMSOptionOutgoingThrottlingNumPerBind): submit_sm evenly spaced, 1000/tps ms apart,
 * no burst. Time saved while idle is not banked; a send that comes up to a quarter interval early keeps the
 * cadence, so a deferred retry that wakes a little early or late does not lose rate. The rate is passed on
 * every call, so a reload takes effect on the next submit.
 */
public final class BindRateLimiter {
	/** the earliest time (nanos) the next submit_sm is due */
	private final AtomicLong nextAt = new AtomicLong(Long.MIN_VALUE);

	private static long interval(int tps) {
		return 1_000_000_000L / tps;
	}

	/** ms until this bind may send (0: now); tps <= 0: no limit */
	public long waitMs(int tps, long nowNanos) {
		if (tps <= 0) {
			return 0L;
		}
		long next = nextAt.get();
		if (next == Long.MIN_VALUE) {
			return 0L;
		}
		long wait = next - interval(tps) / 4 - nowNanos;
		return wait <= 0 ? 0L : (wait + 999_999L) / 1_000_000L;
	}

	/** takes the permit of the next submit_sm; false: not due yet (another thread took it first) */
	public boolean take(int tps, long nowNanos) {
		if (tps <= 0) {
			return true;
		}
		long iv = interval(tps);
		while (true) {
			long next = nextAt.get();
			if (next != Long.MIN_VALUE && nowNanos < next - iv / 4) {
				return false;
			}
			long base = next == Long.MIN_VALUE || nowNanos > next ? nowNanos : next;
			if (nextAt.compareAndSet(next, base + iv)) {
				return true;
			}
		}
	}
}
