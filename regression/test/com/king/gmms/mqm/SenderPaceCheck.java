package com.king.gmms.mqm;

import java.util.ArrayList;
import java.util.List;

/**
 * ResendDRThrottle as one limit shared by all the MQM senders (MQMMessageSender.pace): 4 threads sending 300 at
 * 1000/s together take about 0.3 s, not 0.075 s as with a limit per thread; 0 is no limit.
 */
public class SenderPaceCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	static long sendAll(final int threads, final int each, final int perSecond) throws Exception {
		List<Thread> l = new ArrayList<Thread>();
		long t0 = System.nanoTime();
		for (int i = 0; i < threads; i++) {
			Thread t = new Thread(() -> {
				try {
					for (int k = 0; k < each; k++) MQMMessageSender.pace(perSecond);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			});
			t.start();
			l.add(t);
		}
		for (Thread t : l) t.join();
		return (System.nanoTime() - t0) / 1000000L;
	}

	public static void main(String[] args) throws Exception {
		long ms = sendAll(4, 75, 1000);
		check(ms >= 270 && ms < 1500, "SP-01 4 threads, 300 sends at 1000/s in all: " + ms + " ms (about 300)");
		Thread.sleep(200);   // idle: no saved-up burst afterwards
		long burst = sendAll(1, 100, 1000);
		check(burst >= 90, "SP-02 after an idle time no burst: 100 sends at 1000/s take " + burst + " ms");
		long free = sendAll(4, 1000, 0);
		check(free < 500, "SP-03 0: no limit, 4000 sends in " + free + " ms");
		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
