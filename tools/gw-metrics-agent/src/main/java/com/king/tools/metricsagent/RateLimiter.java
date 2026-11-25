package com.king.tools.metricsagent;

/**
 * agent.read.backfill-mbps: a token bucket over the bytes read from rolled files and far behind the live end, so a
 * long backfill does not compete with the gateway's own disk writes (Decisions 9). At most a second's worth at once.
 */
final class RateLimiter {
    private final double bytesPerMs;
    private final double burst;
    private double tokens;
    private long last = System.nanoTime();

    RateLimiter(int mbPerSecond) {
        this.bytesPerMs = mbPerSecond * 1048576.0 / 1000.0;
        this.burst = mbPerSecond * 1048576.0;
        this.tokens = burst;
    }

    void acquire(int n) throws InterruptedException {
        if (bytesPerMs <= 0) {
            return;
        }
        while (true) {
            long now = System.nanoTime();
            tokens = Math.min(burst, tokens + (now - last) / 1_000_000.0 * bytesPerMs);
            last = now;
            if (tokens >= n || tokens >= burst) {
                tokens -= n;
                return;
            }
            Thread.sleep(Math.max(1, (long) Math.ceil((n - tokens) / bytesPerMs)));
        }
    }
}
