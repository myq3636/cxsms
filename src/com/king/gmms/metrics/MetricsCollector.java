package com.king.gmms.metrics;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight, lock-free metrics collector for A2P gateway monitoring.
 * 
 * Provides three types of metrics:
 * <ul>
 *   <li><b>Counters</b> – monotonically increasing values (e.g., total messages processed)</li>
 *   <li><b>Gauges</b> – point-in-time values (e.g., current buffer size)</li>
 *   <li><b>Timers</b> – cumulative time + invocation count for latency analysis</li>
 * </ul>
 * 
 * All operations are thread-safe and lock-free via {@link AtomicLong}.
 * 
 * <p>Usage:</p>
 * <pre>
 *   MetricsCollector.getInstance().incrementCounter("msg.submit.in");
 *   MetricsCollector.getInstance().setGauge("buffer.out.size", outBuffer.size());
 *   long start = System.nanoTime();
 *   // ... do work ...
 *   MetricsCollector.getInstance().recordTime("session.sendNewMessage", System.nanoTime() - start);
 * </pre>
 */
public class MetricsCollector {

    private static final MetricsCollector INSTANCE = new MetricsCollector();

    // ---- Counters: monotonically increasing ----
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<String, AtomicLong>();

    // ---- Gauges: current snapshot values ----
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<String, AtomicLong>();

    // ---- Timers: cumulative nanos + invocation count ----
    private final ConcurrentHashMap<String, AtomicLong> timerTotalNanos = new ConcurrentHashMap<String, AtomicLong>();
    private final ConcurrentHashMap<String, AtomicLong> timerCounts = new ConcurrentHashMap<String, AtomicLong>();
    private final ConcurrentHashMap<String, AtomicLong> timerBuckets = new ConcurrentHashMap<String, AtomicLong>();
    private static final String[] TIMER_BUCKET_NAMES = new String[] {
        "le_10ms", "le_50ms", "le_100ms", "le_500ms", "le_1000ms", "gt_1000ms"
    };
    private static final long[] TIMER_BUCKET_LIMIT_NANOS = new long[] {
        10L * 1000L * 1000L,
        50L * 1000L * 1000L,
        100L * 1000L * 1000L,
        500L * 1000L * 1000L,
        1000L * 1000L * 1000L
    };
    private volatile boolean enabled = false;

    private MetricsCollector() {
    }

    public static MetricsCollector getInstance() {
        return INSTANCE;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled() {
        return enabled;
    }

    // ==================== Counter operations ====================

    /**
     * Increment a counter by 1.
     */
    public void incrementCounter(String name) {
        if (!enabled) return;
        if (name == null || name.length() == 0) return;
        getOrCreateCounter(name).incrementAndGet();
    }

    /**
     * Increment a counter by the specified delta.
     */
    public void incrementCounter(String name, long delta) {
        if (!enabled) return;
        if (name == null || name.length() == 0) return;
        getOrCreateCounter(name).addAndGet(delta);
    }

    /**
     * Get the current value of a counter.
     */
    public long getCounter(String name) {
        AtomicLong counter = counters.get(name);
        return counter != null ? counter.get() : 0;
    }

    private AtomicLong getOrCreateCounter(String name) {
        AtomicLong counter = counters.get(name);
        if (counter == null) {
            counters.putIfAbsent(name, new AtomicLong(0));
            counter = counters.get(name);
        }
        return counter;
    }

    // ==================== Gauge operations ====================

    /**
     * Set a gauge to an absolute value.
     */
    public void setGauge(String name, long value) {
        if (!enabled) return;
        if (name == null || name.length() == 0) return;
        AtomicLong gauge = gauges.get(name);
        if (gauge == null) {
            gauges.putIfAbsent(name, new AtomicLong(value));
            gauge = gauges.get(name);
        }
        gauge.set(value);
    }

    /**
     * Get the current gauge value.
     */
    public long getGauge(String name) {
        AtomicLong gauge = gauges.get(name);
        return gauge != null ? gauge.get() : 0;
    }

    // ==================== Timer operations ====================

    /**
     * Record a timing measurement (in nanoseconds).
     */
    public void recordTime(String name, long nanos) {
        if (!enabled) return;
        if (name == null || name.length() == 0) return;
        if (nanos < 0) {
            nanos = 0;
        }
        getOrCreateTimerNanos(name).addAndGet(nanos);
        getOrCreateTimerCount(name).incrementAndGet();
        getOrCreateTimerBucket(name, timerBucketName(nanos)).incrementAndGet();
    }

    /**
     * Get the total accumulated time in nanoseconds.
     */
    public long getTimerTotalNanos(String name) {
        AtomicLong t = timerTotalNanos.get(name);
        return t != null ? t.get() : 0;
    }

    /**
     * Get the invocation count for a timer.
     */
    public long getTimerCount(String name) {
        AtomicLong c = timerCounts.get(name);
        return c != null ? c.get() : 0;
    }

    /**
     * Get the average time in milliseconds for a timer.
     */
    public double getTimerAvgMs(String name) {
        long count = getTimerCount(name);
        if (count == 0) return 0.0;
        return (double) getTimerTotalNanos(name) / count / 1_000_000.0;
    }

    private AtomicLong getOrCreateTimerNanos(String name) {
        AtomicLong t = timerTotalNanos.get(name);
        if (t == null) {
            timerTotalNanos.putIfAbsent(name, new AtomicLong(0));
            t = timerTotalNanos.get(name);
        }
        return t;
    }

    private AtomicLong getOrCreateTimerCount(String name) {
        AtomicLong c = timerCounts.get(name);
        if (c == null) {
            timerCounts.putIfAbsent(name, new AtomicLong(0));
            c = timerCounts.get(name);
        }
        return c;
    }

    private AtomicLong getOrCreateTimerBucket(String name, String bucket) {
        String key = timerBucketKey(name, bucket);
        AtomicLong c = timerBuckets.get(key);
        if (c == null) {
            timerBuckets.putIfAbsent(key, new AtomicLong(0));
            c = timerBuckets.get(key);
        }
        return c;
    }

    private String timerBucketName(long nanos) {
        for (int i = 0; i < TIMER_BUCKET_LIMIT_NANOS.length; i++) {
            if (nanos <= TIMER_BUCKET_LIMIT_NANOS[i]) {
                return TIMER_BUCKET_NAMES[i];
            }
        }
        return TIMER_BUCKET_NAMES[TIMER_BUCKET_NAMES.length - 1];
    }

    public static String[] timerBucketNames() {
        String[] copy = new String[TIMER_BUCKET_NAMES.length];
        System.arraycopy(TIMER_BUCKET_NAMES, 0, copy, 0, TIMER_BUCKET_NAMES.length);
        return copy;
    }

    public static String timerBucketKey(String name, String bucket) {
        return name + "." + bucket;
    }

    // ==================== Snapshot ====================

    /**
     * Returns a sorted snapshot of all metrics for reporting.
     */
    public Map<String, Long> snapshotCounters() {
        TreeMap<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    public Map<String, Long> snapshotGauges() {
        TreeMap<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry : gauges.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    /**
     * Returns a snapshot of timer data: count and average latency in ms.
     */
    public Map<String, String> snapshotTimers() {
        TreeMap<String, String> snapshot = new TreeMap<String, String>();
        for (Map.Entry<String, AtomicLong> entry : timerCounts.entrySet()) {
            String name = entry.getKey();
            long count = entry.getValue().get();
            double avgMs = getTimerAvgMs(name);
            snapshot.put(name, String.format("count=%d, avgMs=%.2f", count, avgMs));
        }
        return snapshot;
    }

    public Map<String, Long> snapshotTimerCounts() {
        TreeMap<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry : timerCounts.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    public Map<String, Long> snapshotTimerTotalNanos() {
        TreeMap<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry : timerTotalNanos.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    public Map<String, Long> snapshotTimerBuckets() {
        TreeMap<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry : timerBuckets.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        return snapshot;
    }

    /**
     * Compute the delta between two counter snapshots (current - previous).
     */
    public static Map<String, Long> delta(Map<String, Long> current, Map<String, Long> previous) {
        TreeMap<String, Long> result = new TreeMap<String, Long>();
        for (Map.Entry<String, Long> entry : current.entrySet()) {
            long prev = previous.containsKey(entry.getKey()) ? previous.get(entry.getKey()) : 0;
            result.put(entry.getKey(), entry.getValue() - prev);
        }
        return result;
    }
}
