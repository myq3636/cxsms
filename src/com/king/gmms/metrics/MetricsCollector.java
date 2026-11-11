package com.king.gmms.metrics;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * Lightweight, lock-free metrics collector for A2P gateway monitoring.
 *
 * Provides three types of metrics:
 * <ul>
 *   <li><b>Counters</b> – monotonically increasing values (e.g., total messages processed)</li>
 *   <li><b>Gauges</b> – point-in-time values, set by events or read by a registered supplier at report time</li>
 *   <li><b>Timers</b> – count, total time, buckets and the largest value of the window, for latencies</li>
 * </ul>
 *
 * Counters and timers use {@link LongAdder}: many threads adding to one name do not wait on each other.
 *
 * <p>Usage:</p>
 * <pre>
 *   MetricsCollector.getInstance().incrementCounter("msg.submit.in");
 *   MetricsCollector.getInstance().setGauge("buffer.out.size", outBuffer.size());
 *   MetricsCollector.getInstance().registerGauge("queue.size", () -&gt; queue.size());
 *   long start = System.nanoTime();
 *   // ... do work ...
 *   MetricsCollector.getInstance().recordTime("session.sendNewMessage", System.nanoTime() - start);
 * </pre>
 */
public class MetricsCollector {

    private static final MetricsCollector INSTANCE = new MetricsCollector();

    // ---- Counters: monotonically increasing ----
    private final ConcurrentHashMap<String, LongAdder> counters = new ConcurrentHashMap<String, LongAdder>();

    // ---- Gauges: current snapshot values ----
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<String, AtomicLong>();
    private final ConcurrentHashMap<String, LongSupplier> gaugeSuppliers = new ConcurrentHashMap<String, LongSupplier>();

    // ---- Timers ----
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<String, Timer>();
    /** upper limits of the buckets in ms; one more bucket for anything longer */
    private static final long[] BUCKET_LIMIT_MS = new long[] { 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000 };
    private static final long[] BUCKET_LIMIT_NANOS = new long[BUCKET_LIMIT_MS.length];
    private static final String[] TIMER_BUCKET_NAMES = new String[BUCKET_LIMIT_MS.length + 1];
    static {
        for (int i = 0; i < BUCKET_LIMIT_MS.length; i++) {
            BUCKET_LIMIT_NANOS[i] = BUCKET_LIMIT_MS[i] * 1000000L;
            TIMER_BUCKET_NAMES[i] = "le_" + BUCKET_LIMIT_MS[i] + "ms";
        }
        TIMER_BUCKET_NAMES[BUCKET_LIMIT_MS.length] = "gt_" + BUCKET_LIMIT_MS[BUCKET_LIMIT_MS.length - 1] + "ms";
    }
    private volatile boolean enabled = false;

    private static final class Timer {
        final LongAdder count = new LongAdder();
        final LongAdder nanos = new LongAdder();
        final LongAdder[] buckets = new LongAdder[TIMER_BUCKET_NAMES.length];
        /** largest value since the last snapshot */
        final AtomicLong windowMax = new AtomicLong();

        Timer() {
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new LongAdder();
            }
        }
    }

    /** a timer at one moment: totals since start, and the largest value since the previous snapshot */
    public static final class TimerSnapshot {
        public final long count;
        public final long nanos;
        public final long[] buckets;
        public final long windowMaxNanos;

        TimerSnapshot(long count, long nanos, long[] buckets, long windowMaxNanos) {
            this.count = count;
            this.nanos = nanos;
            this.buckets = buckets;
            this.windowMaxNanos = windowMaxNanos;
        }
    }

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
        getOrCreateCounter(name).increment();
    }

    /**
     * Increment a counter by the specified delta.
     */
    public void incrementCounter(String name, long delta) {
        if (!enabled) return;
        if (name == null || name.length() == 0) return;
        getOrCreateCounter(name).add(delta);
    }

    /**
     * Get the current value of a counter.
     */
    public long getCounter(String name) {
        LongAdder counter = counters.get(name);
        return counter != null ? counter.sum() : 0;
    }

    private LongAdder getOrCreateCounter(String name) {
        LongAdder counter = counters.get(name);
        if (counter == null) {
            counter = counters.computeIfAbsent(name, k -> new LongAdder());
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
     * A gauge read when the metrics are reported, so it never stays at the value of the last event.
     * Registering a name again replaces its supplier.
     */
    public void registerGauge(String name, LongSupplier supplier) {
        if (name == null || name.length() == 0 || supplier == null) return;
        gaugeSuppliers.put(name, supplier);
    }

    public void unregisterGauge(String name) {
        if (name != null) {
            gaugeSuppliers.remove(name);
            gauges.remove(name);
        }
    }

    /**
     * Get the current gauge value.
     */
    public long getGauge(String name) {
        LongSupplier supplier = gaugeSuppliers.get(name);
        if (supplier != null) {
            try {
                return supplier.getAsLong();
            } catch (Exception e) {
                return 0;
            }
        }
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
        Timer t = timers.get(name);
        if (t == null) {
            t = timers.computeIfAbsent(name, k -> new Timer());
        }
        t.count.increment();
        t.nanos.add(nanos);
        t.buckets[bucketIndex(nanos)].increment();
        long max = t.windowMax.get();
        while (nanos > max && !t.windowMax.compareAndSet(max, nanos)) {
            max = t.windowMax.get();
        }
    }

    /**
     * Record a timing measurement in milliseconds (a difference of two System.currentTimeMillis values).
     */
    public void recordMillis(String name, long millis) {
        if (!enabled) return;
        recordTime(name, millis < 0 ? 0 : millis * 1000000L);
    }

    /**
     * Get the invocation count for a timer.
     */
    public long getTimerCount(String name) {
        Timer t = timers.get(name);
        return t != null ? t.count.sum() : 0;
    }

    /**
     * Get the total accumulated time in nanoseconds.
     */
    public long getTimerTotalNanos(String name) {
        Timer t = timers.get(name);
        return t != null ? t.nanos.sum() : 0;
    }

    private static int bucketIndex(long nanos) {
        for (int i = 0; i < BUCKET_LIMIT_NANOS.length; i++) {
            if (nanos <= BUCKET_LIMIT_NANOS[i]) {
                return i;
            }
        }
        return BUCKET_LIMIT_NANOS.length;
    }

    public static String[] timerBucketNames() {
        return TIMER_BUCKET_NAMES.clone();
    }

    /**
     * A percentile in ms from the bucket counts of a window, by linear interpolation inside its bucket;
     * the last bucket ends at maxMs. Close enough to follow trends, not exact.
     */
    public static double percentileMs(long[] buckets, double q, double maxMs) {
        long total = 0;
        for (long b : buckets) {
            total += b;
        }
        if (total <= 0) {
            return 0.0;
        }
        double rank = q * total;
        long seen = 0;
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] <= 0) {
                continue;
            }
            if (seen + buckets[i] >= rank) {
                double lower = i == 0 ? 0.0 : BUCKET_LIMIT_MS[i - 1];
                double upper = i < BUCKET_LIMIT_MS.length ? BUCKET_LIMIT_MS[i] : Math.max(maxMs, lower);
                if (maxMs > 0 && upper > maxMs) {
                    upper = Math.max(lower, maxMs);
                }
                double v = lower + (upper - lower) * ((rank - seen) / buckets[i]);
                return maxMs > 0 ? Math.min(v, maxMs) : v;
            }
            seen += buckets[i];
        }
        return maxMs;
    }

    // ==================== Snapshot ====================

    /**
     * Returns a sorted snapshot of all metrics for reporting.
     */
    public Map<String, Long> snapshotCounters() {
        TreeMap<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, LongAdder> entry : counters.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().sum());
        }
        return snapshot;
    }

    /** set gauges and supplier gauges (a supplier wins over a set value of the same name) */
    public Map<String, Long> snapshotGauges() {
        TreeMap<String, Long> snapshot = new TreeMap<String, Long>();
        for (Map.Entry<String, AtomicLong> entry : gauges.entrySet()) {
            snapshot.put(entry.getKey(), entry.getValue().get());
        }
        for (Map.Entry<String, LongSupplier> entry : gaugeSuppliers.entrySet()) {
            try {
                snapshot.put(entry.getKey(), entry.getValue().getAsLong());
            } catch (Exception e) {
                // a gauge that cannot be read now is left out of this window
            }
        }
        return snapshot;
    }

    /**
     * The timers; the largest value of each is reset, so this is for the one reporter, once per window.
     */
    public Map<String, TimerSnapshot> snapshotTimers() {
        TreeMap<String, TimerSnapshot> snapshot = new TreeMap<String, TimerSnapshot>();
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            Timer t = entry.getValue();
            long[] b = new long[t.buckets.length];
            for (int i = 0; i < b.length; i++) {
                b[i] = t.buckets[i].sum();
            }
            snapshot.put(entry.getKey(), new TimerSnapshot(t.count.sum(), t.nanos.sum(), b, t.windowMax.getAndSet(0)));
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
