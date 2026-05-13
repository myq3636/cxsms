package com.king.gmms.metrics;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;

/**
 * Periodic metrics reporter that logs TPS, latencies, queue depths, and 
 * buffer utilization at a configurable interval.
 * 
 * <p>Outputs two log lines per interval:</p>
 * <ol>
 *   <li><b>TPS line</b> – per-second throughput for each counter (delta / interval)</li>
 *   <li><b>Gauges line</b> – current point-in-time values (buffer sizes, queue depths, etc.)</li>
 *   <li><b>Timers line</b> – average latency and invocation count</li>
 * </ol>
 * 
 * <p>Usage:</p>
 * <pre>
 *   MetricsReporter.getInstance().start(30); // report every 30 seconds
 * </pre>
 */
public class MetricsReporter {

    private static final SystemLogger log = SystemLogger.getSystemLogger(MetricsReporter.class);
    private static final SystemLogger metricsLog = SystemLogger.getSystemLogger("metrics.logger");
    private static final MetricsReporter INSTANCE = new MetricsReporter();

    private final MetricsCollector collector = MetricsCollector.getInstance();
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile String logLevel = "DEBUG";
    private volatile boolean printZero = false;
    private volatile boolean printGauge = false;
    private volatile double minTps = 0.01;
    private volatile int intervalSeconds = 30;

    /** Previous counter snapshot for TPS calculation */
    private volatile Map<String, Long> previousCounters;
    private volatile Map<String, Long> previousTimerCounts;
    private volatile Map<String, Long> previousTimerTotalNanos;
    private volatile Map<String, Long> previousTimerBuckets;
    private volatile long previousTimestamp;

    private MetricsReporter() {
    }

    public static MetricsReporter getInstance() {
        return INSTANCE;
    }

    /**
     * Start periodic reporting.
     * 
     * @param intervalSeconds interval between reports, in seconds
     */
    public synchronized void start(int intervalSeconds) {
        if (running) {
            return;
        }
        this.intervalSeconds = sanitizeInterval(intervalSeconds);
        running = true;
        collector.setEnabled(true);
        previousCounters = collector.snapshotCounters();
        previousTimerCounts = collector.snapshotTimerCounts();
        previousTimerTotalNanos = collector.snapshotTimerTotalNanos();
        previousTimerBuckets = collector.snapshotTimerBuckets();
        previousTimestamp = System.currentTimeMillis();

        scheduler = Executors.newSingleThreadScheduledExecutor(new java.util.concurrent.ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "A2P-MetricsReporter");
                t.setDaemon(true);
                return t;
            }
        });

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    report();
                } catch (Exception e) {
                    log.warn("MetricsReporter error", e);
                }
            }
        }, this.intervalSeconds, this.intervalSeconds, TimeUnit.SECONDS);

        log.info("MetricsReporter started, interval={}s", this.intervalSeconds);
    }

    /**
     * Stop periodic reporting.
     */
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        collector.setEnabled(false);
        log.info("MetricsReporter stopped.");
    }

    public synchronized void restart(int intervalSeconds) {
        stop();
        int sanitizedInterval = sanitizeInterval(intervalSeconds);
        start(sanitizedInterval);
        log.info("MetricsReporter restarted, interval={}s", sanitizedInterval);
    }

    public synchronized void configure(String logLevel, boolean printZero, boolean printGauge, double minTps) {
        if (logLevel != null && logLevel.trim().length() > 0) {
            this.logLevel = logLevel.trim().toUpperCase();
        }
        this.printZero = printZero;
        this.printGauge = printGauge;
        this.minTps = minTps < 0 ? 0.0 : minTps;
        log.info("MetricsReporter configured. logLevel={}, printZero={}, printGauge={}, minTps={}",
                this.logLevel, this.printZero, this.printGauge, this.minTps);
    }

    public synchronized void applyConfig(boolean enabled, int intervalSeconds, String logLevel,
            boolean printZero, boolean printGauge, double minTps) {
        int sanitizedInterval = sanitizeInterval(intervalSeconds);
        configure(logLevel, printZero, printGauge, minTps);
        if (!enabled) {
            stop();
            collector.setEnabled(false);
            log.info("MetricsReporter disabled. interval={}s", sanitizedInterval);
            return;
        }
        if (!running) {
            start(sanitizedInterval);
            return;
        }
        if (this.intervalSeconds != sanitizedInterval) {
            restart(sanitizedInterval);
            return;
        }
        collector.setEnabled(true);
        log.info("MetricsReporter runtime config applied without restart. interval={}s", this.intervalSeconds);
    }

    public boolean isRunning() {
        return running;
    }

    public int getIntervalSeconds() {
        return intervalSeconds;
    }

    private int sanitizeInterval(int intervalSeconds) {
        if (intervalSeconds <= 0) {
            log.warn("Invalid Metrics.ReportIntervalSeconds={}, fallback to 30", intervalSeconds);
            return 30;
        }
        return intervalSeconds;
    }

    /**
     * Perform a single report cycle.
     */
    private void report() {
        long now = System.currentTimeMillis();
        Map<String, Long> currentCounters = collector.snapshotCounters();
        Map<String, Long> currentGauges = collector.snapshotGauges();
        Map<String, Long> currentTimerCounts = collector.snapshotTimerCounts();
        Map<String, Long> currentTimerTotalNanos = collector.snapshotTimerTotalNanos();
        Map<String, Long> currentTimerBuckets = collector.snapshotTimerBuckets();

        // ---- TPS calculation ----
        double elapsedSeconds = (now - previousTimestamp) / 1000.0;
        if (elapsedSeconds <= 0) {
            elapsedSeconds = 1.0;
        }

        Map<String, Long> deltas = MetricsCollector.delta(currentCounters, previousCounters);

        StringBuilder tpsLine = new StringBuilder(256);
        tpsLine.append("tps=\"");
        boolean hasTpsData = false;
        for (Map.Entry<String, Long> entry : deltas.entrySet()) {
            double tps = entry.getValue() / elapsedSeconds;
            if ((printZero || entry.getValue() > 0) && (printZero || tps >= minTps)) {
                if (hasTpsData) {
                    tpsLine.append(",");
                }
                tpsLine.append(entry.getKey()).append("=").append(String.format("%.1f/s", tps));
                hasTpsData = true;
            }
        }
        tpsLine.append("\"");

        // ---- Gauges ----
        StringBuilder gaugeLine = new StringBuilder(256);
        boolean hasGaugeData = false;
        if (printGauge && !currentGauges.isEmpty()) {
            gaugeLine.append(" gauge=\"");
            for (Map.Entry<String, Long> entry : currentGauges.entrySet()) {
                if (hasGaugeData) {
                    gaugeLine.append(",");
                }
                gaugeLine.append(entry.getKey())
                         .append("=").append(entry.getValue());
                hasGaugeData = true;
            }
            gaugeLine.append("\"");
        }

        // ---- Timers ----
        StringBuilder timerLine = new StringBuilder(256);
        boolean hasTimerData = false;
        if (!currentTimerCounts.isEmpty()) {
            timerLine.append(" timer=\"");
            for (Map.Entry<String, Long> entry : currentTimerCounts.entrySet()) {
                String name = entry.getKey();
                long currentCount = entry.getValue();
                long previousCount = previousTimerCounts.containsKey(name) ? previousTimerCounts.get(name) : 0L;
                long countDelta = currentCount - previousCount;
                if (countDelta <= 0) {
                    continue;
                }
                long currentNanos = currentTimerTotalNanos.containsKey(name) ? currentTimerTotalNanos.get(name) : 0L;
                long previousNanos = previousTimerTotalNanos.containsKey(name) ? previousTimerTotalNanos.get(name) : 0L;
                long nanosDelta = currentNanos - previousNanos;
                double avgMs = (double) nanosDelta / countDelta / 1_000_000.0;
                if (hasTimerData) {
                    timerLine.append(",");
                }
                timerLine.append(name)
                         .append("={count=").append(countDelta)
                         .append(", avgMs=").append(String.format("%.2f", avgMs))
                         .append(", buckets={").append(formatBucketDelta(name, currentTimerBuckets, previousTimerBuckets))
                         .append("}}");
                hasTimerData = true;
            }
            timerLine.append("\"");
        }

        if (hasTpsData || hasGaugeData || hasTimerData || printZero) {
            String module = System.getProperty("module", "unknown");
            StringBuilder line = new StringBuilder(512);
            line.append("[METRICS] module=").append(module.toLowerCase())
                .append(" window=").append(intervalSeconds).append("s");
            if (hasTpsData) {
                line.append(" ").append(tpsLine);
            }
            if (hasGaugeData) {
                line.append(gaugeLine);
            }
            if (hasTimerData) {
                line.append(" ").append(timerLine);
            }
            if (!hasTpsData && !hasGaugeData && !hasTimerData) {
                line.append(" no-activity");
            }
            logByConfiguredLevel(line.toString());
        }

        // Save for next delta
        previousCounters = currentCounters;
        previousTimerCounts = currentTimerCounts;
        previousTimerTotalNanos = currentTimerTotalNanos;
        previousTimerBuckets = currentTimerBuckets;
        previousTimestamp = now;
    }

    private String formatBucketDelta(String timerName, Map<String, Long> currentBuckets,
            Map<String, Long> previousBuckets) {
        StringBuilder buckets = new StringBuilder(96);
        String[] bucketNames = MetricsCollector.timerBucketNames();
        for (int i = 0; i < bucketNames.length; i++) {
            String bucket = bucketNames[i];
            String key = MetricsCollector.timerBucketKey(timerName, bucket);
            long current = currentBuckets.containsKey(key) ? currentBuckets.get(key) : 0L;
            long previous = previousBuckets != null && previousBuckets.containsKey(key)
                    ? previousBuckets.get(key) : 0L;
            if (i > 0) {
                buckets.append(",");
            }
            buckets.append(bucket).append("=").append(current - previous);
        }
        return buckets.toString();
    }

    private void logByConfiguredLevel(String line) {
        if ("INFO".equalsIgnoreCase(logLevel)) {
            metricsLog.info(line);
        } else if ("TRACE".equalsIgnoreCase(logLevel)) {
            metricsLog.trace(line);
        } else {
            metricsLog.debug(line);
        }
    }
}
