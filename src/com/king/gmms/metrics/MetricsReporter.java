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
    private static final MetricsReporter INSTANCE = new MetricsReporter();

    private final MetricsCollector collector = MetricsCollector.getInstance();
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;

    /** Previous counter snapshot for TPS calculation */
    private volatile Map<String, Long> previousCounters;
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
        running = true;
        previousCounters = collector.snapshotCounters();
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
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        log.info("MetricsReporter started, interval={}s", intervalSeconds);
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
            scheduler.shutdown();
        }
        log.info("MetricsReporter stopped.");
    }

    /**
     * Perform a single report cycle.
     */
    private void report() {
        long now = System.currentTimeMillis();
        Map<String, Long> currentCounters = collector.snapshotCounters();
        Map<String, Long> currentGauges = collector.snapshotGauges();
        Map<String, String> currentTimers = collector.snapshotTimers();

        // ---- TPS calculation ----
        double elapsedSeconds = (now - previousTimestamp) / 1000.0;
        if (elapsedSeconds <= 0) {
            elapsedSeconds = 1.0;
        }

        Map<String, Long> deltas = MetricsCollector.delta(currentCounters, previousCounters);

        StringBuilder tpsLine = new StringBuilder(256);
        tpsLine.append("[METRICS-TPS]");
        for (Map.Entry<String, Long> entry : deltas.entrySet()) {
            double tps = entry.getValue() / elapsedSeconds;
            if (entry.getValue() > 0) {
                tpsLine.append(" ").append(entry.getKey())
                       .append("=").append(String.format("%.1f/s", tps))
                       .append("(total:").append(currentCounters.get(entry.getKey())).append(")");
            }
        }
        // Always log the TPS line so operators know the reporter is alive
        log.info(tpsLine.toString());

        // ---- Gauges ----
        if (!currentGauges.isEmpty()) {
            StringBuilder gaugeLine = new StringBuilder(256);
            gaugeLine.append("[METRICS-GAUGE]");
            for (Map.Entry<String, Long> entry : currentGauges.entrySet()) {
                gaugeLine.append(" ").append(entry.getKey())
                         .append("=").append(entry.getValue());
            }
            log.info(gaugeLine.toString());
        }

        // ---- Timers ----
        if (!currentTimers.isEmpty()) {
            StringBuilder timerLine = new StringBuilder(256);
            timerLine.append("[METRICS-TIMER]");
            for (Map.Entry<String, String> entry : currentTimers.entrySet()) {
                timerLine.append(" ").append(entry.getKey())
                         .append("={").append(entry.getValue()).append("}");
            }
            log.info(timerLine.toString());
        }

        // Save for next delta
        previousCounters = currentCounters;
        previousTimestamp = now;
    }
}
