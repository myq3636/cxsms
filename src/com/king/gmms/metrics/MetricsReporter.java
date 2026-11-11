package com.king.gmms.metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;

/**
 * Periodic metrics reporter (docs/性能监控埋点评估与设计.md): every window it writes to logger
 * {@code metrics.logger} one line per kind, each a JSON object ({@code Metrics.Format=json}, the default)
 * or key=value text ({@code kv}):
 * <ol>
 *   <li><b>[METRICS-TPS]</b> – per-second throughput for each counter (delta / interval)</li>
 *   <li><b>[METRICS-SSID]</b> – the same per ssid ({@code Metrics.Ssid.Enable})</li>
 *   <li><b>[METRICS-GAUGE]</b> – current point-in-time values ({@code Metrics.PrintGauge})</li>
 *   <li><b>[METRICS-TIMER]</b> – count, average, p50/p95/p99 and largest value of the window</li>
 * </ol>
 * A line longer than about 4 KB is split ("part" 1, 2, ...). The window can also be written to Redis
 * ({@code metrics:{module}:{node}}, {@code Metrics.Redis.Enable}) for {@code A2pCtl metrics}, and a few
 * thresholds ({@code Metrics.Alert.*}) are checked: crossing one logs WARN {@code [METRICS-ALERT]}.
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
    /** a line longer than this is split */
    private static final int MAX_LINE = 4000;
    public static final String REDIS_KEY_PREFIX = "metrics:";
    public static final String REDIS_INDEX = "metrics:index";

    private final MetricsCollector collector = MetricsCollector.getInstance();
    private final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile String logLevel = "INFO";
    private volatile boolean printZero = false;
    private volatile boolean printGauge = true;
    private volatile double minTps = 0.01;
    private volatile int intervalSeconds = 30;
    private volatile boolean json = true;
    private volatile boolean redisEnable = true;
    private volatile Alerts alerts = new Alerts(0, 0, 0, 0, 300);
    /** alert key -> when it was last logged; and how many times it was crossed since */
    private final Map<String, long[]> alertState = new HashMap<String, long[]>();

    /** Previous snapshots for the window deltas */
    private volatile Map<String, Long> previousCounters;
    private volatile Map<String, MetricsCollector.TimerSnapshot> previousTimers;
    private volatile long previousTimestamp;

    /** Metrics.Alert.*: 0 = not checked */
    public static final class Alerts {
        final long streamLagMs;
        final long vendorRttMs;
        final long dbPoolWaiting;
        final double consumerFailRatio;
        final long repeatSeconds;

        public Alerts(long streamLagMs, long vendorRttMs, long dbPoolWaiting, double consumerFailRatio, long repeatSeconds) {
            this.streamLagMs = streamLagMs;
            this.vendorRttMs = vendorRttMs;
            this.dbPoolWaiting = dbPoolWaiting;
            this.consumerFailRatio = consumerFailRatio;
            this.repeatSeconds = repeatSeconds <= 0 ? 300 : repeatSeconds;
        }
    }

    /** one timer over one window */
    static final class TimerWindow {
        final long count;
        final double avgMs;
        final double p50;
        final double p95;
        final double p99;
        final double maxMs;

        TimerWindow(long count, double avgMs, double p50, double p95, double p99, double maxMs) {
            this.count = count;
            this.avgMs = avgMs;
            this.p50 = p50;
            this.p95 = p95;
            this.p99 = p99;
            this.maxMs = maxMs;
        }
    }

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
        previousTimers = collector.snapshotTimers();
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

    /** Metrics.Format (json | kv), Metrics.Redis.Enable, Metrics.Alert.* */
    public synchronized void configureOutput(String format, boolean redisEnable, Alerts alerts) {
        this.json = !"kv".equalsIgnoreCase(format == null ? "" : format.trim());
        this.redisEnable = redisEnable;
        if (alerts != null) {
            this.alerts = alerts;
        }
        log.info("MetricsReporter output: format={}, redis={}, alert streamLagMs={} vendorRttMs={} dbPoolWaiting={} consumerFailRatio={}",
                json ? "json" : "kv", redisEnable, this.alerts.streamLagMs, this.alerts.vendorRttMs,
                this.alerts.dbPoolWaiting, this.alerts.consumerFailRatio);
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

    /** a report now, outside the schedule (tests) */
    public void reportNow() {
        synchronized (this) {
            if (!running) {
                return;
            }
        }
        report();
    }

    /**
     * Perform a single report cycle.
     */
    private synchronized void report() {
        long now = System.currentTimeMillis();
        updateJvmThreadGauges();
        Map<String, Long> currentCounters = collector.snapshotCounters();
        Map<String, Long> currentGauges = collector.snapshotGauges();
        Map<String, MetricsCollector.TimerSnapshot> currentTimers = collector.snapshotTimers();

        // ---- TPS calculation ----
        double elapsedSeconds = (now - previousTimestamp) / 1000.0;
        if (elapsedSeconds <= 0) {
            elapsedSeconds = 1.0;
        }
        Map<String, Long> deltas = MetricsCollector.delta(currentCounters, previousCounters);
        Map<String, Double> tps = new TreeMap<String, Double>();
        Map<String, Double> ssidTps = new TreeMap<String, Double>();
        for (Map.Entry<String, Long> entry : deltas.entrySet()) {
            double v = entry.getValue() / elapsedSeconds;
            if ((printZero || entry.getValue() > 0) && (printZero || v >= minTps)) {
                (entry.getKey().contains(".ssid.") ? ssidTps : tps).put(entry.getKey(), v);
            }
        }

        // ---- Timers ----
        Map<String, TimerWindow> timers = new TreeMap<String, TimerWindow>();
        for (Map.Entry<String, MetricsCollector.TimerSnapshot> entry : currentTimers.entrySet()) {
            MetricsCollector.TimerSnapshot cur = entry.getValue();
            MetricsCollector.TimerSnapshot prev = previousTimers == null ? null : previousTimers.get(entry.getKey());
            long countDelta = cur.count - (prev == null ? 0 : prev.count);
            if (countDelta <= 0) {
                continue;
            }
            long nanosDelta = cur.nanos - (prev == null ? 0 : prev.nanos);
            long[] b = new long[cur.buckets.length];
            for (int i = 0; i < b.length; i++) {
                b[i] = cur.buckets[i] - (prev == null ? 0 : prev.buckets[i]);
            }
            double maxMs = cur.windowMaxNanos / 1000000.0;
            timers.put(entry.getKey(), new TimerWindow(countDelta, (double) nanosDelta / countDelta / 1000000.0,
                    MetricsCollector.percentileMs(b, 0.50, maxMs), MetricsCollector.percentileMs(b, 0.95, maxMs),
                    MetricsCollector.percentileMs(b, 0.99, maxMs), maxMs));
        }

        String module = System.getProperty("module", "unknown").toLowerCase();
        String node = System.getProperty("NodeID", "0");
        boolean any = false;
        any |= emit("METRICS-TPS", "tps", module, node, now, formatRates(tps));
        any |= emit("METRICS-SSID", "tps", module, node, now, formatRates(ssidTps));
        Map<String, String> gaugeValues = new LinkedHashMap<String, String>();
        if (printGauge) {
            for (Map.Entry<String, Long> entry : currentGauges.entrySet()) {
                gaugeValues.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            any |= emit("METRICS-GAUGE", "gauge", module, node, now, gaugeValues);
        }
        Map<String, String> timerValues = formatTimers(timers);
        any |= emit("METRICS-TIMER", "timer", module, node, now, timerValues);
        if (!any && printZero) {
            emit("METRICS-TPS", "tps", module, node, now, java.util.Collections.<String, String>emptyMap(), true);
        }

        if (redisEnable) {
            writeRedis(module, node, now, formatRates(tps), formatGauges(currentGauges), timerValues);
        }
        checkAlerts(module, now, deltas, currentGauges, timers);

        // Save for next delta
        previousCounters = currentCounters;
        previousTimers = currentTimers;
        previousTimestamp = now;
    }

    private Map<String, String> formatRates(Map<String, Double> rates) {
        Map<String, String> out = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Double> e : rates.entrySet()) {
            out.put(e.getKey(), json ? String.format(java.util.Locale.ROOT, "%.2f", e.getValue()) : String.format(java.util.Locale.ROOT, "%.1f/s", e.getValue()));
        }
        return out;
    }

    private Map<String, String> formatGauges(Map<String, Long> gauges) {
        Map<String, String> out = new LinkedHashMap<String, String>();
        for (Map.Entry<String, Long> e : gauges.entrySet()) {
            out.put(e.getKey(), String.valueOf(e.getValue()));
        }
        return out;
    }

    private Map<String, String> formatTimers(Map<String, TimerWindow> timers) {
        Map<String, String> out = new LinkedHashMap<String, String>();
        for (Map.Entry<String, TimerWindow> e : timers.entrySet()) {
            TimerWindow t = e.getValue();
            if (json) {
                out.put(e.getKey(), String.format(java.util.Locale.ROOT, "{\"count\":%d,\"avg\":%.2f,\"p50\":%.2f,\"p95\":%.2f,\"p99\":%.2f,\"max\":%.2f}",
                        t.count, t.avgMs, t.p50, t.p95, t.p99, t.maxMs));
            } else {
                out.put(e.getKey(), String.format(java.util.Locale.ROOT, "{count=%d, avgMs=%.2f, p50=%.2f, p95=%.2f, p99=%.2f, maxMs=%.2f}",
                        t.count, t.avgMs, t.p50, t.p95, t.p99, t.maxMs));
            }
        }
        return out;
    }

    private boolean emit(String tag, String field, String module, String node, long now, Map<String, String> values) {
        return emit(tag, field, module, node, now, values, false);
    }

    /** the values as one line, or several of about MAX_LINE when long; false when there is nothing */
    private boolean emit(String tag, String field, String module, String node, long now, Map<String, String> values,
            boolean evenEmpty) {
        if (values.isEmpty() && !evenEmpty) {
            return false;
        }
        List<String> parts = new ArrayList<String>();
        StringBuilder body = new StringBuilder(512);
        for (Map.Entry<String, String> e : values.entrySet()) {
            String item = json ? quote(e.getKey()) + ":" + e.getValue() : e.getKey() + "=" + e.getValue();
            if (body.length() > 0 && body.length() + item.length() > MAX_LINE) {
                parts.add(body.toString());
                body.setLength(0);
            }
            if (body.length() > 0) {
                body.append(",");
            }
            body.append(item);
        }
        parts.add(body.toString());
        for (int i = 0; i < parts.size(); i++) {
            StringBuilder line = new StringBuilder(parts.get(i).length() + 128);
            line.append('[').append(tag).append("] ");
            if (json) {
                line.append("{\"module\":").append(quote(module)).append(",\"node\":").append(quote(node))
                    .append(",\"ts\":").append(now).append(",\"window\":").append(intervalSeconds);
                if (parts.size() > 1) {
                    line.append(",\"part\":").append(i + 1);
                }
                line.append(",\"").append(field).append("\":{").append(parts.get(i)).append("}}");
            } else {
                line.append("module=").append(module).append(" node=").append(node)
                    .append(" window=").append(intervalSeconds).append("s");
                if (parts.size() > 1) {
                    line.append(" part=").append(i + 1);
                }
                line.append(' ').append(field).append("=\"").append(parts.get(i)).append('"');
            }
            logByConfiguredLevel(line.toString());
        }
        return true;
    }

    static String quote(String s) {
        StringBuilder b = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                b.append('\\').append(c);
            } else if (c < 0x20) {
                b.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
            } else {
                b.append(c);
            }
        }
        return b.append('"').toString();
    }

    /** JSON of formatted values (rates and timers are JSON numbers / objects already in json format) */
    private static String jsonObject(Map<String, String> values, boolean valuesAreJson) {
        StringBuilder b = new StringBuilder(256).append('{');
        for (Map.Entry<String, String> e : values.entrySet()) {
            if (b.length() > 1) {
                b.append(',');
            }
            b.append(quote(e.getKey())).append(':').append(valuesAreJson ? e.getValue() : quote(e.getValue()));
        }
        return b.append('}').toString();
    }

    /** metrics:{module}:{node} (Hash, TTL 3 windows) and its member in metrics:index, for A2pCtl metrics */
    private void writeRedis(String module, String node, long now, Map<String, String> tps, Map<String, String> gauges,
            Map<String, String> timers) {
        try {
            com.king.redis.RedisConnection c = com.king.redis.RedisClient.getInstance().getStateRedis();
            if (c == null) {
                return;
            }
            final String key = REDIS_KEY_PREFIX + module + ":" + node;
            final Map<String, String> h = new HashMap<String, String>();
            h.put("module", module);
            h.put("node", node);
            h.put("ts", String.valueOf(now));
            h.put("window", String.valueOf(intervalSeconds));
            h.put("tps", jsonObject(tps, json));
            h.put("gauge", jsonObject(gauges, true));
            h.put("timer", json ? jsonObject(timers, true) : jsonObject(timers, false));
            final long ttl = Math.max(90, intervalSeconds * 3L);
            c.runPipeline(p -> {
                p.del(key);
                p.hset(key, h);
                p.expire(key, ttl);
                p.zadd(REDIS_INDEX, now, key);
            });
        } catch (Throwable e) {
            long n = com.king.gmms.util.LogRateLimiter.allow("metrics-redis", 600000L);
            if (n > 0) {
                log.warn("Metrics not written to Redis ({} times in the last 10 min): {}", n, e.toString());
            }
        }
    }

    // ==================== Alerts ====================

    private void checkAlerts(String module, long now, Map<String, Long> deltas, Map<String, Long> gauges,
            Map<String, TimerWindow> timers) {
        Alerts a = alerts;
        if (a.streamLagMs > 0 || a.vendorRttMs > 0) {
            String lag = "." + MetricsNames.TIMER_STREAM_LAG + ".";
            String rtt = "." + MetricsNames.TIMER_VENDOR_SUBMIT_RTT + ".";
            for (Map.Entry<String, TimerWindow> e : timers.entrySet()) {
                String name = e.getKey();
                double p95 = e.getValue().p95;
                if (a.streamLagMs > 0 && name.contains(lag) && p95 > a.streamLagMs) {
                    alert(module, now, "stream_lag", name, p95, a.streamLagMs, e.getValue().count);
                } else if (a.vendorRttMs > 0 && name.contains(rtt)
                        && name.indexOf('.', name.indexOf(rtt) + rtt.length()) < 0 && p95 > a.vendorRttMs) {
                    // per channel only: a bind of the channel would alert the same
                    alert(module, now, "vendor_rtt", name, p95, a.vendorRttMs, e.getValue().count);
                }
            }
        }
        if (a.dbPoolWaiting > 0) {
            for (Map.Entry<String, Long> e : gauges.entrySet()) {
                if (e.getKey().contains(".db.pool.") && e.getKey().endsWith(".waiting") && e.getValue() > a.dbPoolWaiting) {
                    alert(module, now, "db_pool_waiting", e.getKey(), e.getValue(), a.dbPoolWaiting, 0);
                }
            }
        }
        if (a.consumerFailRatio > 0) {
            // {module}.consumer.process.{type}.fail against {module}.stream.in.{type}.read
            String failPrefix = MetricsNames.module() + "." + MetricsNames.COMPONENT_CONSUMER + "." + MetricsNames.FLOW_PROCESS + ".";
            String readPrefix = MetricsNames.module() + "." + MetricsNames.COMPONENT_STREAM + "." + MetricsNames.FLOW_IN + ".";
            for (Map.Entry<String, Long> e : deltas.entrySet()) {
                String name = e.getKey();
                if (!name.startsWith(failPrefix) || !name.endsWith("." + MetricsNames.ACTION_FAIL) || e.getValue() <= 0) {
                    continue;
                }
                String type = name.substring(failPrefix.length(), name.length() - MetricsNames.ACTION_FAIL.length() - 1);
                Long read = deltas.get(readPrefix + type + "." + MetricsNames.ACTION_READ);
                long total = read == null ? 0 : read;
                if (total < 20) {
                    continue;   // too few to tell
                }
                double ratio = (double) e.getValue() / total;
                if (ratio > a.consumerFailRatio) {
                    alert(module, now, "consumer_fail_ratio", name, ratio, a.consumerFailRatio, total);
                }
            }
        }
    }

    /** WARN [METRICS-ALERT] {...}; the same alert again only after Metrics.Alert.RepeatSeconds, with how often it was crossed */
    private void alert(String module, long now, String kind, String metric, double value, double threshold, long count) {
        String key = kind + "|" + metric;
        long[] st = alertState.get(key);
        if (st == null) {
            st = new long[] { 0L, 0L };
            alertState.put(key, st);
        }
        st[1]++;
        if (st[0] > 0 && now - st[0] < alerts.repeatSeconds * 1000L) {
            return;
        }
        long times = st[1];
        st[0] = now;
        st[1] = 0;
        String line = "[METRICS-ALERT] {\"module\":" + quote(module) + ",\"alert\":\"" + kind + "\",\"metric\":" + quote(metric)
                + ",\"value\":" + String.format(java.util.Locale.ROOT, "%.2f", value)
                + ",\"threshold\":" + String.format(java.util.Locale.ROOT, "%.2f", threshold)
                + ",\"count\":" + count + ",\"windows\":" + times + "}";
        log.warn(line);
    }

    private void updateJvmThreadGauges() {
        collector.setGauge(MetricsNames.gauge("jvm.thread", "count"), threadMXBean.getThreadCount());
        collector.setGauge(MetricsNames.gauge("jvm.thread", "daemon_count"), threadMXBean.getDaemonThreadCount());
        collector.setGauge(MetricsNames.gauge("jvm.thread", "peak_count"), threadMXBean.getPeakThreadCount());
        collector.setGauge(MetricsNames.gauge("jvm.thread", "total_started"), threadMXBean.getTotalStartedThreadCount());
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
