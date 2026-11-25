package com.king.tools.metricsagent;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The lines of MetricsReporter (docs/性能监控指标说明.md 6.1): "{log4j time} [METRICS-TPS|SSID|GAUGE|TIMER] {json} " in
 * the metrics files, "... [METRICS-ALERT] {json}" in the main logs. Only the tag and the JSON are read; the time of a
 * window is its ts. An alert has no ts: the time stamp of its log line (yyMMdd HH:mm:ss,SSS) is used.
 */
final class LineParser {
    enum Type { WINDOW, KV, BAD, OTHER }

    record Parsed(Type type, String module, String node, long ts, int window, String field, ObjectNode values,
            int bytes) {
        static final Parsed KV = new Parsed(Type.KV, null, null, 0, 0, null, null, 0);
        static final Parsed BAD = new Parsed(Type.BAD, null, null, 0, 0, null, null, 0);
        static final Parsed OTHER = new Parsed(Type.OTHER, null, null, 0, 0, null, null, 0);
    }

    private static final String MARK = "[METRICS-";
    private static final String ALERT = "[METRICS-ALERT]";
    private static final Pattern LOG_TIME = Pattern.compile("(\\d{6} \\d{2}:\\d{2}:\\d{2},\\d{3})");
    private static final DateTimeFormatter LOG_TIME_FMT = DateTimeFormatter.ofPattern("yyMMdd HH:mm:ss,SSS");

    private final ObjectMapper om;
    private final ZoneId zone;

    LineParser(ObjectMapper om, ZoneId zone) {
        this.om = om;
        this.zone = zone;
    }

    Parsed parseMetrics(String line) {
        int i = line.indexOf(MARK);
        if (i < 0) {
            return Parsed.OTHER;
        }
        int j = line.indexOf(']', i);
        if (j < 0) {
            return Parsed.BAD;
        }
        String tag = line.substring(i + MARK.length(), j);
        String field;
        String source;
        switch (tag) {
            case "TPS" -> { field = "tps"; source = "tps"; }
            case "SSID" -> { field = "ssid"; source = "tps"; }
            case "GAUGE" -> { field = "gauge"; source = "gauge"; }
            case "TIMER" -> { field = "timer"; source = "timer"; }
            default -> { return Parsed.OTHER; }
        }
        String json = line.substring(j + 1).trim();
        if (!json.startsWith("{")) {
            return Parsed.KV;
        }
        try {
            JsonNode n = om.readTree(json);
            JsonNode values = n.get(source);
            String module = n.path("module").asText(null);
            long ts = n.path("ts").asLong(0);
            if (module == null || module.isEmpty() || ts <= 0 || values == null || !values.isObject()) {
                return Parsed.BAD;
            }
            return new Parsed(Type.WINDOW, module, n.path("node").asText("0"), ts, n.path("window").asInt(0), field,
                    (ObjectNode) values, line.length());
        } catch (Exception e) {
            return Parsed.BAD;
        }
    }

    /** the alert with its ts, null when the line is not one */
    ObjectNode parseAlert(String line) {
        int i = line.indexOf(ALERT);
        if (i < 0) {
            return null;
        }
        String json = line.substring(i + ALERT.length()).trim();
        if (!json.startsWith("{")) {
            return null;
        }
        try {
            JsonNode n = om.readTree(json);
            if (!n.isObject() || !n.hasNonNull("module")) {
                return null;
            }
            ObjectNode a = (ObjectNode) n;
            a.put("ts", logTime(line.substring(0, i)));
            return a;
        } catch (Exception e) {
            return null;
        }
    }

    long logTime(String prefix) {
        Matcher m = LOG_TIME.matcher(prefix);
        if (m.find()) {
            try {
                return LocalDateTime.parse(m.group(1), LOG_TIME_FMT).atZone(zone).toInstant().toEpochMilli();
            } catch (Exception e) {
                // fall through
            }
        }
        return System.currentTimeMillis();
    }
}
