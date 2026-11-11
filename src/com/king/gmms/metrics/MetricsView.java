package com.king.gmms.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;

/**
 * A2pCtl metrics [MODULE] [--json]: the last window each module wrote to Redis ({@link MetricsReporter},
 * Metrics.Redis.Enable). Without MODULE one line per module and node; with it every metric of that module.
 */
public final class MetricsView {
    private MetricsView() {
    }

    public static int print(Jedis j, String module, boolean json) {
        List<String> keys = j.zrange(MetricsReporter.REDIS_INDEX, 0, -1);
        long now = System.currentTimeMillis();
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();
        for (String key : keys) {
            Map<String, String> h = j.hgetAll(key);
            if (h == null || h.isEmpty()) {
                j.zrem(MetricsReporter.REDIS_INDEX, key);   // expired: the module stopped
                continue;
            }
            if (module == null || module.equalsIgnoreCase(h.get("module"))) {
                rows.add(h);
            }
        }
        if (rows.isEmpty()) {
            System.out.println(module == null ? "No metrics in Redis (Metrics.Redis.Enable=false, or no module running)"
                    : "No metrics of " + module + " in Redis");
            return 1;
        }
        if (json) {
            for (Map<String, String> h : rows) {
                JSONObject o = new JSONObject(true);
                o.put("module", h.get("module"));
                o.put("node", h.get("node"));
                o.put("ts", Long.parseLong(h.get("ts")));
                o.put("window", Integer.parseInt(h.get("window")));
                o.put("tps", object(h.get("tps")));
                o.put("gauge", object(h.get("gauge")));
                o.put("timer", object(h.get("timer")));
                System.out.println(o.toJSONString());
            }
            return 0;
        }
        if (module == null) {
            System.out.println(String.format("%-24s %-5s %5s %9s %9s %9s %9s %10s %10s %10s", "module", "node", "age",
                    "in/s", "out/s", "dr/s", "backlog", "lag p95", "rtt p95", "e2e p95"));
            for (Map<String, String> h : rows) {
                JSONObject tps = object(h.get("tps"));
                JSONObject gauge = object(h.get("gauge"));
                JSONObject timer = object(h.get("timer"));
                System.out.println(String.format("%-24s %-5s %4ds %9.1f %9.1f %9.1f %9d %10s %10s %10s", h.get("module"),
                        h.get("node"), (now - Long.parseLong(h.get("ts"))) / 1000,
                        sum(tps, ".business.", ".received"),
                        sum(tps, ".business.", ".smpp_sent") + sum(tps, ".business.", ".http_sent"),
                        sum(tps, ".business.", ".sent_to_customer"),
                        (long) sum(gauge, ".redis.stream.", ".xlen"),
                        ms(maxP95(timer, "." + MetricsNames.TIMER_STREAM_LAG + ".", false)),
                        ms(maxP95(timer, "." + MetricsNames.TIMER_VENDOR_SUBMIT_RTT + ".", true)),
                        ms(maxP95(timer, "." + MetricsNames.TIMER_E2E_SUBMIT, false))));
            }
            System.out.println("in: messages received, out: sent to vendors, dr: DRs to customers; backlog: entries in"
                    + " the streams; p95 in ms of the last window. Every metric: metrics MODULE");
            return 0;
        }
        for (Map<String, String> h : rows) {
            System.out.println(h.get("module") + " node " + h.get("node") + ", " + (now - Long.parseLong(h.get("ts"))) / 1000
                    + "s ago, window " + h.get("window") + "s");
            section("per second", object(h.get("tps")));
            section("gauges", object(h.get("gauge")));
            section("timers (ms)", object(h.get("timer")));
        }
        return 0;
    }

    private static void section(String title, JSONObject o) {
        if (o.isEmpty()) {
            return;
        }
        System.out.println("  " + title + ":");
        for (String k : new TreeMap<String, Object>(o).keySet()) {
            Object v = o.get(k);
            if (v instanceof JSONObject) {
                JSONObject t = (JSONObject) v;
                System.out.println(String.format("    %-70s count=%d avg=%.2f p50=%.2f p95=%.2f p99=%.2f max=%.2f", k,
                        t.getLongValue("count"), t.getDoubleValue("avg"), t.getDoubleValue("p50"), t.getDoubleValue("p95"),
                        t.getDoubleValue("p99"), t.getDoubleValue("max")));
            } else {
                System.out.println(String.format("    %-70s %s", k, v));
            }
        }
    }

    private static JSONObject object(String s) {
        try {
            JSONObject o = s == null || s.isEmpty() ? null : JSON.parseObject(s);
            return o == null ? new JSONObject() : o;
        } catch (Exception e) {
            return new JSONObject();
        }
    }

    private static double sum(JSONObject o, String contains, String suffix) {
        double n = 0;
        for (String k : o.keySet()) {
            if (k.contains(contains) && k.endsWith(suffix)) {
                n += number(o.get(k));
            }
        }
        return n;
    }

    /** the largest p95 of the timers with that part in the name; perChannel: not the per-bind ones */
    private static double maxP95(JSONObject o, String part, boolean perChannel) {
        double max = -1;
        for (String k : o.keySet()) {
            int i = k.indexOf(part);
            if (i < 0 || !(o.get(k) instanceof JSONObject)) {
                continue;
            }
            if (perChannel && k.indexOf('.', i + part.length()) >= 0) {
                continue;
            }
            JSONObject t = (JSONObject) o.get(k);
            max = Math.max(max, t.containsKey("p95") ? t.getDoubleValue("p95") : -1);
        }
        return max;
    }

    private static double number(Object v) {
        if (v instanceof Number) {
            return ((Number) v).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(v).replace("/s", ""));
        } catch (Exception e) {
            return 0;
        }
    }

    private static String ms(double v) {
        return v < 0 ? "-" : String.format("%.1f", v);
    }
}
