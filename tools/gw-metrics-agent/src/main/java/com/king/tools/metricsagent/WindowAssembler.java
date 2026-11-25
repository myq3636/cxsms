package com.king.tools.metricsagent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * One window of a module = its TPS, SSID, GAUGE and TIMER lines of one report (and the parts of a line split at about
 * 4KB), all with the same (module, node, ts), written one after the other (Decisions 4). A window is complete when a
 * later ts comes from the same file, or after agent.assemble-idle without a line. The module prefix of the names is
 * dropped; nothing else is changed.
 */
final class WindowAssembler {
    private static final class Pending {
        final String module;
        final String node;
        final long ts;
        int window;
        final Map<String, ObjectNode> fields = new LinkedHashMap<>();
        final List<Ledger.Unit> units = new ArrayList<>();
        int bytes;
        long updated;

        Pending(String module, String node, long ts) {
            this.module = module;
            this.node = node;
            this.ts = ts;
        }
    }

    private static final String[] FIELDS = { "tps", "ssid", "gauge", "timer" };

    private final ItemSink sink;
    private final ObjectMapper om;
    private final long idleMs;
    private final Map<Tailer, LinkedHashMap<String, Pending>> open = new HashMap<>();

    WindowAssembler(ItemSink sink, ObjectMapper om, long idleMs) {
        this.sink = sink;
        this.om = om;
        this.idleMs = idleMs;
    }

    void add(Tailer t, LineParser.Parsed r, Ledger.Unit u, long now) {
        LinkedHashMap<String, Pending> m = open.computeIfAbsent(t, k -> new LinkedHashMap<>());
        for (Iterator<Pending> it = m.values().iterator(); it.hasNext();) {
            Pending p = it.next();
            if (p.ts < r.ts()) {
                complete(p);
                it.remove();
            }
        }
        Pending p = m.computeIfAbsent(r.module() + "|" + r.node() + "|" + r.ts(), k -> new Pending(r.module(), r.node(), r.ts()));
        p.window = Math.max(p.window, r.window());
        ObjectNode f = p.fields.computeIfAbsent(r.field(), k -> om.createObjectNode());
        String prefix = r.module() + ".";
        for (Iterator<Map.Entry<String, JsonNode>> it = r.values().fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> e = it.next();
            String k = e.getKey();
            f.set(k.startsWith(prefix) ? k.substring(prefix.length()) : k, e.getValue());
        }
        p.units.add(u);
        p.bytes += r.bytes();
        p.updated = now;
    }

    void flushIdle(long now) {
        for (LinkedHashMap<String, Pending> m : open.values()) {
            for (Iterator<Pending> it = m.values().iterator(); it.hasNext();) {
                Pending p = it.next();
                if (now - p.updated >= idleMs) {
                    complete(p);
                    it.remove();
                }
            }
        }
    }

    void flushAll() {
        flushIdle(Long.MAX_VALUE);
    }

    int pending() {
        int n = 0;
        for (LinkedHashMap<String, Pending> m : open.values()) {
            n += m.size();
        }
        return n;
    }

    private void complete(Pending p) {
        ObjectNode w = om.createObjectNode();
        w.put("module", p.module);
        w.put("node", p.node);
        w.put("ts", p.ts);
        w.put("window", p.window);
        for (String f : FIELDS) {
            ObjectNode v = p.fields.get(f);
            if (v != null && !v.isEmpty()) {
                w.set(f, v);
            }
        }
        sink.add(new Outbox.Item(w, false, p.bytes, p.units));
    }
}
