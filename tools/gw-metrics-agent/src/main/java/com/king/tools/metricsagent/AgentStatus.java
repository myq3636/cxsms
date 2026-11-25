package com.king.tools.metricsagent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** the agent's own counters and state for the heartbeat (Decisions 7) */
final class AgentStatus {
    private static final Logger log = LoggerFactory.getLogger(AgentStatus.class);
    private static final long WARN_EVERY_MS = 600_000;

    final long startedAt = System.currentTimeMillis();
    final AtomicLong skippedKvLines = new AtomicLong();
    final AtomicLong badLines = new AtomicLong();
    final AtomicLong overLongLines = new AtomicLong();
    volatile String lastError = "";
    volatile boolean retrying;
    private final Deque<String> badSamples = new ArrayDeque<>();
    private final List<ObjectNode> gaps = new ArrayList<>();
    private final ObjectMapper om;
    private long kvWarnedAt;
    private long badWarnedAt;

    AgentStatus(ObjectMapper om) {
        this.om = om;
    }

    void kv(String file) {
        long n = skippedKvLines.incrementAndGet();
        long now = System.currentTimeMillis();
        if (now - kvWarnedAt >= WARN_EVERY_MS) {
            kvWarnedAt = now;
            log.warn("{}: metrics lines in the kv format skipped ({} so far): set Metrics.Format=json in the gateway", file, n);
        }
    }

    synchronized void bad(String file, String line) {
        long n = badLines.incrementAndGet();
        if (badSamples.size() == 5) {
            badSamples.pollFirst();
        }
        badSamples.addLast(file + ": " + (line.length() > 300 ? line.substring(0, 300) + "..." : line));
        long now = System.currentTimeMillis();
        if (now - badWarnedAt >= WARN_EVERY_MS) {
            badWarnedAt = now;
            log.warn("{}: lines not understood ({} so far), e.g. {}", file, n, badSamples.peekLast());
        }
    }

    synchronized List<String> badSamples() {
        return new ArrayList<>(badSamples);
    }

    synchronized void gap(String file, String module, long from, long to, String reason) {
        ObjectNode g = om.createObjectNode();
        g.put("file", file);
        g.put("module", module);
        g.put("from", from);
        g.put("to", to);
        g.put("reason", reason);
        gaps.add(g);
        log.warn("{}: no data from {} to {} ({})", file, from, to, reason);
    }

    synchronized List<ObjectNode> gaps() {
        return new ArrayList<>(gaps);
    }

    /** reported to Portal: not sent again */
    synchronized void gapsSent(List<ObjectNode> sent) {
        gaps.removeAll(sent);
    }
}
