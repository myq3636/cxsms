package com.king.tools.metricsagent;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends the outbox to Portal (Decisions 5): a batch every agent.send-interval, at once while more than a batch waits,
 * a heartbeat alone every agent.heartbeat-interval when idle. On 200 the batch is taken off and the positions move on
 * (saved at once); 413 halves the batch; 401 waits a minute; anything else backs off 10, 20, 40 ... 300 s, reading
 * pausing once the outbox is full.
 */
final class Sender {
    private static final Logger log = LoggerFactory.getLogger(Sender.class);

    private final AgentProperties p;
    private final Outbox outbox;
    private final PortalClient client;
    private final Engine engine;
    private final PositionStore store;
    private final AgentStatus status;
    private final ObjectMapper om;
    private final String host;
    private final String version;
    private long backoffBaseMs = 10_000;
    private long backoffMaxMs = 300_000;
    private long authRetryMs = 60_000;
    private ScheduledExecutorService exec;
    private long lastSend;
    private long nextTry;
    private long backoffMs;
    private int batchWindows;
    private long authLoggedAt;

    Sender(AgentProperties p, Outbox outbox, PortalClient client, Engine engine, PositionStore store, AgentStatus status,
            ObjectMapper om, String host, String version) {
        this.p = p;
        this.outbox = outbox;
        this.client = client;
        this.engine = engine;
        this.store = store;
        this.status = status;
        this.om = om;
        this.host = host;
        this.version = version;
        this.batchWindows = p.getBatch().getMaxWindows();
        this.lastSend = System.currentTimeMillis() - p.getHeartbeatInterval().toMillis();
    }

    /** for the tests */
    void backoff(long baseMs, long maxMs, long authMs) {
        this.backoffBaseMs = baseMs;
        this.backoffMaxMs = maxMs;
        this.authRetryMs = authMs;
    }

    void start() {
        exec = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "sender");
            t.setDaemon(true);
            return t;
        });
        exec.scheduleWithFixedDelay(this::tickSafe, 200, 200, TimeUnit.MILLISECONDS);
    }

    void stop() {
        if (exec != null) {
            exec.shutdownNow();
            try {
                exec.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void tickSafe() {
        try {
            tick(System.currentTimeMillis());
        } catch (Exception e) {
            log.error("sender: {}", e.toString(), e);
        }
    }

    synchronized void tick(long now) {
        if (now < nextTry) {
            return;
        }
        int queued = outbox.size();
        boolean backlog = queued >= batchWindows;
        boolean due = (queued > 0 && (now - lastSend >= p.getSendInterval().toMillis() || backlog))
                || now - lastSend >= p.getHeartbeatInterval().toMillis();
        if (due) {
            sendOnce(now);
        }
    }

    /** one batch (or a heartbeat alone); true when Portal took it */
    synchronized boolean sendOnce(long now) {
        List<Outbox.Item> batch = outbox.peek(batchWindows, p.getBatch().getMaxSize().toBytes());
        List<ObjectNode> gaps = status.gaps();
        long id = store.batchId() + 1;
        PortalClient.Reply r;
        try {
            byte[] body = om.writeValueAsBytes(body(batch, gaps, id, now));
            r = client.post(body, id);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            fail(now, e.toString());
            return false;
        }
        if (r.status() == 200) {
            outbox.ack(batch);
            status.gapsSent(gaps);
            store.batchId(id);
            try {
                store.save(engine.committed());
            } catch (Exception e) {
                log.warn("positions not saved: {}", e.toString());
            }
            if (status.retrying) {
                log.info("Portal takes batches again (batch {})", id);
            }
            lastSend = now;
            backoffMs = 0;
            nextTry = 0;
            status.retrying = false;
            status.lastError = "";
            batchWindows = p.getBatch().getMaxWindows();
            return true;
        }
        if (r.status() == 413 && batch.size() > 1) {
            batchWindows = Math.max(1, Math.min(batchWindows, batch.size()) / 2);
            log.warn("Portal: 413 for {} items, sending {} windows a batch", batch.size(), batchWindows);
            return false;
        }
        if (r.status() == 401) {
            status.lastError = "401: Portal does not accept the signature (agent.secret / gw.metrics.agents of Portal)";
            status.retrying = true;
            nextTry = now + authRetryMs;
            if (now - authLoggedAt >= 60_000) {
                authLoggedAt = now;
                log.error(status.lastError);
            }
            return false;
        }
        fail(now, "HTTP " + r.status() + " " + abbreviate(r.body()));
        return false;
    }

    private void fail(long now, String why) {
        backoffMs = backoffMs == 0 ? backoffBaseMs : Math.min(backoffMaxMs, backoffMs * 2);
        nextTry = now + backoffMs;
        if (!status.retrying || !why.equals(status.lastError)) {
            log.warn("Portal: {}, retrying in {} s", why, backoffMs / 1000);
        }
        status.retrying = true;
        status.lastError = why;
    }

    /** on stop: what is queued, for at most the timeout; true when nothing is left */
    boolean drain(Duration timeout) {
        long end = System.currentTimeMillis() + timeout.toMillis();
        while (outbox.size() > 0) {
            long now = System.currentTimeMillis();
            if (now >= end) {
                return false;
            }
            if (now >= nextTry) {
                sendOnce(now);
            } else {
                try {
                    Thread.sleep(Math.min(200, end - now));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    ObjectNode body(List<Outbox.Item> batch, List<ObjectNode> gaps, long id, long now) {
        ObjectNode root = om.createObjectNode();
        root.put("host", host);
        root.put("agentVersion", version);
        root.put("batchId", id);
        root.put("sentAt", now);
        ArrayNode ws = root.putArray("windows");
        ArrayNode as = root.putArray("alerts");
        for (Outbox.Item it : batch) {
            (it.alert() ? as : ws).add(it.json());
        }
        ObjectNode hb = root.putObject("heartbeat");
        hb.put("startedAt", status.startedAt);
        hb.put("uptimeSec", (now - status.startedAt) / 1000);
        engine.heartbeat(hb, now);
        hb.put("outboxWindows", outbox.windows());
        hb.put("retrying", status.retrying);
        hb.put("lastError", status.lastError);
        hb.put("skippedKvLines", status.skippedKvLines.get());
        hb.put("badLines", status.badLines.get());
        hb.put("overLongLines", status.overLongLines.get());
        ArrayNode g = hb.putArray("gaps");
        gaps.forEach(g::add);
        return root;
    }

    static String abbreviate(String s) {
        if (s == null) {
            return "";
        }
        s = s.replaceAll("\\s+", " ");
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }
}
