package com.king.tools.metricsagent;

import java.nio.file.Paths;
import java.time.Duration;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * The daemon: takes the lock, loads the positions, starts the reader and the sender. On stop (SIGTERM from systemd):
 * stops reading, sends what is queued for at most 10 s, saves the positions, releases the lock. Killed before that,
 * the last batch is read again and deduped by Portal (Decisions 6).
 */
@Component
@ConditionalOnProperty(name = "agent.mode", havingValue = "daemon")
class AgentDaemon implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(AgentDaemon.class);

    private final AgentProperties p;
    private final ObjectMapper om;
    private final AgentStatus status;
    private final GwMetricsAgentApplication.AgentInfo info;
    private AgentLock lock;
    private PositionStore store;
    private Engine engine;
    private Sender sender;
    private volatile boolean running;

    AgentDaemon(AgentProperties p, ObjectMapper om, AgentStatus status, GwMetricsAgentApplication.AgentInfo info) {
        this.p = p;
        this.om = om;
        this.status = status;
        this.info = info;
    }

    @Override
    public void start() {
        try {
            lock = AgentLock.acquire(Paths.get(p.getStateDir()));
            store = new PositionStore(Paths.get(p.getStateDir()), om);
            store.load();
            PortalClient client = PortalClient.create(p, info.host());
            Outbox outbox = new Outbox(p.getOutbox().getMaxWindows(), p.getOutbox().getMaxSize().toBytes());
            engine = new Engine(p, store, outbox, status, om);
            sender = new Sender(p, outbox, client, engine, store, status, om, info.host(), info.version());
            engine.start();
            sender.start();
            running = true;
            log.info("gw-metrics-agent {} started: host {}, logs {}, rolled {}, Portal {}", info.version(), info.host(),
                    p.getLogDir(), p.getRolled().getDir(), p.getPortal().getUrl());
        } catch (Exception e) {
            closeLock();
            throw new IllegalStateException("gw-metrics-agent not started: " + e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        if (!running) {
            return;
        }
        engine.stop();
        boolean drained = sender.drain(Duration.ofSeconds(10));
        sender.stop();
        try {
            store.save(engine.committed());
        } catch (Exception e) {
            log.warn("positions not saved at stop: {}", e.toString());
        }
        engine.close();
        closeLock();
        running = false;
        log.info("gw-metrics-agent stopped{}", drained ? "" : " (some windows not sent: read again at the next start)");
    }

    private void closeLock() {
        if (lock != null) {
            try {
                lock.close();
            } catch (Exception e) {
                // released with the process anyway
            }
            lock = null;
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
