package com.king.tools.metricsagent;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The reader (Decisions 2): one thread finds the files (every second), reads them in turn, parses the lines and
 * assembles the windows into the sink; it stops reading while the sink is full. In the daemon it also saves the
 * positions every second (the sender saves them after each batch Portal took).
 */
final class Engine implements Tailer.LineSink {
    private static final Logger log = LoggerFactory.getLogger(Engine.class);
    private static final int POLL_BUDGET = 1 << 20;
    private static final long SCAN_EVERY_MS = 1000;
    private static final long IDLE_SLEEP_MS = 200;

    private final AgentProperties p;
    private final PositionStore store;
    private final ItemSink sink;
    private final AgentStatus status;
    private final ObjectMapper om;
    private final LineParser parser;
    private final WindowAssembler assembler;
    private final RateLimiter limiter;
    private final RolledFiles rolled;
    private final Path logDir;
    private final List<PathMatcher> metricsInclude;
    private final List<PathMatcher> alertInclude;
    private final List<PathMatcher> alertExclude;
    private final Map<String, Tailer> tailers = new ConcurrentSkipListMap<>();
    private volatile boolean running;
    private Thread thread;
    private long lastScan = Long.MIN_VALUE / 2;
    private long lastSave;

    Engine(AgentProperties p, PositionStore store, ItemSink sink, AgentStatus status, ObjectMapper om) {
        this.p = p;
        this.store = store;
        this.sink = sink;
        this.status = status;
        this.om = om;
        String zone = p.getAlert().getZone();
        this.parser = new LineParser(om, zone == null || zone.isBlank() ? ZoneId.systemDefault() : ZoneId.of(zone));
        this.assembler = new WindowAssembler(sink, om, p.getAssembleIdle().toMillis());
        this.limiter = new RateLimiter(p.getRead().getBackfillMbps());
        this.rolled = new RolledFiles(Paths.get(p.getRolled().getDir()), p.getRolled().getPattern());
        this.logDir = Paths.get(p.getLogDir());
        this.metricsInclude = globs(p.getMetrics().getInclude());
        this.alertInclude = globs(p.getAlert().getInclude());
        this.alertExclude = globs(p.getAlert().getExclude());
    }

    static List<PathMatcher> globs(String csv) {
        List<PathMatcher> out = new ArrayList<>();
        if (csv != null) {
            for (String g : csv.split(",")) {
                if (!g.isBlank()) {
                    out.add(FileSystems.getDefault().getPathMatcher("glob:" + g.trim()));
                }
            }
        }
        return out;
    }

    static boolean any(List<PathMatcher> ms, Path name) {
        for (PathMatcher m : ms) {
            if (m.matches(name)) {
                return true;
            }
        }
        return false;
    }

    Tailer.Kind kindOf(String fileName) {
        Path n = Paths.get(fileName);
        if (any(metricsInclude, n)) {
            return Tailer.Kind.METRICS;
        }
        if (any(alertInclude, n) && !any(alertExclude, n)) {
            return Tailer.Kind.ALERT;
        }
        return null;
    }

    RolledFiles rolled() {
        return rolled;
    }

    /** new files in the log dir get a tailer, started at their recorded position */
    void scan() {
        if (!Files.isDirectory(logDir)) {
            return;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(logDir)) {
            for (Path f : ds) {
                String name = f.getFileName().toString();
                if (tailers.containsKey(name) || !Files.isRegularFile(f)) {
                    continue;
                }
                Tailer.Kind k = kindOf(name);
                if (k == null) {
                    continue;
                }
                Tailer t = new Tailer(name, k, f, rolled, p.getRolled().getMaxHold().toMillis(), this);
                String start = k == Tailer.Kind.METRICS ? p.getStart().getMetrics() : p.getStart().getAlert();
                try {
                    t.init(store.get(name), "end".equalsIgnoreCase(start));
                    tailers.put(name, t);
                    log.info("following {} ({})", name, k);
                } catch (IOException e) {
                    log.warn("{}: cannot open: {}", name, e.toString());
                }
            }
        } catch (IOException e) {
            log.warn("cannot list {}: {}", logDir, e.toString());
        }
    }

    /** one round over the files; true when anything was read */
    boolean pollOnce(long now) throws InterruptedException {
        if (now - lastScan >= SCAN_EVERY_MS) {
            lastScan = now;
            scan();
        }
        boolean progressed = false;
        for (Tailer t : tailers.values()) {
            if (sink.full()) {
                break;
            }
            try {
                progressed |= t.poll(POLL_BUDGET, limiter, now) > 0;
            } catch (IOException e) {
                log.warn("{}: read failed: {}", t.name, e.toString());
            }
        }
        assembler.flushIdle(now);
        return progressed;
    }

    /** --dry-run and --replay: read until nothing more comes or the sink is full, then complete what is pending */
    void drainOnce() throws InterruptedException {
        int quiet = 0;
        while (quiet < 2 && !sink.full()) {
            boolean any = pollOnce(System.currentTimeMillis());
            quiet = any ? 0 : quiet + 1;
            boolean allFinished = !tailers.isEmpty();
            for (Tailer t : tailers.values()) {
                allFinished &= t.finished();
            }
            if (allFinished) {
                break;
            }
        }
        assembler.flushAll();
    }

    void addFixed(Tailer t) {
        tailers.put(t.livePath.toString(), t);
        lastScan = Long.MAX_VALUE / 2;   // a replay reads the given files only
    }

    void start() {
        running = true;
        thread = new Thread(this::loop, "tail-reader");
        thread.start();
    }

    private void loop() {
        while (running) {
            try {
                long now = System.currentTimeMillis();
                boolean progressed = pollOnce(now);
                if (now - lastSave >= 1000) {
                    lastSave = now;
                    store.save(committed());
                }
                if (!progressed) {
                    Thread.sleep(IDLE_SLEEP_MS);
                }
            } catch (InterruptedException e) {
                break;
            } catch (Exception e) {
                log.error("reader: {}", e.toString(), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    break;
                }
            }
        }
    }

    /** stops reading; windows not complete are left unsent and read again next time (their lines are not done) */
    void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void close() {
        for (Tailer t : tailers.values()) {
            t.close();
        }
    }

    Map<String, Position> committed() {
        Map<String, Position> out = new TreeMap<>();
        for (Tailer t : tailers.values()) {
            Position pos = t.ledger.committed();
            if (pos != null) {
                out.put(t.name, pos);
            }
        }
        return out;
    }

    Collection<Tailer> tailers() {
        return tailers.values();
    }

    @Override
    public void line(Tailer t, Position start, String text) {
        long now = System.currentTimeMillis();
        if (t.kind == Tailer.Kind.ALERT) {
            ObjectNode a = parser.parseAlert(text);
            if (a == null) {
                status.bad(t.name, text);
                return;
            }
            sink.add(new Outbox.Item(a, true, text.length(), List.of(t.ledger.hold(start))));
            return;
        }
        LineParser.Parsed r = parser.parseMetrics(text);
        switch (r.type()) {
            case WINDOW -> {
                if (t.gapFrom != null) {
                    long gapFrom = t.gapFrom;
                    t.gapFrom = null;
                    if (r.ts() - gapFrom > Math.max(2L * r.window(), 60L) * 1000) {
                        status.gap(t.name, r.module(), gapFrom, r.ts(), "the rolled files between were gone");
                    }
                }
                t.lastTs = Math.max(t.lastTs, r.ts());
                assembler.add(t, r, t.ledger.hold(start), now);
            }
            case KV -> status.kv(t.name);
            case BAD -> status.bad(t.name, text);
            default -> {
            }
        }
    }

    @Override
    public void overLong(Tailer t) {
        status.overLongLines.incrementAndGet();
    }

    /** the files part of the heartbeat (Decisions 7) */
    void heartbeat(ObjectNode hb, long now) {
        ArrayNode files = hb.putArray("files");
        long oldest = Long.MAX_VALUE;
        for (Tailer t : tailers.values()) {
            ObjectNode f = files.addObject();
            f.put("name", t.name);
            f.put("kind", t.kind.name().toLowerCase());
            f.put("reading", t.reading);
            f.put("lagBytes", t.lagBytes);
            f.put("backfill", t.backfill);
            f.put("lastTs", t.lastTs);
            f.put("heldDeleted", t.heldSinceMs > 0);
            f.put("pending", t.ledger.pendingCount());
            if (t.kind == Tailer.Kind.METRICS && (t.backfill || t.lagBytes > 0) && t.lastTs > 0) {
                oldest = Math.min(oldest, t.lastTs);
            }
        }
        if (oldest != Long.MAX_VALUE) {
            hb.put("unreadOldestTs", oldest);
            hb.put("lagWarn", now - oldest > p.getLagWarn().toMillis());
        }
    }
}
