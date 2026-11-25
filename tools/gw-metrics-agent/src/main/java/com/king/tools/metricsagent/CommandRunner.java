package com.king.tools.metricsagent;

import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * The one-shot commands (Decisions 8): --check, --dry-run [--limit=N], --replay=FILES [--from=T] [--to=T],
 * --reset=FILE --to=begin|end. Output on stdout; exit code 0 when all is well.
 */
@Component
@ConditionalOnProperty(name = "agent.mode", havingValue = "command")
class CommandRunner implements ApplicationRunner, ExitCodeGenerator {
    private static final Pattern ROLLED_SUFFIX = Pattern.compile("^(.*?)(?:\\.(\\d{4}-\\d{2}-\\d{2})(?:-(\\d+))?)?(?:\\.gz)?$");
    private static final long CHECK_TAIL_BYTES = 256 * 1024;

    private final AgentProperties p;
    private final ObjectMapper om;
    private final AgentStatus status;
    private final GwMetricsAgentApplication.AgentInfo info;
    private PrintStream out = System.out;
    private int exitCode;

    CommandRunner(AgentProperties p, ObjectMapper om, AgentStatus status, GwMetricsAgentApplication.AgentInfo info) {
        this.p = p;
        this.om = om;
        this.status = status;
        this.info = info;
    }

    void out(PrintStream ps) {
        this.out = ps;
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }

    @Override
    public void run(ApplicationArguments a) throws Exception {
        if (a.containsOption("check")) {
            exitCode = check();
        } else if (a.containsOption("dry-run")) {
            exitCode = dryRun(intOption(a, "limit", 20));
        } else if (a.containsOption("replay")) {
            exitCode = replay(a.getOptionValues("replay"), timeOption(a, "from", Long.MIN_VALUE), timeOption(a, "to", Long.MAX_VALUE));
        } else if (a.containsOption("reset")) {
            exitCode = reset(a.getOptionValues("reset"), a.containsOption("to") ? a.getOptionValues("to").get(0) : "");
        }
        out.flush();
    }

    // ---------------------------------------------------------------- --check

    int check() throws Exception {
        boolean ok = true;
        out.println("gw-metrics-agent " + info.version() + ", host " + info.host());
        out.println("Portal     " + p.getPortal().getUrl());
        out.println("logs       " + p.getLogDir() + (Files.isDirectory(Paths.get(p.getLogDir())) ? "" : "  (MISSING)"));
        out.println("rolled     " + p.getRolled().getDir() + "  pattern " + p.getRolled().getPattern()
                + (Files.isDirectory(Paths.get(p.getRolled().getDir())) ? "" : "  (MISSING)"));
        out.println("state      " + Paths.get(p.getStateDir()).toAbsolutePath());
        ok &= Files.isDirectory(Paths.get(p.getLogDir()));

        PositionStore store = new PositionStore(Paths.get(p.getStateDir()), om);
        store.load();
        Engine engine = new Engine(p, store, new ListSink(0), status, om);
        engine.scan();
        out.println();
        out.printf("%-34s %-7s %12s %12s %8s  %s%n", "file", "kind", "position", "lag bytes", "rolled", "reading");
        for (Tailer t : engine.tailers()) {
            Position pos = t.ledger.committed();
            List<RolledFiles.Rolled> r = engine.rolled().list(t.name);
            out.printf("%-34s %-7s %12s %12s %8d  %s%s%n", t.name, t.kind.name().toLowerCase(),
                    pos == null ? "-" : String.valueOf(pos.offset()), t.lagBytes < 0 ? "-" : String.valueOf(t.lagBytes),
                    r.size(), t.reading, t.backfill ? "  (rolled file)" : "");
        }
        for (String name : store.all().keySet()) {
            boolean followed = engine.tailers().stream().anyMatch(t -> t.name.equals(name));
            if (!followed) {
                out.println("  recorded but not in the log dir now: " + name);
            }
        }
        engine.close();

        out.println();
        out.println("last " + CHECK_TAIL_BYTES / 1024 + " KB of each metrics file:");
        LineParser parser = new LineParser(om, ZoneId.systemDefault());
        List<String> samples = new ArrayList<>();
        for (Tailer t : engine.tailers()) {
            if (t.kind != Tailer.Kind.METRICS) {
                continue;
            }
            int windows = 0, kv = 0, bad = 0;
            for (String line : tailLines(t.livePath)) {
                switch (parser.parseMetrics(line).type()) {
                    case WINDOW -> windows++;
                    case KV -> { kv++; if (samples.size() < 5) samples.add(t.name + ": " + cut(line)); }
                    case BAD -> { bad++; if (samples.size() < 5) samples.add(t.name + ": " + cut(line)); }
                    default -> { }
                }
            }
            out.printf("  %-34s %6d metrics lines, %d kv (Metrics.Format=kv: not sent), %d not understood%n", t.name,
                    windows, kv, bad);
        }
        samples.forEach(s -> out.println("    e.g. " + s));

        out.println();
        try {
            PortalClient c = PortalClient.create(p, info.host());
            ObjectNode body = baseBody(0);
            body.putObject("heartbeat").put("check", true);
            PortalClient.Reply r = c.post(om.writeValueAsBytes(body), 0);
            out.println("Portal: HTTP " + r.status() + " " + Sender.abbreviate(r.body()));
            ok &= r.status() == 200;
        } catch (Exception e) {
            out.println("Portal: " + e);
            ok = false;
        }
        out.println(ok ? "OK" : "NOT OK");
        return ok ? 0 : 1;
    }

    private List<String> tailLines(Path f) {
        List<String> lines = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(f.toFile(), "r")) {
            long from = Math.max(0, raf.length() - CHECK_TAIL_BYTES);
            byte[] b = new byte[(int) (raf.length() - from)];
            raf.seek(from);
            raf.readFully(b);
            String[] ls = new String(b, StandardCharsets.UTF_8).split("\n");
            for (int i = from > 0 ? 1 : 0; i < ls.length; i++) {
                lines.add(ls[i]);
            }
        } catch (IOException e) {
            // an unreadable file shows as no lines
        }
        return lines;
    }

    // ---------------------------------------------------------------- --dry-run

    int dryRun(int limit) throws Exception {
        PositionStore store = new PositionStore(Paths.get(p.getStateDir()), om);
        store.load();
        ListSink sink = new ListSink(limit);
        Engine engine = new Engine(p, store, sink, status, om);
        engine.drainOnce();
        engine.close();
        int w = 0, al = 0;
        for (Outbox.Item it : sink.items) {
            out.println(om.writeValueAsString(it.json()));
            if (it.alert()) {
                al++;
            } else {
                w++;
            }
        }
        out.println("dry run: " + w + " windows, " + al + " alerts from the current positions (nothing sent, positions unchanged)");
        return 0;
    }

    // ---------------------------------------------------------------- --replay

    int replay(List<String> specs, long from, long to) throws Exception {
        List<Path> files = new ArrayList<>();
        for (String s : specs) {
            files.addAll(resolve(s));
        }
        if (files.isEmpty()) {
            out.println("no file matches " + specs);
            return 1;
        }
        files.sort(Comparator.comparing((Path f) -> rolledKey(f)));
        PortalClient c = PortalClient.create(p, info.host());
        ReplaySink sink = new ReplaySink(c, from, to);
        Engine engine = new Engine(p, new PositionStore(Paths.get(p.getStateDir()), om), sink, status, om);
        for (Path f : files) {
            String base = baseName(f);
            Tailer.Kind k = engine.kindOf(base);
            if (k == null) {
                out.println("skipped (neither a metrics file nor a main log): " + f);
                continue;
            }
            out.println("replaying " + f + " (" + k.name().toLowerCase() + ")");
            engine.addFixed(Tailer.fixed(f, k, engine));
        }
        engine.drainOnce();
        engine.close();
        sink.flush();
        out.println("replay: " + sink.windows + " windows, " + sink.alerts + " alerts sent in " + sink.batches
                + " batches" + (sink.failed ? " (some batches failed, see above)" : ""));
        return sink.failed ? 1 : 0;
    }

    private List<Path> resolve(String spec) throws IOException {
        List<Path> out = new ArrayList<>();
        Path sp = Paths.get(spec);
        List<Path> dirs = new ArrayList<>();
        String glob;
        if (sp.getParent() != null) {
            dirs.add(sp.getParent());
            glob = sp.getFileName().toString();
        } else {
            dirs.add(Paths.get(p.getRolled().getDir()));
            dirs.add(Paths.get(p.getLogDir()));
            glob = spec;
        }
        PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:" + glob);
        for (Path d : dirs) {
            if (!Files.isDirectory(d)) {
                continue;
            }
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(d)) {
                for (Path f : ds) {
                    if (Files.isRegularFile(f) && m.matches(f.getFileName())) {
                        out.add(f);
                    }
                }
            }
        }
        return out;
    }

    static String baseName(Path f) {
        Matcher m = ROLLED_SUFFIX.matcher(f.getFileName().toString());
        return m.matches() ? m.group(1) : f.getFileName().toString();
    }

    /** name, then date and index, the live file (no date) last */
    static String rolledKey(Path f) {
        Matcher m = ROLLED_SUFFIX.matcher(f.getFileName().toString());
        if (!m.matches() || m.group(2) == null) {
            return f.getFileName() + "|9999-99-99|999999";
        }
        return m.group(1) + "|" + m.group(2) + "|" + String.format("%06d", m.group(3) == null ? 0 : Integer.parseInt(m.group(3)));
    }

    // ---------------------------------------------------------------- --reset

    int reset(List<String> names, String to) throws Exception {
        if (!"begin".equals(to) && !"end".equals(to)) {
            out.println("--reset=FILE needs --to=begin or --to=end");
            return 2;
        }
        AgentLock lock;
        try {
            lock = AgentLock.acquire(Paths.get(p.getStateDir()));
        } catch (IllegalStateException e) {
            out.println(e.getMessage());
            return 2;
        }
        try (lock) {
            PositionStore store = new PositionStore(Paths.get(p.getStateDir()), om);
            store.load();
            for (String name : names) {
                Path live = Paths.get(p.getLogDir()).resolve(name);
                if (!Files.isRegularFile(live)) {
                    out.println("no such file: " + live);
                    return 1;
                }
                try (Segment s = Segment.open(live, true, "end".equals(to) ? Files.size(live) : 0)) {
                    Position pos = s.positionAt(s.position(), 0);
                    store.put(name, pos);
                    out.println(name + ": position set to " + pos.offset() + " (" + to + ")");
                }
            }
            store.save(Map.of());
        }
        return 0;
    }

    // ---------------------------------------------------------------- helpers

    private ObjectNode baseBody(long batchId) {
        ObjectNode b = om.createObjectNode();
        b.put("host", info.host());
        b.put("agentVersion", info.version());
        b.put("batchId", batchId);
        b.put("sentAt", System.currentTimeMillis());
        b.putArray("windows");
        b.putArray("alerts");
        return b;
    }

    private static String cut(String s) {
        return s.length() > 200 ? s.substring(0, 200) + "..." : s;
    }

    private static int intOption(ApplicationArguments a, String name, int dflt) {
        return a.containsOption(name) && !a.getOptionValues(name).isEmpty() ? Integer.parseInt(a.getOptionValues(name).get(0)) : dflt;
    }

    private static long timeOption(ApplicationArguments a, String name, long dflt) {
        if (!a.containsOption(name) || a.getOptionValues(name).isEmpty()) {
            return dflt;
        }
        return parseTime(a.getOptionValues(name).get(0));
    }

    /** epoch ms, yyyy-MM-dd, yyyy-MM-dd HH:mm or yyyy-MM-dd HH:mm:ss (local time) */
    static long parseTime(String s) {
        s = s.trim();
        if (s.matches("\\d{10,}")) {
            return Long.parseLong(s);
        }
        ZoneId z = ZoneId.systemDefault();
        if (s.length() == 10) {
            return LocalDate.parse(s).atStartOfDay(z).toInstant().toEpochMilli();
        }
        String f = s.length() == 16 ? "yyyy-MM-dd HH:mm" : "yyyy-MM-dd HH:mm:ss";
        return LocalDateTime.parse(s.replace('T', ' '), DateTimeFormatter.ofPattern(f)).atZone(z).toInstant().toEpochMilli();
    }

    /** --dry-run: up to limit windows */
    static final class ListSink implements ItemSink {
        final List<Outbox.Item> items = new ArrayList<>();
        private final int limit;
        private int windows;

        ListSink(int limit) {
            this.limit = limit;
        }

        @Override
        public void add(Outbox.Item item) {
            if (!item.alert()) {
                if (windows >= limit) {
                    return;   // a round reads up to 1 MB: what comes past the limit is not shown
                }
                windows++;
            }
            items.add(item);
        }

        @Override
        public boolean full() {
            return windows >= limit;
        }
    }

    /** --replay: the items in [from, to] posted as they come, a batch at a time (batch id -1: not the daemon's) */
    final class ReplaySink implements ItemSink {
        private final PortalClient client;
        private final long from;
        private final long to;
        private final List<Outbox.Item> buf = new ArrayList<>();
        int windows;
        int alerts;
        int batches;
        boolean failed;

        ReplaySink(PortalClient client, long from, long to) {
            this.client = client;
            this.from = from;
            this.to = to;
        }

        @Override
        public void add(Outbox.Item item) {
            long ts = item.json().path("ts").asLong(0);
            if (ts < from || ts > to) {
                return;
            }
            buf.add(item);
            if (buf.size() >= p.getBatch().getMaxWindows()) {
                flush();
            }
        }

        @Override
        public boolean full() {
            return false;
        }

        void flush() {
            if (buf.isEmpty()) {
                return;
            }
            ObjectNode body = baseBody(-1);
            ArrayNode ws = (ArrayNode) body.get("windows");
            ArrayNode as = (ArrayNode) body.get("alerts");
            int w = 0, a = 0;
            for (Outbox.Item it : buf) {
                if (it.alert()) {
                    as.add(it.json());
                    a++;
                } else {
                    ws.add(it.json());
                    w++;
                }
            }
            body.putObject("heartbeat").put("replay", true);
            for (int attempt = 1; ; attempt++) {
                try {
                    PortalClient.Reply r = client.post(om.writeValueAsBytes(body), -1);
                    if (r.status() == 200) {
                        windows += w;
                        alerts += a;
                        batches++;
                        break;
                    }
                    out.println("batch of " + buf.size() + ": HTTP " + r.status() + " " + Sender.abbreviate(r.body()));
                } catch (Exception e) {
                    out.println("batch of " + buf.size() + ": " + e);
                }
                if (attempt == 3) {
                    failed = true;
                    break;
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    failed = true;
                    break;
                }
            }
            buf.clear();
        }
    }
}
