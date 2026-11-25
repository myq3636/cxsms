package com.king.tools.metricsagent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Follows one log file by its name in agent.log-dir, across log4j's rollovers (Decisions 3): reads whole lines only,
 * hands the lines holding its marker to the sink with the position they start at, and tells its Ledger how far it
 * read. Touched by the reader thread only; the sender reads the volatile snapshot fields for the heartbeat.
 */
final class Tailer {
    private static final Logger log = LoggerFactory.getLogger(Tailer.class);
    static final int MAX_LINE = 1 << 20;
    private static final long EOF_SETTLE_MS = 1000;
    private static final long FAR_BEHIND = 8L << 20;
    private static final byte[] METRICS_MARK = "[METRICS-".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] ALERT_MARK = "[METRICS-ALERT]".getBytes(StandardCharsets.US_ASCII);

    enum Kind { METRICS, ALERT }

    interface LineSink {
        void line(Tailer t, Position start, String text);

        void overLong(Tailer t);
    }

    final String name;
    final Kind kind;
    final Path livePath;
    final Ledger ledger = new Ledger(null);
    private final RolledFiles rolled;
    private final long maxHoldMs;
    private final LineSink sink;
    private final byte[] mark;
    private final boolean fixed;
    private final byte[] buf = new byte[65536];

    private Segment seg;
    private byte[] carry = new byte[8192];
    private int carryLen;
    private long carryBytes;
    private long carryStart;
    private boolean skipping;
    private long rotatedAt;
    private long eofSince;
    private boolean finished;
    /** the ts of the last window read (by the Engine) */
    volatile long lastTs;
    /** the recorded file was gone at the start: the gap begins here (the Engine closes it at the next window) */
    Long gapFrom;

    volatile long lagBytes;
    volatile boolean backfill;
    volatile long heldSinceMs;
    volatile String reading = "";

    Tailer(String name, Kind kind, Path livePath, RolledFiles rolled, long maxHoldMs, LineSink sink) {
        this(name, kind, livePath, rolled, maxHoldMs, sink, false);
    }

    private Tailer(String name, Kind kind, Path livePath, RolledFiles rolled, long maxHoldMs, LineSink sink, boolean fixed) {
        this.name = name;
        this.kind = kind;
        this.livePath = livePath;
        this.rolled = rolled;
        this.maxHoldMs = maxHoldMs;
        this.sink = sink;
        this.mark = kind == Kind.ALERT ? ALERT_MARK : METRICS_MARK;
        this.fixed = fixed;
    }

    /** --replay: one file from its start to its end, no rollovers */
    static Tailer fixed(Path file, Kind kind, LineSink sink) throws IOException {
        Tailer t = new Tailer(file.getFileName().toString(), kind, file, null, 0, sink, true);
        t.use(Segment.open(file, false, 0));
        return t;
    }

    /** where to start: the recorded position, found again by inode and head; without one, the start or the end */
    void init(Position stored, boolean startAtEnd) throws IOException {
        if (stored == null) {
            if (Files.exists(livePath)) {
                use(Segment.open(livePath, true, startAtEnd ? Files.size(livePath) : 0));
            }
            return;
        }
        lastTs = stored.lastTs();
        FileIds.Head head = stored.head();
        if (Files.exists(livePath) && FileIds.inode(livePath) == stored.inode() && FileIds.matches(livePath, head)) {
            long size = Files.size(livePath);
            if (size >= stored.offset()) {
                use(Segment.open(livePath, true, stored.offset()));
            } else {
                log.warn("{}: shorter ({} bytes) than read before ({}): truncated, read again from its start", name, size,
                        stored.offset());
                use(Segment.open(livePath, true, 0));
            }
            return;
        }
        List<RolledFiles.Rolled> cands = rolled.list(name);
        for (RolledFiles.Rolled r : cands) {
            if (FileIds.matches(r.path(), head)) {
                log.info("{}: rolled while stopped, continuing in {} at {}", name, r.path().getFileName(), stored.offset());
                use(Segment.open(r.path(), false, stored.offset()));
                return;
            }
        }
        if (Files.exists(livePath) && FileIds.matches(livePath, head) && Files.size(livePath) >= stored.offset()) {
            use(Segment.open(livePath, true, stored.offset()));
            return;
        }
        if (kind == Kind.ALERT) {
            log.info("{}: the file read before is gone, alerts from the end of the live file", name);
            if (Files.exists(livePath)) {
                use(Segment.open(livePath, true, Files.size(livePath)));
            }
            return;
        }
        gapFrom = stored.lastTs() > 0 ? stored.lastTs() : null;
        LocalDate from = stored.lastTs() > 0
                ? Instant.ofEpochMilli(stored.lastTs()).atZone(ZoneId.systemDefault()).toLocalDate()
                : LocalDate.MIN;
        for (RolledFiles.Rolled r : cands) {
            if (!r.date().isBefore(from)) {
                log.warn("{}: the file read before (up to {}) is gone (past the log retention?), reading from {}", name,
                        Instant.ofEpochMilli(stored.lastTs()), r.path().getFileName());
                use(Segment.open(r.path(), false, 0));
                return;
            }
        }
        log.warn("{}: the file read before (up to {}) is gone, reading the live file from its start", name,
                Instant.ofEpochMilli(stored.lastTs()));
        if (Files.exists(livePath)) {
            use(Segment.open(livePath, true, 0));
        }
    }

    boolean finished() {
        return finished;
    }

    /** reads at most budget bytes; the bytes read */
    int poll(int budget, RateLimiter limiter, long now) throws IOException, InterruptedException {
        if (finished) {
            return 0;
        }
        if (seg == null) {
            if (fixed || !Files.exists(livePath)) {
                return 0;
            }
            use(Segment.open(livePath, true, 0));
        }
        if (seg.live && !fixed) {
            checkLive(now);
            if (seg == null) {
                return 0;
            }
        }
        boolean limited = !seg.live || rotatedAt > 0 || behind() > FAR_BEHIND;
        int total = 0;
        boolean eof = false;
        while (total < budget) {
            int want = Math.min(buf.length, budget - total);
            if (limited) {
                limiter.acquire(want);
            }
            int n = seg.read(buf, want);
            if (n < 0) {
                eof = true;
                break;
            }
            split(n);
            total += n;
        }
        ledger.read(seg.positionAt(carryStart, lastTs));
        if (eof) {
            atEnd(now);
        } else {
            eofSince = 0;
        }
        if (seg != null && seg.live && rotatedAt > 0 && now - rotatedAt > maxHoldMs) {
            toRolledCopy();
        }
        snapshot();
        return total;
    }

    /** rotated (another inode under the name, or none) or truncated (same inode, shorter than read) */
    private void checkLive(long now) throws IOException {
        if (rotatedAt > 0) {
            return;
        }
        if (!Files.exists(livePath) || FileIds.inode(livePath) != seg.inode) {
            rotatedAt = now;
            log.info("{}: rolled over, reading the rest of the old file", name);
            return;
        }
        if (seg.size() < seg.position()) {
            log.warn("{}: shorter ({} bytes) than read ({}): truncated, reading again from its start", name, seg.size(),
                    seg.position());
            use(Segment.open(livePath, true, 0));
        }
    }

    private void atEnd(long now) throws IOException {
        if (fixed) {
            flushCarry();
            seg.close();
            seg = null;
            finished = true;
            return;
        }
        if (seg.live && rotatedAt == 0) {
            eofSince = 0;
            return;
        }
        if (seg.live) {
            // the old file of a rollover: read once more a second later before letting it go
            if (eofSince == 0) {
                eofSince = now;
                return;
            }
            if (now - eofSince < EOF_SETTLE_MS) {
                return;
            }
        }
        next();
    }

    /** the file after the one just read: the next rolled one, or the live file from its start */
    private void next() throws IOException {
        FileIds.Head h = seg.head();
        long ino = seg.inode;
        boolean wasLive = seg.live;
        flushCarry();
        seg.close();
        seg = null;
        List<RolledFiles.Rolled> cands = rolled.list(name);
        int at = -1;
        for (int i = cands.size() - 1; i >= 0; i--) {
            if (FileIds.matches(cands.get(i).path(), h)) {
                at = i;
                break;
            }
        }
        if (at >= 0 && at + 1 < cands.size()) {
            use(Segment.open(cands.get(at + 1).path(), false, 0));
            return;
        }
        if (Files.exists(livePath) && (!wasLive || FileIds.inode(livePath) != ino)) {
            use(Segment.open(livePath, true, 0));
        }
    }

    /** held past agent.rolled.max-hold: let the deleted file go, read the rest from its copy in the rolled dir */
    private void toRolledCopy() throws IOException {
        FileIds.Head h = seg.head();
        long off = seg.position();
        for (RolledFiles.Rolled r : rolled.list(name)) {
            if (FileIds.matches(r.path(), h)) {
                Segment s = Segment.open(r.path(), false, off);
                seg.close();
                seg = s;
                rotatedAt = 0;
                eofSince = 0;
                log.info("{}: old file held over {} ms, reading the rest from {}", name, maxHoldMs, r.path().getFileName());
                return;
            }
        }
    }

    private void use(Segment s) {
        if (seg != null && seg != s) {
            seg.close();
        }
        seg = s;
        carryLen = 0;
        carryBytes = 0;
        skipping = false;
        carryStart = s.position();
        rotatedAt = 0;
        eofSince = 0;
        ledger.read(s.positionAt(carryStart, lastTs));
        snapshot();
    }

    private long behind() throws IOException {
        long size = seg.size();
        return size < 0 ? 0 : size - seg.position();
    }

    private void split(int n) {
        int from = 0;
        for (int i = 0; i < n; i++) {
            if (buf[i] == '\n') {
                append(from, i - from);
                endLine();
                from = i + 1;
            }
        }
        append(from, n - from);
    }

    private void append(int from, int len) {
        carryBytes += len;
        if (skipping || len == 0) {
            return;
        }
        if (carryLen + len > MAX_LINE) {
            skipping = true;
            carryLen = 0;
            return;
        }
        if (carryLen + len > carry.length) {
            byte[] b = new byte[Math.max(carry.length * 2, carryLen + len)];
            System.arraycopy(carry, 0, b, 0, carryLen);
            carry = b;
        }
        System.arraycopy(buf, from, carry, carryLen, len);
        carryLen += len;
    }

    /** a whole line in carry (the newline counted, not stored) */
    private void endLine() {
        long start = carryStart;
        carryStart = start + carryBytes + 1;
        if (skipping) {
            sink.overLong(this);
        } else if (indexOf(carry, carryLen, mark) >= 0) {
            int len = carryLen > 0 && carry[carryLen - 1] == '\r' ? carryLen - 1 : carryLen;
            sink.line(this, seg.positionAt(start, lastTs), new String(carry, 0, len, StandardCharsets.UTF_8));
        }
        carryLen = 0;
        carryBytes = 0;
        skipping = false;
    }

    /** the last line of a file read to its end without a newline */
    private void flushCarry() {
        if (carryBytes > 0) {
            carryBytes--;   // endLine counts a newline that is not there
            endLine();
        }
    }

    private void snapshot() {
        try {
            if (seg == null) {
                lagBytes = 0;
                backfill = false;
                reading = "";
            } else {
                backfill = !seg.live || rotatedAt > 0;
                long size = seg.size();
                lagBytes = size < 0 ? -1 : Math.max(0, size - carryStart);
                reading = seg.path.toString();
            }
            heldSinceMs = seg != null && seg.live && rotatedAt > 0 ? rotatedAt : 0;
        } catch (IOException e) {
            lagBytes = -1;
        }
    }

    void close() {
        if (seg != null) {
            seg.close();
            seg = null;
        }
    }

    static int indexOf(byte[] a, int len, byte[] m) {
        outer:
        for (int i = 0; i + m.length <= len; i++) {
            for (int j = 0; j < m.length; j++) {
                if (a[i + j] != m[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }
}
