package com.king.tools.metricsagent;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** following one file: appends, partial lines, truncation, a reused inode, rollovers running and while stopped */
class TailerTest {
    private static final String NAME = "M1_metrics.log";
    private static final RateLimiter NO_LIMIT = new RateLimiter(0);

    @TempDir
    Path root;
    LogFixture fx;
    List<String> got;
    List<Position> starts;
    Tailer.LineSink sink;
    long now = 1_000_000;

    @BeforeEach
    void setUp() throws IOException {
        fx = new LogFixture(root);
        got = new ArrayList<>();
        starts = new ArrayList<>();
        sink = new Tailer.LineSink() {
            @Override
            public void line(Tailer t, Position start, String text) {
                got.add(text);
                starts.add(start);
            }

            @Override
            public void overLong(Tailer t) {
                got.add("<over long>");
            }
        };
    }

    Tailer tailer(long maxHoldMs) {
        return new Tailer(NAME, Tailer.Kind.METRICS, fx.live(NAME), new RolledFiles(fx.rolledDir, "{name}.{date}-{i}.gz"),
                maxHoldMs, sink);
    }

    /** polls until a round reads nothing, the clock moving 2 s a round (past the 1 s settle of a rolled-over file) */
    void drain(Tailer t, int budget) throws Exception {
        int quiet = 0;
        for (int i = 0; i < 10000 && quiet < 4; i++) {
            now += 2000;
            quiet = t.poll(budget, NO_LIMIT, now) > 0 ? 0 : quiet + 1;
        }
    }

    static String[] lines(String prefix, int n) {
        return IntStream.rangeClosed(1, n).mapToObj(i -> "x [METRICS-TPS] " + prefix + i).toArray(String[]::new);
    }

    @Test
    void appendsAndPartialLines() throws Exception {
        fx.append(NAME, "other log line", "a [METRICS-TPS] 1");
        fx.appendRaw(NAME, "b [METRICS-TPS] 2");
        Tailer t = tailer(60000);
        t.init(null, false);
        drain(t, 1 << 20);
        assertThat(got).containsExactly("a [METRICS-TPS] 1");
        assertThat(t.ledger.committed().offset()).as("before the partial line").isEqualTo("other log line\na [METRICS-TPS] 1\n".length());
        fx.appendRaw(NAME, "\n");
        drain(t, 1 << 20);
        assertThat(got).containsExactly("a [METRICS-TPS] 1", "b [METRICS-TPS] 2");
        assertThat(t.ledger.committed().offset()).isEqualTo(Files.size(fx.live(NAME)));
        assertThat(starts.get(1).offset()).isEqualTo("other log line\na [METRICS-TPS] 1\n".length());
    }

    @Test
    void startAtEndSkipsWhatIsThere() throws Exception {
        fx.append(NAME, lines("old", 3));
        Tailer t = tailer(60000);
        t.init(null, true);
        fx.append(NAME, lines("new", 2));
        drain(t, 1 << 20);
        assertThat(got).containsExactly(lines("new", 2));
    }

    @Test
    void truncatedFileIsReadAgainFromItsStart() throws Exception {
        fx.append(NAME, lines("a", 5));
        Tailer t = tailer(60000);
        t.init(null, false);
        drain(t, 1 << 20);
        try (FileChannel ch = FileChannel.open(fx.live(NAME), StandardOpenOption.WRITE)) {
            ch.truncate(0);
        }
        fx.append(NAME, lines("b", 1));
        drain(t, 1 << 20);
        assertThat(got).containsExactly(concat(lines("a", 5), lines("b", 1)));
    }

    @Test
    void reusedInodeWithOtherContentIsReadFromItsStart() throws Exception {
        fx.append(NAME, lines("a", 20));
        Tailer t = tailer(60000);
        t.init(null, false);
        drain(t, 1 << 20);
        Position recorded = t.ledger.committed();
        // the same inode, other content as long as before or longer: only the head tells it apart
        String other = String.join("\n", lines("z", 30)) + "\n";
        try (FileChannel ch = FileChannel.open(fx.live(NAME), StandardOpenOption.WRITE)) {
            ch.write(ByteBuffer.wrap(other.getBytes(StandardCharsets.UTF_8)), 0);
        }
        assertThat(FileIds.inode(fx.live(NAME))).isEqualTo(recorded.inode());
        got.clear();
        Tailer again = tailer(60000);
        again.init(recorded, false);
        drain(again, 1 << 20);
        assertThat(got).containsExactly(lines("z", 30));
    }

    @Test
    void rolloverWhileRunningReadsTheRestOfTheOldFile() throws Exception {
        fx.append(NAME, lines("a", 50));
        Tailer t = tailer(60000);
        t.init(null, false);
        t.poll(100, NO_LIMIT, now);            // part of the old file read
        fx.append(NAME, lines("late", 3));      // written before the rollover, not read yet
        fx.roll(NAME, "2026-11-25", 1, true);   // renamed, compressed, deleted: the open handle still reads it
        fx.append(NAME, lines("b", 4));
        drain(t, 100);
        assertThat(got).containsExactly(concat(lines("a", 50), lines("late", 3), lines("b", 4)));
        assertThat(t.heldSinceMs).as("the old file let go").isZero();
    }

    @Test
    void rolloversWhileStoppedAreReadInOrderFromTheRecordedPosition() throws Exception {
        fx.append(NAME, lines("a", 3));
        Tailer t = tailer(60000);
        t.init(null, false);
        drain(t, 1 << 20);
        Position recorded = t.ledger.committed();
        t.close();
        // stopped: one more line, then three rollovers (two compressed, the last still plain)
        fx.append(NAME, lines("a-late", 1));
        fx.roll(NAME, "2026-11-25", 1, true);
        fx.append(NAME, lines("b", 2));
        fx.roll(NAME, "2026-11-25", 2, true);
        fx.append(NAME, lines("c", 2));
        fx.roll(NAME, "2026-11-26", 1, false);
        fx.append(NAME, lines("d", 2));
        got.clear();
        Tailer again = tailer(60000);
        again.init(recorded, false);
        drain(again, 1 << 20);
        assertThat(got).containsExactly(concat(lines("a-late", 1), lines("b", 2), lines("c", 2), lines("d", 2)));
    }

    @Test
    void heldPastMaxHoldReadsTheRestFromTheGz() throws Exception {
        fx.append(NAME, lines("a", 400));
        Tailer t = tailer(0);
        t.init(null, false);
        t.poll(1000, NO_LIMIT, now);
        fx.roll(NAME, "2026-11-25", 1, true);
        fx.append(NAME, lines("b", 2));
        now += 1;
        t.poll(1000, NO_LIMIT, now);           // sees the rollover
        now += 1;
        t.poll(1000, NO_LIMIT, now);           // held past 0 ms: switches to the .gz at the same offset
        assertThat(t.reading).endsWith(".gz");
        drain(t, 1000);
        assertThat(got).containsExactly(concat(lines("a", 400), lines("b", 2)));
    }

    @Test
    void recordedFileGoneStartsFromTheRolledFilesOfThatDayAndMarksAGap() throws Exception {
        fx.append(NAME, lines("old", 2));
        fx.roll(NAME, "2026-11-20", 1, true);
        fx.append(NAME, lines("day", 2));
        fx.roll(NAME, "2026-11-24", 1, true);
        fx.append(NAME, lines("live", 1));
        long lastTs = LocalDate.parse("2026-11-23").atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        Position gone = new Position("/gone", 1, 10, 12345, 999, lastTs);
        Tailer t = tailer(60000);
        t.init(gone, false);
        drain(t, 1 << 20);
        assertThat(got).containsExactly(concat(lines("day", 2), lines("live", 1)));
        assertThat(t.gapFrom).isEqualTo(lastTs);
    }

    @Test
    void overLongLinesAreSkipped() throws Exception {
        fx.appendRaw(NAME, "y [METRICS-TPS] " + "x".repeat(Tailer.MAX_LINE + 10) + "\n");
        fx.append(NAME, lines("a", 1));
        Tailer t = tailer(60000);
        t.init(null, false);
        drain(t, 1 << 20);
        assertThat(got).containsExactly("<over long>", lines("a", 1)[0]);
        assertThat(t.ledger.committed().offset()).isEqualTo(Files.size(fx.live(NAME)));
    }

    @Test
    void rolledFilesAreListedInOrderPlainBeforeItsGz() throws Exception {
        Files.createFile(fx.rolledDir.resolve(NAME + ".2026-11-25-10.gz"));
        Files.createFile(fx.rolledDir.resolve(NAME + ".2026-11-25-2.gz"));
        Files.createFile(fx.rolledDir.resolve(NAME + ".2026-11-24-1.gz"));
        Files.createFile(fx.rolledDir.resolve(NAME + ".2026-11-25-10"));
        Files.createFile(fx.rolledDir.resolve("M2_metrics.log.2026-11-25-1.gz"));
        List<String> names = new RolledFiles(fx.rolledDir, "{name}.{date}-{i}.gz").list(NAME).stream()
                .map(r -> r.path().getFileName().toString()).toList();
        assertThat(names).containsExactly(NAME + ".2026-11-24-1.gz", NAME + ".2026-11-25-2.gz", NAME + ".2026-11-25-10");
    }

    static String[] concat(String[]... parts) {
        List<String> all = new ArrayList<>();
        for (String[] p : parts) {
            all.addAll(List.of(p));
        }
        return all.toArray(String[]::new);
    }
}
