package com.king.tools.metricsagent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.GZIPOutputStream;

/**
 * A log dir and its rolled dir, written the way the gateway's log4j2 does it: lines appended to {name}; a rollover
 * renames it to rolled/{name}.{date}-{i}, creates a new {name}, then (compressing) writes the .gz and deletes the
 * renamed file.
 */
final class LogFixture {
    final Path logDir;
    final Path rolledDir;

    LogFixture(Path root) throws IOException {
        this.logDir = Files.createDirectories(root.resolve("log"));
        this.rolledDir = Files.createDirectories(root.resolve("log/previous"));
    }

    Path live(String name) {
        return logDir.resolve(name);
    }

    void append(String name, String... lines) throws IOException {
        StringBuilder b = new StringBuilder();
        for (String l : lines) {
            b.append(l).append('\n');
        }
        appendRaw(name, b.toString());
    }

    void appendRaw(String name, String s) throws IOException {
        Files.writeString(live(name), s, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** the renamed file (plain) */
    Path rollRename(String name, String date, int i) throws IOException {
        Path to = rolledDir.resolve(name + "." + date + "-" + i);
        Files.move(live(name), to);
        Files.createFile(live(name));
        return to;
    }

    /** log4j's compress action: the .gz written, the renamed file deleted */
    Path compress(Path plain) throws IOException {
        Path gz = plain.resolveSibling(plain.getFileName() + ".gz");
        try (InputStream in = Files.newInputStream(plain); OutputStream out = new GZIPOutputStream(Files.newOutputStream(gz))) {
            in.transferTo(out);
        }
        Files.delete(plain);
        return gz;
    }

    Path roll(String name, String date, int i, boolean gzip) throws IOException {
        Path p = rollRename(name, date, i);
        return gzip ? compress(p) : p;
    }

    /** a window of MetricsReporter (json): the TPS line split in two parts, SSID, GAUGE, TIMER */
    static String[] window(String module, long ts) {
        String h = "{\"module\":\"" + module + "\",\"node\":\"0\",\"ts\":" + ts + ",\"window\":30";
        String t = "261125 10:00:00,000 ";
        return new String[] {
            t + "[METRICS-TPS] " + h + ",\"part\":1,\"tps\":{\"" + module + ".business.a\":1.50}} ",
            t + "[METRICS-TPS] " + h + ",\"part\":2,\"tps\":{\"" + module + ".business.b\":2.50}} ",
            t + "[METRICS-SSID] " + h + ",\"tps\":{\"" + module + ".ssid.9995.insubmit.received\":3.10}} ",
            t + "[METRICS-GAUGE] " + h + ",\"gauge\":{\"" + module + ".redis.stream.server_to_core.xlen\":7}} ",
            t + "[METRICS-TIMER] " + h + ",\"timer\":{\"" + module + ".e2e.submit\":{\"count\":3,\"avg\":1.00,\"p50\":1.00,\"p95\":2.00,\"p99\":2.00,\"max\":3.00}}} ",
        };
    }
}
