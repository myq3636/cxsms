package com.king.tools.metricsagent;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The rolled files of a live file in agent.rolled.dir, by agent.rolled.pattern (the filePattern of the gateway's
 * log4j2: {name}.{date}-{i}.gz), oldest first. While log4j compresses one, the renamed plain file and the .gz may
 * both be there: the plain one is kept (same content).
 */
final class RolledFiles {
    record Rolled(Path path, LocalDate date, int index, boolean gzip) {
    }

    private final Path dir;
    private final String pattern;
    private final Map<String, Pattern> regexByName = new HashMap<>();

    RolledFiles(Path dir, String pattern) {
        this.dir = dir;
        this.pattern = pattern;
    }

    Path dir() {
        return dir;
    }

    List<Rolled> list(String name) {
        Pattern re = regexByName.computeIfAbsent(name, this::regex);
        Map<String, Rolled> byBase = new LinkedHashMap<>();
        if (!Files.isDirectory(dir)) {
            return new ArrayList<>();
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                String f = p.getFileName().toString();
                Matcher m = re.matcher(f);
                if (!m.matches() || !Files.isRegularFile(p)) {
                    continue;
                }
                String dg = m.group("date"), ig = m.group("i");
                LocalDate d = dg == null || dg.isEmpty() ? LocalDate.MIN : LocalDate.parse(dg);
                int i = ig == null || ig.isEmpty() ? 0 : Integer.parseInt(ig);
                boolean gz = f.endsWith(".gz");
                String base = gz ? f.substring(0, f.length() - 3) : f;
                Rolled r = new Rolled(p, d, i, gz);
                Rolled old = byBase.get(base);
                if (old == null || (old.gzip() && !gz)) {
                    byBase.put(base, r);
                }
            }
        } catch (IOException e) {
            return new ArrayList<>();
        }
        List<Rolled> out = new ArrayList<>(byBase.values());
        out.sort(Comparator.comparing(Rolled::date).thenComparingInt(Rolled::index));
        return out;
    }

    private Pattern regex(String name) {
        String p = pattern.endsWith(".gz") ? pattern.substring(0, pattern.length() - 3) : pattern;
        StringBuilder re = new StringBuilder();
        Matcher m = Pattern.compile("\\{(name|date|i)\\}").matcher(p);
        int at = 0;
        while (m.find()) {
            re.append(Pattern.quote(p.substring(at, m.start())));
            switch (m.group(1)) {
                case "name" -> re.append(Pattern.quote(name));
                case "date" -> re.append("(?<date>\\d{4}-\\d{2}-\\d{2})");
                default -> re.append("(?<i>\\d+)");
            }
            at = m.end();
        }
        re.append(Pattern.quote(p.substring(at))).append("(\\.gz)?");
        if (!p.contains("{date}")) {
            re.append("(?<date>)?");
        }
        if (!p.contains("{i}")) {
            re.append("(?<i>)?");
        }
        return Pattern.compile(re.toString());
    }
}
