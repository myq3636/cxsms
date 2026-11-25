package com.king.tools.metricsagent;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * state/positions.json (Decisions 6): the safe position of every file and the last batch id. Written to a temporary
 * file, forced to disk and renamed over the old one, so a crash at any point leaves the old or the new one whole.
 */
final class PositionStore {
    record State(int version, long batchId, Map<String, Position> files) {
    }

    private final Path file;
    private final ObjectMapper om;
    private final Map<String, Position> positions = new TreeMap<>();
    private long batchId;
    private String lastWritten;

    PositionStore(Path stateDir, ObjectMapper om) {
        this.file = stateDir.resolve("positions.json");
        this.om = om;
    }

    synchronized void load() throws IOException {
        positions.clear();
        batchId = 0;
        if (Files.exists(file)) {
            State s = om.readValue(file.toFile(), State.class);
            if (s.files() != null) {
                positions.putAll(s.files());
            }
            batchId = s.batchId();
        }
    }

    synchronized Position get(String name) {
        return positions.get(name);
    }

    synchronized Map<String, Position> all() {
        return new TreeMap<>(positions);
    }

    synchronized void put(String name, Position p) {
        positions.put(name, p);
    }

    synchronized long batchId() {
        return batchId;
    }

    synchronized void batchId(long id) {
        batchId = id;
    }

    /** the committed positions merged in (files not followed now keep theirs), written when changed */
    synchronized void save(Map<String, Position> committed) throws IOException {
        positions.putAll(committed);
        String json = om.writerWithDefaultPrettyPrinter().writeValueAsString(new State(1, batchId, positions));
        if (json.equals(lastWritten)) {
            return;
        }
        Files.createDirectories(file.getParent());
        Path tmp = file.resolveSibling("positions.json.tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer b = ByteBuffer.wrap(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            while (b.hasRemaining()) {
                ch.write(b);
            }
            ch.force(true);
        }
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        lastWritten = json;
    }
}
