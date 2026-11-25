package com.king.tools.metricsagent;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** state/agent.lock: one agent on a host (Decisions 7) */
final class AgentLock implements Closeable {
    private final FileChannel ch;
    private final FileLock lock;

    private AgentLock(FileChannel ch, FileLock lock) {
        this.ch = ch;
        this.lock = lock;
    }

    static AgentLock acquire(Path stateDir) throws IOException {
        Files.createDirectories(stateDir);
        FileChannel ch = FileChannel.open(stateDir.resolve("agent.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock l;
        try {
            l = ch.tryLock();
        } catch (OverlappingFileLockException e) {
            l = null;
        }
        if (l == null) {
            ch.close();
            throw new IllegalStateException("another gw-metrics-agent holds " + stateDir.resolve("agent.lock")
                    + " (stop it first: systemctl stop gw-metrics-agent)");
        }
        return new AgentLock(ch, l);
    }

    @Override
    public void close() throws IOException {
        lock.release();
        ch.close();
    }
}
