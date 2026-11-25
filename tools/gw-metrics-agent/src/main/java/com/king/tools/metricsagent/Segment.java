package com.king.tools.metricsagent;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * One physical file being read: the live file (a FileChannel, kept open across a rollover so its end is read even
 * after log4j renamed, compressed and deleted it) or a rolled one (plain, or .gz read through GZIPInputStream). Only
 * ever opened for reading.
 */
final class Segment implements Closeable {
    final Path path;
    final boolean live;
    final boolean gzip;
    final long inode;
    private final FileChannel ch;
    private final InputStream in;
    private FileIds.Head head;
    private long pos;
    private boolean exhausted;

    private Segment(Path path, boolean live, long inode, FileChannel ch, InputStream in, FileIds.Head head) {
        this.path = path;
        this.live = live;
        this.gzip = in != null;
        this.inode = inode;
        this.ch = ch;
        this.in = in;
        this.head = head;
    }

    /** opened at offset; a .gz skips to it (past its end: positioned at the end) */
    static Segment open(Path p, boolean live, long offset) throws IOException {
        long ino = FileIds.inode(p);
        if (FileIds.isGzip(p)) {
            FileIds.Head h = FileIds.head(p, FileIds.HEAD);
            InputStream in = FileIds.open(p);
            Segment s = new Segment(p, false, ino, null, in, h);
            long skipped = 0;
            while (skipped < offset) {
                long n = in.skip(offset - skipped);
                if (n <= 0) {
                    if (in.read() < 0) {
                        s.exhausted = true;
                        break;
                    }
                    n = 1;
                }
                skipped += n;
            }
            s.pos = skipped;
            return s;
        }
        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        Segment s = new Segment(p, live, ino, ch, null, FileIds.head(ch, FileIds.HEAD));
        s.pos = Math.min(offset, ch.size());
        ch.position(s.pos);
        return s;
    }

    /** bytes read into buf, -1 at the (current) end */
    int read(byte[] buf, int max) throws IOException {
        if (exhausted) {
            return -1;
        }
        int n;
        if (ch != null) {
            n = ch.read(ByteBuffer.wrap(buf, 0, Math.min(max, buf.length)));
        } else {
            n = in.read(buf, 0, Math.min(max, buf.length));
            if (n < 0) {
                exhausted = true;
            }
        }
        if (n > 0) {
            pos += n;
        }
        return n == 0 ? -1 : n;
    }

    long position() {
        return pos;
    }

    /** the size of a plain file now (it may still grow), -1 for .gz */
    long size() throws IOException {
        return ch != null ? ch.size() : -1;
    }

    /** the head, taken again while a young file is shorter than FileIds.HEAD */
    FileIds.Head head() {
        if (ch != null && head.len() < FileIds.HEAD) {
            try {
                head = FileIds.head(ch, FileIds.HEAD);
            } catch (IOException e) {
                // keep the shorter one: still a prefix of the content
            }
        }
        return head;
    }

    Position positionAt(long offset, long lastTs) {
        FileIds.Head h = head();
        return new Position(path.toString(), inode, h.len(), h.crc(), offset, lastTs);
    }

    @Override
    public void close() {
        try {
            if (ch != null) {
                ch.close();
            }
            if (in != null) {
                in.close();
            }
        } catch (IOException e) {
            // nothing held after a failed close of a read-only file
        }
    }
}
