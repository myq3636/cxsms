package com.king.tools.metricsagent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

/**
 * Which file is which: the inode, and a CRC of the first bytes ("head") as the inode of a deleted file may be given to
 * a new one (Decisions 3). The head of a .gz file is the one of its content, so a rolled and compressed file is found
 * again by the head of the live file it was.
 */
final class FileIds {
    static final int HEAD = 256;

    private FileIds() {
    }

    /** the first up to HEAD bytes: length and CRC32 */
    record Head(int len, long crc) {
        static final Head EMPTY = new Head(0, 0L);

        static Head of(byte[] b, int len) {
            CRC32 c = new CRC32();
            c.update(b, 0, len);
            return new Head(len, c.getValue());
        }
    }

    static long inode(Path p) throws IOException {
        return ((Number) Files.getAttribute(p, "unix:ino")).longValue();
    }

    static boolean isGzip(Path p) {
        return p.getFileName().toString().endsWith(".gz");
    }

    /** the head of the file's content, at most max bytes */
    static Head head(Path p, int max) throws IOException {
        byte[] b = new byte[max];
        int n = 0;
        try (InputStream in = open(p)) {
            while (n < max) {
                int r = in.read(b, n, max - n);
                if (r < 0) {
                    break;
                }
                n += r;
            }
        }
        return Head.of(b, n);
    }

    /** the head of an open plain file, read at absolute positions (the channel's position is not moved) */
    static Head head(FileChannel ch, int max) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(max);
        long at = 0;
        while (bb.hasRemaining()) {
            int r = ch.read(bb, at);
            if (r <= 0) {
                break;
            }
            at += r;
        }
        return Head.of(bb.array(), bb.position());
    }

    /** the content starts with the bytes the head was taken of */
    static boolean matches(Path p, Head h) {
        if (h == null || h.len() == 0) {
            return true;
        }
        try {
            Head c = head(p, h.len());
            return c.len() == h.len() && c.crc() == h.crc();
        } catch (IOException e) {
            return false;
        }
    }

    static InputStream open(Path p) throws IOException {
        InputStream in = Files.newInputStream(p, StandardOpenOption.READ);
        return isGzip(p) ? new GZIPInputStream(in, 65536) : in;
    }
}
