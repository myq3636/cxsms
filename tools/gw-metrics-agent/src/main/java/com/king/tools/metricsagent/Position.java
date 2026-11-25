package com.king.tools.metricsagent;

/**
 * Where a file was read to (state/positions.json, Decisions 6): the file read then (path, inode, head of its content)
 * and the byte offset in its content (after decompression for .gz). lastTs: the ts of the last window read before it.
 */
record Position(String path, long inode, int headLen, long headCrc, long offset, long lastTs) {

    FileIds.Head head() {
        return new FileIds.Head(headLen, headCrc);
    }

    Position at(long newOffset, long newLastTs) {
        return new Position(path, inode, headLen, headCrc, newOffset, newLastTs);
    }
}
