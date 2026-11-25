package com.king.tools.metricsagent;

import java.util.ArrayDeque;

/**
 * The safe position of one file (Decisions 6): the start of the earliest line whose window or alert Portal has not
 * taken yet, or, with none pending, the end of the last whole line read. Lines that are not sent (other log lines,
 * kv lines) need no entry. Units are taken in the order the lines were read; Portal dedupes what is read again.
 */
final class Ledger {
    static final class Unit {
        final Position start;
        volatile boolean done;

        Unit(Position start) {
            this.start = start;
        }
    }

    private final ArrayDeque<Unit> pending = new ArrayDeque<>();
    private Position lastRead;

    Ledger(Position initial) {
        this.lastRead = initial;
    }

    synchronized Unit hold(Position start) {
        Unit u = new Unit(start);
        pending.addLast(u);
        return u;
    }

    synchronized void read(Position end) {
        lastRead = end;
    }

    /** null while nothing is known (no file yet) */
    synchronized Position committed() {
        while (!pending.isEmpty() && pending.peekFirst().done) {
            pending.pollFirst();
        }
        return pending.isEmpty() ? lastRead : pending.peekFirst().start;
    }

    synchronized int pendingCount() {
        committed();
        return pending.size();
    }
}
