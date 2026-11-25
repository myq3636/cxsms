package com.king.tools.metricsagent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The windows and alerts read and not yet taken by Portal, in reading order. Full (agent.outbox.*): the reader stops
 * reading and its positions stay, the log files being the buffer (Decisions 6). One sender takes from the head.
 */
final class Outbox implements ItemSink {
    private final int maxWindows;
    private final long maxBytes;
    private final ArrayDeque<Item> items = new ArrayDeque<>();
    private int windows;
    private long bytes;

    Outbox(int maxWindows, long maxBytes) {
        this.maxWindows = maxWindows;
        this.maxBytes = maxBytes;
    }

    @Override
    public synchronized void add(Item item) {
        items.addLast(item);
        if (!item.alert()) {
            windows++;
        }
        bytes += item.bytes();
    }

    @Override
    public synchronized boolean full() {
        return windows >= maxWindows || bytes >= maxBytes;
    }

    /** the first items up to the limits (at least one), not removed */
    synchronized List<Item> peek(int limitWindows, long limitBytes) {
        List<Item> out = new ArrayList<>();
        int w = 0;
        long b = 0;
        for (Item it : items) {
            if (!out.isEmpty() && (b + it.bytes() > limitBytes || (!it.alert() && w >= limitWindows))) {
                break;
            }
            out.add(it);
            b += it.bytes();
            if (!it.alert()) {
                w++;
            }
        }
        return out;
    }

    /** Portal took these (the head of the queue): removed, their lines done for the positions */
    synchronized void ack(List<Item> sent) {
        Iterator<Item> it = items.iterator();
        for (Item s : sent) {
            if (!it.hasNext()) {
                break;
            }
            Item head = it.next();
            if (head != s) {
                throw new IllegalStateException("ack out of order");
            }
            it.remove();
            if (!head.alert()) {
                windows--;
            }
            bytes -= head.bytes();
            for (Ledger.Unit u : head.units()) {
                u.done = true;
            }
        }
    }

    synchronized int windows() {
        return windows;
    }

    synchronized int size() {
        return items.size();
    }

    /** an assembled window or an alert, as sent, with the lines it was read from */
    record Item(ObjectNode json, boolean alert, int bytes, List<Ledger.Unit> units) {
    }
}
