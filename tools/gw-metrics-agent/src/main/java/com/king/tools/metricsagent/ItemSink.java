package com.king.tools.metricsagent;

/** where assembled windows and alerts go: the Outbox, or a list for --dry-run / --replay */
interface ItemSink {
    void add(Outbox.Item item);

    boolean full();
}
