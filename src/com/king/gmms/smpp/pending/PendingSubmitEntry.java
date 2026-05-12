package com.king.gmms.smpp.pending;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import com.king.message.gmms.GmmsMessage;

public class PendingSubmitEntry implements Delayed {
    private final String pendingKey;
    private final String sequence;
    private final long createTime;
    private final long expireAt;
    private volatile long nextCheckAt;
    private volatile GmmsMessage message;
    private volatile boolean resultPending;

    public PendingSubmitEntry(String pendingKey, String sequence, GmmsMessage message, long timeoutMs) {
        this.pendingKey = pendingKey;
        this.sequence = sequence;
        this.message = message;
        this.createTime = System.currentTimeMillis();
        this.expireAt = createTime + timeoutMs;
        this.nextCheckAt = this.expireAt;
        this.resultPending = false;
    }

    public String getPendingKey() {
        return pendingKey;
    }

    public String getSequence() {
        return sequence;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getExpireAt() {
        return expireAt;
    }

    public GmmsMessage getMessage() {
        return message;
    }

    public boolean isExpired(long now) {
        return expireAt <= now;
    }

    public long getDelay(TimeUnit unit) {
        return unit.convert(nextCheckAt - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public int compareTo(Delayed other) {
        if (other == this) {
            return 0;
        }
        long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
        return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
    }

    public boolean isResultPending() {
        return resultPending;
    }

    public void markResultPending(GmmsMessage message) {
        if (message != null) {
            this.message = message;
        }
        this.resultPending = true;
    }

    public void reschedule(long delayMs) {
        long safeDelayMs = delayMs > 0 ? delayMs : 1000L;
        this.nextCheckAt = System.currentTimeMillis() + safeDelayMs;
    }
}
