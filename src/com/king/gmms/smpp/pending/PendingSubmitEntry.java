package com.king.gmms.smpp.pending;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.king.message.gmms.GmmsMessage;

public class PendingSubmitEntry implements Delayed {
    private final String pendingKey;
    private final String sequence;
    private final long createTime;
    private final long expireAt;
    private volatile long nextCheckAt;
    private volatile GmmsMessage message;
    private volatile boolean resultPending;
    // M-13: 本 entry 是否持有 window 许可(put/tryPut 真正 acquire 的为 true；redis 复活的为 false)，释放时据此避免过度释放。
    private volatile boolean permitHeld = true;
    // H-16: 产结果认领——响应路径与超时 worker 竞争同一 entry，仅认领成功者产结果，避免重复产；产失败可释放认领供重试。
    private final AtomicBoolean producing = new AtomicBoolean(false);
    // M-14: 产结果失败重试计数——超过上限即放弃并释放 window 许可，避免无限重试钉死 entry 导致 window 枯竭。
    private volatile int resultRetryCount = 0;

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

    public long getNextCheckAt() {
        return nextCheckAt;
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
        // M-16: 比较绝对到期时刻而非两次活动 getDelay(读时钟两次会导致排序不稳定，破坏 DelayQueue 堆)。
        if (other instanceof PendingSubmitEntry) {
            return Long.compare(this.nextCheckAt, ((PendingSubmitEntry) other).nextCheckAt);
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

    public boolean isPermitHeld() {
        return permitHeld;
    }

    public void setPermitHeld(boolean permitHeld) {
        this.permitHeld = permitHeld;
    }

    /** H-16: 尝试认领"产结果"权(CAS false→true)；成功者独占产结果。 */
    public boolean tryClaimProduce() {
        return producing.compareAndSet(false, true);
    }

    /** H-16: 产结果失败时释放认领，允许后续重试再次认领。 */
    public void releaseProduceClaim() {
        producing.set(false);
    }

    /** M-14: 产结果失败重试 +1 并返回新值。 */
    public int incrementAndGetResultRetry() {
        return ++resultRetryCount;
    }

    public int getResultRetryCount() {
        return resultRetryCount;
    }
}
