package com.king.gmms.recipientlimit;

/**
 * Applies the effective recipient limit to a shared Redis-style counter.
 */
public final class RecipientSendCountLimiter {
    public static final int WINDOW_SECONDS = 24 * 60 * 60;
    public static final String KEY_PREFIX = "Duplicate:";

    private RecipientSendCountLimiter() {
    }

    public interface Counter {
        Long incrementAndExpire(String key, int expireSeconds);
    }

    public enum Outcome {
        DISABLED,
        ALLOWED,
        REJECTED,
        STORAGE_ERROR
    }

    public static Decision check(int effectiveLimit, String recipientAddress, Counter counter) {
        if (effectiveLimit <= 0) {
            return evaluate(effectiveLimit, recipientAddress, Long.valueOf(0L));
        }
        if (counter == null) {
            return evaluate(effectiveLimit, recipientAddress, null);
        }

        String key = buildKey(recipientAddress);
        Long currentCount = counter.incrementAndExpire(key, WINDOW_SECONDS);
        return evaluate(effectiveLimit, recipientAddress, currentCount);
    }

    public static Decision evaluate(int effectiveLimit, String recipientAddress, Long currentCount) {
        String key = buildKey(recipientAddress);
        if (effectiveLimit <= 0) {
            return new Decision(Outcome.DISABLED, effectiveLimit, 0L, key);
        }
        if (currentCount == null) {
            return new Decision(Outcome.STORAGE_ERROR, effectiveLimit, 0L, key);
        }
        Outcome outcome = currentCount.longValue() > effectiveLimit
                ? Outcome.REJECTED : Outcome.ALLOWED;
        return new Decision(outcome, effectiveLimit, currentCount.longValue(), key);
    }

    public static String buildKey(String recipientAddress) {
        return KEY_PREFIX + recipientAddress;
    }

    public static boolean isRejected(int effectiveLimit, Long currentCount) {
        return effectiveLimit > 0 && currentCount != null
                && currentCount.longValue() > effectiveLimit;
    }

    public static final class Decision {
        private final Outcome outcome;
        private final int effectiveLimit;
        private final long currentCount;
        private final String key;

        private Decision(Outcome outcome, int effectiveLimit, long currentCount, String key) {
            this.outcome = outcome;
            this.effectiveLimit = effectiveLimit;
            this.currentCount = currentCount;
            this.key = key;
        }

        public Outcome getOutcome() {
            return outcome;
        }

        public int getEffectiveLimit() {
            return effectiveLimit;
        }

        public long getCurrentCount() {
            return currentCount;
        }

        public String getKey() {
            return key;
        }

        public boolean isRejected() {
            return outcome == Outcome.REJECTED;
        }
    }
}
