package com.king.gmms.messagequeue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.resps.Tuple;

/**
 * Outbox of MTs routed to the Client module (stream:mt:routed), one entry per outbound
 * attempt: core:outbox:submit:{msgId}:{split}:{retriedNumber}. Entries live on the submit MQ
 * Redis next to the routed streams; they are written by the produce Lua in
 * {@link StreamQueueManager} together with the stream entry.
 *
 * <p>Exactly one result counts per attempt: {@link #claimResult} consumes the entry, a
 * second result for the same attempt (a late submit_resp after a timeout, or the timeout
 * after the real result) finds it gone and is dropped.
 *
 * <p>The scanner produces a timeout result when the Client never answers:
 * <ul>
 * <li>still unread in the routed stream and the Client group stopped reading
 * (CoreOutboundUnreadStallMs), or the hard timeout passed: the entry is deleted from the
 * stream first, so it can no longer be sent, and the result is COMMUNICATION_ERROR;</li>
 * <li>read and acked by the Client (sent to the vendor) but no result {@link #respWaitMs} after the scanner first saw
 * it sent (the time in the routed stream does not count): SUBMIT_RESP_ERROR;</li>
 * <li>read but not acked (the Client is still on it): SUBMIT_RESP_ERROR at the hard timeout.</li>
 * </ul>
 * The retry policy then decides on a resend, as for any failed submit.
 */
public class CoreOutboundOutboxManager {
    private static final SystemLogger log = SystemLogger.getSystemLogger(CoreOutboundOutboxManager.class);
    private static final CoreOutboundOutboxManager instance = new CoreOutboundOutboxManager();

    private static final String OUTBOX_PREFIX = "core:outbox:submit:";
    private static final String OUTBOX_META_SUFFIX = ":meta";
    private static final String OUTBOX_DONE_SUFFIX = ":done";
    private static final String OUTBOX_TIMEOUT_ZSET = "zset:core:outbox:submit:timeout";
    private static final String CLIENT_OUTBOUND_GROUP = "ClientOutboundSubmitGroup";
    private static final long DEFAULT_RECHECK_MS = TimeUnit.MINUTES.toMillis(1);
    private static final long DEFAULT_UNREAD_STALL_MS = TimeUnit.MINUTES.toMillis(2);
    private static final long DEFAULT_RESP_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(3);
    private static final long DEFAULT_HARD_TIMEOUT_MS = TimeUnit.HOURS.toMillis(3);
    /** after the channel's own response timeout, for the Client to write its timeout result */
    private static final long RESP_MARGIN_MS = TimeUnit.MINUTES.toMillis(1);
    private static final int DEFAULT_PAYLOAD_TTL_SECONDS = (int) TimeUnit.HOURS.toSeconds(4);
    private static final int DEFAULT_DONE_TTL_SECONDS = (int) TimeUnit.MINUTES.toSeconds(30);

    public enum Claim {
        /** first result of the attempt (or the retry of the same result entry): process it */
        FIRST,
        /** the attempt already has a result: drop this one */
        STALE,
        /** no outbox entry was ever written for it (HTTP, outbox disabled, ...): process it */
        UNTRACKED
    }

    // KEYS[1]=outbox KEYS[2]=zset KEYS[3]=meta KEYS[4]=done ARGV[1]=resultId ARGV[2]=ttl
    private static final String CLAIM_LUA =
            "if redis.call('EXISTS', KEYS[1]) == 1 then "
            + "  redis.call('DEL', KEYS[1], KEYS[3]) "
            + "  redis.call('ZREM', KEYS[2], KEYS[1]) "
            + "  redis.call('SETEX', KEYS[4], ARGV[2], ARGV[1]) "
            + "  return 'FIRST' "
            + "end "
            + "local d = redis.call('GET', KEYS[4]) "
            + "if d then "
            + "  if d == ARGV[1] then return 'FIRST' end "
            + "  return 'STALE' "
            + "end "
            + "return 'UNTRACKED'";

    // KEYS[1]=meta ARGV[1]=now: the time the scanner first saw the attempt sent, appended to the meta (APPEND keeps its
    // TTL); 0 when there is no meta
    private static final String SENT_SEEN_LUA =
            "if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end "
            + "redis.call('APPEND', KEYS[1], '\\n' .. ARGV[1]) "
            + "return 1";

    // KEYS[1]=stream ARGV[1]=group ARGV[2]=id: 1 deleted unread entry, 0 gone, -1 being read.
    // No group yet (the Client never started) means nobody read it.
    private static final String PULL_BACK_LUA =
            "local p = redis.pcall('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1) "
            + "if type(p) == 'table' and not p.err and #p > 0 then return -1 end "
            + "return redis.call('XDEL', KEYS[1], ARGV[2])";

    private final AtomicBoolean scannerStarted = new AtomicBoolean(false);
    private final ScheduledExecutorService scanner = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(A2PThreadGroup.getInstance(), runnable, "CoreOutboundOutboxScanner");
            thread.setDaemon(true);
            return thread;
        }
    });

    private CoreOutboundOutboxManager() {
        startScanner();
    }

    public static CoreOutboundOutboxManager getInstance() {
        return instance;
    }

    /**
     * Outbox part of the produce Lua for msg, or null when the message gets no outbox.
     * The produce Lua sets the entry, its timeout zset member and the meta (stream, id, created).
     */
    public OutboxWrite outboxWrite(GmmsMessage msg, String streamKey) {
        if (!enabled() || !needOutbox(msg, streamKey)) {
            return null;
        }
        String key = outboxKey(msg);
        if (key == null) {
            return null;
        }
        long firstCheck = System.currentTimeMillis() + Math.min(unreadStallMs(), respTimeoutMs());
        return new OutboxWrite(key, OUTBOX_TIMEOUT_ZSET, metaKey(key), payloadTtlSeconds(), firstCheck);
    }

    /**
     * Decide whether a submit result counts. Results without an outbox entry are processed as
     * before. Redis errors are thrown so the result stays pending and is retried.
     */
    public Claim claimResult(GmmsMessage msg) {
        if (!enabled()) {
            return Claim.UNTRACKED;
        }
        String key = outboxKey(msg);
        if (key == null) {
            return Claim.UNTRACKED;
        }
        String resultId = msg.getRedisStreamID() == null ? "-" : msg.getRedisStreamID();
        List<String> keys = Arrays.asList(key, OUTBOX_TIMEOUT_ZSET, metaKey(key), key + OUTBOX_DONE_SUFFIX);
        List<String> args = Arrays.asList(resultId, String.valueOf(doneTtlSeconds()));
        // the outbox is on the master of the MT's outbound stream (written with it); with several submit MQ masters the
        // others are tried when it is not there (e.g. the address changed after the produce)
        RedisConnection first = redisFor(msg);
        Claim claim = Claim.valueOf(String.valueOf(first.evalScript(CLAIM_LUA, keys, args)));
        if (claim == Claim.UNTRACKED) {
            for (RedisConnection other : RedisClient.getInstance().mqs(true)) {
                if (other != first) {
                    claim = Claim.valueOf(String.valueOf(other.evalScript(CLAIM_LUA, keys, args)));
                    if (claim != Claim.UNTRACKED) {
                        break;
                    }
                }
            }
        }
        return claim;
    }

    /** The submit MQ master of the MT's outbound stream */
    private static RedisConnection redisFor(GmmsMessage msg) {
        RedisClient rc = RedisClient.getInstance();
        if (rc.submitMqCount() <= 1) {
            return rc.getSubmitMqRedis();
        }
        try {
            return rc.submitMq(StreamQueueManager.getInstance().outboundTargetKey(msg));
        } catch (Exception e) {
            return rc.getSubmitMqRedis();
        }
    }

    private void startScanner() {
        if (!scannerStarted.compareAndSet(false, true)) {
            return;
        }
        long intervalMs = longProperty("CoreOutboundOutboxScanIntervalMs", 1000L);
        scanner.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    scanTimeouts();
                } catch (Exception e) {
                    log.warn("Core outbound outbox scan failed", e);
                }
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void scanTimeouts() {
        if (!enabled() || RedisClient.getInstance().getSubmitMqRedis() == null) {
            return;
        }
        for (RedisConnection redis : RedisClient.getInstance().mqs(true)) {
            scanTimeouts(redis);
        }
    }

    /** The timed-out entries of one submit MQ master (their meta and stream are on it too) */
    private void scanTimeouts(RedisConnection redis) {
        int batchSize = intProperty("CoreOutboundOutboxScanBatchSize", 100);
        for (int i = 0; i < batchSize; i++) {
            long now = System.currentTimeMillis();
            List<Tuple> tuples = redis.zpopmin(OUTBOX_TIMEOUT_ZSET);
            if (tuples == null || tuples.isEmpty()) {
                return;
            }
            Tuple tuple = tuples.get(0);
            String key = tuple.getElement();
            double score = tuple.getScore();
            if (score > now) {
                redis.zadd(OUTBOX_TIMEOUT_ZSET, score, key);
                return;
            }
            String value = redis.getString(key);
            if (value == null) {
                // already claimed by a result
                redis.delObject(metaKey(key));
                continue;
            }
            try {
                checkTimeout(redis, key, value, now);
            } catch (Exception e) {
                redis.zadd(OUTBOX_TIMEOUT_ZSET, now + recheckMs(), key);
                log.warn("Core outbound outbox timeout check failed, rescheduled. key=" + key, e);
            }
        }
    }

    private void checkTimeout(RedisConnection redis, String key, String value, long now) {
        GmmsMessage msg = SerializableHandler.convertRedisMssage2GmmsMessage(value);
        OutboxMeta meta = loadMeta(redis, key, now);
        long age = Math.max(0L, now - meta.createdAt);
        boolean hard = age >= hardTimeoutMs();
        String state;
        if (meta.streamKey == null || meta.redisId == null) {
            state = "waiting_result";
        } else if (redis.streamPendingEntryExists(meta.streamKey, CLIENT_OUTBOUND_GROUP, meta.redisId)) {
            state = "in_pel";
        } else if (redis.streamEntryExists(meta.streamKey, meta.redisId)) {
            state = "in_stream";
        } else {
            state = "waiting_result";
        }

        if ("in_stream".equals(state)) {
            long idle = redis.minConsumerIdleMs(meta.streamKey, CLIENT_OUTBOUND_GROUP);
            boolean stalled = idle < 0 || idle >= unreadStallMs();
            if (hard || (stalled && age >= unreadStallMs())) {
                Object pulled = redis.evalScript(PULL_BACK_LUA, Arrays.asList(meta.streamKey),
                        Arrays.asList(CLIENT_OUTBOUND_GROUP, meta.redisId));
                long r = pulled instanceof Long ? (Long) pulled : 0L;
                if (r == 1L) {
                    timeout(redis, key, msg, GmmsStatus.COMMUNICATION_ERROR,
                            "unread, pulled back from " + meta.streamKey + (hard ? " (hard timeout)" : " (client stalled, idle=" + idle + "ms)"));
                    return;
                }
                // the Client took it meanwhile: check again later
            }
            defer(redis, key, msg, now + recheckMs(), state, age);
        } else if ("in_pel".equals(state)) {
            if (hard) {
                timeout(redis, key, msg, GmmsStatus.SUBMIT_RESP_ERROR, "read by the client but not acked");
            } else {
                defer(redis, key, msg, now + recheckMs(), state, age);
            }
        } else {
            // sent: the response wait counts from when the scanner first saw it sent, not from the produce (a long time in
            // the routed stream is no reason to fail it: docs/DR早到重试与outbox应答超时改进设计.md 1)
            long wait = respWaitMs(msg);
            if (hard) {
                timeout(redis, key, msg, GmmsStatus.SUBMIT_RESP_ERROR, "sent by the client, no result (hard timeout, ageMs="
                        + age + ")");
            } else if (meta.sentSeenAt > 0) {
                long since = now - meta.sentSeenAt;
                if (since >= wait) {
                    timeout(redis, key, msg, GmmsStatus.SUBMIT_RESP_ERROR, "sent by the client, no result (ageMs=" + age
                            + ", sentSeenAgoMs=" + since + ", waitMs=" + wait + ")");
                } else {
                    defer(redis, key, msg, meta.sentSeenAt + wait, state, age);
                }
            } else if (markSentSeen(redis, key, now)) {
                defer(redis, key, msg, now + wait, "sent_seen", age);
            } else if (age >= wait) {
                // no meta: from the produce, as before
                timeout(redis, key, msg, GmmsStatus.SUBMIT_RESP_ERROR, "sent by the client, no result (no meta, ageMs="
                        + age + ", waitMs=" + wait + ")");
            } else {
                defer(redis, key, msg, meta.createdAt + wait, state, age);
            }
        }
    }

    private boolean markSentSeen(RedisConnection redis, String key, long now) {
        Object r = redis.evalScript(SENT_SEEN_LUA, Arrays.asList(metaKey(key)), Arrays.asList(String.valueOf(now)));
        return r instanceof Long && (Long) r == 1L;
    }

    /**
     * How long a sent attempt waits for its result: CoreOutboundRespTimeoutMs, or longer when the channel's Client waits
     * longer for the response ({@link A2PCustomerInfo#getSubmitRespTimeoutMs} and a minute for its timeout result), so
     * the Client's own timeout comes first.
     */
    public static long respWaitMs(GmmsMessage msg) {
        long wait = respTimeoutMs();
        try {
            A2PCustomerInfo channel = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(msg.getRSsID());
            if (channel != null) {
                wait = Math.max(wait, channel.getSubmitRespTimeoutMs() + RESP_MARGIN_MS);
            }
        } catch (Exception ignored) {
            // no channel configuration: CoreOutboundRespTimeoutMs
        }
        return wait;
    }

    /** Produce the timeout result; the entry stays and is claimed by whichever result comes first. */
    private void timeout(RedisConnection redis, String key, GmmsMessage msg, GmmsStatus status, String reason) {
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
                || GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
            msg.setStatus(status);
            msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
        }
        if (StreamQueueManager.getInstance().produceResult(msg)) {
            log.warn(msg, "Core outbound outbox timeout result produced. key={}, status={}, reason={}",
                    key, status.getCode(), reason);
        } else {
            redis.zadd(OUTBOX_TIMEOUT_ZSET, System.currentTimeMillis()
                    + longProperty("CoreOutboundOutboxResultRetryMs", 5000L), key);
            log.warn(msg, "Core outbound outbox timeout result produce failed, requeued. key={}", key);
        }
    }

    private void defer(RedisConnection redis, String key, GmmsMessage msg, long nextCheckAt, String state, long age) {
        redis.zadd(OUTBOX_TIMEOUT_ZSET, nextCheckAt, key);
        if (log.isDebugEnabled()) {
            log.debug(msg, "Core outbound outbox deferred. key={}, state={}, ageMs={}, nextCheckInMs={}",
                    key, state, age, nextCheckAt - System.currentTimeMillis());
        }
    }

    private String metaKey(String key) {
        return key + OUTBOX_META_SUFFIX;
    }

    private OutboxMeta loadMeta(RedisConnection redis, String key, long now) {
        String value = redis.getString(metaKey(key));
        if (value == null || value.length() == 0) {
            return new OutboxMeta(null, null, now, 0L);
        }
        String[] parts = value.split("\n", -1);
        String streamKey = parts.length > 0 ? emptyToNull(parts[0]) : null;
        String redisId = parts.length > 1 ? emptyToNull(parts[1]) : null;
        long createdAt = now;
        if (parts.length > 2) {
            try {
                createdAt = Long.parseLong(parts[2]);
            } catch (Exception ignored) {
                createdAt = now;
            }
        }
        long sentSeenAt = 0L;
        if (parts.length > 3) {
            try {
                sentSeenAt = Long.parseLong(parts[3]);
            } catch (Exception ignored) {
                sentSeenAt = 0L;
            }
        }
        return new OutboxMeta(streamKey, redisId, createdAt, sentSeenAt);
    }

    private String emptyToNull(String value) {
        return value == null || value.length() == 0 ? null : value;
    }

    private static final class OutboxMeta {
        private final String streamKey;
        private final String redisId;
        private final long createdAt;
        /** when the scanner first saw it sent, 0 before */
        private final long sentSeenAt;

        private OutboxMeta(String streamKey, String redisId, long createdAt, long sentSeenAt) {
            this.streamKey = streamKey;
            this.redisId = redisId;
            this.createdAt = createdAt;
            this.sentSeenAt = sentSeenAt;
        }
    }

    /** Keys and arguments the produce Lua needs to write the outbox entry. */
    public static final class OutboxWrite {
        public final String key;
        public final String zset;
        public final String metaKey;
        public final int ttlSeconds;
        public final long firstCheckAt;

        OutboxWrite(String key, String zset, String metaKey, int ttlSeconds, long firstCheckAt) {
            this.key = key;
            this.zset = zset;
            this.metaKey = metaKey;
            this.ttlSeconds = ttlSeconds;
            this.firstCheckAt = firstCheckAt;
        }
    }

    private boolean needOutbox(GmmsMessage msg, String streamKey) {
        if (msg == null || streamKey == null || !streamKey.startsWith(StreamQueueManager.STR_MT_ROUTED_PREFIX)) {
            return false;
        }
        return GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
                || GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType());
    }

    /** One entry per outbound attempt: split parts and retries share the msgId. */
    private String outboxKey(GmmsMessage msg) {
        if (msg == null) {
            return null;
        }
        String id = msg.getMsgID();
        if (id == null || id.length() == 0) {
            id = msg.getInMsgID();
        }
        if (id == null || id.length() == 0) {
            return null;
        }
        return OUTBOX_PREFIX + id + ":" + msg.getSplitStatus() + ":" + msg.getRetriedNumber();
    }


    private boolean enabled() {
        return Boolean.parseBoolean(GmmsUtility.getInstance().getCommonProperty("CoreOutboundOutboxEnable", "true"));
    }

    private static int intProperty(String key, int defaultValue) {
        try {
            return Integer.parseInt(GmmsUtility.getInstance().getCommonProperty(key,
                    String.valueOf(defaultValue)).trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static long longProperty(String key, long defaultValue) {
        try {
            return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key,
                    String.valueOf(defaultValue)).trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }

    private static long positiveLongProperty(String key, long defaultValue, long minValue) {
        long value = longProperty(key, defaultValue);
        return value < minValue ? defaultValue : value;
    }

    private static long recheckMs() {
        return positiveLongProperty("CoreOutboundOutboxRecheckMs", DEFAULT_RECHECK_MS, 1000L);
    }

    private static long unreadStallMs() {
        return positiveLongProperty("CoreOutboundUnreadStallMs", DEFAULT_UNREAD_STALL_MS, 1000L);
    }

    private static long respTimeoutMs() {
        return positiveLongProperty("CoreOutboundRespTimeoutMs", DEFAULT_RESP_TIMEOUT_MS, 1000L);
    }

    private static long hardTimeoutMs() {
        return Math.max(positiveLongProperty("CoreOutboundOutboxHardTimeoutMs", DEFAULT_HARD_TIMEOUT_MS, 1000L),
                respTimeoutMs());
    }

    /**
     * How long the :done marker of a claimed attempt lives: it only has to outlast a second result of the same attempt
     * (a submit_resp after the timeout result, the timeout after the real one), which comes within minutes; before it
     * had the payload's TTL (4 hours, for the hard timeout) and at high TPS filled the submit Redis
     * (docs/Redis CPU瓶颈分析.md 9).
     */
    private static int doneTtlSeconds() {
        long ttlSeconds = positiveLongProperty("CoreOutboundOutboxDoneTTLSeconds", DEFAULT_DONE_TTL_SECONDS, 60L);
        return ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds;
    }

    private static int payloadTtlSeconds() {
        long configured = positiveLongProperty("CoreOutboundOutboxPayloadTTLSeconds",
                DEFAULT_PAYLOAD_TTL_SECONDS, 60L);
        long minSeconds = TimeUnit.MILLISECONDS.toSeconds(hardTimeoutMs() + recheckMs()) + 60L;
        long ttlSeconds = Math.max(configured, minSeconds);
        return ttlSeconds > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ttlSeconds;
    }
}
