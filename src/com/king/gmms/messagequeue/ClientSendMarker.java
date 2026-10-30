package com.king.gmms.messagequeue;

import java.util.Arrays;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;
import com.king.redis.RedisConnection;

/**
 * Send-once marker of an outbound MT on the Client: client:sent:{msgId}:{split}:{retriedNumber},
 * one per send attempt of Core (same key parts as the Core outbox), on the submit MQ next to the
 * routed stream.
 *
 * <p>A routed entry can be handed to the Client twice (ack failed, Client stopped between send and
 * ack, claimed by auto-claim while still queued for sending). Before sending, {@link #claim}:
 * <ul>
 * <li>no marker: write "sending" (S|owner|startMs) and send;</li>
 * <li>sent (D): an earlier send succeeded and only its ack failed, just ack;</li>
 * <li>sending by another consumer within ClientSendLeaseMs: it is sending now, leave the entry pending;</li>
 * <li>sending, lease over: its owner stopped while sending and it is unknown whether the vendor got
 * it. Not sent again: just ack, the Core outbox reports it (no result within CoreOutboundRespTimeoutMs).</li>
 * </ul>
 * After a send, {@link #ackSent} marks it sent, acks and deletes the marker in one Lua; if not sent
 * (window full, no permit, rejected) {@link #release} or {@link #ackFailed} remove it.
 * ClientSendIdempotency=false sends without a marker.
 */
public final class ClientSendMarker {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ClientSendMarker.class);
	private static final String PREFIX = "client:sent:";
	private static final int TTL_SECONDS = 24 * 3600;
	/** distinguishes this JVM from a stopped one whose thread ids may repeat */
	private static final String JVM_ID = Long.toHexString(new java.security.SecureRandom().nextLong());

	public enum Claim {
		/** this consumer sends it */
		SEND,
		/** sent before (or unknown after a stopped sender): do not send, ack */
		ACK_ONLY,
		/** another consumer is sending it: do not send, leave pending */
		BUSY
	}

	// KEYS[1]=marker ARGV: 1 owner, 2 nowMs, 3 leaseMs, 4 ttl
	private static final String CLAIM_LUA =
			"local v = redis.call('GET', KEYS[1]) "
			+ "if not v then "
			+ "  redis.call('SET', KEYS[1], 'S|' .. ARGV[1] .. '|' .. ARGV[2], 'EX', tonumber(ARGV[4])) "
			+ "  return 'SEND' "
			+ "end "
			+ "if v == 'D' then return 'SENT' end "
			+ "local a = string.find(v, '|', 3, true) "
			+ "local owner = a and string.sub(v, 3, a - 1) or '' "
			+ "local at = a and tonumber(string.sub(v, a + 1)) or 0 "
			+ "if owner == ARGV[1] then "
			+ "  redis.call('SET', KEYS[1], 'S|' .. ARGV[1] .. '|' .. ARGV[2], 'EX', tonumber(ARGV[4])) "
			+ "  return 'SEND' "
			+ "end "
			+ "if at and tonumber(ARGV[2]) - at < tonumber(ARGV[3]) then return 'BUSY' end "
			+ "return 'STALE'";

	// KEYS[1]=stream KEYS[2]=marker ARGV: 1 group, 2 id, 3 markSent(1/0)
	private static final String ACK_LUA =
			"if ARGV[3] == '1' and redis.call('EXISTS', KEYS[2]) == 1 then "
			+ "  redis.call('SET', KEYS[2], 'D', 'KEEPTTL') "
			+ "end "
			+ "local acked = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2]) "
			+ "if acked > 0 then "
			+ "  redis.call('XDEL', KEYS[1], ARGV[2]) "
			+ "  redis.call('DEL', KEYS[2]) "
			+ "elseif ARGV[3] ~= '1' then "
			+ "  redis.call('DEL', KEYS[2]) "
			+ "end "
			+ "return acked";

	private ClientSendMarker() {
	}

	/** ClientSendIdempotency (default true) */
	public static boolean enabled() {
		return !"false".equalsIgnoreCase(GmmsUtility.getInstance().getCommonProperty("ClientSendIdempotency", "true"));
	}

	/** Owner of a claim: consumer, JVM and thread. */
	public static String owner(String consumerName) {
		return consumerName + ":" + JVM_ID + ":" + Thread.currentThread().getId();
	}

	public static String key(GmmsMessage msg) {
		String id = msg.getMsgID();
		if (id == null || id.length() == 0) {
			id = msg.getRedisStreamID();
		}
		return PREFIX + id + ":" + msg.getSplitStatus() + ":" + msg.getRetriedNumber();
	}

	private static RedisConnection redis() {
		return RedisClient.getInstance().getSubmitMqRedis();
	}

	/** Claim the send of msg for owner. Redis errors are thrown: the caller must not send. */
	public static Claim claim(GmmsMessage msg, String owner) {
		String key = key(msg);
		Object r = redis().evalScript(CLAIM_LUA, Arrays.asList(key), Arrays.asList(owner,
				String.valueOf(System.currentTimeMillis()), String.valueOf(longProperty("ClientSendLeaseMs", 60000L)),
				String.valueOf(TTL_SECONDS)));
		String state = String.valueOf(r);
		if ("SEND".equals(state)) {
			return Claim.SEND;
		}
		if ("BUSY".equals(state)) {
			log.info(msg, "Outbound MT is being sent by another consumer, left pending. marker={}", key);
			return Claim.BUSY;
		}
		if ("SENT".equals(state)) {
			log.info(msg, "Outbound MT redelivered after it was sent, acked without sending. marker={}", key);
		} else {
			log.warn(msg, "Outbound MT redelivered after its sender stopped while sending, not sent again "
					+ "(the Core outbox reports it). marker={}", key);
		}
		return Claim.ACK_ONLY;
	}

	/** Sent: mark sent, ack and delete the marker (kept as sent if the ack does not go through). */
	public static boolean ackSent(String streamKey, String group, String redisId, GmmsMessage msg) {
		return ack(streamKey, group, redisId, msg, true);
	}

	/** Not sent and done with (failure result written, or ACK_ONLY): ack and delete the marker. */
	public static boolean ackFailed(String streamKey, String group, String redisId, GmmsMessage msg) {
		return ack(streamKey, group, redisId, msg, false);
	}

	private static boolean ack(String streamKey, String group, String redisId, GmmsMessage msg, boolean sent) {
		Object r = redis().evalScript(ACK_LUA, Arrays.asList(streamKey, key(msg)),
				Arrays.asList(group, redisId, sent ? "1" : "0"));
		boolean acked = r instanceof Long && (Long) r > 0;
		StreamQueueManager.getInstance().traceAck(streamKey, group, redisId, msg, true, acked);
		if (!acked) {
			log.warn(msg, "Outbound stream ack returned 0 (ack_returned_zero). streamKey={}, redisId={}", streamKey, redisId);
		}
		return acked;
	}

	/** Not sent, entry stays pending for a retry: remove the marker so the retry can send. */
	public static void release(GmmsMessage msg) {
		try {
			redis().evalScript("return redis.call('DEL', KEYS[1])", Arrays.asList(key(msg)),
					java.util.Collections.<String>emptyList());
		} catch (Exception e) {
			// the retry by this consumer reclaims its own marker; another consumer waits for the lease
			log.warn(msg, "Outbound send marker not released. marker={}", key(msg), e);
		}
	}

	private static long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(GmmsUtility.getInstance().getCommonProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			return defaultValue;
		}
	}
}
