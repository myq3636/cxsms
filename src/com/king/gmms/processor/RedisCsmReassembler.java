package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisConnection;
import com.king.redis.SerializableHandler;

/**
 * CSM 长短信合并的 Redis Hash 持久化缓冲（取代旧的内存 4-buffer + CSMQ 双写）。
 *
 * <p>每条长短信对应一个 Redis Hash：key = {@code csm:{ossid}:{sender}:{recipient}:{sarRef}:{total}}，
 * field = 分段号，value = 用 {@link SerializableHandler} 序列化的整段消息（与 stream body 同一格式）。
 * 同组另有：
 * <ul>
 * <li>{@code csm:ids:{...}}：分段号 → msgId；</li>
 * <li>{@code csm:asm:{...}}：集齐后正在组装外发的占用 owner|startMs，另一个消费者在 CoreMtLeaseMs 内不重复外发；</li>
 * <li>{@code csm:done:{...}}：已完成（外发或过期 DR）的组，分段号 → msgId。已完成组里的分段被重投
 * （msgId 相同）只确认消费，不会再建一个残缺的组；客户复用参考号发的新消息（msgId 不同）照常合并。</li>
 * </ul>
 *
 * <p>采用 produce-before-DEL：集齐时只读取整组并在内存组装；由调用方在外发持久化成功后再调用
 * {@link #complete} 收尾。崩溃在 produce 之前 → hash 完整保留 → 重投重组重发。过期未集齐由
 * {@code csm:index} ZSET 过期索引 + {@link CsmExpirySweeper} 处理（回客户过期 DR）。
 *
 * <p>依赖 STATE Redis，且该 Redis 必须 {@code maxmemory-policy noeviction}（半条 hash 被淘汰 = 丢段）。
 */
public class RedisCsmReassembler {

	private static final SystemLogger log = SystemLogger.getSystemLogger(RedisCsmReassembler.class);
	private static final RedisCsmReassembler INSTANCE = new RedisCsmReassembler();

	static final String KEY_PREFIX = "csm:";
	/** 过期索引 ZSET：member=hashKey, score=到期时间戳(ms)。 */
	public static final String INDEX_ZSET = "csm:index";
	/** hash 的 EXPIRE 相对过期索引到期时间额外的宽限秒数，保证 sweeper 删 hash 前能读到残段。 */
	static final int EXPIRE_GRACE_SECONDS = 600;

	// KEYS: 1 hash, 2 index, 3 ids, 4 asm, 5 done
	// ARGV: 1 seq, 2 payload, 3 msgId, 4 total, 5 ttlSeconds, 6 expireAtMs, 7 owner, 8 nowMs, 9 leaseMs
	private static final String STORE_LUA =
			"if ARGV[3] ~= '' then "
			+ "  local d = redis.call('HGET', KEYS[5], ARGV[1]) "
			+ "  if d then "
			+ "    if d == ARGV[3] then return {'DONE'} end "
			+ "    redis.call('DEL', KEYS[5]) "
			+ "  end "
			+ "end "
			+ "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) "
			+ "redis.call('HSET', KEYS[3], ARGV[1], ARGV[3]) "
			+ "if redis.call('TTL', KEYS[1]) < 0 then "
			+ "  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[5])) "
			+ "  redis.call('ZADD', KEYS[2], tonumber(ARGV[6]), KEYS[1]) "
			+ "end "
			+ "if redis.call('TTL', KEYS[3]) < 0 then redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5])) end "
			+ "if redis.call('HLEN', KEYS[1]) < tonumber(ARGV[4]) then return {'WAIT'} end "
			+ "local asm = redis.call('GET', KEYS[4]) "
			+ "if asm then "
			+ "  local sep = string.find(asm, '|', 1, true) "
			+ "  if sep then "
			+ "    local owner = string.sub(asm, 1, sep - 1) "
			+ "    local at = tonumber(string.sub(asm, sep + 1)) "
			+ "    if owner ~= ARGV[7] and at and tonumber(ARGV[8]) - at < tonumber(ARGV[9]) then "
			+ "      return {'BUSY'} "
			+ "    end "
			+ "  end "
			+ "end "
			+ "redis.call('SET', KEYS[4], ARGV[7] .. '|' .. ARGV[8], 'EX', tonumber(ARGV[5])) "
			+ "local all = redis.call('HGETALL', KEYS[1]) "
			+ "table.insert(all, 1, 'FULL') "
			+ "return all";

	// KEYS: 1 hash, 2 index, 3 ids, 4 asm, 5 done; ARGV: 1 ttlSeconds
	private static final String FINISH_LUA =
			"redis.call('DEL', KEYS[1], KEYS[4]) "
			+ "redis.call('ZREM', KEYS[2], KEYS[1]) "
			+ "if redis.call('EXISTS', KEYS[3]) == 1 then "
			+ "  redis.call('RENAME', KEYS[3], KEYS[5]) "
			+ "  redis.call('EXPIRE', KEYS[5], tonumber(ARGV[1])) "
			+ "end "
			+ "return 1";

	// KEYS: 1 index; ARGV: 1 nowMs, 2 batch, 3 leaseUntilMs
	private static final String CLAIM_LUA =
			"local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2])) "
			+ "for i = 1, #due do redis.call('ZADD', KEYS[1], tonumber(ARGV[3]), due[i]) end "
			+ "return due";

	public static RedisCsmReassembler getInstance() {
		return INSTANCE;
	}

	/** CsmReassembleMode: redis (default) or legacy (in-memory cache + CSMQ). */
	public static boolean enabled() {
		return !"legacy".equalsIgnoreCase(
				GmmsUtility.getInstance().getCommonProperty("CsmReassembleMode", "redis").trim());
	}

	private RedisConnection redis() {
		return GmmsUtility.getInstance().getRedisClient().getStateRedis();
	}

	private int ttlSeconds() {
		int ms = GmmsUtility.getInstance().getCsmIntegrityCacheTimeout(); // 默认 3600000ms
		int s = ms / 1000;
		return s > 0 ? s : 3600;
	}

	public String buildKey(GmmsMessage seg) {
		return KEY_PREFIX + seg.getOSsID() + ":" + seg.getSenderAddress() + ":"
				+ seg.getRecipientAddress() + ":" + seg.getSarMsgRefNum() + ":" + seg.getSarTotalSeqments();
	}

	/** hash, index, ids, asm, done keys of a group */
	private static List<String> groupKeys(String hashKey) {
		String rest = hashKey.substring(KEY_PREFIX.length());
		return Arrays.asList(hashKey, INDEX_ZSET, KEY_PREFIX + "ids:" + rest,
				KEY_PREFIX + "asm:" + rest, KEY_PREFIX + "done:" + rest);
	}

	/**
	 * 存入一个 CSM 分段。
	 *
	 * @param owner 组装占用的持有者（模块:线程），同一持有者可以重入
	 * @return COMPLETE(含组装消息，组装失败时为 null) / INCOMPLETE(已持久) / DONE(所在组已完成) /
	 *         BUSY(另一个消费者正在组装外发) / FAILED(存储失败)
	 */
	public CsmStoreResult storeSegment(GmmsMessage seg, String owner) {
		String key = buildKey(seg);
		String payload;
		try {
			payload = SerializableHandler.convertGmmsMessage2RedisMessage(seg);
		} catch (Exception e) {
			log.error(seg, "CSM serialize segment failed. key=" + key, e);
			return CsmStoreResult.failed();
		}
		if (payload == null) {
			log.error(seg, "CSM serialize segment returned null. key=" + key);
			return CsmStoreResult.failed();
		}
		int ttl = ttlSeconds();
		// 过期索引到期 = now + ttl；hash 的 EXPIRE = ttl + grace，比索引晚，保证 sweeper 仍能读到残段。
		long now = System.currentTimeMillis();
		long indexScore = now + (long) ttl * 1000L;
		String msgId = seg.getMsgID() == null ? "" : seg.getMsgID();
		List<?> r;
		try {
			r = (List<?>) redis().evalScript(STORE_LUA, groupKeys(key), Arrays.asList(
					String.valueOf(seg.getSarSegmentSeqNum()), payload, msgId,
					String.valueOf(seg.getSarTotalSeqments()), String.valueOf(ttl + EXPIRE_GRACE_SECONDS),
					String.valueOf(indexScore), owner, String.valueOf(now),
					String.valueOf(longProperty("CoreMtLeaseMs", 60000L))));
		} catch (Exception e) {
			log.error(seg, "CSM store segment failed. key=" + key, e);
			return CsmStoreResult.failed();     // 存储失败 -> 不 ACK -> PEL 重试
		}
		String state = String.valueOf(r.get(0));
		if ("WAIT".equals(state)) {
			return CsmStoreResult.incomplete(); // 已持久 -> ACK
		}
		if ("DONE".equals(state)) {
			log.info(seg, "CSM segment redelivered, its group is already done. key={}", key);
			return CsmStoreResult.done();
		}
		if ("BUSY".equals(state)) {
			log.info(seg, "CSM group is being sent by another consumer, left pending. key={}", key);
			return CsmStoreResult.busy();
		}
		List<String> flat = new ArrayList<String>();
		for (int i = 1; i < r.size(); i++) {
			flat.add(r.get(i) == null ? null : String.valueOf(r.get(i)));
		}
		return CsmStoreResult.complete(assemble(flat, seg)); // 组装失败时为 null，调用方兜底
	}

	/**
	 * 收尾：外发持久化成功（或拒绝 / 过期 DR 已写出）后调用，删除合并 hash、过期索引项和组装占用，
	 * 并把分段 msgId 记为已完成。produce 失败则不调用，hash 保留供重投重组。
	 */
	public void complete(GmmsMessage seg) {
		try {
			finish(buildKey(seg));
		} catch (Exception e) {
			// the message is sent: the group stays complete in Redis and the sweeper finishes it without a DR
			log.warn(seg, "CSM finish failed, left to the expiry sweeper. key={}", buildKey(seg), e);
		}
	}

	void finish(String hashKey) {
		redis().evalScript(FINISH_LUA, groupKeys(hashKey),
				Arrays.asList(String.valueOf(ttlSeconds() + EXPIRE_GRACE_SECONDS)));
	}

	/**
	 * 领取最多 batch 个已到期的组：在索引中把它们的到期时间推后 leaseMs，别的扫描者在此期间看不到；
	 * 处理完调用 {@link #finish}，处理中崩溃则占用到期后被重新领取。
	 */
	List<String> claimExpired(long nowMs, int batch, long leaseMs) {
		List<?> r = (List<?>) redis().evalScript(CLAIM_LUA, Arrays.asList(INDEX_ZSET),
				Arrays.asList(String.valueOf(nowMs), String.valueOf(batch), String.valueOf(nowMs + leaseMs)));
		List<String> out = new ArrayList<String>();
		for (Object o : r) {
			if (o != null) {
				out.add(String.valueOf(o));
			}
		}
		return out;
	}

	/**
	 * 把 HGETALL 扁平列表 [seq1,payload1,seq2,payload2,...] 反序列化并组装成整条消息。
	 * 以 seq==1 分段为组装基准（确定性、重投稳定）；DR 标志按各段 OR 合并。
	 *
	 * @return 组装后的消息；任一步异常返回 null（调用方兜底）。
	 */
	private GmmsMessage assemble(List<String> flat, GmmsMessage anySeg) {
		try {
			CsmValueInfoMark mark = new CsmValueInfoMark();
			GmmsMessage base = null;
			for (int i = 0; i + 1 < flat.size(); i += 2) {
				String payload = flat.get(i + 1);
				if (payload == null) {
					continue;
				}
				GmmsMessage m = SerializableHandler.convertRedisMssage2GmmsMessage(payload);
				if (m == null) {
					continue;
				}
				mark.add(CsmUtility.getCsmValueInfoFromGmmsMessage(m));
				if (m.getDeliveryReport()) {
					mark.setDeliverReport(true);     // DR 标志 OR 合并
				}
				if (m.getSarSegmentSeqNum() == 1) {
					base = m;                        // seq==1 作为组装基准
				}
			}
			if (base == null) {
				base = anySeg;                       // 兜底（理论上 seq==1 必在集齐集合中）
			}
			return CsmUtility.assembleCsm(mark, base);
		} catch (Exception e) {
			log.error(anySeg, "CSM assemble failed", e);
			return null;
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
