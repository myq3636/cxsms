package com.king.gmms.processor;

import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;

/**
 * CSM 长短信合并的 Redis Hash 持久化缓冲（取代旧的内存 4-buffer + CSMQ 双写）。
 *
 * <p>每条长短信对应一个 Redis Hash：key = {@code csm:{ossid}:{sender}:{recipient}:{sarRef}:{total}}，
 * field = 分段号，value = 用 {@link SerializableHandler} 序列化的整段消息（与 stream body 同一格式）。
 *
 * <p>采用 produce-before-DEL：集齐时只读取整组并在内存组装；由调用方在外发持久化成功后再调用
 * {@link #complete} 删除 hash（完成令牌）。崩溃在 produce 之前 → hash 完整保留 → 重投重组重发（不丢，
 * 与非 CSM 主链 at-least-once 对称）。过期未集齐由 Redis EXPIRE + {@code csm:index} ZSET 过期索引 +
 * sweeper 处理（回客户过期 DR）。
 *
 * <p>依赖 STATE Redis，且该 Redis 必须 {@code maxmemory-policy noeviction}（半条 hash 被淘汰 = 丢段）。
 */
public class RedisCsmReassembler {

	private static final SystemLogger log = SystemLogger.getSystemLogger(RedisCsmReassembler.class);
	private static final RedisCsmReassembler INSTANCE = new RedisCsmReassembler();

	static final String KEY_PREFIX = "csm:";
	/** 过期索引 ZSET：member=hashKey, score=到期时间戳(ms)。用于过期未集齐的 sweeper（CsmExpiryTask）。 */
	public static final String INDEX_ZSET = "csm:index";
	/** hash 的 EXPIRE 相对过期索引到期时间额外的宽限秒数，保证 sweeper 删 hash 前能读到残段。 */
	static final int EXPIRE_GRACE_SECONDS = 600;

	public static RedisCsmReassembler getInstance() {
		return INSTANCE;
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

	/**
	 * 存入一个 CSM 分段。
	 *
	 * @return {@link CsmStoreResult}：COMPLETE(含组装消息，组装失败时为 null) / INCOMPLETE(已持久) / FAILED(存储失败)。
	 */
	public CsmStoreResult storeSegment(GmmsMessage seg) {
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
		int total = seg.getSarTotalSeqments();
		int ttl = ttlSeconds();
		// 过期索引到期 = now + ttl（sweeper 据此处理过期未集齐、回客户过期 DR）；
		// hash 的 EXPIRE = ttl + grace，比索引晚，保证 sweeper 在 hash 被 Redis 删除前仍能读到残段。
		long indexScore = System.currentTimeMillis() + (long) ttl * 1000L;
		int hashTtl = ttl + EXPIRE_GRACE_SECONDS;
		List<String> flat = GmmsUtility.getInstance().getRedisClient().csmReassemble(
				key, INDEX_ZSET, String.valueOf(seg.getSarSegmentSeqNum()), payload, total, hashTtl, indexScore);
		if (flat == null) {
			return CsmStoreResult.failed();     // 存储失败 -> 不 ACK -> PEL 重试
		}
		if (flat.isEmpty()) {
			return CsmStoreResult.incomplete(); // 已持久 -> ACK
		}
		GmmsMessage assembled = assemble(flat, seg);  // 集齐, 内存组装
		return CsmStoreResult.complete(assembled);    // assembled 可能为 null(组装失败), 调用方兜底
	}

	/**
	 * 完成提交令牌：produce 持久化成功后调用，删除合并 hash 与过期索引项。
	 * produce 失败则不调用，hash 保留供重投重组。
	 */
	public void complete(GmmsMessage seg) {
		GmmsUtility.getInstance().getRedisClient()
				.csmDeleteIfComplete(buildKey(seg), INDEX_ZSET, seg.getSarTotalSeqments());
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
}
