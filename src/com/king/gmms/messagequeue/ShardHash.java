package com.king.gmms.messagequeue;

/**
 * The shard of a key: its String.hashCode mixed with the MurmurHash3 32-bit finalizer (fmix32), then modulo the shard
 * count. String.hashCode alone modulo a power of 2 depends on the low bits only, and as 31^2 = 1 (mod 64) a shard out
 * of 64 came from two sums of the characters: keys of digits (phone numbers) piled up on a few shards (PF007: 2715 to
 * 9334 MTs per routed stream, the busy ones waited minutes; docs/DR早到重试与outbox应答超时改进设计.md). The same key
 * always gives the same shard, so the parts of a long message (their key has the same sarMsgRefNum) stay together.
 */
public final class ShardHash {
	private ShardHash() {
	}

	/**
	 * The shard of a key of one ssid, out of totalShards: the ssid uses only S of them (its StreamShards in cm, else
	 * StreamShardsDefault, else all), so that a customer or channel with little traffic does not spread over every
	 * shard (2000 customers x 64 shards were about 770 thousand streams with a message every two minutes each:
	 * docs/多连接性能分析与优化设计.md 3.1). Its S shards are spread from an offset of the ssid, so that the ssids
	 * fall on every Core and Client, TotalShards / S + 1 apart: an odd step, so that they alternate between the MQ
	 * masters (a sharded stream is on the master shard % masters; with TotalShards / S apart, 8 of 64, all the streams
	 * of an ssid were on one of 2 masters: PF002, docs/5000TPS一小时测试瓶颈分析.md 6). Even over 2 or 4 masters (not 3:
	 * 9 is a multiple of 3); distinct as TotalShards is a power of 2. The same key still always gives the same shard.
	 */
	public static int of(int ssid, String key, int totalShards) {
		return totalShards <= 1 ? 0 : of(ssid, key, totalShards, shardsOf(ssid, totalShards));
	}

	/** {@link #of(int, String, int)} with the ssid's shard count given */
	static int of(int ssid, String key, int totalShards, int shards) {
		if (totalShards <= 1) {
			return 0;
		}
		if (shards <= 0 || shards >= totalShards) {
			return of(key, totalShards);
		}
		int local = of(key, shards);
		int base = of(String.valueOf(ssid), totalShards);
		return (base + local * (totalShards / shards + 1)) % totalShards;
	}

	/**
	 * StreamShards of the ssid (cm), else StreamShardsDefault (GmmsConfig, default 8: divides 64, 3:3:2 over 3 Cores and
	 * 4:4 over 2 Clients; 0 = TotalShards), at most totalShards
	 */
	static int shardsOf(int ssid, int totalShards) {
		int s = 0;
		try {
			com.king.gmms.domain.A2PCustomerInfo c = com.king.gmms.GmmsUtility.getInstance().getCustomerManager()
					.getCustomerBySSID(ssid);
			if (c != null) {
				s = c.getStreamShards();
			}
			if (s <= 0) {
				s = Integer.parseInt(com.king.gmms.GmmsUtility.getInstance().getCommonProperty("StreamShardsDefault", "8").trim());
			}
		} catch (Exception e) {
			s = 0;
		}
		return s <= 0 || s > totalShards ? totalShards : s;
	}

	public static int of(String key, int totalShards) {
		if (totalShards <= 1 || key == null) {
			return 0;
		}
		return (mix(key.hashCode()) & Integer.MAX_VALUE) % totalShards;
	}

	/** MurmurHash3 fmix32 */
	static int mix(int h) {
		h ^= h >>> 16;
		h *= 0x85ebca6b;
		h ^= h >>> 13;
		h *= 0xc2b2ae35;
		h ^= h >>> 16;
		return h;
	}
}
