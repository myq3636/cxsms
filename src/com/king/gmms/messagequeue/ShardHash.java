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
