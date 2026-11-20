package com.king.gmms.messagequeue;

import java.util.Random;

/**
 * ShardHash: keys of phone numbers spread evenly over 64 shards (String.hashCode alone piled them up on a few: PF007),
 * and the same key always gives the same shard.
 */
public class ShardHashCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	public static void main(String[] args) {
		int shards = 64, keys = 640000;
		int[] mixed = new int[shards], plain = new int[shards];
		Random r = new Random(7);
		for (int i = 0; i < keys; i++) {
			String key = "9981_MKT_86138" + String.format("%08d", r.nextInt(100000000));
			mixed[ShardHash.of(key, shards)]++;
			plain[(key.hashCode() & Integer.MAX_VALUE) % shards]++;
		}
		check(ratio(plain) > 2.0, "SH-01 String.hashCode % 64 on phone numbers: busiest / emptiest shard = "
				+ String.format("%.2f", ratio(plain)) + " (the old spread)");
		check(ratio(mixed) < 1.1, "SH-02 ShardHash: busiest / emptiest shard = " + String.format("%.2f", ratio(mixed)));
		String csm = "9971_MKT_8613857543388_42";
		check(ShardHash.of(csm, shards) == ShardHash.of(new String(csm.toCharArray()), shards) && ShardHash.of(csm, 1) == 0
				&& ShardHash.of(null, shards) == 0, "SH-03 same key, same shard; one shard or no key: 0");
		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}

	static double ratio(int[] c) {
		int min = Integer.MAX_VALUE, max = 0;
		for (int v : c) { min = Math.min(min, v); max = Math.max(max, v); }
		return (double) max / Math.max(1, min);
	}
}
