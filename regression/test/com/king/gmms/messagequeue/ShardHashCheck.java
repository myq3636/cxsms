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
		// ---- StreamShards: an ssid on S of the shards, the ssids spread over all of them ----
		java.util.Set<Integer> one = new java.util.HashSet<>();
		boolean same = true;
		for (int i = 0; i < 10000; i++) {
			String key = "20007_MC0007_870007" + String.format("%07d", r.nextInt(10000000));
			int g = ShardHash.of(20007, key, shards, 4);
			one.add(g);
			same &= g == ShardHash.of(20007, key, shards, 4);
		}
		java.util.List<Integer> sorted = new java.util.ArrayList<>(one);
		java.util.Collections.sort(sorted);
		boolean evenStep = sorted.size() == 4 && (sorted.get(1) - sorted.get(0)) % 16 == 0 && (sorted.get(3) - sorted.get(2)) % 16 == 0;
		check(same && evenStep, "SH-04 StreamShards 4: 10000 keys of one ssid on 4 shards 16 apart, the same key on the same one: " + sorted);
		int[] global = new int[shards];
		for (int ssid = 20001; ssid <= 22000; ssid++) {
			for (int i = 0; i < 4; i++) global[ShardHash.of(ssid, ssid + "_K" + i + "_" + r.nextInt(1000000), shards, 4)]++;
		}
		java.util.Set<String> used = new java.util.HashSet<>();
		for (int ssid = 20001; ssid <= 22000; ssid++) {
			for (int i = 0; i < 200; i++) used.add(ssid + ":" + ShardHash.of(ssid, "k" + i, shards, 4));
		}
		check(ratio(global) < 2.0 && used.size() == 2000 * 4,
				"SH-05 2000 ssids x 4 shards: every one of the 64 used, busiest / emptiest = " + String.format("%.2f", ratio(global))
						+ ", streams " + used.size());
		check(ShardHash.of(9981, "9981_MKT_8613857543388", shards, 64) == ShardHash.of("9981_MKT_8613857543388", shards)
				&& ShardHash.of(9981, "9981_MKT_8613857543388", shards, 0) == ShardHash.of("9981_MKT_8613857543388", shards),
				"SH-06 StreamShards 0 or >= TotalShards: the same shard as before");
		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}

	static double ratio(int[] c) {
		int min = Integer.MAX_VALUE, max = 0;
		for (int v : c) { min = Math.min(min, v); max = Math.max(max, v); }
		return (double) max / Math.max(1, min);
	}
}
