package com.king.gmms.ha;

import java.util.*;

/** MS-28: even, fewest moves, no overlap, every shard owned; the examples of the design 6.4, and every count up to 9 modules. */
public class ShardPlanCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}
	static Map<String, TreeSet<Integer>> of(String... pairs) {
		Map<String, TreeSet<Integer>> m = new TreeMap<>();
		for (int i = 0; i < pairs.length; i += 2) m.put(pairs[i], ShardPlan.parse(pairs[i + 1]));
		return m;
	}
	static String text(Map<String, TreeSet<Integer>> m) { return ShardPlan.format(m); }

	public static void main(String[] a) {
		// ---- the design examples ----
		Map<String, TreeSet<Integer>> three = of("CoreEngine1", "0-5", "CoreEngine2", "6-10", "CoreEngine3", "11-15");
		Map<String, TreeSet<Integer>> cur = new TreeMap<>(three);
		cur.put("CoreEngine4", new TreeSet<>());
		Map<String, TreeSet<Integer>> four = ShardPlan.balance(cur, List.of("CoreEngine1", "CoreEngine2", "CoreEngine3", "CoreEngine4"), 16);
		check(text(four).equals("{CoreEngine1=0-3, CoreEngine2=6-9, CoreEngine3=11-14, CoreEngine4=4-5,10,15}"), "SP-01a 3 Cores to 4: " + text(four));
		check(ShardPlan.moves(cur, four).size() == 4, "SP-01b 4 shards move: " + ShardPlan.moves(cur, four));
		Map<String, TreeSet<Integer>> back = ShardPlan.balance(four, List.of("CoreEngine1", "CoreEngine2", "CoreEngine3"), 16);
		check(text(back).equals("{CoreEngine1=0-5, CoreEngine2=6-10, CoreEngine3=11-15}"), "SP-01c 4 to 3 (CoreEngine4 goes): " + text(back));
		check(ShardPlan.moves(four, back).size() == 4 && ShardPlan.moves(four, back).stream().allMatch(m -> "CoreEngine4".equals(m.from)),
				"SP-01d only the shards of the one that goes move: " + ShardPlan.moves(four, back));

		// ---- production: 2 Clients 0-7 / 8-15, a third ----
		Map<String, TreeSet<Integer>> clients = of("MultiSmppClient1", "0-7", "MultiSmppClient2", "8-15", "MultiSmppClient3", "none");
		Map<String, TreeSet<Integer>> c3 = ShardPlan.balance(clients, List.of("MultiSmppClient1", "MultiSmppClient2", "MultiSmppClient3"), 16);
		check(text(c3).equals("{MultiSmppClient1=0-5, MultiSmppClient2=8-12, MultiSmppClient3=6-7,13-15}") && ShardPlan.moves(clients, c3).size() == 5,
				"SP-02 2 Clients to 3: the extra shard stays with the one holding most, 5 move: " + text(c3));

		// ---- every count, every start: even, covers, fewest moves ----
		boolean ok = true;
		String bad = "";
		Random r = new Random(7);
		for (int total : new int[] { 1, 2, 16, 36 }) {
			for (int n = 1; n <= 9; n++) {
				for (int round = 0; round < 30; round++) {
					// a random valid start among n modules (some may own nothing), then one comes or one goes
					Map<String, TreeSet<Integer>> start = new TreeMap<>();
					for (int i = 1; i <= n; i++) start.put("M" + i, new TreeSet<>());
					for (int s = 0; s < total; s++) start.get("M" + (1 + r.nextInt(n))).add(s);
					List<String> members = new ArrayList<>(start.keySet());
					boolean adding = round % 2 == 0 || n == 1;
					if (adding) { members.add("M" + (n + 1)); start.put("M" + (n + 1), new TreeSet<>()); } else members.remove("M" + (1 + r.nextInt(n)));
					Map<String, TreeSet<Integer>> t = ShardPlan.balance(start, members, total);
					int min = Integer.MAX_VALUE, max = 0;
					for (String m : members) { min = Math.min(min, t.get(m).size()); max = Math.max(max, t.get(m).size()); }
					// fewest: a module never loses a shard it may keep, so a move only goes from above quota (or the one leaving) to below
					int need = 0;
					for (String m : members) need += Math.max(0, t.get(m).size() - start.get(m).size());
					boolean fine = ShardPlan.problems(t, total).isEmpty() && max - min <= 1 && t.keySet().equals(new TreeSet<>(members))
							&& ShardPlan.moves(start, t).size() == need;
					if (!fine) { ok = false; bad = total + " shards, " + start + " -> " + t; }
				}
			}
		}
		check(ok, "SP-03 1 to 9 modules, 1/2/16/36 shards, random starts, one comes or goes: no overlap, all owned, sizes differ by 1 at most, fewest moves" + (ok ? "" : ": " + bad));

		// ---- shards nobody owns, by hand, all to one ----
		Map<String, TreeSet<Integer>> hole = of("CoreEngine1", "0-5", "CoreEngine2", "8-15");
		Map<String, TreeSet<Integer>> filled = ShardPlan.balance(hole, List.of("CoreEngine1", "CoreEngine2"), 16);
		check(ShardPlan.problems(filled, 16).isEmpty() && ShardPlan.moves(hole, filled).stream().anyMatch(m -> m.from == null),
				"SP-04a shards nobody owns are handed out too: " + text(filled) + " " + ShardPlan.moves(hole, filled));
		Map<String, TreeSet<Integer>> took = ShardPlan.take(three, "CoreEngine4", ShardPlan.parse("4-5"), 16);
		check(text(took).equals("{CoreEngine1=0-3, CoreEngine2=6-10, CoreEngine3=11-15, CoreEngine4=4-5}"), "SP-04b --shards 4-5: taken from whoever has them: " + text(took));
		Map<String, TreeSet<Integer>> all = ShardPlan.giveAll(three, "CoreEngine2", "CoreEngine1", 16);
		check(text(all).equals("{CoreEngine1=0-10, CoreEngine3=11-15}"), "SP-04c --shards-to: all to one: " + text(all));
		check(ShardPlan.problems(of("A", "0-8", "B", "8-15"), 16).toString().contains("shard 8 owned by [A, B]")
				&& ShardPlan.problems(of("A", "0-6", "B", "8-15"), 16).toString().contains("nobody: [7]")
				&& ShardPlan.problems(of("A", "0-16"), 16).toString().contains("shard 16"), "SP-04d overlaps, holes and shards beyond TotalShards found");
		check(ShardPlan.format(ShardPlan.parse("none")).equals("none") && ShardPlan.format(ShardPlan.parse("0,1,2,5,7-8")).equals("0-2,5,7-8"), "SP-04e none and ranges read and written");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
