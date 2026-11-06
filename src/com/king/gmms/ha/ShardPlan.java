package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Who owns which shard after a module comes or goes (docs/模块动态扩缩容设计.md 6.4): as even as it can be, with as
 * few shards moved as possible. Each module keeps its lowest shards up to its share; the rest, the shards of a module
 * that goes and the shards nobody owns are handed out lowest first, in module name order.
 */
public final class ShardPlan {
	/** shard moves from one module to another; from is null for a shard nobody owns */
	public static final class Move {
		public final int shard;
		public final String from;
		public final String to;

		public Move(int shard, String from, String to) {
			this.shard = shard;
			this.from = from;
			this.to = to;
		}

		public String toString() {
			return shard + ": " + (from == null ? "(nobody)" : from) + " -> " + to;
		}
	}

	private ShardPlan() {
	}

	/** members own all shards 0..total-1 afterwards; current may hold modules that go */
	public static Map<String, TreeSet<Integer>> balance(Map<String, ? extends Set<Integer>> current, List<String> members, int total) {
		if (members.isEmpty()) {
			throw new IllegalArgumentException("no module to own the shards");
		}
		final Map<String, Integer> held = new TreeMap<String, Integer>();
		for (String m : members) {
			held.put(m, current.containsKey(m) ? inRange(current.get(m), total).size() : 0);
		}
		List<String> byHeld = new ArrayList<String>(new TreeSet<String>(members));
		Collections.sort(byHeld, new Comparator<String>() {
			public int compare(String a, String b) {
				int c = held.get(b) - held.get(a);
				return c != 0 ? c : a.compareTo(b);
			}
		});
		Map<String, Integer> quota = new TreeMap<String, Integer>();
		int base = total / members.size();
		int extra = total % members.size();
		for (int i = 0; i < byHeld.size(); i++) {
			// the modules holding most get the extra shard: fewer moves
			quota.put(byHeld.get(i), base + (i < extra ? 1 : 0));
		}
		Map<String, TreeSet<Integer>> target = new TreeMap<String, TreeSet<Integer>>();
		Set<Integer> kept = new HashSet<Integer>();
		for (String m : quota.keySet()) {
			TreeSet<Integer> mine = new TreeSet<Integer>();
			if (current.containsKey(m)) {
				for (Integer s : inRange(current.get(m), total)) {
					if (mine.size() < quota.get(m) && kept.add(s)) {
						mine.add(s);
					}
				}
			}
			target.put(m, mine);
		}
		List<Integer> pool = new ArrayList<Integer>();
		for (int s = 0; s < total; s++) {
			if (!kept.contains(s)) {
				pool.add(s);
			}
		}
		int next = 0;
		for (String m : quota.keySet()) {
			while (target.get(m).size() < quota.get(m)) {
				target.get(m).add(pool.get(next++));
			}
		}
		return target;
	}

	/** all shards of the module that goes to one other module; the others keep theirs */
	public static Map<String, TreeSet<Integer>> giveAll(Map<String, ? extends Set<Integer>> current, String from, String to, int total) {
		Map<String, TreeSet<Integer>> target = copy(current, total);
		TreeSet<Integer> moved = target.remove(from);
		if (!target.containsKey(to)) {
			target.put(to, new TreeSet<Integer>());
		}
		if (moved != null) {
			target.get(to).addAll(moved);
		}
		return target;
	}

	/** the module takes exactly these shards, from whoever owns them */
	public static Map<String, TreeSet<Integer>> take(Map<String, ? extends Set<Integer>> current, String module, Set<Integer> shards, int total) {
		Map<String, TreeSet<Integer>> target = copy(current, total);
		for (TreeSet<Integer> owned : target.values()) {
			owned.removeAll(shards);
		}
		target.put(module, new TreeSet<Integer>(inRange(shards, total)));
		return target;
	}

	/** what is wrong with an assignment: overlaps and shards nobody owns; empty when none */
	public static List<String> problems(Map<String, ? extends Set<Integer>> target, int total) {
		List<String> problems = new ArrayList<String>();
		Map<Integer, List<String>> owners = new TreeMap<Integer, List<String>>();
		for (Map.Entry<String, ? extends Set<Integer>> e : target.entrySet()) {
			for (Integer s : e.getValue()) {
				if (s < 0 || s >= total) {
					problems.add(e.getKey() + " has shard " + s + ", TotalShards is " + total);
					continue;
				}
				if (!owners.containsKey(s)) {
					owners.put(s, new ArrayList<String>());
				}
				owners.get(s).add(e.getKey());
			}
		}
		for (Map.Entry<Integer, List<String>> o : owners.entrySet()) {
			if (o.getValue().size() > 1) {
				problems.add("shard " + o.getKey() + " owned by " + o.getValue());
			}
		}
		List<Integer> missing = new ArrayList<Integer>();
		for (int s = 0; s < total; s++) {
			if (!owners.containsKey(s)) {
				missing.add(s);
			}
		}
		if (!missing.isEmpty()) {
			problems.add("shards owned by nobody: " + missing);
		}
		return problems;
	}

	/** One shard at a time; a shard of several owners counts as the first one's. */
	public static List<Move> moves(Map<String, ? extends Set<Integer>> current, Map<String, ? extends Set<Integer>> target) {
		List<Move> moves = new ArrayList<Move>();
		Map<Integer, String> owner = new TreeMap<Integer, String>();
		for (Map.Entry<String, ? extends Set<Integer>> e : new TreeMap<String, Set<Integer>>(current).entrySet()) {
			for (Integer s : e.getValue()) {
				if (!owner.containsKey(s)) {
					owner.put(s, e.getKey());
				}
			}
		}
		for (Map.Entry<String, ? extends Set<Integer>> e : new TreeMap<String, Set<Integer>>(target).entrySet()) {
			for (Integer s : new TreeSet<Integer>(e.getValue())) {
				String from = owner.get(s);
				if (!e.getKey().equals(from)) {
					moves.add(new Move(s, from, e.getKey()));
				}
			}
		}
		Collections.sort(moves, new Comparator<Move>() {
			public int compare(Move a, Move b) {
				return a.shard - b.shard;
			}
		});
		return moves;
	}

	/** "0-3,8" / "none" / "" (none) */
	public static TreeSet<Integer> parse(String text) {
		TreeSet<Integer> shards = new TreeSet<Integer>();
		if (text == null || text.trim().length() == 0 || "none".equalsIgnoreCase(text.trim())) {
			return shards;
		}
		for (String part : text.split(",")) {
			String p = part.trim();
			if (p.length() == 0) {
				continue;
			}
			int dash = p.indexOf('-');
			if (dash > 0) {
				int a = Integer.parseInt(p.substring(0, dash).trim());
				int b = Integer.parseInt(p.substring(dash + 1).trim());
				if (a > b) {
					throw new IllegalArgumentException("bad shard range " + p);
				}
				for (int s = a; s <= b; s++) {
					shards.add(s);
				}
			} else {
				shards.add(Integer.parseInt(p));
			}
		}
		return shards;
	}

	/** 0,1,2,3,8 -> 0-3,8; nothing -> none */
	public static String format(Set<Integer> shards) {
		return ShardAssignmentChecker.ranges(shards);
	}

	public static String format(Map<String, ? extends Set<Integer>> assignment) {
		Map<String, String> text = new LinkedHashMap<String, String>();
		for (Map.Entry<String, ? extends Set<Integer>> e : new TreeMap<String, Set<Integer>>(assignment).entrySet()) {
			text.put(e.getKey(), format(e.getValue()));
		}
		return text.toString();
	}

	private static Set<Integer> inRange(Set<Integer> shards, int total) {
		Set<Integer> in = new TreeSet<Integer>();
		for (Integer s : shards) {
			if (s >= 0 && s < total) {
				in.add(s);
			}
		}
		return in;
	}

	private static Map<String, TreeSet<Integer>> copy(Map<String, ? extends Set<Integer>> current, int total) {
		Map<String, TreeSet<Integer>> copy = new TreeMap<String, TreeSet<Integer>>();
		for (Map.Entry<String, ? extends Set<Integer>> e : current.entrySet()) {
			copy.put(e.getKey(), new TreeSet<Integer>(inRange(e.getValue(), total)));
		}
		return copy;
	}
}
