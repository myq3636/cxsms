package com.king.gmms.ha;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;

/**
 * Cross-module check of the stream shard configuration. Every server, core and client node
 * publishes system:shards:{group}:{module}:{node} = "TotalShards|shards it owns" with each
 * module status heartbeat; once a minute the published views are compared within each group of
 * nodes that consume the same streams:
 * <ul>
 * <li>core: stream:mt:pending, inbound DRs, results;</li>
 * <li>client: SMPP clients, stream:mt:routed;</li>
 * <li>httpclient: HTTP clients (CommonHttpClient), stream:http:submit / stream:http:dr. They
 * report the module role "client" but consume other streams, so their shards are counted apart:
 * an SMPP and an HTTP client owning the same shard is no overlap, and one family missing a shard
 * is not hidden by the other.</li>
 * </ul>
 * <ul>
 * <li>TotalShards differs between nodes: ERROR (writers and readers compute different shards);</li>
 * <li>two nodes of a group own the same shard: WARN;</li>
 * <li>a shard 0..TotalShards-1 owned by no node of a group: ERROR, its streams are never consumed.</li>
 * </ul>
 * Server nodes consume all DR shards of their module, they only take part in the TotalShards check.
 * The shards of every node are logged (INFO) at the first check and whenever they change.
 * Problems are logged only; a report is repeated at most every 10 minutes while it does not change.
 */
public final class ShardAssignmentChecker {
	private static final SystemLogger log = SystemLogger.getSystemLogger(ShardAssignmentChecker.class);
	private static final String PREFIX = "system:shards:";
	private static final long CHECK_INTERVAL_MS = 60000L;
	private static final long REPEAT_MS = 600000L;

	private static long lastCheck = 0L;
	private static long lastLogged = 0L;
	private static String lastReport = "";
	private static String lastSummary = "";
	/** group this module publishes under instead of its module role (HTTP clients: httpclient) */
	private static volatile String groupOverride = null;

	private ShardAssignmentChecker() {
	}

	static boolean covers(String role) {
		return "core".equals(role) || "client".equals(role) || "httpclient".equals(role) || "server".equals(role);
	}

	/** Publish this module's shards under group instead of its role (set before the heartbeat starts). */
	public static void setShardGroup(String group) {
		groupOverride = group;
	}

	/** Publish this node's shard view (called with each module status heartbeat), then check if due. */
	public static void publish(GmmsUtility g, String role, String module, String node, int ttlSeconds) {
		if (groupOverride != null && covers(role)) {
			role = groupOverride;
		}
		if (!covers(role)) {
			return;
		}
		RedisClient redis = g.getRedisClient();
		if (redis == null) {
			return;
		}
		redis.setString(PREFIX + role + ":" + module + ":" + node,
				g.getTotalShards() + "|" + join(g.getMyShards()), ttlSeconds);
		boolean due;
		synchronized (ShardAssignmentChecker.class) {
			due = System.currentTimeMillis() - lastCheck >= CHECK_INTERVAL_MS;
			if (due) {
				lastCheck = System.currentTimeMillis();
			}
		}
		if (due) {
			check(redis);
		}
	}

	/** Compare all published views; returns the problems found (empty when consistent). */
	public static synchronized List<String> check(RedisClient redis) {
		Map<String, Integer> totals = new TreeMap<String, Integer>();
		Map<String, Map<String, Set<Integer>>> owned = new TreeMap<String, Map<String, Set<Integer>>>();
		for (String key : redis.getStateRedis().scanAll(PREFIX + "*")) {
			String[] k = key.split(":", 5);
			String value = redis.getString(key);
			if (k.length < 5 || value == null || value.indexOf('|') < 0) {
				continue;
			}
			String role = k[2];
			String node = role + ":" + k[3] + ":" + k[4];
			try {
				totals.put(node, Integer.parseInt(value.substring(0, value.indexOf('|')).trim()));
			} catch (NumberFormatException e) {
				continue;
			}
			if (!"server".equals(role)) {
				Map<String, Set<Integer>> byNode = owned.get(role);
				if (byNode == null) {
					byNode = new TreeMap<String, Set<Integer>>();
					owned.put(role, byNode);
				}
				byNode.put(node, parse(value.substring(value.indexOf('|') + 1)));
			}
		}
		List<String> problems = new ArrayList<String>();
		Set<Integer> distinct = new TreeSet<Integer>(totals.values());
		if (distinct.size() > 1) {
			problems.add("ERROR TotalShards differs between nodes: " + totals);
		}
		int total = distinct.isEmpty() ? 0 : ((TreeSet<Integer>) distinct).last();
		for (Map.Entry<String, Map<String, Set<Integer>>> role : owned.entrySet()) {
			Map<Integer, List<String>> owners = new TreeMap<Integer, List<String>>();
			for (Map.Entry<String, Set<Integer>> node : role.getValue().entrySet()) {
				for (Integer shard : node.getValue()) {
					List<String> list = owners.get(shard);
					if (list == null) {
						list = new ArrayList<String>();
						owners.put(shard, list);
					}
					list.add(node.getKey());
				}
			}
			List<String> overlaps = new ArrayList<String>();
			for (Map.Entry<Integer, List<String>> o : owners.entrySet()) {
				if (o.getValue().size() > 1) {
					overlaps.add(o.getKey() + "=" + o.getValue());
				}
			}
			if (!overlaps.isEmpty()) {
				problems.add("WARN " + role.getKey() + " shards owned by more than one node: " + overlaps);
			}
			List<Integer> missing = new ArrayList<Integer>();
			for (int s = 0; s < total; s++) {
				if (!owners.containsKey(s)) {
					missing.add(s);
				}
			}
			if (!missing.isEmpty()) {
				problems.add("ERROR " + role.getKey() + " shards owned by no node (never consumed): " + missing
						+ ", TotalShards=" + total + ", nodes=" + role.getValue());
			}
		}
		logSummary(totals, owned);
		report(problems);
		return problems;
	}

	/** Shards of every node, per group: logged when the picture changes (first check, node added/gone, config). */
	private static void logSummary(Map<String, Integer> totals, Map<String, Map<String, Set<Integer>>> owned) {
		StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, Map<String, Set<Integer>>> role : owned.entrySet()) {
			sb.append(role.getKey()).append(": ");
			boolean first = true;
			for (Map.Entry<String, Set<Integer>> node : role.getValue().entrySet()) {
				if (!first) {
					sb.append(", ");
				}
				first = false;
				sb.append(node.getKey()).append('=').append(ranges(node.getValue()));
			}
			sb.append("; ");
		}
		List<String> servers = new ArrayList<String>();
		for (String node : totals.keySet()) {
			if (node.startsWith("server:")) {
				servers.add(node);
			}
		}
		if (!servers.isEmpty()) {
			sb.append("server (all DR shards of their module): ").append(servers).append("; ");
		}
		sb.append("TotalShards=").append(new TreeSet<Integer>(totals.values()));
		String summary = sb.toString();
		if (!summary.equals(lastSummary)) {
			log.info("[SHARD-CHECK] shard owners: {}", summary);
			lastSummary = summary;
		}
	}

	/** 0,1,2,3,8,9 -> 0-3,8-9 */
	static String ranges(Set<Integer> shards) {
		StringBuilder sb = new StringBuilder();
		Integer start = null, prev = null;
		for (Integer s : new TreeSet<Integer>(shards)) {
			if (start == null) {
				start = s;
			} else if (s != prev + 1) {
				appendRange(sb, start, prev);
				start = s;
			}
			prev = s;
		}
		if (start != null) {
			appendRange(sb, start, prev);
		}
		return sb.length() == 0 ? "none" : sb.toString();
	}

	private static void appendRange(StringBuilder sb, int a, int b) {
		if (sb.length() > 0) {
			sb.append(',');
		}
		sb.append(a);
		if (b != a) {
			sb.append('-').append(b);
		}
	}

	private static void report(List<String> problems) {
		String text = problems.toString();
		long now = System.currentTimeMillis();
		if (problems.isEmpty()) {
			if (!lastReport.isEmpty()) {
				log.info("Shard assignment consistent again");
			}
		} else if (!text.equals(lastReport) || now - lastLogged >= REPEAT_MS) {
			for (String p : problems) {
				if (p.startsWith("ERROR")) {
					log.error("[SHARD-CHECK] {}", p);
				} else {
					log.warn("[SHARD-CHECK] {}", p);
				}
			}
			lastLogged = now;
		}
		lastReport = problems.isEmpty() ? "" : text;
	}

	private static String join(Set<Integer> shards) {
		StringBuilder sb = new StringBuilder();
		for (Integer s : new TreeSet<Integer>(shards)) {
			if (sb.length() > 0) {
				sb.append(',');
			}
			sb.append(s);
		}
		return sb.toString();
	}

	private static Set<Integer> parse(String value) {
		Set<Integer> set = new TreeSet<Integer>();
		for (String p : value.split(",")) {
			try {
				if (p.trim().length() > 0) {
					set.add(Integer.parseInt(p.trim()));
				}
			} catch (NumberFormatException ignored) {
				// skip
			}
		}
		return set;
	}
}
