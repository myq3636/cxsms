package com.king.gmms.routing.rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.king.gmms.domain.A2PCustomerConfig;
import com.king.gmms.domain.A2PCustomerMultiValue;
import com.king.gmms.domain.AttrPair;
import com.king.gmms.domain.CustomerConfigurationException;
import com.king.gmms.domain.Group;
import com.king.gmms.routing.rule.RoutingRule.Layer;

/**
 * The rules of one routing file, indexed by layer and level (docs/路由重构设计.md 2.1, 2.4, 2.6):
 * <pre>
 * A2PSecRouting / A2PSecBackupRouting / A2PSecBackup2Routing ... A2PSecBackup10Routing
 * A2PNumberRouting                      (no backup)
 * A2POPRouting  / A2POPBackupRouting  / A2POPBackup2Routing  ... A2POPBackup10Routing
 * A2PMainRouting / A2PBackupRouting   / A2PBackup2Routing    ... A2PBackup10Routing
 * </pre>
 * A file with a wrong rule fails as a whole ({@link #parse} throws); the old {@code ...RoutingRelay} names and
 * unknown {@code ...Routing} names are only warned about.
 */
public final class RuleSet {
	public static final int MAX_LEVEL = 10;
	/** an operator rule for every operator */
	public static final int ANY_OPERATOR = -2;
	public static final String ALL = A2PCustomerConfig.ALL_ROUTING_INFO_KEY;

	private static final Pattern NAME = Pattern.compile("(?i)A2P(Sec|OP)?(Main|Number|Backup(\\d*))?Routing");

	final String name;
	final int ssid;
	/** [layer][level]: group key -> rules by priority; null when the file has none */
	private final Map<String, List<RoutingRule>>[][] groups;
	/** [layer][level]: the longest prefix (layers SEC, MAIN) */
	private final int[][] longest;
	final List<String> warnings;
	private final int count;

	@SuppressWarnings("unchecked")
	private RuleSet(String name, int ssid, List<RoutingRule> rules, List<String> warnings) {
		this.name = name;
		this.ssid = ssid;
		this.warnings = warnings;
		this.count = rules.size();
		groups = new Map[Layer.values().length][MAX_LEVEL + 1];
		longest = new int[Layer.values().length][MAX_LEVEL + 1];
		for (RoutingRule r : rules) {
			Map<String, List<RoutingRule>> g = groups[r.layer.ordinal()][r.level];
			if (g == null) {
				g = new HashMap<String, List<RoutingRule>>();
				groups[r.layer.ordinal()][r.level] = g;
			}
			for (String k : r.keys) {
				List<RoutingRule> l = g.get(k);
				if (l == null) {
					l = new ArrayList<RoutingRule>();
					g.put(k, l);
				}
				l.add(r);
				if (!ALL.equals(k) && (r.layer == Layer.MAIN || r.layer == Layer.SEC)) {
					longest[r.layer.ordinal()][r.level] = Math.max(longest[r.layer.ordinal()][r.level], k.length());
				}
			}
		}
		for (Map<String, List<RoutingRule>>[] byLevel : groups) {
			for (Map<String, List<RoutingRule>> g : byLevel) {
				if (g != null) {
					for (List<RoutingRule> l : g.values()) {
						Collections.sort(l, (a, b) -> Integer.compare(a.priority, b.priority));
					}
				}
			}
		}
	}

	public String name() {
		return name;
	}

	public int ssid() {
		return ssid;
	}

	public int size() {
		return count;
	}

	public List<String> warnings() {
		return warnings;
	}

	/**
	 * The rules in use, for Redis (routingInfo{core}.Rules:{file}, docs/路由配置说明.md 7.1):
	 * "layer:level:group:priority" -> "Relay={ssids},conditions...,Source=...", plus "_file" and "_warnings".
	 */
	public Map<String, String> describe() {
		Map<String, String> out = new java.util.TreeMap<String, String>();
		out.put("_file", ssid > 0 ? "CCBSSID=" + ssid
				: "default of service type " + (name.endsWith(com.king.gmms.Constant.DEFAULT_ROUTING_SUFFER)
						? name.substring(0, name.length() - com.king.gmms.Constant.DEFAULT_ROUTING_SUFFER.length()) : name));
		if (!warnings.isEmpty()) {
			out.put("_warnings", String.join(" | ", warnings));
		}
		for (RoutingRule r : all()) {
			Map<String, String> v = new java.util.LinkedHashMap<String, String>();
			v.put("Relay", r.relay.ssidText());
			if (!r.senders.isEmpty()) v.put("Sender", String.join(";", r.senders));
			if (!r.excludeSenders.isEmpty()) v.put("ExcludeSender", String.join(";", r.excludeSenders));
			if (!r.keywordIds.isEmpty()) v.put("KeywordId", String.join(";", r.keywordIds));
			if (!r.excludeKeywordIds.isEmpty()) v.put("ExcludeKeywordId", String.join(";", r.excludeKeywordIds));
			if (!r.excludePrefixes.isEmpty()) v.put("ExcludePhonePrefix", String.join(";", r.excludePrefixes));
			if (r.templateId != null) v.put("ReplaceContentByTemplate", r.templateId);
			if (!r.templateSenders.isEmpty()) v.put("IsReplaceTemplateBySender", "true");
			v.put("Source", r.source);
			String json = com.alibaba.fastjson.JSON.toJSONString(v);
			for (String k : r.keys) {
				out.put(r.layer.label + ":" + r.level + ":" + k + ":" + r.priority, json);
			}
		}
		return out;
	}

	/** every rule of the file */
	List<RoutingRule> all() {
		List<RoutingRule> out = new ArrayList<RoutingRule>();
		for (Map<String, List<RoutingRule>>[] byLevel : groups) {
			for (Map<String, List<RoutingRule>> g : byLevel) {
				if (g != null) {
					for (List<RoutingRule> l : g.values()) {
						for (RoutingRule r : l) {
							if (!out.contains(r)) {
								out.add(r);
							}
						}
					}
				}
			}
		}
		return out;
	}

	boolean has(Layer layer, int level) {
		return groups[layer.ordinal()][level] != null;
	}

	/** the groups that apply to the recipient / operator, most specific first */
	List<String> groupKeys(Layer layer, int level, String recipient, int operator) {
		Map<String, List<RoutingRule>> g = groups[layer.ordinal()][level];
		if (g == null) {
			return Collections.emptyList();
		}
		List<String> keys = new ArrayList<String>(4);
		switch (layer) {
		case NUMBER:
			if (g.containsKey(recipient)) {
				keys.add(recipient);
			}
			break;
		case OP:
			if (operator > 0) {
				for (int op : new int[] { operator, operator / 10000 * 10000, ANY_OPERATOR }) {
					String k = String.valueOf(op);
					if (g.containsKey(k) && !keys.contains(k)) {
						keys.add(k);
					}
				}
			}
			break;
		default:
			for (int len = Math.min(longest[layer.ordinal()][level], recipient.length()); len > 0; len--) {
				String k = recipient.substring(0, len);
				if (g.containsKey(k)) {
					keys.add(k);
				}
			}
			if (g.containsKey(ALL)) {
				keys.add(ALL);
			}
		}
		return keys;
	}

	List<RoutingRule> rules(Layer layer, int level, String key) {
		Map<String, List<RoutingRule>> g = groups[layer.ordinal()][level];
		List<RoutingRule> l = g == null ? null : g.get(key);
		return l == null ? Collections.<RoutingRule>emptyList() : l;
	}

	/** a config name of the new routing (A2PMainRouting, A2PBackup3Routing, A2POPRouting ...) */
	public static boolean isRoutingKey(String key) {
		Object[] ll = layerLevel(key);
		return ll != null && ll[0] != null;
	}

	/** the layer and level of a config name, null when it is not one of the routing names */
	static Object[] layerLevel(String key) {
		Matcher m = NAME.matcher(key.trim());
		if (!m.matches()) {
			return null;
		}
		String prefix = m.group(1), kind = m.group(2);
		if (kind == null) {
			if ("OP".equalsIgnoreCase(prefix) || "Sec".equalsIgnoreCase(prefix)) {
				return new Object[] { "OP".equalsIgnoreCase(prefix) ? Layer.OP : Layer.SEC, 0 };   // A2POPRouting, A2PSecRouting
			}
			return null;   // A2PRouting
		}
		if ("Main".equalsIgnoreCase(kind)) {
			return prefix == null ? new Object[] { Layer.MAIN, 0 } : null;
		}
		if ("Number".equalsIgnoreCase(kind)) {
			return prefix == null ? new Object[] { Layer.NUMBER, 0 } : null;
		}
		String n = m.group(3);
		int level = n == null || n.isEmpty() ? 1 : Integer.parseInt(n);
		if (level < 1 || level > MAX_LEVEL || "1".equals(n)) {
			return new Object[] { null, level };   // out of range (A2PBackup1Routing is written A2PBackupRouting)
		}
		Layer layer = prefix == null ? Layer.MAIN : "OP".equalsIgnoreCase(prefix) ? Layer.OP : Layer.SEC;
		return new Object[] { layer, level };
	}

	/**
	 * The rules of a routing file. ssidOf: channel short name to ssid (-1 when there is none).
	 * Throws CustomerConfigurationException naming the file, the config and the rule when a rule is wrong.
	 */
	public static RuleSet parse(A2PCustomerConfig block, int ssid, ToIntFunction<String> ssidOf) throws CustomerConfigurationException {
		String file = block.getShortName();
		List<String> warnings = new ArrayList<String>();
		List<RoutingRule> rules = new ArrayList<RoutingRule>();
		for (Map.Entry<String, String> line : block.getLinesWithOriginalNames().entrySet()) {
			String key = line.getKey();
			if (key.toLowerCase().contains("routingrelay") || "A2PContentRouting".equalsIgnoreCase(key)) {
				warnings.add(key + ": the old routing config name is not used any more (docs/路由重构设计.md 第 5 节), convert it");
				continue;
			}
			Object[] ll = layerLevel(key);
			if (ll == null) {
				if (key.toLowerCase().contains("routing") && key.toLowerCase().startsWith("a2p")) {
					warnings.add(key + ": not a routing config name, not used");
				}
				continue;
			}
			if (ll[0] == null) {
				warnings.add(key + ": backup level " + ll[1] + " is not supported (A2PBackupRouting, A2PBackup2Routing ... A2PBackup10Routing), not used");
				continue;
			}
			Layer layer = (Layer) ll[0];
			int level = (Integer) ll[1];
			A2PCustomerMultiValue value = block.parseRoutingMultiValue(key);
			if (value == null) {
				throw new CustomerConfigurationException(file + " " + key + ": wrong format");
			}
			int n = 0;
			for (Group g : value.getAllGroups()) {
				n++;
				String src = file + " " + key + " #" + n;
				rules.add(rule(g, layer, level, src, ssidOf, warnings));
			}
		}
		checkPriorities(file, rules);
		return new RuleSet(file, ssid, rules, warnings);
	}

	private static String attr(Group g, String name) {
		AttrPair a = g.getAttr(name);
		if (a == null || a.getStringValue() == null) {
			return null;
		}
		String v = a.getStringValue().trim();
		return v.isEmpty() ? null : v;
	}

	private static List<String> list(String v) {
		List<String> out = new ArrayList<String>();
		if (v != null) {
			for (String s : v.split(A2PCustomerConfig.SUB_VALUE_SEP)) {
				s = s.trim();
				if (s.startsWith("+")) {
					s = s.substring(1);
				}
				if (!s.isEmpty() && !out.contains(s)) {
					out.add(s);
				}
			}
		}
		return out;
	}

	private static Set<String> lowerSet(String v) {
		Set<String> out = new LinkedHashSet<String>();
		if (v != null) {
			for (String s : v.split(A2PCustomerConfig.SUB_VALUE_SEP)) {
				s = s.trim().toLowerCase();
				if (!s.isEmpty()) {
					out.add(s);
				}
			}
		}
		return out;
	}

	private static RoutingRule rule(Group g, Layer layer, int level, String src, ToIntFunction<String> ssidOf,
			List<String> warnings) throws CustomerConfigurationException {
		String p = attr(g, "Priority");
		if (p == null) {
			throw new CustomerConfigurationException(src + ": no Priority (1-100)");
		}
		int priority;
		try {
			priority = Integer.parseInt(p);
		} catch (NumberFormatException e) {
			throw new CustomerConfigurationException(src + ": Priority=" + p + " is not a number (1-100)");
		}
		if (priority < 1 || priority > 100) {
			throw new CustomerConfigurationException(src + ": Priority=" + p + " is not 1-100");
		}
		Relay relay;
		try {
			relay = Relay.parse(attr(g, "Relay"), ssidOf, warnings);
		} catch (RuntimeException e) {
			throw new CustomerConfigurationException(src + ": wrong Relay: " + e.getMessage());
		}
		if (relay == null) {
			throw new CustomerConfigurationException(src + ": no Relay");
		}
		for (int i = 0; i < relay.ssids.length; i++) {
			if (relay.ssids[i] <= 0) {
				warnings.add(src + ": Relay channel " + relay.names[i] + " is not in cm.cfg, the MTs this rule matches fail as not routed");
			}
		}
		List<String> keys;
		switch (layer) {
		case NUMBER:
			keys = list(attr(g, "Number"));
			if (keys.isEmpty()) {
				throw new CustomerConfigurationException(src + ": no Number");
			}
			break;
		case OP:
			keys = list(attr(g, "Opid"));
			if (keys.isEmpty()) {
				throw new CustomerConfigurationException(src + ": no Opid");
			}
			for (String k : keys) {
				try {
					Integer.parseInt(k);
				} catch (NumberFormatException e) {
					throw new CustomerConfigurationException(src + ": Opid=" + k + " is not a number");
				}
			}
			break;
		default:
			keys = list(attr(g, "PhonePrefix"));
			if (keys.isEmpty() || keys.contains(ALL)) {
				keys = Collections.singletonList(ALL);
			}
		}
		Set<String> senders = lowerSet(attr(g, "Sender"));
		Set<String> excludeSenders = lowerSet(attr(g, "ExcludeSender"));
		for (String s : senders) {
			if (excludeSenders.contains(s)) {
				throw new CustomerConfigurationException(src + ": " + s + " is both in Sender and in ExcludeSender");
			}
		}
		List<String> excludePrefixes = list(attr(g, "ExcludePhonePrefix"));
		if (layer == Layer.NUMBER && !excludePrefixes.isEmpty()) {
			throw new CustomerConfigurationException(src + ": ExcludePhonePrefix is not used with Number");
		}
		if ((layer == Layer.MAIN || layer == Layer.SEC) && !keys.contains(ALL)) {
			for (String x : excludePrefixes) {
				boolean under = false;
				for (String k : keys) {
					under |= x.startsWith(k) && x.length() > k.length();
				}
				if (!under) {
					throw new CustomerConfigurationException(src + ": ExcludePhonePrefix=" + x + " is not a part of PhonePrefix " + keys);
				}
			}
		}
		List<String> templateSenders = null;
		if ("true".equalsIgnoreCase(attr(g, "IsReplaceTemplateBySender")) && attr(g, "Sender") != null) {
			templateSenders = new ArrayList<String>();
			for (String x : attr(g, "Sender").split(A2PCustomerConfig.SUB_VALUE_SEP)) {
				if (!x.trim().isEmpty()) {
					templateSenders.add(x.trim());
				}
			}
		}
		return new RoutingRule(layer, level, priority, keys, senders, excludeSenders, list(attr(g, "KeywordId")),
				list(attr(g, "ExcludeKeywordId")), excludePrefixes, attr(g, "ReplaceContentByTemplate"), templateSenders, relay, src);
	}

	/** two rules of one group with the same priority: which one first is not clear, the file fails (2.5) */
	private static void checkPriorities(String file, List<RoutingRule> rules) throws CustomerConfigurationException {
		Map<String, RoutingRule> seen = new HashMap<String, RoutingRule>();
		for (RoutingRule r : rules) {
			for (String k : r.keys) {
				String g = r.layer + "/" + r.level + "/" + k + "/" + r.priority;
				RoutingRule other = seen.put(g, r);
				if (other != null) {
					throw new CustomerConfigurationException(file + ": " + other.source + " and " + r.source + " have the same Priority="
							+ r.priority + " for " + (ALL.equals(k) ? "all numbers" : k));
				}
			}
		}
	}
}
