package com.king.gmms.routing.rule;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.king.framework.SystemLogger;

/** The rules of every routing file, by customer ssid and by file name; replaced as a whole on reload. */
public final class RuleIndex implements RuleRouter.Tables {
	private static final SystemLogger log = SystemLogger.getSystemLogger(RuleIndex.class);
	public static final RuleIndex EMPTY = new RuleIndex(Collections.<RuleSet>emptyList());

	private final Map<Integer, RuleSet> bySsid = new HashMap<Integer, RuleSet>();
	private final Map<String, RuleSet> byName = new HashMap<String, RuleSet>();
	/** ReplaceContentByTemplate: "ossid:rssid" or "ossid:rssid:sender" -> prefix -> template id (as before) */
	private final Map<String, Map<String, String>> templates = new HashMap<String, Map<String, String>>();
	/** ReplaceContentByTemplate of the rule that routed the MT: "ossid:rssid:{layer}:{level}:{group}:{priority}" -> rule */
	private final Map<String, RoutingRule> templateRules = new HashMap<String, RoutingRule>();

	public RuleIndex(Collection<RuleSet> sets) {
		for (RuleSet s : sets) {
			byName.put(s.name, s);
			if (s.ssid > 0) {
				RuleSet other = bySsid.put(s.ssid, s);
				if (other != null) {
					log.warn("routing files {} and {} have the same CCBSSID {}: {} is used", other.name, s.name, s.ssid, s.name);
				}
			}
			for (RoutingRule r : s.all()) {
				if (r.templateId == null || r.layer == RoutingRule.Layer.OP) {
					continue;
				}
				for (int rssid : r.relay.ssids) {
					for (String group : r.keys) {
						templateRules.put(s.ssid + ":" + rssid + ":" + r.layer.label + ":" + r.level + ":" + group + ":" + r.priority, r);
					}
					List<String> keys = new java.util.ArrayList<String>();
					if (r.templateSenders.isEmpty()) {
						keys.add(s.ssid + ":" + rssid);
					} else {
						for (String sender : r.templateSenders) {
							keys.add(s.ssid + ":" + rssid + ":" + sender);
						}
					}
					for (String k : keys) {
						Map<String, String> byPrefix = templates.get(k);
						if (byPrefix == null) {
							byPrefix = new HashMap<String, String>();
							templates.put(k, byPrefix);
						}
						for (String prefix : r.keys) {
							byPrefix.put(prefix, r.templateId);
						}
					}
				}
			}
		}
	}

	/**
	 * The template of the rule that routed the MT (its routingRule "{main layer}|{layer}:{level}:{group}:{priority}"), null
	 * when that rule has none or (IsReplaceTemplateBySender) not for this sender.
	 */
	public String templateOfRule(int ossid, int rssid, String routingRule, String sender) {
		RoutingRule r = templateRules.get(ruleKey(ossid, rssid, routingRule));
		if (r == null) {
			return null;
		}
		if (!r.templateSenders.isEmpty() && (sender == null || !r.templateSenders.contains(sender))) {
			return null;
		}
		return r.templateId;
	}

	private static String ruleKey(int ossid, int rssid, String routingRule) {
		int bar = routingRule.indexOf('|');
		return ossid + ":" + rssid + ":" + (bar < 0 ? routingRule : routingRule.substring(bar + 1));
	}

	/** the content templates of the rules with ReplaceContentByTemplate */
	public Map<String, Map<String, String>> templates() {
		return templates;
	}

	public RuleSet ofCustomer(int ossid) {
		return bySsid.get(ossid);
	}

	public RuleSet byName(String name) {
		return name == null ? null : byName.get(name);
	}

	public Collection<RuleSet> sets() {
		return Collections.unmodifiableCollection(byName.values());
	}
}
