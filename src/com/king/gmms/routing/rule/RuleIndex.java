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
