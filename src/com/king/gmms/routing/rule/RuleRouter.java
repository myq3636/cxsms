package com.king.gmms.routing.rule;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.routing.rule.RoutingRule.Layer;
import com.king.message.gmms.GmmsMessage;

/**
 * MT routing by rules (docs/路由重构设计.md 2.1-2.4, 2.7, 2.8):
 * <ul>
 * <li>{@link #main}: layers second → number → operator → main, the customer's file and then the default file of its
 * service type; in a layer the groups most specific first, in a group the rules by Priority; the first rule that
 * matches decides: its channel not usable is a routing failure.</li>
 * <li>{@link #backup}: level by level from the given one; in level N the layer of the main route, then the main
 * layer, the customer's file and then the default file; channels already used are skipped; a rule whose channel
 * cannot be used sends to the next level.</li>
 * </ul>
 * The system routing replacement is applied to each channel a rule picks; "already used" and "usable" are judged on
 * the channel after it.
 */
public final class RuleRouter {
	private static final SystemLogger log = SystemLogger.getSystemLogger(RuleRouter.class);

	public enum Status {
		/** a channel to send to */
		OK,
		/** no rule matched (main route), or no level left (backup) */
		NO_RULE,
		/** a rule matched, its channel does not exist or does not take this content (main route only) */
		UNAVAILABLE,
		/** a rule matched, its channel is in ROP failed mode: the MT waits in SMQ (main route only) */
		ROP_FAILED
	}

	/** the result of a selection */
	public static final class Choice {
		public final Status status;
		/** the channel to send to, after the replacement */
		public final int rssid;
		/** the channel the rule picked */
		public final int ruleRssid;
		public final RoutingRule rule;
		public final int level;
		public final String group;
		public final String file;
		public final String reason;

		Choice(Status status, int rssid, int ruleRssid, RoutingRule rule, int level, String group, String file, String reason) {
			this.status = status;
			this.rssid = rssid;
			this.ruleRssid = ruleRssid;
			this.rule = rule;
			this.level = level;
			this.group = group;
			this.file = file;
			this.reason = reason;
		}

		public boolean ok() {
			return status == Status.OK;
		}

		/** what the message keeps (routingRule): "{layer of the main route}|{layer}:{level}:{group}:{priority}" */
		public String ruleText(Layer mainLayer) {
			if (rule == null) {
				return null;
			}
			return (mainLayer == null ? rule.layer : mainLayer).label + "|" + rule.layer.label + ":" + level + ":" + group + ":"
					+ rule.priority;
		}

		/** for [MT-ROUTE] / [MT-REROUTE] */
		public String describe() {
			if (rule == null) {
				return reason == null ? "no rule" : reason;
			}
			StringBuilder b = new StringBuilder();
			b.append("by ").append(rule.layer.label).append(level == 0 ? "" : " backup" + level).append(' ')
				.append(rule.layer == Layer.OP ? "opid=" : rule.layer == Layer.NUMBER ? "number=" : "prefix=").append(group)
				.append(" priority=").append(rule.priority).append(" file=").append(file);
			if (ruleRssid != rssid) {
				b.append(" rule=").append(ruleRssid).append(" replaced=").append(rssid);
			}
			if (reason != null) {
				b.append(' ').append(reason);
			}
			return b.toString();
		}
	}

	/** the tables of all routing files, swapped as a whole on reload */
	public interface Tables {
		RuleSet ofCustomer(int ossid);

		RuleSet byName(String name);
	}

	private final A2PCustomerManager ctm;

	public RuleRouter(A2PCustomerManager ctm) {
		this.ctm = ctm;
	}

	static String recipient(GmmsMessage msg) {
		String r = msg.getRecipientAddress() == null ? "" : msg.getRecipientAddress().trim();
		return r.startsWith("+") ? r.substring(1) : r;
	}

	/** the customer's routing file, then the default file of its service type */
	private RuleSet[] sets(Tables t, GmmsMessage msg) {
		RuleSet own = t.ofCustomer(msg.getOSsID());
		A2PCustomerInfo cust = ctm.getCustomerBySSID(msg.getOSsID());
		RuleSet def = cust == null ? null : t.byName(cust.getCustomerServiceType() + com.king.gmms.Constant.DEFAULT_ROUTING_SUFFER);
		return new RuleSet[] { own, def };
	}

	/** the main route of a new MT; second: the recipient is in the customer's second-routing cache */
	public Choice main(GmmsMessage msg, boolean second) {
		Tables t = ctm.routingRules();
		String recipient = recipient(msg);
		RoutingRule.KeywordCheck kw = keywords();
		for (RuleSet set : sets(t, msg)) {
			if (set == null) {
				continue;
			}
			for (Layer layer : Layer.values()) {
				if (layer == Layer.SEC && !second || layer == Layer.OP && msg.getRoperator() <= 0) {
					continue;
				}
				Object[] hit = match(set, layer, 0, msg, recipient, kw);
				if (hit == null) {
					continue;
				}
				RoutingRule rule = (RoutingRule) hit[0];
				String group = (String) hit[1];
				boolean first = ctm.routeToFirstChannel(msg.getOSsID(), msg);
				// a recipient on the routing whitelist: the first channel that is not a blackhole (as before)
				int ruleRssid = rule.relay.pick(msg.getOSsID(), first ? s -> !ctm.isBlackhole(s) : null, first);
				int rssid = ruleRssid > 0 ? ctm.replacedChannel(msg, ruleRssid) : ruleRssid;
				String bad = unusable(rssid, msg);
				if (bad != null) {
					return new Choice(Status.UNAVAILABLE, rssid, ruleRssid, rule, 0, group, set.name, bad);
				}
				if (ctm.isInRopFailedMode(rssid)) {
					return new Choice(Status.ROP_FAILED, rssid, ruleRssid, rule, 0, group, set.name, "channel in ROP failed mode");
				}
				return new Choice(Status.OK, rssid, ruleRssid, rule, 0, group, set.name, null);
			}
		}
		return new Choice(Status.NO_RULE, 0, 0, null, 0, null, null, "no routing rule matched");
	}

	/**
	 * A backup channel from level fromLevel on, skipping the channels in used (after the replacement).
	 * mainLayer: the layer the main route came from (null: main layer).
	 */
	public Choice backup(GmmsMessage msg, int fromLevel, Layer mainLayer, Set<Integer> used) {
		Set<Layer> layers = new LinkedHashSet<Layer>();
		if (mainLayer != null && mainLayer != Layer.NUMBER) {
			layers.add(mainLayer);
		}
		layers.add(Layer.MAIN);
		return backup(msg, fromLevel, layers, used);
	}

	/**
	 * The main route found no usable channel (no rule matched, or its channel does not exist / does not take this
	 * content): the backup levels from 1, at routing time. main: what the main route found; second as for main.
	 */
	public Choice backupForNewMt(GmmsMessage msg, Choice main, boolean second) {
		Set<Layer> layers = new LinkedHashSet<Layer>();
		if (main.rule != null) {
			if (main.rule.layer != Layer.NUMBER) {
				layers.add(main.rule.layer);
			}
		} else {
			if (second) {
				layers.add(Layer.SEC);
			}
			if (msg.getRoperator() > 0) {
				layers.add(Layer.OP);
			}
		}
		layers.add(Layer.MAIN);
		return backup(msg, 1, layers, Collections.<Integer>emptySet());
	}

	private Choice backup(GmmsMessage msg, int fromLevel, Set<Layer> layers, Set<Integer> used) {
		Tables t = ctm.routingRules();
		String recipient = recipient(msg);
		RoutingRule.KeywordCheck kw = keywords();
		final Set<Integer> skip = used == null ? Collections.<Integer>emptySet() : used;
		boolean first = ctm.routeToFirstChannel(msg.getOSsID(), msg);
		levels:
		for (int level = Math.max(1, fromLevel); level <= RuleSet.MAX_LEVEL; level++) {
			for (RuleSet set : sets(t, msg)) {
				if (set == null) {
					continue;
				}
				for (Layer layer : layers) {
					if (layer == Layer.OP && msg.getRoperator() <= 0) {
						continue;
					}
					Object[] hit = match(set, layer, level, msg, recipient, kw);
					if (hit == null) {
						continue;
					}
					RoutingRule rule = (RoutingRule) hit[0];
					String group = (String) hit[1];
					int ruleRssid = rule.relay.pick(msg.getOSsID(),
							s -> s <= 0 || !skip.contains(ctm.replacedChannel(msg, s)) && !(first && ctm.isBlackhole(s)), first);
					if (ruleRssid == 0) {
						if (log.isDebugEnabled()) {
							log.debug(msg, "[MT-REROUTE] {} backup{} {}: every channel already used, level not matched", set.name, level, rule.source);
						}
						continue;
					}
					int rssid = ruleRssid > 0 ? ctm.replacedChannel(msg, ruleRssid) : ruleRssid;
					String bad = unusable(rssid, msg);
					if (bad == null && ctm.isInRopFailedMode(rssid)) {
						bad = "channel in ROP failed mode";
					}
					if (bad != null) {
						log.info(msg, "[MT-REROUTE] {} backup{} {} rssid={}: {}, next level", set.name, level, rule.source, rssid, bad);
						continue levels;
					}
					return new Choice(Status.OK, rssid, ruleRssid, rule, level, group, set.name, null);
				}
			}
		}
		return new Choice(Status.NO_RULE, 0, 0, null, 0, null, null, "no backup level left");
	}

	/** null when the channel can take the MT, else why not */
	private String unusable(int rssid, GmmsMessage msg) {
		A2PCustomerInfo r = rssid > 0 ? ctm.getCustomerBySSID(rssid) : null;
		if (r == null) {
			return "channel " + rssid + " not in cm.cfg";
		}
		if (!r.isRSupportContentLenghtAllow(msg)) {
			return "content length not allowed by channel " + rssid;
		}
		return null;
	}

	private RoutingRule.KeywordCheck keywords() {
		return (m, k) -> ctm.contentKeywordMatches(m, k);
	}

	/** [rule, group] of the first rule that matches in this layer and level, or null */
	private Object[] match(RuleSet set, Layer layer, int level, GmmsMessage msg, String recipient, RoutingRule.KeywordCheck kw) {
		if (!set.has(layer, level)) {
			return null;
		}
		List<String> keys = set.groupKeys(layer, level, recipient, msg.getRoperator());
		for (String key : keys) {
			for (RoutingRule r : set.rules(layer, level, key)) {
				StringBuilder why = log.isDebugEnabled() ? new StringBuilder() : null;
				if (r.matches(msg, recipient, kw, why)) {
					return new Object[] { r, key };
				}
				if (why != null && why.length() > 0) {
					log.debug(msg, "[MT-ROUTE] {} not matched by {}", r.source, why);
				}
			}
		}
		return null;
	}
}
