package com.king.gmms.routing.rule;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.king.message.gmms.GmmsMessage;

/**
 * One rule of a routing file (docs/路由重构设计.md 2.2): it matches when every condition written holds and no
 * exclusion does; it then routes by its {@link Relay}. Rules of one group (layer, level, prefix / number / operator)
 * are tried by Priority, smallest first.
 */
public final class RoutingRule {
	/** the layers, tried in this order (2.1) */
	public enum Layer {
		SEC("second"), NUMBER("number"), OP("op"), MAIN("main");

		public final String label;

		Layer(String label) {
			this.label = label;
		}

		public static Layer byLabel(String s) {
			for (Layer l : values()) {
				if (l.label.equals(s)) {
					return l;
				}
			}
			return null;
		}
	}

	/** checks the content keywords of a message (ContentTemplate.checkContent) */
	public interface KeywordCheck {
		boolean matches(GmmsMessage msg, String keywordId);
	}

	final Layer layer;
	/** 0 main route, 1..10 backup level */
	final int level;
	final int priority;
	/** layers SEC, MAIN: prefixes ("all" for every number); NUMBER: numbers; OP: operator ids as text */
	final List<String> keys;
	final Set<String> senders;
	final Set<String> excludeSenders;
	final List<String> keywordIds;
	final List<String> excludeKeywordIds;
	final List<String> excludePrefixes;
	/** ReplaceContentByTemplate, kept as before: the content of an MT to this rule's channels is replaced */
	final String templateId;
	/** IsReplaceTemplateBySender=true: the template only for the Sender values (as written) */
	final List<String> templateSenders;
	final Relay relay;
	/** file, config name and position, for logs and reload errors */
	final String source;

	RoutingRule(Layer layer, int level, int priority, List<String> keys, Set<String> senders, Set<String> excludeSenders,
			List<String> keywordIds, List<String> excludeKeywordIds, List<String> excludePrefixes, String templateId,
			List<String> templateSenders, Relay relay, String source) {
		this.layer = layer;
		this.level = level;
		this.priority = priority;
		this.keys = keys;
		this.senders = senders == null ? Collections.<String>emptySet() : senders;
		this.excludeSenders = excludeSenders == null ? Collections.<String>emptySet() : excludeSenders;
		this.keywordIds = keywordIds == null ? Collections.<String>emptyList() : keywordIds;
		this.excludeKeywordIds = excludeKeywordIds == null ? Collections.<String>emptyList() : excludeKeywordIds;
		this.excludePrefixes = excludePrefixes == null ? Collections.<String>emptyList() : excludePrefixes;
		this.templateId = templateId;
		this.templateSenders = templateSenders == null ? Collections.<String>emptyList() : templateSenders;
		this.relay = relay;
		this.source = source;
	}

	public Layer layer() {
		return layer;
	}

	public int level() {
		return level;
	}

	public int priority() {
		return priority;
	}

	public Relay relay() {
		return relay;
	}

	public String source() {
		return source;
	}

	/**
	 * The conditions and exclusions on the message (the group, i.e. prefix / number / operator, already matched).
	 * why, when not null, gets the condition that failed (for the DEBUG log of an exclusion).
	 */
	boolean matches(GmmsMessage msg, String recipient, KeywordCheck keywords, StringBuilder why) {
		String sender = msg.getSenderAddress() == null ? "" : msg.getSenderAddress().trim().toLowerCase();
		if (!senders.isEmpty() && !senders.contains(sender)) {
			return false;
		}
		if (excludeSenders.contains(sender)) {
			if (why != null) {
				why.append("ExcludeSender=").append(sender);
			}
			return false;
		}
		for (String p : excludePrefixes) {
			if (recipient.startsWith(p)) {
				if (why != null) {
					why.append("ExcludePhonePrefix=").append(p);
				}
				return false;
			}
		}
		if (!keywordIds.isEmpty()) {
			boolean any = false;
			for (String k : keywordIds) {
				if (keywords != null && keywords.matches(msg, k)) {
					any = true;
					break;
				}
			}
			if (!any) {
				return false;
			}
		}
		for (String k : excludeKeywordIds) {
			if (keywords != null && keywords.matches(msg, k)) {
				if (why != null) {
					why.append("ExcludeKeywordId=").append(k);
				}
				return false;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		return source + " Priority=" + priority + " Relay=" + relay.text;
	}
}
