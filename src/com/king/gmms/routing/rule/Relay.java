package com.king.gmms.routing.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import com.king.gmms.GmmsUtility;

/**
 * The Relay of a routing rule: {@code V1} one channel, {@code V1:70,V2:30} by weight, {@code V1,V2} in turn
 * (docs/路由重构设计.md 2.2). Channels are resolved to their ssid when the routing file is loaded; a name that is
 * not a channel gives ssid -1 (the rule then routes to a channel that does not exist).
 */
public final class Relay {
	final String text;
	final int[] ssids;
	final String[] names;
	/** 0 for every channel: in turn; else by weight */
	final int[] weights;
	final boolean weighted;
	private final AtomicInteger turn = new AtomicInteger();

	Relay(String text, int[] ssids, String[] names, int[] weights) {
		this.text = text;
		this.ssids = ssids;
		this.names = names;
		this.weights = weights;
		boolean w = false;
		for (int x : weights) {
			w |= x > 0;
		}
		this.weighted = w;
	}

	public int size() {
		return ssids.length;
	}

	public String text() {
		return text;
	}

	/** the channels as loaded: ssids (-1 for a name that is no channel), with the weights when weighted */
	public String ssidText() {
		StringBuilder b = new StringBuilder();
		for (int i = 0; i < ssids.length; i++) {
			if (i > 0) {
				b.append(',');
			}
			b.append(ssids[i]);
			if (weighted) {
				b.append(':').append(weights[i]);
			}
		}
		return b.toString();
	}

	/**
	 * A channel of the relay, only among those {@code allowed} accepts; 0 when none is.
	 * firstFirst: the first allowed one (a recipient on the routing whitelist, as before).
	 */
	int pick(int ossid, IntPredicate allowed, boolean firstFirst) {
		List<Integer> idx = new ArrayList<Integer>(ssids.length);
		for (int i = 0; i < ssids.length; i++) {
			if (allowed == null || allowed.test(ssids[i])) {
				idx.add(i);
			}
		}
		if (idx.isEmpty()) {
			return 0;
		}
		if (idx.size() == 1 || firstFirst) {
			return ssids[idx.get(0)];
		}
		if (weighted) {
			int total = 0;
			for (int i : idx) {
				total += weights[i];
			}
			if (total <= 0) {
				return ssids[idx.get(0)];
			}
			int r = GmmsUtility.getRandomValue(ossid, total);
			int cum = 0;
			for (int i : idx) {
				cum += weights[i];
				if (r < cum) {
					return ssids[i];
				}
			}
			return ssids[idx.get(idx.size() - 1)];
		}
		int n = turn.getAndIncrement() & Integer.MAX_VALUE;
		return ssids[idx.get(n % idx.size())];
	}

	/** {@code V1:70,V2:30}, {@code V1,V2}, {@code V1}; the part after a ';' is ignored (as before), returned in warn */
	static Relay parse(String text, java.util.function.ToIntFunction<String> ssidOf, List<String> warn) {
		String t = text == null ? "" : text.trim();
		int semi = t.indexOf(';');
		if (semi >= 0) {
			warn.add("Relay=" + t + ": the part after ';' is not used");
			t = t.substring(0, semi).trim();
		}
		if (t.isEmpty()) {
			return null;
		}
		String[] parts = t.split(",");
		List<Integer> ssid = new ArrayList<Integer>();
		List<String> name = new ArrayList<String>();
		List<Integer> weight = new ArrayList<Integer>();
		boolean anyWeight = false, noWeight = false;
		for (String p : parts) {
			p = p.trim();
			if (p.isEmpty()) {
				continue;
			}
			int c = p.indexOf(':');
			String n = c > 0 ? p.substring(0, c).trim() : p;
			int w = 0;
			if (c > 0) {
				w = Integer.parseInt(p.substring(c + 1).trim());
				if (w < 0) {
					throw new IllegalArgumentException("negative weight in Relay=" + text);
				}
				anyWeight = true;
			} else {
				noWeight = true;
			}
			name.add(n);
			ssid.add(ssidOf.applyAsInt(n));
			weight.add(w);
		}
		if (ssid.isEmpty()) {
			return null;
		}
		if (anyWeight && noWeight && ssid.size() > 1) {
			warn.add("Relay=" + text + ": channels without a weight get 0");
		}
		int[] s = new int[ssid.size()], w = new int[ssid.size()];
		for (int i = 0; i < s.length; i++) {
			s[i] = ssid.get(i);
			w[i] = weight.get(i);
		}
		return new Relay(t, s, name.toArray(new String[0]), w);
	}
}
