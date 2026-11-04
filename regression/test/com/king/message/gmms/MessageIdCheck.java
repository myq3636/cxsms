package com.king.message.gmms;

import java.lang.reflect.Field;
import java.util.*;

import com.king.gmms.ha.MessageIdSetup;

/** The real generator with a clock under test control: v1 unchanged, v2 format, repeats, restart, clock changes. */
public class MessageIdCheck {
	static int failures = 0;
	static long now = 1793483151000L; // 2026-11-01 05:45:51 here is just a fixed second
	static final MessageIdGenerator G = MessageIdGenerator.getInstance();

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}
	static void setStatic(String name, Object v) throws Exception {
		Field f = MessageIdGenerator.class.getDeclaredField(name);
		f.setAccessible(true);
		f.set(null, v);
	}
	/** a new process: nothing kept but what it is given */
	static void restart(int no, long perSecond, long offset, long saved) throws Exception {
		setStatic("v2Counter", 0L);
		setStatic("v2HexCounter", 0L);
		MessageIdGenerator.enableV2(no, perSecond, offset > 0, saved, 0L);
	}
	static void restartHex(int no, boolean client, long saved) throws Exception {
		setStatic("v2Counter", 0L);
		setStatic("v2HexCounter", 0L);
		MessageIdGenerator.enableV2(no, 5000, client, 0L, saved);
	}
	static Set<Integer> takeHex(int n) {
		Set<Integer> s = new HashSet<Integer>();
		for (int i = 0; i < n; i++) s.add(G.generateHexID(0));
		return s;
	}
	static void v1() throws Exception {
		setStatic("v2ModuleNo", 0);
		setStatic("v2Counter", 0L);
	}
	static Set<String> take(int n) {
		Set<String> s = new HashSet<String>();
		for (int i = 0; i < n; i++) s.add(G.generateDecID(0));
		return s;
	}

	public static void main(String[] a) throws Exception {
		setStatic("clock", (java.util.function.LongSupplier) () -> now);

		// ---- v1: as before ----
		Set<String> v1a = new HashSet<String>();
		Calendar c = new GregorianCalendar();
		for (int i = 0; i < 20; i++) v1a.add(G.generateDecID(1));
		String one = G.generateDecID(1);
		int hourOfMonth = c.get(Calendar.DAY_OF_MONTH) * 24 + c.get(Calendar.HOUR_OF_DAY);
		int sec = c.get(Calendar.MINUTE) * 60 + c.get(Calendar.SECOND);
		int prefix = Integer.parseInt(one.substring(0, 6));
		int expect = (hourOfMonth * 10000 + sec + 5000) % 1000000;
		check(one.length() == 10 && Math.abs(prefix - expect) <= 5, "ID-01a v1 (default): the time and module formula as before: " + one);
		String sid = MessageIdGenerator.generateCommonStringID();
		check(sid.matches("\\d{12,15}"), "ID-01b v1: generateCommonStringID as before: " + sid);
		String mid = MessageIdGenerator.generateCommonMsgID(8888);
		check(mid.matches("8888_\\d{14,17}"), "ID-01c v1: generateCommonMsgID as before: " + mid);
		check(MessageIdGenerator.generateCommonMsgID(8888, 1).matches("8888_\\d{13}2\\d{4}"), "ID-01d v1: the SMPP msgId as before");
		int hv1 = G.generateHexID(1);
		check(hv1 >>> 28 == c.get(Calendar.DAY_OF_WEEK) && (hv1 & 0xC00) == 0x400, "ID-01f v1: generateHexID as before: " + Integer.toHexString(hv1));
		int hv1b = G.generateHexID(4);
		check((hv1b & 0xC00) == 0, "ID-14z control, v1: module 4 does not fit the 2 module bits, it runs into the time: " + Integer.toHexString(hv1b));

		// ---- the old format does collide: what v2 is for ----
		setStatic("msgStrSn", 41);
		String h1 = MessageIdGenerator.generateCommonStringID();
		setStatic("msgStrSn", 41);
		String h2 = MessageIdGenerator.generateCommonStringID();
		check(h1.equals(h2), "ID-12z control, v1: two HTTP modules, same second and sequence: same id: " + h1);

		// ---- v2 format ----
		restart(2, 5000, 0, 0);
		String id = G.generateDecID(99);
		check("2415755000".equals(id), "ID-02a v2 Server2 at second 1793483151: first digit 2, counter 1793483151 x 5000: " + id);
		check(id.matches("[1-6]\\d{9}"), "ID-02b ten decimal digits");
		restart(1, 5000, MessageIdGenerator.V2_COUNTER_SPACE / 2, 0);
		id = G.generateDecID(99);
		check("1915755000".equals(id), "ID-02c v2 Client1: half a cycle further: " + id);
		check(G.generateMaxDecID(1).length() == 13, "ID-13 SMSOptionSupportMaxInMsgID: 13 digits as before, under v2 too");

		// ---- v2 hex ----
		now = 1793483151000L;
		for (int no = 1; no <= 9; no++) {
			restartHex(no, false, 0L);
			String hx = Integer.toHexString(G.generateHexID(99));
			long v = Long.parseLong(hx, 16);
			check(hx.length() == 8 && v >>> 28 == no, "ID-14a v2 hex, module " + no + ": 8 hex digits, module number in the top 4 bits: " + hx);
			check(String.valueOf(v).length() <= 10, "ID-14b as decimal in a DR: at most 10 digits: " + v);
		}
		restartHex(2, false, 0L);
		int sh = G.generateHexID(0);
		restartHex(2, true, 0L);
		int ch = G.generateHexID(0);
		check(Math.abs((sh & 0x0FFFFFFF) - (ch & 0x0FFFFFFF)) == 1 << 27, "ID-14c Client half a cycle from the Server: " + Integer.toHexString(sh) + " " + Integer.toHexString(ch));
		Set<Integer> hexes = new HashSet<Integer>();
		int hexTotal = 0;
		for (int t = 0; t < 2000; t++) {
			now = 1793483151000L + t * 1000L;
			for (boolean client : new boolean[] { false, true })
				for (int no = 1; no <= 9; no++) {
					restartHex(no, client, 0L);
					for (int i = 0; i < 40; i++) { hexes.add(G.generateHexID(0)); hexTotal++; }
				}
		}
		check(hexes.size() == hexTotal, "ID-14d 9 Servers and 9 Clients, 2000 s: " + hexTotal + " hex ids, " + (hexTotal - hexes.size()) + " twice");
		now = 1793483151000L;
		restartHex(1, false, 0L);
		hexes = takeHex(10000);
		check(hexes.size() == 10000, "ID-14e 10000 within one second (v1 repeats after 1024 in 2 s): none twice");
		check(MessageIdGenerator.v2AheadSeconds() == 4, "ID-14f ahead of the clock: " + MessageIdGenerator.v2AheadSeconds());
		long hexMark = MessageIdGenerator.v2HexCounter();
		now += 1000L;
		restartHex(1, false, 0L);
		Set<Integer> hexNo = takeHex(1000);
		hexNo.retainAll(hexes);
		check(hexNo.size() == 1000, "ID-14z2 control: restart without the mark: used again: " + hexNo.size());
		restartHex(1, false, hexMark + 20000);
		Set<Integer> hexYes = takeHex(1000);
		hexYes.retainAll(hexes);
		check(hexYes.isEmpty(), "ID-14g restart with the mark: none used again");
		now = 1793483151000L;
		restartHex(5, false, 0L);
		hexes = takeHex(100);
		now -= 3600000L;
		hexNo = takeHex(100);
		hexNo.retainAll(hexes);
		check(hexNo.isEmpty(), "ID-14h clock back one hour: none used again");
		now = (268435456L / 2000L * 6680L - 1L) * 1000L;
		restartHex(9, false, 268435456L * 6680L - 2);
		List<String> hexEdge = new ArrayList<String>();
		for (int i = 0; i < 3; i++) hexEdge.add(Integer.toHexString(G.generateHexID(0)));
		check(hexEdge.get(0).equals("9fffffff") && hexEdge.get(1).equals("90000000"), "ID-14i past 2^28: back to the start, the module number stays: " + hexEdge);

		// ---- every module, one clock: no id twice ----
		Set<String> all = new HashSet<String>();
		int total = 0;
		for (int t = 0; t < 2000; t++) {
			now = 1793483151000L + t * 1000L;
			for (long off : new long[] { 0L, MessageIdGenerator.V2_COUNTER_SPACE / 2 })
				for (int no = 1; no <= 9; no++) {
					// each module has its own counter: as many processes
					restart(no, 5000, off, 0);
					for (int i = 0; i < 40; i++) { all.add(G.generateDecID(0)); total++; }
				}
		}
		check(all.size() == total, "ID-03 9 Servers and 9 Clients, 2000 s, same clock: " + total + " ids, " + (total - all.size()) + " twice");

		// ---- above K: no wait, no repeat, ahead of the clock ----
		now = 1793483151000L;
		restart(1, 5000, 0, 0);
		long t0 = System.nanoTime();
		all = take(200000);
		double perSecond = 200000 / ((System.nanoTime() - t0) / 1e9);
		System.out.println("INFO generator " + Math.round(perSecond) + " ids per second");
		check(all.size() == 200000, "ID-05a 200000 ids within one second of the clock: none twice");
		check(perSecond > 10000, "ID-05b the generator is no limit at 10000 TPS: " + Math.round(perSecond) + " per second");
		check(MessageIdGenerator.v2AheadSeconds() == 39, "ID-05c ahead of the clock: " + MessageIdGenerator.v2AheadSeconds() + " s");
		now += 100000L;
		id = G.generateDecID(0);
		check(MessageIdGenerator.v2AheadSeconds() == 0, "ID-05d traffic down: the clock catches up: " + MessageIdGenerator.v2AheadSeconds());

		// ---- restart while ahead ----
		now = 1793483151000L;
		restart(1, 5000, 0, 0);
		Set<String> before = take(100000);
		long mark = MessageIdGenerator.v2Counter();
		now += 3000L;
		restart(1, 5000, 0, 0);
		Set<String> without = take(1000);
		int again = 0; for (String s : without) if (before.contains(s)) again++;
		check(again == 1000, "ID-06z control: restart ahead of the clock without the mark: ids used again: " + again);
		restart(1, 5000, 0, mark + 20000);
		Set<String> with = take(1000);
		again = 0; for (String s : with) if (before.contains(s)) again++;
		check(again == 0, "ID-06a restart with the saved mark: none used again");

		// ---- clock back ----
		now = 1793483151000L;
		restart(3, 5000, 0, 0);
		before = take(100);
		now -= 3600000L;
		Set<String> back = take(100);
		again = 0; for (String s : back) if (before.contains(s)) again++;
		check(again == 0, "ID-07a clock back one hour while running: none used again");
		long before2 = MessageIdGenerator.v2Counter();
		restart(3, 5000, 0, before2 + 20000);
		back = take(100);
		again = 0; for (String s : back) if (before.contains(s)) again++;
		check(again == 0, "ID-07b and after a restart, with the mark");

		// ---- end of a cycle ----
		now = (MessageIdGenerator.V2_COUNTER_SPACE / 5000L * 358666L - 1L) * 1000L;   // one second before a multiple of 10^9
		restart(4, 5000, 0, MessageIdGenerator.V2_COUNTER_SPACE * 358666L - 2);
		List<String> edge = new ArrayList<String>();
		for (int i = 0; i < 4; i++) edge.add(G.generateDecID(0));
		check(edge.get(0).equals("4999999999") && edge.get(1).equals("4000000000") && edge.get(2).equals("4000000001"), "ID-10 past 10^9: back to the start, ten digits: " + edge);

		// ---- the warning for a short cycle ----
		check(MessageIdSetup.cycleShorterThan(10000L * 3600, 3600000L, 1800), "ID-05e 10000 TPS (cycle 27.8 h) against 30 h: warns");
		check(!MessageIdSetup.cycleShorterThan(10000L * 3600, 3600000L, 1440), "ID-05f against 1 day: no warning");
		check(!MessageIdSetup.cycleShorterThan(5000L * 3600, 3600000L, 1440), "ID-05g 5000 TPS against 1 day: no warning");

		// ---- HTTP ----
		now = 1793483151000L;
		restart(1, 5000, 0, 0);
		setStatic("msgStrSn", 41);
		String a1 = MessageIdGenerator.generateCommonStringID();
		restart(2, 5000, 0, 0);
		setStatic("msgStrSn", 41);
		String a2 = MessageIdGenerator.generateCommonStringID();
		check(!a1.equals(a2), "ID-12a v2: two HTTP modules, same second and sequence: different: " + a1 + " " + a2);
		check(a1.matches("\\d{10}10042") && a1.length() == 15 && a2.matches("\\d{10}20042"), "ID-12b fixed 15 digits, not more than the longest v1 id: " + a1);
		setStatic("msgidSn", 41);
		restart(1, 5000, 0, 0);
		String m1 = MessageIdGenerator.generateCommonMsgID(8888);
		setStatic("msgidSn", 41);
		restart(2, 5000, 0, 0);
		String m2 = MessageIdGenerator.generateCommonMsgID(8888);
		check(m1.endsWith("10042") && m2.endsWith("20042"), "ID-12c msgId: module number in front of the sequence: " + m1 + " " + m2);
		setStatic("msgidSn", 41);
		String firstSeven = MessageIdGenerator.generateCommonMsgID(8888, 0);
		restart(7, 5000, 0, 0);
		setStatic("msgidSn", 41);
		String seventh = MessageIdGenerator.generateCommonMsgID(8888, 6);
		check(firstSeven.endsWith("20042") && seventh.endsWith("70042"), "ID-15a SMPP msgId of the seventh module: 7, not the position % 6 again: " + seventh);
		setStatic("v2ModuleNo", 0);
		setStatic("msgidSn", 41);
		String v1a2 = MessageIdGenerator.generateCommonMsgID(8888, 0);
		setStatic("msgidSn", 41);
		String v1b2 = MessageIdGenerator.generateCommonMsgID(8888, 6);
		check(v1a2.endsWith("10042") && v1b2.endsWith("10042"), "ID-15z control, v1: first and seventh module share the digit: " + v1a2 + " " + v1b2);
		restart(2, 5000, 0, 0);
		MessageIdGenerator.setIncludeNodeId(true);
		check(MessageIdGenerator.generateCommonInMsgID(8888).matches("8888_0_\\d{13}2\\d{4}"), "ID-12d in-id with MessageIdIncludeNodeId still works");
		MessageIdGenerator.setIncludeNodeId(false);
		check(MessageIdGenerator.generateCommonOutMsgID(8888).matches("8888_\\d{13}2\\d{4}"), "ID-12e out-id: module number too");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
