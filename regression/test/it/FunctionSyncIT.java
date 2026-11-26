package it;

import java.lang.reflect.Field;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.StreamAttempt;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.recipientlimit.RecipientDailyLimitPolicy;
import com.king.gmms.recipientlimit.RecipientSendCountLimitService;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * The functions of commit bd28bb8 ("function sync") through Core's real MT consumer:
 * RL the 24 h recipient limit by country (SMSOptionRecipientMaxSendCountIn24HByCountry over
 * SMSOptionRecipientMaxSendCountIn24H, the country from the number);
 * NP SMSOptionCustomerTypeCountry: the countries CustomerTypeBySender (PA / NonPA) applies to (65 without it); 65 not
 * listed is NonPA; NonPA: only the global sender blacklist and the customer's own whitelist, else rejected; PA: the
 * customer's whitelist, then the default one (wl, then rp, else rejected; passed when it has no entry for the country);
 * the sender blacklist values kept as written (a space after the colon is part of the value);
 * PH {@phone#N} in SMSOptionContentReplacementPolicy: the last N digits of the recipient;
 * TP spaces of a template's code removed;
 * SL the sender black and white lists matched by a set lookup (equal to an entry, special characters and spaces
 * included) instead of scanning alternations of 50 entries; the rp replacement of the list's customer (-1: every one).
 */
public class FunctionSyncIT extends CoreRetryIT {
	static final int V = 801;

	/** what Core did with an MT: "routed" (to V) with its text and sender, "dr <code>", or "none" */
	static String result(GmmsMessage m) throws Exception {
		final String id = m.getMsgID();
		q.produceSubmitMessage(m);
		waitFor(() -> !inStreams("stream:mt:routed:" + V + ":*", id).isEmpty() || !inStreams("stream:dr:pending:" + m.getOSsID() + ":*", id).isEmpty(), 6000);
		List<GmmsMessage> r = inStreams("stream:mt:routed:" + V + ":*", id);
		if (!r.isEmpty()) return "routed sender=" + r.get(0).getSenderAddress() + " text=[" + r.get(0).getTextContent() + "]";
		List<GmmsMessage> d = inStreams("stream:dr:pending:" + m.getOSsID() + ":*", id);
		if (!d.isEmpty()) return "dr " + d.get(d.size() - 1).getStatusCode();
		return "none";
	}

	static String sendTo(int ossid, String sender, String to, String text) throws Exception {
		return result(msg(ossid, sender, to, text));
	}

	/** the n-th of count sends that was rejected first (0: none) */
	static int firstRejected(int ossid, String to, int count) throws Exception {
		for (int i = 1; i <= count; i++) {
			String r = sendTo(ossid, "18667991226", to, "limit " + i);
			if (r.equals("dr " + GmmsStatus.UNDELIVERY_DAILY_LIMIT.getCode())) return i;   // 2122 in the DR to the customer is 10403
			if (!r.startsWith("routed")) { System.out.println("INFO RL " + ossid + " " + to + " #" + i + ": " + r); return -i; }
		}
		return 0;
	}

	/** the sender list match before 2026-11-26 (A2PCustomerManager.constructSenderListPattern + find, equals) */
	static List<java.util.regex.Pattern> oldPatterns(List<String> prefixes) {
		List<String> l = new ArrayList<String>(prefixes);
		Collections.sort(l, (x, y) -> y.length() - x.length());
		List<java.util.regex.Pattern> list = new ArrayList<java.util.regex.Pattern>();
		StringBuffer buffer = null;
		String spString = "$^()[]{}*+?.\\";
		for (int i = 0; i < l.size(); i++) {
			String exp = l.get(i);
			StringBuilder sb = new StringBuilder();
			for (int j = 0; j < exp.length(); j++) {
				String ch = exp.substring(j, j + 1);
				sb.append(spString.contains(ch) ? "\\" + ch : ch);
			}
			if (i % 50 == 0) { buffer = new StringBuffer(); buffer.append(sb); }
			else buffer.append("|").append(sb);
			if ((i + 1) % 50 == 0 || (i + 1) == l.size()) { list.add(java.util.regex.Pattern.compile(buffer.toString())); buffer = null; }
		}
		return list;
	}
	static boolean oldMatch(List<java.util.regex.Pattern> ps, String sender) {
		for (java.util.regex.Pattern p : ps) {
			java.util.regex.Matcher m = p.matcher(sender);
			while (m.find()) if (sender.equals(m.group())) return true;
		}
		return false;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder(cust("V801", V, "Role: Vendor\n") + cust("BH", 490, "Role: blackhole\n"));
		// RL
		cm.append(cust("C810", 810, "CustomerTypeBySender: VIP\nSMSOptionRecipientMaxSendCountIn24H: 3\nSMSOptionRecipientMaxSendCountIn24HByCountry: (Country=CN,MaxCount=2);(Country=SG,MaxCount=0)\n"));
		cm.append(cust("C811", 811, "CustomerTypeBySender: VIP\nSMSOptionRecipientMaxSendCountIn24H: 2\nSMSOptionRecipientMaxSendCountIn24HByCountry: (Country=ZZ,MaxCount=1)\n"));
		cm.append(cust("C812", 812, "CustomerTypeBySender: VIP\nSMSOptionRecipientMaxSendCountIn24HByCountry: (Country=cn,MaxCount=1)\n"));
		cm.append(cust("C813", 813, "CustomerTypeBySender: VIP\nSMSOptionRecipientMaxSendCountIn24H: 2\n"));
		// NP
		cm.append(cust("C820", 820, "SMSOptionCustomerTypeCountry: 353\n"));   // NonPA (the default) for Ireland
		cm.append(cust("C821", 821, ""));                                       // NonPA, no list: 65 as before
		cm.append(cust("C822", 822, "SMSOptionCustomerTypeCountry: all\n"));
		cm.append(cust("C823", 823, "CustomerTypeBySender: VIP\nSMSOptionCustomerTypeCountry: 44\n"));
		cm.append(cust("C824", 824, "CustomerTypeBySender: PA\nSMSOptionCustomerTypeCountry: 353\n"));   // PA for Ireland only
		cm.append(cust("C825", 825, "CustomerTypeBySender: PA\nSMSOptionCustomerTypeCountry: 65,353\n"));
		cm.append(cust("C826", 826, "CustomerTypeBySender: PA\n"));                                      // PA, no list: 65
		cm.append(cust("C828", 828, "CustomerTypeBySender: PA\nSMSOptionCustomerTypeCountry: 852\n"));
		cm.append(cust("C829", 829, "CustomerTypeBySender: PA\nSMSOptionCustomerTypeCountry: 353\n"));   // PA for Ireland, its own 65 and 91 entries
		cm.append(cust("C832", 832, "CustomerTypeBySender: PA\nSMSOptionCustomerTypeCountry: 65\n"));   // its own rp for OWNRP too
		cm.append(cust("C827", 827, "CustomerTypeBySender: PA\nSMSOptionCustomerTypeCountry: 353\n"));   // PA for Ireland, no whitelist of its own
		// PH, TP
		cm.append(cust("C830", 830, "SMSOptionContentReplacementPolicy: (prefix=86,value=\\{@phone#(\\d+)\\}:\\{@phone#(\\d+)\\})\n"));
		cm.append(cust("C831", 831, "SMSOptionContentReplacementPolicy: (prefix=86,value=\\{@phone#(\\d+)\\}:\\{@phone#(\\d+)\\}|foo:bar)\n"));
		cm.append(cust("C840", 840, "SMSOptionTemplateCodeRegex: \\d{2} \\d{2}\n"));
		appendCm(cm.toString());
		for (int s : new int[] { 810, 811, 812, 813, 820, 821, 822, 823, 824, 825, 826, 827, 828, 829, 830, 831, 832 })
			route("C" + s, s, "A2PMainRouting: (Priority=30,PhonePrefix=86;65;1;44;353;852;91;66,Relay=V801)");
		route("C840", 840, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T9)");
		conf("contentTemplate.cfg", "[T9]\nEnglish: your code is $shortcode$ ok\n");
		StringBuilder big = new StringBuilder("44:UKBAD\n65:SGBAD\n91:INBAD\n86:A.B*C\n86:[X]\n86:a b\n86: LEAD\n86:+8613 800\n86:x$^(y)?{z}\\w\n");
		for (int i = 0; i < 5000; i++) big.append("66:BIG").append(i).append(i % 7 == 0 ? ".*" : "").append('\n');   // 5000 entries for TH
		conf("senderactiondir/-1_sender_blacklist.cfg", big.toString());
		conf("senderactiondir/832_sender_whitelist.cfg", "65:rp:OWNRP:OTHERNEW\n65:wl:A.B c:x\n");
		conf("senderactiondir/829_sender_whitelist.cfg", "65:wl:OWNSG:x\n65:rp:OWNRP:SGNEW\n91:wl:OWNIN:x\n");
		conf("senderactiondir/829_sender_blacklist.cfg", "91:CUSTINBAD\n");
		conf("senderactiondir/820_sender_whitelist.cfg", "353:wl:OWNIE:x\n");
		// a space after the colon: the value was trimmed before this commit
		conf("senderactiondir/821_sender_blacklist.cfg", "86:NOSPBAD\n86: SPBAD\n");
		conf("senderactiondir/-1_sender_whitelist.cfg", "65:wl:DEFOK:x\n44:wl:UKOK:x\n353:rp:IESCAM:LikelyScam\n353:wl:IEWL:x\n91:wl:DEFIN:x\n91:rp:DEFINRP:INNEW\n");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nBlackholeSsid=490\n").getBytes("UTF-8"));
		try {
			start();
			q = StreamQueueManager.getInstance();
			Thread.sleep(800);
			A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();

			// ---- RL: the policy ----
			RecipientDailyLimitPolicy p = RecipientDailyLimitPolicy.parse(3, "(Country=CN,MaxCount=2);(Country=sg,MaxCount=0)");
			check(p.getLimit("CN") == 2 && p.getLimit("SG") == 0 && p.getLimit("US") == 3 && p.getLimit(null) == 3, "RL-01 policy: CN 2, SG 0, others the default 3");
			for (String bad : new String[] { "(Country=ZZ,MaxCount=1)", "(Country=CN,MaxCount=-1)", "(Country=CN)", "(Country=CN,MaxCount=1);(Country=CN,MaxCount=2)", "(Country=CN,Max=1)" }) {
				boolean threw = false;
				try { RecipientDailyLimitPolicy.parse(3, bad); } catch (IllegalArgumentException e) { threw = true; }
				check(threw, "RL-02 rejected: " + bad);
			}
			A2PCustomerInfo c810 = ctm.getCustomerBySSID(810), c811 = ctm.getCustomerBySSID(811), c812 = ctm.getCustomerBySSID(812);
			check(c810.getSmsOptionRecipientMaxSendCountIn24H("8613800000001") == 2 && c810.getSmsOptionRecipientMaxSendCountIn24H("+6591234567") == 0
					&& c810.getSmsOptionRecipientMaxSendCountIn24H("12025550123") == 3, "RL-03 the customer's limit by the recipient's country: CN 2, SG 0, US 3");
			check(c811.getSmsOptionRecipientMaxSendCountIn24H("8613800000001") == 2, "RL-04 a wrong country list (ZZ): ignored, the default 2 for all");
			check(c812.getSmsOptionRecipientMaxSendCountIn24H("8613800000001") == 1 && c812.getSmsOptionRecipientMaxSendCountIn24H("12025550123") == 0,
					"RL-05 only the country list: CN 1 (country code in lower case), others no limit");

			// ---- RL: through Core ----
			int f;
			f = firstRejected(810, "8613800000011", 4);
			check(f == 3, "RL-06 C810 to CN (MaxCount 2): the 3rd rejected (DR 10403): " + f);
			f = firstRejected(810, "12025550111", 5);
			check(f == 4, "RL-07 C810 to US (the default 3): the 4th rejected: " + f);
			f = firstRejected(810, "6591230011", 6);
			check(f == 0, "RL-08 C810 to SG (MaxCount 0): no limit, 6 sent: " + f);
			f = firstRejected(811, "8613800000012", 3);
			check(f == 3, "RL-09 C811 wrong list: the default 2, the 3rd rejected: " + f);
			f = firstRejected(812, "8613800000013", 2);
			check(f == 2, "RL-10 C812 only CN 1: the 2nd rejected: " + f);
			f = firstRejected(812, "12025550113", 4);
			check(f == 0, "RL-11 C812 to US: no limit: " + f);
			f = firstRejected(813, "8613800000014", 3);
			check(f == 3, "RL-12 C813 without a country list: the default 2 as before, the 3rd rejected: " + f);
			check(jedis.ttl("Duplicate:8613800000011") > 86000, "RL-13 the counter Duplicate:{recipient} lives 24 h: " + jedis.ttl("Duplicate:8613800000011"));

			// a redelivered MT (its Core died before the ack) counts its recipient once (StreamAttempt.countOnce before this commit)
			GmmsMessage rd = Batch3IT.pendingMt("8888_RLRD");
			jedis.del("Duplicate:" + rd.getRecipientAddress());
			StreamAttempt.begin(rd, "nodeA:1");
			RecipientSendCountLimitService.isRejected(ctm.getCustomerBySSID(813), rd.getRecipientAddress(), GmmsUtility.getInstance().getRedisClient());
			StreamAttempt.abandon();
			StreamAttempt.begin(rd, "nodeB:1");
			RecipientSendCountLimitService.isRejected(ctm.getCustomerBySSID(813), rd.getRecipientAddress(), GmmsUtility.getInstance().getRedisClient());
			StreamAttempt.abandon();
			check("1".equals(jedis.get("Duplicate:" + rd.getRecipientAddress())),
					"RL-14 a redelivered MT counts its recipient once: " + jedis.get("Duplicate:" + rd.getRecipientAddress()));

			// ---- NP ----
			String r;
			String wl = "dr " + GmmsStatus.REJECTED_SDWL.getCode(), bl = "dr " + GmmsStatus.REJECTED_SDBL.getCode();
			// PA for Ireland only (C824)
			r = sendTo(824, "IEWL", "353851234501", "np"); check(r.equals("routed sender=IEWL text=[np]"), "NP-01 PA for IE, sender in the default wl of 353: sent unchanged: " + r);
			r = sendTo(824, "IESCAM", "353851234502", "np"); check(r.equals("routed sender=LikelyScam text=[np]"), "NP-02 PA for IE, not in wl, an rp rule: sent, sender replaced: " + r);
			r = sendTo(824, "OTHER", "353851234503", "np"); check(r.equals(wl), "NP-03 PA for IE, neither wl nor rp: rejected 10802: " + r);
			r = sendTo(824, "DEFOK", "6591230001", "np"); check(r.equals(wl), "NP-04 PA for IE only, to SG: NonPA (only its own whitelist, none): rejected although in the default wl: " + r);
			r = sendTo(824, "OTHER", "8613800000041", "np"); check(r.startsWith("routed"), "NP-05 PA for IE, to CN (not listed, not 65): the usual check, sent: " + r);
			// PA for both (C825), PA without a list (C826: 65 as before)
			r = sendTo(825, "DEFOK", "6591230002", "np"); check(r.startsWith("routed sender=DEFOK"), "NP-06 PA for 65,353, to SG: the default whitelist: sent: " + r);
			r = sendTo(825, "IESCAM", "353851234504", "np"); check(r.equals("routed sender=LikelyScam text=[np]"), "NP-07 PA for 65,353, to IE: rp: " + r);
			r = sendTo(826, "DEFOK", "6591230003", "np"); check(r.startsWith("routed sender=DEFOK"), "NP-08 PA without a list, to SG: PA as before: " + r);
			r = sendTo(826, "SGBAD", "6591230004", "np"); check(r.equals(bl), "NP-09 PA, to SG, sender in the global blacklist: rejected 10801: " + r);
			// PA for Ireland (C829): to SG it is NonPA, only its own whitelist counts
			r = sendTo(829, "OWNSG", "6591230011", "np"); check(r.equals("routed sender=OWNSG text=[np]"), "NP-22 PA for IE, to SG, sender in its own wl of 65: sent: " + r);
			r = sendTo(829, "OWNRP", "6591230012", "np"); check(r.equals("routed sender=SGNEW text=[np]"), "NP-23 PA for IE, to SG, its own rp of 65: sent, sender replaced: " + r);
			r = sendTo(829, "DEFOK", "6591230013", "np"); check(r.equals(wl), "NP-24 PA for IE, to SG, sender only in the default wl of 65: rejected (NonPA): " + r);
			r = sendTo(829, "SGBAD", "6591230014", "np"); check(r.equals(bl), "NP-25 PA for IE, to SG, global blacklist: rejected 10801: " + r);
			// to India (91, not listed, not 65): the usual check, its own whitelist first
			r = sendTo(829, "OWNIN", "919812345601", "np"); check(r.equals("routed sender=OWNIN text=[np]"), "NP-26 PA for IE, to IN, its own wl of 91: sent: " + r);
			r = sendTo(829, "DEFIN", "919812345602", "np"); check(r.equals(wl), "NP-27 PA for IE, to IN, its own whitelist has 91 but not this sender: rejected (the default one not asked): " + r);
			r = sendTo(829, "CUSTINBAD", "919812345603", "np"); check(r.equals(bl), "NP-28 PA for IE, to IN, its own blacklist of 91: rejected 10801: " + r);
			r = sendTo(829, "INBAD", "919812345604", "np"); check(r.equals(bl), "NP-29 PA for IE, to IN, the global blacklist of 91: rejected 10801: " + r);
			// C827: PA for Ireland without a whitelist of its own: to IN the default whitelist
			r = sendTo(827, "DEFIN", "919812345605", "np"); check(r.equals("routed sender=DEFIN text=[np]"), "NP-30 no whitelist of its own, to IN, in the default wl of 91: sent: " + r);
			r = sendTo(827, "DEFINRP", "919812345606", "np"); check(r.equals("routed sender=INNEW text=[np]"), "NP-31 to IN, the default rp of 91: sent, sender replaced: " + r);
			r = sendTo(827, "OTHER", "919812345607", "np"); check(r.equals(wl), "NP-32 to IN, neither wl nor rp in the default whitelist of 91: rejected: " + r);
			r = sendTo(827, "OTHER", "8613800000046", "np"); check(r.startsWith("routed"), "NP-33 to CN, no 86 entry anywhere: sent: " + r);
			// PA for a country the default whitelist has nothing for: passed
			r = sendTo(828, "OTHER", "85291230001", "np"); check(r.startsWith("routed"), "NP-10 PA for 852, the default whitelist has no 852: sent: " + r);
			// NonPA for Ireland (C820, with its own whitelist 353:wl:OWNIE)
			r = sendTo(820, "OWNIE", "353851234505", "np"); check(r.equals("routed sender=OWNIE text=[np]"), "NP-11 NonPA for IE, in its own whitelist: sent: " + r);
			r = sendTo(820, "IEWL", "353851234506", "np"); check(r.equals(wl), "NP-12 NonPA for IE, only in the default wl: rejected: " + r);
			r = sendTo(820, "IESCAM", "353851234507", "np"); check(r.equals(wl), "NP-13 NonPA for IE, a default rp rule: rejected (no replacement): " + r);
			r = sendTo(820, "DEFOK", "6591230005", "np"); check(r.equals(wl), "NP-14 NonPA, to SG: NonPA as before: rejected: " + r);
			r = sendTo(820, "SGBAD", "6591230006", "np"); check(r.equals(bl), "NP-15 NonPA, to SG, global blacklist: rejected 10801: " + r);
			// NonPA without a list, all, VIP
			r = sendTo(821, "DEFOK", "6591230007", "np"); check(r.equals(wl), "NP-16 NonPA without a list, to SG: rejected as before: " + r);
			r = sendTo(821, "OTHER", "8613800000042", "np"); check(r.startsWith("routed"), "NP-17 NonPA without a list, to CN: sent as before: " + r);
			r = sendTo(822, "OTHER", "8613800000043", "np"); check(r.equals(wl), "NP-18 NonPA for all, to CN, not in its own whitelist: rejected: " + r);
			r = sendTo(823, "OTHER", "6591230008", "np"); check(r.startsWith("routed"), "NP-19 VIP: no sender check: " + r);
			// the blacklist values as written: '86: SPBAD' is ' SPBAD'
			r = sendTo(821, "NOSPBAD", "8613800000044", "np"); check(r.equals(bl), "NP-20a customer blacklist 86:NOSPBAD: rejected: " + r);
			r = sendTo(821, "SPBAD", "8613800000045", "np"); check(r.startsWith("routed"), "NP-20b customer blacklist '86: SPBAD' keeps the space: sender SPBAD not in it, sent: " + r);
			// no default whitelist at all (no -1 file)
			Field wlf = A2PCustomerManager.class.getDeclaredField("ssidPrefixListPatternForSenderWLList");
			wlf.setAccessible(true);
			@SuppressWarnings("unchecked")
			Map<String, Object> wlMap = (Map<String, Object>) wlf.get(ctm);
			Object saved = wlMap.remove("-1");
			GmmsMessage ie = msg(824, "OTHER", "353851234508", "np");
			r = result(ie);
			check(r.startsWith("routed") && logLine(ie.getMsgID(), "NullPointerException") == null,
					"NP-21 PA for IE without any default whitelist: sent (nothing to check against), no NullPointerException: " + r);
			if (saved != null) wlMap.put("-1", saved);

			// ---- SL: the sender lists by a set lookup ----
			Random rnd = new Random(7);
			String chars = "ABab01 .*+?$^()[]{}\\-_:/@#";
			List<String> entries = new ArrayList<String>();
			for (int i = 0; i < 3000; i++) {
				StringBuilder e = new StringBuilder();
				for (int j = 0, n = 1 + rnd.nextInt(8); j < n; j++) e.append(chars.charAt(rnd.nextInt(chars.length())));
				entries.add(e.toString());
			}
			for (int i = 0; i < 500; i++) entries.add(entries.get(i) + "X");   // entries that are prefixes of others
			List<java.util.regex.Pattern> old = oldPatterns(entries);
			Set<String> set = new HashSet<String>(entries);
			int probes = 0, differ = 0;
			for (int i = 0; i < 4000; i++) {
				String probe = i % 2 == 0 ? entries.get(rnd.nextInt(entries.size())) : entries.get(rnd.nextInt(entries.size())) + (i % 4 == 1 ? "" : "Z");
				if (i % 8 == 3) probe = probe.substring(0, Math.max(1, probe.length() - 1));
				probes++;
				if (oldMatch(old, probe) != set.contains(probe)) { differ++; if (differ < 4) System.out.println("INFO SL differs: [" + probe + "]"); }
			}
			check(differ == 0, "SL-01 3500 entries of letters, digits, spaces, regex characters, prefixes of each other: the set gives the old answer for "
					+ probes + " senders: " + differ + " differ");
			r = sendTo(821, "A.B*C", "8613800000051", "sl"); check(r.equals(bl), "SL-02 blacklist A.B*C: the sender A.B*C rejected: " + r);
			r = sendTo(821, "AxBBC", "8613800000052", "sl"); check(r.startsWith("routed"), "SL-03 . and * are not wildcards: AxBBC sent: " + r);
			r = sendTo(821, "[X]", "8613800000053", "sl"); check(r.equals(bl), "SL-04 blacklist [X]: rejected: " + r);
			r = sendTo(821, "a b", "8613800000054", "sl"); check(r.equals(bl), "SL-05 blacklist 'a b' (a space inside): rejected: " + r);
			r = sendTo(821, "ab", "8613800000055", "sl"); check(r.startsWith("routed"), "SL-06 'ab' is not 'a b': sent: " + r);
			r = sendTo(821, " LEAD", "8613800000056", "sl"); check(r.equals(bl), "SL-07 '86: LEAD' is ' LEAD': the sender ' LEAD' rejected: " + r);
			r = sendTo(821, "LEAD", "8613800000057", "sl"); check(r.startsWith("routed"), "SL-08 and 'LEAD' sent: " + r);
			r = sendTo(821, "+8613 800", "8613800000058", "sl"); check(r.equals(bl), "SL-09 blacklist '+8613 800': rejected: " + r);
			r = sendTo(821, "x$^(y)?{z}\\w", "8613800000059", "sl"); check(r.equals(bl), "SL-10 blacklist x$^(y)?{z}\\w: rejected: " + r);
			r = sendTo(832, "OWNRP", "6591230021", "sl"); check(r.equals("routed sender=OTHERNEW text=[sl]"), "SL-11 rp of C832 for OWNRP: its own replacement OTHERNEW: " + r);
			r = sendTo(829, "OWNRP", "6591230022", "sl"); check(r.equals("routed sender=SGNEW text=[sl]"), "SL-12 rp of C829 for the same sender: its own SGNEW (one table for all customers before): " + r);
			r = sendTo(832, "A.B c", "6591230023", "sl"); check(r.equals("routed sender=A.B c text=[sl]"), "SL-13 whitelist 'A.B c': sent: " + r);
			r = sendTo(832, "AxB c", "6591230024", "sl"); check(r.equals(wl), "SL-14 'AxB c' not in it: rejected: " + r);
			r = sendTo(821, "BIG4999", "66912345601", "sl"); check(r.equals(bl), "SL-15 5000 entries for 66, the last: rejected: " + r);
			r = sendTo(821, "BIG7.*", "66912345602", "sl"); check(r.equals(bl), "SL-16 entry BIG7.* (as written): rejected: " + r);
			r = sendTo(821, "BIG7XX", "66912345603", "sl"); check(r.startsWith("routed"), "SL-17 BIG7XX (.* is not a wildcard): sent: " + r);
			GmmsMessage tm = msg(821, "NOTINBIG", "66912345604", "t");
			for (int i = 0; i < 2000; i++) ctm.doSenderBlacklistCheck(-1, tm);
			long t0 = System.nanoTime();
			for (int i = 0; i < 20000; i++) ctm.doSenderBlacklistCheck(-1, tm);
			double us = (System.nanoTime() - t0) / 1000.0 / 20000;
			System.out.println("INFO SL-18 doSenderBlacklistCheck with 5000 entries for the country: " + String.format("%.2f", us) + " us");
			check(us < 10, "SL-18 the default blacklist with 5000 entries for the country: " + String.format("%.2f", us) + " us a check (30-70 us scanning before)");

			// ---- PH ----
			r = sendTo(830, "S", "8613800001234", "code for {@phone#4} ok"); check(r.endsWith("text=[code for 1234 ok]"), "PH-01 {@phone#4}: the last 4 digits: " + r);
			r = sendTo(830, "S", "+8613800005678", "a {@phone#2} b {@phone#3}"); check(r.endsWith("text=[a 78 b 678]"), "PH-02 two of them, + removed: " + r);
			r = sendTo(830, "S", "8613800001234", "all {@phone#18}"); check(r.endsWith("text=[all 8613800001234]"), "PH-03 more digits than the number: the whole number: " + r);
			r = sendTo(830, "S", "12025550123", "us {@phone#4}"); check(r.endsWith("text=[us {@phone#4}]"), "PH-04 another prefix: unchanged: " + r);
			r = sendTo(830, "S", "8613800001234", "big {@phone#19}"); check(r.endsWith("text=[big {@phone#19}]"), "PH-05 N over 18: unchanged (error logged), still sent: " + r);
			r = sendTo(831, "S", "8613800001234", "foo {@phone#4}"); check(r.endsWith("text=[bar 1234]"), "PH-06 with another replacement of the prefix: both: " + r);

			// ---- TP ----
			r = sendTo(840, "S", "8613800000031", "code 12 34 here"); check(r.endsWith("text=[your code is 1234 ok]"), "TP-01 code '12 34' (SMSOptionTemplateCodeRegex): spaces removed: " + r);
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		}
		exit();
	}
}
