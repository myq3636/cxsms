package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.messagequeue.CoreMQMStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * Replacements on the way out of Core: SMSOptionContentReplacementPolicy (customer), ReplaceContentByTemplate (routing rule,
 * contentTemplate.cfg), SMSOptionOriNumberLenControl (channel), SenderReplacement (senderReplacement.cfg, customer and
 * channel side), SMSOption*Prefix4NewMT / Prefix4DRMO, the channel's lists. What the channel's outbound stream gets.
 * The channel side ones (C, D6, prefixes, lists) lived in the internal Core->Client session of the old modules (InternalCoreEngineSession
 * sendNewMessage, f49bd6d), which stream mode never calls.
 */
public class ReplacementIT extends CoreContentIT {
	static GmmsMessage ucs2(int ossid, String sender, String recipient, String text) throws Exception {
		GmmsMessage m = msg(ossid, sender, recipient, text);
		m.setContentType(GmmsMessage.AIC_CS_UCS2);
		m.setMessageSize(text.getBytes(GmmsMessage.AIC_CS_UCS2).length);
		return m;
	}
	static String text(GmmsMessage g) { return g == null ? "none" : "[" + g.getTextContent() + "]"; }
	static boolean is(GmmsMessage g, String text) { return g != null && text.equals(g.getTextContent()); }
	/** the status of the OUT_SUBMIT CDR of a refused MT */
	static String cdrStatus(String id) throws Exception {
		waitFor(() -> !cdrLines(id, "OUT_SUBMIT").isEmpty(), 4000);
		List<String> l = cdrLines(id, "OUT_SUBMIT");
		if (l.isEmpty()) return "none";
		for (String c : new String[] { "2111", "2112", "2121", "2201" }) if (l.get(0).contains("," + c + ",")) return c;
		return "other";
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		String retry = "SMSOptionSubmitRetryCode: 2500\nSMSOptionMTRetryPolicy: 1S,1S\n";
		StringBuilder cm = new StringBuilder();
		cm.append(cust("V801", 801, ""));
		cm.append(cust("V802", 802, "SMSOptionOriNumberLenControl: (Min=8,Max=11,Suffix=00)\n"));
		cm.append(cust("V803", 803, "SMSOptionAdvancedSenderReplacement: true\n"));
		cm.append(cust("V804", 804, "ChlMsgSplit: yes\nSMSOptionAsciiMsgSplitLen: 70\nMsgSplitLen: 70\n"));
		cm.append(cust("V807", 807, retry));
		cm.append(cust("V808", 808, ""));
		cm.append(cust("V805", 805, "SMSOptionAddOriPrefix4NewMT: 00\nSMSOptionRemoveRecPrefix4NewMT: 86\n"));
		cm.append(cust("V806", 806, "ChlMsgSplit: yes\nSMSOptionAsciiMsgSplitLen: 70\nMsgSplitLen: 70\nSMSOptionAdvancedSenderReplacement: true\n"));
		cm.append(cust("V809", 809, ""));
		cm.append(cust("V816", 816, ""));
		cm.append(cust("V817", 817, retry + "SMSOptionAddOriPrefix4NewMT: 11\n"));
		cm.append(cust("V818", 818, "SMSOptionAddOriPrefix4NewMT: 22\n"));
		cm.append(cust("V815", 815, "CustomerTypeBySender: VIP\n"));
		// A: content replacement policy
		cm.append(cust("R810", 810, "SMSOptionContentReplacementPolicy: (prefix=all,value=foo:bar|hello^world:hi|spam|你好:您好)"
				+ ";(prefix=86,value=baz:qux|\\{@phone#(\\d+)\\}:\\{@phone#(\\d+)\\})"
				+ ";(prefix=8613,value=one:two);(prefix=852,value=hk:HKG|ab:AB)\n"));
		cm.append(cust("R811", 811, "SMSOptionContentReplacementPolicy: (prefix=all,value=price:$5|foo:bar)\n"));
		cm.append(cust("R812", 812, "SMSOptionContentReplacementPolicy: (prefix=all,value=[0-9]{6}:******)\n"));
		cm.append(cust("R813", 813, "SMSOptionContentReplacementPolicy: (prefix=all,value=LONGWORD:0123456789012345678901234567890123456789)\n"));
		cm.append(cust("R814", 814, "SMSOptionContentReplacementPolicy: (prefix=all,value=foo:foofoo)\n"));
		// B: templates
		for (int c : new int[] { 820, 821, 822, 823, 824, 825, 827, 828, 829 }) cm.append(cust("R" + c, c, ""));
		cm.append(cust("R826", 826, "SMSOptionTemplateCodeRegex: [A-Z]{2}[0-9]{4}\n"));
		// C, D
		cm.append(cust("R830", 830, ""));
		cm.append(cust("R840", 840, "SMSOptionAdvancedSenderReplacement: true\n"));
		cm.append(cust("R841", 841, ""));
		cm.append(cust("R842", 842, ""));
		for (int c : new int[] { 845, 846, 847, 848, 849, 853 }) cm.append(cust("R" + c, c, ""));
		cm.append(cust("R850", 850, "SMSOptionAddOriPrefix4DRMO: 99\n"));
		cm.append(cust("R851", 851, "SMSOptionContentReplacementPolicy: (prefix=all,value=foo:foofoo)\n"));
		cm.append(cust("R852", 852, "SMSOptionContentReplacementPolicy: (prefix=all,value=([0-9]{3})[0-9]{4}:$1****)\n"));
		appendCm(cm.toString());

		for (int c : new int[] { 810, 811, 812 }) route("R" + c, c, "A2PMainRouting: (Priority=30,PhonePrefix=86;852,Relay=V801)");
		route("R813", 813, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V804)");
		route("R814", 814, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V807)", "A2PBackupRouting: (Priority=30,PhonePrefix=86,Relay=V808)");
		route("R820", 820, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T1)");
		route("R821", 821, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T2)");
		route("R822", 822, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T3)");
		route("R823", 823, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T86);(Priority=30,PhonePrefix=8613,Relay=V801,ReplaceContentByTemplate=T8613)");
		route("R824", 824, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T1);(Priority=30,PhonePrefix=8613,Relay=V801)");
		route("R825", 825, "A2PMainRouting: (Priority=30,PhonePrefix=86,Sender=SND1,Relay=V801,ReplaceContentByTemplate=T1,IsReplaceTemplateBySender=true);(Priority=31,PhonePrefix=86,Relay=V801)");
		route("R826", 826, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T1)");
		route("R827", 827, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T5)");
		route("R828", 828, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V807,ReplaceContentByTemplate=T1)", "A2PBackupRouting: (Priority=30,PhonePrefix=86,Relay=V808)");
		route("R829", 829, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801,ReplaceContentByTemplate=T9)");
		route("R830", 830, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V802)");
		route("R840", 840, "A2PMainRouting: (Priority=30,PhonePrefix=86;852,Relay=V801)");
		route("R841", 841, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801)");
		route("R842", 842, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V803)");
		route("R845", 845, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V805)");
		route("R846", 846, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V806)");
		route("R847", 847, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V809)");
		route("R848", 848, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V816)");
		route("R849", 849, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V817)", "A2PBackupRouting: (Priority=30,PhonePrefix=86,Relay=V818)");
		route("R850", 850, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V803)");
		route("R851", 851, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V817)");
		route("R852", 852, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V801)");
		route("R853", 853, "A2PMainRouting: (Priority=30,PhonePrefix=86,Relay=V815)");

		conf("contentTemplate.cfg", "[T1]\nEnglish: your code is $shortcode$ ok\nUNICODE: 验证码$shortcode$\n\n"
				+ "[T2]\nEnglish: sender tpl $shortcode$\nSender: TPLSENDER\n\n"
				+ "[T3_a]\nEnglish: pool a $shortcode$\nSender: POOLSENDER\n\n"
				+ "[T86]\nEnglish: T86 $shortcode$\n\n[T8613]\nEnglish: T8613 $shortcode$\n\n"
				+ "[T5]\nUNICODE: 只有中文$shortcode$\n");
		conf("senderReplacement.cfg", "[S840]\nCCBSSID: 840\nSenderReplacement: (PhonePrefix=86,SenderAddress=OLD1,ReplaceTo=NEW1,Ossid=840,Rssid=-1)"
				+ ";(PhonePrefix=8613,SenderAddress=OLD1,ReplaceTo=NEW13,Ossid=840,Rssid=-1);(PhonePrefix=all,SenderAddress=all,ReplaceTo=ALLNEW,Ossid=840,Rssid=-1)\n\n"
				+ "[S841]\nCCBSSID: 841\nSenderReplacement: (PhonePrefix=86,SenderAddress=OLD1,ReplaceTo=NEW1,Ossid=841,Rssid=-1)\n\n"
				+ "[S803]\nCCBSSID: 803\nSenderReplacement: (PhonePrefix=86,SenderAddress=OLD3,ReplaceTo=NEW3,Ossid=842,Rssid=803);(PhonePrefix=86,SenderAddress=OLD3,ReplaceTo=NEW3X,Ossid=-1,Rssid=803)\n\n"
				+ "[S806]\nCCBSSID: 806\nSenderReplacement: (PhonePrefix=86,SenderAddress=OLD6,ReplaceTo=NEW6,Ossid=-1,Rssid=806)\n");
		conf("senderactiondir/809_sender_blacklist.cfg", "86:VBAD\n");
		conf("senderactiondir/815_sender_blacklist.cfg", "86:VBAD\n");
		conf("senderactiondir/816_sender_whitelist.cfg", "86:wl:VGOOD:x\n");
		conf("recipientactiondir/809_recipient_blacklist.cfg", "8613899999999\n");
		conf("contentactiondir/809_content_blacklist.cfg", "86:vendorbad\n");
		conf("senderactiondir/-1_sender_blacklist.cfg", "");
		conf("senderactiondir/-1_sender_whitelist.cfg", "");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\n").getBytes("UTF-8"));
		msmFactory = RecordingStore::new;
		final String N13 = "8613812345678", N15 = "8615912345678", HK = "85212345678";
		try {
			start();
			q = StreamQueueManager.getInstance();
			ResultStreamConsumer.getInstance().start();
			CoreMQMStreamConsumer.getInstance().start();
			com.king.gmms.messagequeue.InboundDRStreamConsumer.getInstance().start();
			Thread.sleep(800);
			GmmsMessage g;

			// ================= A. SMSOptionContentReplacementPolicy =================
			g = sent(msg(810, "S", N15, "foo hello world spam end"), 801);
			check(g != null && g.getTextContent().startsWith("bar hi") && !g.getTextContent().contains("spam") && g.getTextContent().endsWith("end"),
					"CR-01 prefix=all: foo:bar, hello^world (^ is a space), spam alone is removed: " + text(g));
			g = sent(msg(810, "S", N15, "baz hk"), 801);
			check(is(g, "qux hk"), "CR-02a prefix=86: applies to 86 only: " + text(g));
			g = sent(msg(810, "S", HK, "baz hk"), 801);
			check(is(g, "baz HKG"), "CR-02b prefix=852: its own group, not 86's: " + text(g));
			g = sent(msg(810, "S", HK, "ab"), 801);
			check(is(g, "AB"), "CR-02c ab:AB, a change of case only (key and value equal ignoring case are taken as a phone pattern): " + text(g));
			g = sent(msg(810, "S", N13, "baz one"), 801);
			check(is(g, "qux two"), "CR-03 8613 number: both prefix=86 and prefix=8613 apply: " + text(g));
			g = sent(msg(810, "S", N15, "code {@phone#4} end"), 801);
			check(is(g, "code 5678 end"), "CR-04a {@phone#4}: the last 4 digits of the recipient: " + text(g));
			g = sent(msg(810, "S", N13, "code {@phone#4} end"), 801);
			check(is(g, "code 5678 end"), "CR-04b the same to 8613: prefix=8613's one:two must not change the placeholder first: " + text(g));
			g = sent(msg(810, "S", "+" + N15, "baz"), 801);
			check(is(g, "qux"), "CR-05 recipient with +: matches prefix=86: " + text(g));
			g = sent(ucs2(810, "S", N15, "你好世界"), 801);
			check(is(g, "您好世界") && g.getMessageSize() == 8, "CR-06 UCS2 text replaced, size in UCS2 bytes: " + show(g));
			g = sent(msg(812, "S", N15, "code 123456 end"), 801);
			check(is(g, "code ****** end"), "CR-07 the key is a regex: [0-9]{6}: " + text(g));
			g = sent(msg(811, "S", N15, "price foo"), 801);
			check(is(g, "$5 bar"), "CR-08 a value with $ (taken as a group reference by replaceAll): " + text(g));
			GmmsMessage lm = msg(813, "S", N15, "head LONGWORD " + "x".repeat(40));
			q.produceSubmitMessage(lm);
			final String lid = lm.getMsgID();
			waitFor(() -> inStreams("stream:mt:routed:804:*", lid).size() >= 2, 6000);
			List<GmmsMessage> lp = inStreams("stream:mt:routed:804:*", lid);
			StringBuilder lt = new StringBuilder(); for (GmmsMessage p : lp) lt.append(p.getTextContent());
			check(lp.size() == 2 && lt.toString().contains("0123456789012345678901234567890123456789"),
					"CR-09 replaced to 86 characters on a channel splitting by 70: split after the replacement: " + lp.size() + " parts");
			GmmsMessage r1 = sent(msg(814, "S", N15, "foo"), 807);
			check(is(r1, "foofoo"), "CR-10a foo:foofoo on the first send: " + text(r1));
			if (r1 != null) {
				fail(r1, GmmsStatus.COMMUNICATION_ERROR);
				GmmsMessage b1 = out(808, r1.getMsgID(), 1);
				check(is(b1, "foofoo"), "CR-10b the backup route gets it replaced once, not twice: " + text(b1));
			}

			// ================= B. ReplaceContentByTemplate =================
			g = sent(msg(820, "S", N15, "your otp 1234"), 801);
			check(is(g, "your code is 1234 ok"), "TP-01 code of 4 digits into the English template: " + text(g));
			g = sent(msg(820, "S", N15, "otp 12-34"), 801);
			check(is(g, "your code is 1234 ok"), "TP-02 12-34: the - removed: " + text(g));
			g = sent(ucs2(820, "S", N15, "验证码 5678"), 801);
			check(is(g, "验证码5678") && GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(g.getContentType()) && !g.isGsm7bit(),
					"TP-03 UCS2 message: the UNICODE template, UCS2: " + show(g));
			g = sent(msg(827, "S", N15, "otp 4321"), 801);
			check(is(g, "只有中文4321") && GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(g.getContentType()),
					"TP-04 template with UNICODE only, ASCII message: the UNICODE text, UCS2: " + show(g));
			g = sent(msg(821, "S", N15, "otp 1234"), 801);
			check(is(g, "sender tpl 1234"), "TP-05a template T2: text: " + text(g));
			check(g != null && "TPLSENDER".equals(g.getSenderAddress()), "TP-05b template T2 with Sender: the sender replaced: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(822, "S", N15, "otp 1234"), 801);
			check(is(g, "pool a 1234") && "POOLSENDER".equals(g.getSenderAddress()),
					"TP-06 T3 not a template, T3_a is: taken from the pool, with its sender: " + show(g));
			g = sent(msg(823, "S", N13, "otp 1234"), 801);
			GmmsMessage g2 = sent(msg(823, "S", N15, "otp 1234"), 801);
			check(is(g, "T8613 1234") && is(g2, "T86 1234"), "TP-07 rules 86 (T86) and 8613 (T8613): the template of the rule that routed it: " + text(g) + " " + text(g2));
			g = sent(msg(824, "S", N13, "otp 1234"), 801);
			check(is(g, "otp 1234"), "TP-08 8613 routed by the rule without a template (86 has one, same channel): not replaced: " + text(g));
			g = sent(msg(825, "SND1", N15, "otp 1234"), 801);
			g2 = sent(msg(825, "SND2", N15, "otp 1234"), 801);
			check(is(g, "your code is 1234 ok") && is(g2, "otp 1234"), "TP-09 IsReplaceTemplateBySender, Sender=SND1: SND1 replaced, SND2 not: " + text(g) + " " + text(g2));
			g = sent(msg(826, "S", N15, "ref AB1234 x"), 801);
			check(is(g, "your code is AB1234 ok"), "TP-10 SMSOptionTemplateCodeRegex [A-Z]{2}[0-9]{4}: " + text(g));
			GmmsMessage t1 = sent(msg(828, "S", N15, "otp 2468"), 807);
			check(is(t1, "your code is 2468 ok"), "TP-11a primary channel with the template: replaced: " + text(t1));
			if (t1 != null) {
				fail(t1, GmmsStatus.COMMUNICATION_ERROR);
				GmmsMessage tb = out(808, t1.getMsgID(), 1);
				check(is(tb, "otp 2468"), "TP-11b backup route without a template: the customer's text: " + text(tb));
			}
			GmmsMessage nt = msg(829, "S", N15, "otp 1357");
			q.produceSubmitMessage(nt);
			String d = drOf(829, nt.getMsgID());
			check(out(801, nt.getMsgID(), 1) == null && String.valueOf(GmmsStatus.REJECT_TEMPLATE_ERROR.getCode()).equals(d),
					"TP-12 template T9 in no file: not sent, DR " + d);

			// ================= C. SMSOptionOriNumberLenControl (channel V802: Min 8, Max 11, Suffix 00) =================
			g = sent(msg(830, "1234567", N15, "hi"), 802);
			check(g != null && "123456700".equals(g.getSenderAddress()), "OL-01 7 digits, under Min 8: Suffix 00 appended: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(830, "123456789012", N15, "hi"), 802);
			check(g != null && "12345678901".equals(g.getSenderAddress()), "OL-02 12 digits, over Max 11: cut to 11: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(830, "12345678", N15, "hi"), 802);
			check(g != null && "12345678".equals(g.getSenderAddress()), "OL-03 8 digits: unchanged: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(830, "ABC", N15, "hi"), 802);
			check(g != null && "ABC00".equals(g.getSenderAddress()), "OL-05 alphanumeric ABC: Suffix appended too: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(841, "ABC", N15, "hi"), 801);
			check(g != null && "ABC".equals(g.getSenderAddress()), "OL-04 a channel without the option: unchanged");

			// ================= D. sender and recipient replacement =================
			g = sent(msg(840, "OLD1", N13, "hi"), 801);
			check(g != null && "NEW13".equals(g.getSenderAddress()), "SR-01 SenderReplacement, customer side: 8613 is the longest prefix: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(840, "OLD1", N15, "hi"), 801);
			check(g != null && "NEW1".equals(g.getSenderAddress()), "SR-02 8615: PhonePrefix=86: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(840, "OTHER", HK, "hi"), 801);
			check(g != null && "ALLNEW".equals(g.getSenderAddress()), "SR-03 852, another sender: PhonePrefix=all, SenderAddress=all: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(840, "OTHER", N15, "hi"), 801);
			check(g != null && "ALLNEW".equals(g.getSenderAddress()), "SR-04 86 but no rule for this sender: falls to PhonePrefix=all: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(841, "OLD1", N15, "hi"), 801);
			check(g != null && "OLD1".equals(g.getSenderAddress()), "SR-05 the customer without SMSOptionAdvancedSenderReplacement: not replaced");
			g = sent(msg(842, "OLD3", N15, "hi"), 803);
			check(g != null && "NEW3".equals(g.getSenderAddress()), "SR-06 SenderReplacement, channel side (V803 SMSOptionAdvancedSenderReplacement, Ossid=842,Rssid=803): " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(842, "OTHER", N15, "hi"), 803);
			check(g != null && "OTHER".equals(g.getSenderAddress()), "SR-06b channel side, no rule for this sender: unchanged: " + (g == null ? "none" : g.getSenderAddress()));
			g = sent(msg(850, "OLD3", N15, "hi"), 803);
			check(g != null && "NEW3X".equals(g.getSenderAddress()), "SR-06c channel side, Ossid=-1: for any customer: " + (g == null ? "none" : g.getSenderAddress()));

			// ---- Prefix4NewMT of the channel (V805: AddOri 00, RemoveRec 86) ----
			g = sent(msg(845, "12345", N15, "hi"), 805);
			check(g != null && "0012345".equals(g.getSenderAddress()) && "15912345678".equals(g.getRecipientAddress()),
					"PX-01 SMSOptionAddOriPrefix4NewMT 00, SMSOptionRemoveRecPrefix4NewMT 86: " + (g == null ? "none" : g.getSenderAddress() + " " + g.getRecipientAddress()));

			// ---- long SMS: every part with the same sender (V806 splits by 70, its SenderReplacement OLD6 -> NEW6) ----
			GmmsMessage ls = msg(846, "OLD6", N15, "y".repeat(150));
			q.produceSubmitMessage(ls);
			final String lsid = ls.getMsgID();
			waitFor(() -> inStreams("stream:mt:routed:806:*", lsid).size() >= 3, 6000);
			List<GmmsMessage> lsp = inStreams("stream:mt:routed:806:*", lsid);
			boolean same = lsp.size() >= 3;
			for (GmmsMessage p : lsp) same &= "NEW6".equals(p.getSenderAddress());
			check(same, "LS-01 3 parts, each with the channel's sender NEW6: " + lsp.size() + " parts");

			// ---- the channel's lists (V809), whitelist (V816), a VIP channel (V815) ----
			GmmsMessage lb = msg(847, "VBAD", N15, "hi");
			q.produceSubmitMessage(lb);
			check(out(809, lb.getMsgID(), 1) == null && "2111".equals(cdrStatus(lb.getMsgID())), "LT-01 sender on the channel's blacklist: not sent, 2111: " + cdrStatus(lb.getMsgID()));
			GmmsMessage lr = msg(847, "S", "8613899999999", "hi");
			q.produceSubmitMessage(lr);
			check(out(809, lr.getMsgID(), 1) == null && "2121".equals(cdrStatus(lr.getMsgID())), "LT-02 recipient on the channel's blacklist: 2121: " + cdrStatus(lr.getMsgID()));
			GmmsMessage lc = msg(847, "S", N15, "a vendorbad text");
			q.produceSubmitMessage(lc);
			check(out(809, lc.getMsgID(), 1) == null && "2201".equals(cdrStatus(lc.getMsgID())), "LT-03 content on the channel's blacklist: 2201: " + cdrStatus(lc.getMsgID()));
			g = sent(msg(847, "S", N15, "fine"), 809);
			check(g != null, "LT-04 nothing listed: sent");
			GmmsMessage lw = msg(848, "OTHER", N15, "hi");
			q.produceSubmitMessage(lw);
			check(out(816, lw.getMsgID(), 1) == null && "2112".equals(cdrStatus(lw.getMsgID())), "LT-05 channel's whitelist (V816) without this sender: 2112: " + cdrStatus(lw.getMsgID()));
			g = sent(msg(848, "VGOOD", N15, "hi"), 816);
			check(g != null, "LT-06 sender on the channel's whitelist: sent");
			g = sent(msg(853, "VBAD", N15, "hi"), 815);
			check(g != null, "LT-07 a VIP channel (CustomerTypeBySender: VIP): its sender lists not checked: sent");

			// ---- a backup channel and a retry start from the sender before the channel's steps ----
			GmmsMessage bk = sent(msg(849, "S", N15, "hi"), 817);
			check(bk != null && "11S".equals(bk.getSenderAddress()), "RB-01 main channel V817 (AddOri 11): " + (bk == null ? "none" : bk.getSenderAddress()));
			if (bk != null) {
				fail(bk, GmmsStatus.COMMUNICATION_ERROR);
				GmmsMessage b2 = out(818, bk.getMsgID(), 1);
				check(b2 != null && "22S".equals(b2.getSenderAddress()), "RB-02 backup channel V818 (AddOri 22): 22S, not 2211S: " + (b2 == null ? "none" : b2.getSenderAddress()));
			}
			GmmsMessage rq = sent(msg(851, "S", N15, "foo"), 817);
			check(rq != null && "11S".equals(rq.getSenderAddress()) && is(rq, "foofoo"), "RB-03 first send: 11S, foofoo: " + show(rq));
			if (rq != null) {
				fail(rq, GmmsStatus.COMMUNICATION_ERROR);
				sqlSeen("INSERT", "RMQ", rq.getMsgID(), 1);
				q.produceMqmSubmitMessage(fromRmq(rq, 1));
				GmmsMessage r2 = out(817, rq.getMsgID(), 2);
				check(r2 != null && "11S".equals(r2.getSenderAddress()) && is(r2, "foofoo"), "RB-04 retried from RMQ: the same 11S, foofoo (not 1111S, foofoofoofoo): " + show(r2));
			}

			// ---- the customer's DR gets the customer's sender back, then its Prefix4DRMO ----
			GmmsMessage dm = sent(msg(850, "OLD3", N15, "hi"), 803);
			if (dm != null) {
				CoreDrBizIT.accept(dm);
				CoreDrBizIT.vendorDr(dm, GmmsStatus.DELIVERED);
				GmmsMessage dd = dr(850, dm.getMsgID(), 6000);
				check(dd != null && "99OLD3".equals(dd.getSenderAddress()), "DR-01 sent as NEW3X; the DR: the customer's OLD3 back, SMSOptionAddOriPrefix4DRMO 99: " + (dd == null ? "none" : dd.getSenderAddress()));
			} else check(false, "DR-01 not sent");

			// ---- $1 still a group reference ----
			g = sent(msg(852, "S", N15, "call 1234567 now"), 801);
			check(is(g, "call 123**** now"), "CR-08b ([0-9]{3})[0-9]{4}:$1****: the group reference works: " + text(g));
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
