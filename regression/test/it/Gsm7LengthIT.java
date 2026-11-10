package it;

import java.io.File;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.message.gmms.GmmsMessage;

/**
 * Core's one-SMS length check (NeedCheckMsgSize) on GSM 7-bit MTs whose characters need ISO-8859-1 or UCS2
 * (docs/GSM7bit消息长度计算优化设计.md 3.1): up to 160 GSM characters, extension characters counted twice; ASCII and
 * data_coding 3 as before. And the parser of a mode-1 customer meeting bytes above 0x7F: as before, plus a warning.
 */
public class Gsm7LengthIT extends CsmCdrIT {
	static int n = 0;

	static String text(String head, char fill, int length) {
		StringBuilder b = new StringBuilder(head);
		while (b.length() < length) b.append(fill);
		return b.toString();
	}

	/** an MT of the customer, as the Server parsed it: GSM 7-bit (mode 1: messageSize is the byte count) or 8-bit */
	static GmmsMessage mt7(int ossid, String text, String contentType, boolean gsm7bit) {
		GmmsMessage m = mt("" + ossid + "_G7" + (++n) + "_1", ossid, 0, 0);
		m.setTextContent(text);
		m.setContentType(contentType);
		m.setGsm7bit(gsm7bit);
		m.setMessageSize(gsm7bit ? com.king.gmms.util.charset.Convert.gsm7Septets(text) : text.length());
		m.setInnerTransaction(new TransactionURI(new ModuleURI("127.0.0.1", "MultiSmppServer1"), "conn1"));
		return m;
	}

	/** "routed" or "2250" */
	static String outcome(GmmsMessage m) throws Exception {
		process(m);
		String tag = m.getMsgID().split("_")[1];
		waitFor(() -> !sentOut(8, tag).isEmpty() || logLine(m.getMsgID(), "status=2250") != null, 4000);
		if (!sentOut(8, tag).isEmpty()) return "routed";
		if (logLine(m.getMsgID(), "status=2250") != null) return "2250";
		return "neither";
	}

	static void expect(String what, GmmsMessage m, String expected) throws Exception {
		// as it came in: routing a GSM 7-bit MT to a channel without 7-bit drops the flag
		String in = m.getTextContent().length() + " chars, " + com.king.gmms.util.charset.Convert.gsm7Septets(m.getTextContent()) + " GSM, "
				+ m.getContentType() + ", gsm7bit=" + m.isGsm7bit();
		String got = outcome(m);
		check(expected.equals(got), what + ": " + got + " (" + in + ")");
	}

	/** the MTs Core sent towards the channel for this one */
	static List<GmmsMessage> parts(GmmsMessage m, int rssid) throws Exception {
		process(m);
		String tag = m.getMsgID().split("_")[1];
		waitFor(() -> !sentOut(rssid, tag).isEmpty(), 4000);
		Thread.sleep(200);
		return sentOut(rssid, tag);
	}
	static String sizes(List<GmmsMessage> ps) {
		List<Integer> l = new ArrayList<>();
		for (GmmsMessage p : ps) l.add(p.getMessageSize());
		return l.toString();
	}

	static long logCount(String text) throws Exception {
		long c = 0;
		for (String l : Files.readAllLines(new File(home + "log/it.log").toPath(), java.nio.charset.StandardCharsets.ISO_8859_1)) if (l.contains(text)) c++;
		return c;
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		// channel ssids 62-64: 20-25 are customers of the base cm.cfg
		appendCm("\n[CUST8892]\nCCBSSID: 8892\nA2PPlayerType: operator\nProtocol: SMPP\nNeedCheckMsgSize: false\n"
				+ "\n[VENDOR62]\nCCBSSID: 62\nA2PPlayerType: operator\nProtocol: SMPP\nChlMsgSplit: yes\n"
				+ "\n[VENDOR63]\nCCBSSID: 63\nA2PPlayerType: operator\nProtocol: SMPP\nChlMsgSplit: yes\nMsgSplitLen: 160\n"
				+ "\n[VENDOR64]\nCCBSSID: 64\nA2PPlayerType: operator\nProtocol: SMPP\nChlMsgSplit: yes\nSMSOptionSupportOutgoingGSM7bit: 1\n"
				+ "\n[CUST8897]\nCCBSSID: 8897\nA2PPlayerType: operator\nProtocol: SMPP\nNeedCheckMsgSize: false\n"
				+ "\n[CUST8898]\nCCBSSID: 8898\nA2PPlayerType: operator\nProtocol: SMPP\nNeedCheckMsgSize: false\n"
				+ "\n[CUST8899]\nCCBSSID: 8899\nA2PPlayerType: operator\nProtocol: SMPP\nNeedCheckMsgSize: false\n"
				+ "\n[VENDOR65]\nCCBSSID: 65\nA2PPlayerType: operator\nProtocol: SMPP\nChlMsgSplit: yes\nMsgSplitLen: 140\nSMSOptionSupportOutgoingGSM7bit: 1\n"
				+ "\n[VENDOR66]\nCCBSSID: 66\nA2PPlayerType: operator\nProtocol: SMPP\nChlMsgSplit: yes\nMsgSplitLen: 140\nSMSOptionAsciiMsgSplitLen: 160\nSMSOptionSupportOutgoingGSM7bit: 1\n"
				+ "\n[CUST8900]\nCCBSSID: 8900\nA2PPlayerType: operator\nProtocol: SMPP\nNeedCheckMsgSize: false\n"
				+ "\n[CUST8901]\nCCBSSID: 8901\nA2PPlayerType: operator\nProtocol: SMPP\nNeedCheckMsgSize: false\n");
		route("CUST8892", 8892, "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR8)");
		route("CUST8897", 8897, "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR62)");
		route("CUST8898", 8898, "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR63)");
		route("CUST8899", 8899, "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR64)");
		route("CUST8900", 8900, "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR65)");
		route("CUST8901", 8901, "A2PRoutingRelay: (PhonePrefix=86,Relay=VENDOR66)");
		try {
			boot();
			String iso = GmmsMessage.AIC_CS_ISO8859_1, ucs2 = GmmsMessage.AIC_CS_UCS2, ascii = GmmsMessage.AIC_CS_ASCII;

			// ---- ASCII: not checked, as before ----
			expect("G7-01 GSM 7-bit, 160 ASCII characters", mt7(8888, text("", 'a', 160), ascii, true), "routed");
			expect("G7-01b GSM 7-bit, 200 ASCII characters (message_payload): not checked, as before", mt7(8888, text("", 'a', 200), ascii, true), "routed");

			// ---- GSM 7-bit whose characters need ISO-8859-1 or UCS2 ----
			GmmsMessage cafe = mt7(8888, text("Café ", 'a', 160), iso, true);
			expect("G7-02 GSM 7-bit with é, 160 characters (was 2250)", cafe, "routed");
			expect("G7-04 GSM 7-bit with Δ (UCS2), 160 characters (was 2250)", mt7(8888, text("Δ ", 'a', 160), ucs2, true), "routed");
			expect("G7-05 GSM 7-bit with é, 161 characters", mt7(8888, text("Café ", 'a', 161), iso, true), "2250");
			expect("G7-06a é, 150 more and 5 x '{' : 160 GSM characters", mt7(8888, text("é", 'a', 150) + "{{{{{", iso, true), "routed");
			expect("G7-06b é, 151 more and 5 x '{' : 161 GSM characters", mt7(8888, text("é", 'a', 151) + "{{{{{", iso, true), "2250");

			// ---- 8-bit (data_coding 3): as before ----
			expect("G7-05b data_coding 3 (ISO-8859-1), 160 bytes", mt7(8888, text("Café ", 'a', 160), iso, false), "2250");
			expect("G7-05c data_coding 3, 140 bytes", mt7(8888, text("Café ", 'a', 140), iso, false), "routed");
			expect("G7-11 NeedCheckMsgSize false: not checked", mt7(8892, text("Café ", 'a', 160), iso, false), "routed");

			// ---- splitting for the channel: an 8-bit ISO text holds 140 bytes when MsgSplitLen is not set ----
			List<GmmsMessage> ps = parts(mt7(8897, text("Café ", 'a', 160), iso, false), 62);
			boolean fits = !ps.isEmpty();
			for (GmmsMessage p : ps) fits &= p.getMessageSize() <= 140;
			check(ps.size() == 2 && fits, "G7-13a data_coding 3, 160 bytes, channel without MsgSplitLen: 2 parts of at most 140: " + sizes(ps));
			ps = parts(mt7(8897, text("Café ", 'a', 140), iso, false), 62);
			check(ps.size() == 1, "G7-13b 140 bytes: 1: " + sizes(ps));
			ps = parts(mt7(8897, text("", 'a', 160), ascii, false), 62);
			check(ps.size() == 1, "G7-13c ASCII, 160: 1, as before: " + sizes(ps));
			ps = parts(mt7(8898, text("Café ", 'a', 160), iso, false), 63);
			check(ps.size() == 1, "G7-14 channel with MsgSplitLen 160: its value, 1 part: " + sizes(ps));
			ps = parts(mt7(8899, text("Café ", 'a', 160), iso, true), 64);
			check(ps.size() == 1 && ps.get(0).isGsm7bit(), "G7-15 GSM 7-bit with é, 160, channel with 7-bit: stays 7-bit, 1 part: " + sizes(ps));
			ps = parts(mt7(8897, text("Café ", 'a', 160), iso, true), 62);
			fits = !ps.isEmpty();
			for (GmmsMessage p : ps) fits &= p.getMessageSize() <= 140 && !p.isGsm7bit();
			check(ps.size() == 2 && fits, "G7-16 the same to a channel without 7-bit: sent as ISO 8-bit, 2 parts of at most 140: " + sizes(ps));

			// ---- MsgSplitLen 140: ASCII and GSM 7-bit take it too, unless SMSOptionAsciiMsgSplitLen is set ----
			ps = parts(mt7(8900, text("", 'a', 160), ascii, false), 65);
			check(ps.size() == 2, "G7-17a MsgSplitLen 140 only: ASCII 160 is split by 140: " + sizes(ps));
			ps = parts(mt7(8900, text("Café ", 'a', 160), iso, true), 65);
			check(ps.size() == 2, "G7-17b MsgSplitLen 140 only: GSM 7-bit with é, 160, is split by 140: " + sizes(ps));
			ps = parts(mt7(8901, text("", 'a', 160), ascii, false), 66);
			check(ps.size() == 1, "G7-18a and SMSOptionAsciiMsgSplitLen 160: ASCII 160 is 1: " + sizes(ps));
			ps = parts(mt7(8901, text("Café ", 'a', 160), iso, true), 66);
			check(ps.size() == 1, "G7-18b and SMSOptionAsciiMsgSplitLen 160: GSM 7-bit 160 is 1: " + sizes(ps));
			ps = parts(mt7(8901, text("Café ", 'a', 160), iso, false), 66);
			check(ps.size() == 2, "G7-18c ISO data_coding 3, 160: MsgSplitLen 140: " + sizes(ps));

			// ---- CDR of G7-02 ----
			Thread.sleep(2500);
			List<String> in = cdrLines(cafe.getMsgID(), "IN_SUBMIT");
			check(!in.isEmpty() && in.get(0).contains(",\"ASCII\","), "G7-12 IN_SUBMIT of a GSM 7-bit MT: charset ASCII, as before: " + (in.isEmpty() ? "no CDR" : "found"));

			// ---- the parser, mode 1, bytes above 0x7F ----
			Class<?> c = Class.forName("com.king.gmms.protocol.smpp.Smpp");
			Constructor<?> k = c.getDeclaredConstructor();
			k.setAccessible(true);
			Object smpp = k.newInstance();
			Method parse = c.getDeclaredMethod("handleGSM7bitMessageContent", GmmsMessage.class, boolean.class, byte[].class, int.class);
			parse.setAccessible(true);
			GmmsMessage gsm = new GmmsMessage();
			gsm.setOSsID(8893);
			parse.invoke(smpp, gsm, false, new byte[] { 0x43, 0x61, 0x66, 0x05 }, 1);
			check("Café".equals(gsm.getTextContent()) && iso.equals(gsm.getContentType()) && gsm.isGsm7bit() && logCount("[GSM7] ossid=8893") == 0,
					"G7-05e real GSM 7-bit (é = 0x05): Café, ISO-8859-1, gsm7bit, no warning");
			GmmsMessage wrong = new GmmsMessage();
			wrong.setOSsID(8894);
			parse.invoke(smpp, wrong, false, new byte[] { 0x43, 0x61, 0x66, (byte) 0xE9 }, 1);
			check("Caf?".equals(wrong.getTextContent()) && ascii.equals(wrong.getContentType()) && wrong.isGsm7bit(),
					"G7-05d byte 0xE9 on a mode-1 customer: parsed as before (Caf?, ASCII, gsm7bit)");
			check(waitFor(() -> logCount("[GSM7] ossid=8894") == 1, 3000), "G7-05f and a warning");
			GmmsMessage again = new GmmsMessage();
			again.setOSsID(8894);
			parse.invoke(smpp, again, false, new byte[] { (byte) 0xA3 }, 1);
			Thread.sleep(500);
			check(logCount("[GSM7] ossid=8894") == 1, "G7-05g the next one within 10 minutes: no second line (rate limited)");
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		exit();
	}
}
