package it;

import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.ResultStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** What leaves Core: split, encoding, UDH, content and sender replacement, templates, HTTP channel; anti-spam, service type, expiry, schedule; reloads taking effect. */
public class CoreContentIT extends CoreRetryIT {
	static final String LONG = String.join("", Collections.nCopies(25, "0123456789"));   // 250 ASCII

	/** port addressing, not a concatenation header (those are turned into segment fields and removed) */
	static final byte[] PORT_UDH = { 6, 5, 4, 0x0B, (byte) 0x84, 0x23, (byte) 0xF0 };

	static GmmsMessage sent(GmmsMessage m, int rssid) throws Exception {
		q.produceSubmitMessage(m);
		return out(rssid, m.getMsgID(), 1);
	}
	static void reload(String cmd) throws Exception {
		jedis.publish("channel:gmms:control", cmd);
		Thread.sleep(1500);
	}
	static String show(GmmsMessage g) {
		return g == null ? "none" : "type=" + g.getContentType() + " gsm7=" + g.isGsm7bit() + " udh=" + (g.getUdh() == null ? "null" : g.getUdh().length) + " split=" + g.getSplitStatus() + " sar=" + g.getSarSegmentSeqNum() + "/" + g.getSarTotalSeqments() + " size=" + g.getMessageSize() + " sender=" + g.getSenderAddress() + " text=[" + g.getTextContent() + "]";
	}
	static String drOf(int ossid, String id) throws Exception {
		GmmsMessage d = dr(ossid, id, 4000);
		return d == null ? "none" : String.valueOf(d.getStatusCode());
	}

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder();
		cm.append(cust("V701", 701, ""));
		cm.append(cust("V702", 702, "ChlMsgSplit: yes\nSMSOptionAsciiMsgSplitLen: 100\nMsgSplitLen: 100\n"));
		cm.append(cust("V703", 703, "SMSOptionConvert2GSM7bit: true\nSMSOptionSupportOutgoingGSM7bit: 1\n"));
		cm.append(cust("V704", 704, "SMSOptionForceTrans2ASCII: true\n"));
		cm.append(cust("V705", 705, "SmsOptionIsSupportUDH: No\n"));
		cm.append(cust("V706", 706, "SMSOptionSupportOutgoingAntiSpam: true\n"));
		cm.append("\n[V707]\nCCBSSID: 707\nA2PPlayerType: operator\nProtocol: HTTP\nChlQueue: CommonHttpClient\nChlInit: true\nChlURL: http://127.0.0.1:18080/sms\n");
		cm.append(cust("R200", 200, ""));
		cm.append(cust("R201", 201, "SupportMsgSplit: no\n"));
		cm.append(cust("R202", 202, ""));
		cm.append(cust("R203", 203, ""));
		cm.append(cust("R204", 204, ""));
		cm.append(cust("R205", 205, "SMSOptionContentReplacementPolicy: (prefix=all,value=foo:bar);(prefix=86,value=baz:qux)\n"));
		cm.append(cust("R206", 206, ""));
		cm.append(cust("R207", 207, "DrSuccForTemplateReplaceFailed: true\n"));
		cm.append(cust("R208", 208, "SMSOptionSupportIncomingAntiSpam: true\n"));
		cm.append(cust("R209", 209, ""));
		cm.append(cust("R210", 210, "ExpireTime: 10\n"));
		cm.append(cust("R211", 211, "SMSOptionAdvancedSenderReplacement: true\n"));
		cm.append(cust("R212", 212, ""));
		cm.append(cust("R213", 213, ""));
		appendCm(cm.toString());
		route("R200", 200, "A2PRoutingRelay: (PhonePrefix=86,Relay=V702)");
		route("R201", 201, "A2PRoutingRelay: (PhonePrefix=86,Relay=V702)");
		route("R202", 202, "A2PRoutingRelay: (PhonePrefix=86,Relay=V703)");
		route("R203", 203, "A2PRoutingRelay: (PhonePrefix=86,Relay=V704)");
		route("R204", 204, "A2PRoutingRelay: (PhonePrefix=86,Relay=V705)");
		route("R205", 205, "A2PRoutingRelay: (PhonePrefix=86;852,Relay=V701)");
		route("R206", 206, "A2PRoutingRelay: (PhonePrefix=86,Relay=V701,ReplaceContentByTemplate=T1)");
		route("R207", 207, "A2PRoutingRelay: (PhonePrefix=86,Relay=V701,ReplaceContentByTemplate=T1)");
		route("R208", 208, "A2PRoutingRelay: (PhonePrefix=86,Relay=V701)");
		route("R209", 209, "A2PRoutingRelay: (PhonePrefix=86,Relay=V706)");
		route("R210", 210, "A2PRoutingRelay: (PhonePrefix=86,Relay=V701)");
		route("R211", 211, "A2PRoutingRelay: (PhonePrefix=86,Relay=V701)");
		route("R212", 212, "A2PRoutingRelay: (PhonePrefix=86,Relay=V707)");
		route("R213", 213, "A2PRoutingRelay: (PhonePrefix=86,Relay=V701)");
		conf("contentTemplate.cfg", "[T1]\nEnglish: your code is $shortcode$ ok\n");
		conf("antiSpam.cfg", "[A208]\nCCBSSID: 208\nIN_ASCII: spamword\n\n[A706]\nCCBSSID: 706\nOUT_ASCII: outbad\n");
		conf("senderReplacement.cfg", "[S211]\nCCBSSID: 211\nSenderReplacement: (PhonePrefix=86,SenderAddress=OLDID,ReplaceTo=NEWID,Ossid=211,Rssid=-1)\n");
		conf("senderactiondir/-1_sender_blacklist.cfg", "");
		conf("senderactiondir/-1_sender_whitelist.cfg", "");
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\n").getBytes("UTF-8"));
		msmFactory = RecordingStore::new;
		try {
			start();
			q = StreamQueueManager.getInstance();
			ResultStreamConsumer.getInstance().start();
			Thread.sleep(800);
			GmmsMessage m, g;

			// ---- split ----
			m = msg(200, "18667991226", DST, LONG);
			q.produceSubmitMessage(m);
			final String sid = m.getMsgID();
			waitFor(() -> inStreams("stream:mt:routed:702:*", sid).size() >= 3, 8000);
			List<GmmsMessage> parts = inStreams("stream:mt:routed:702:*", sid);
			StringBuilder text = new StringBuilder(); for (GmmsMessage p : parts) text.append(p.getTextContent());
			System.out.println("INFO split parts=" + parts.size() + " first " + (parts.isEmpty() ? "" : show(parts.get(0))));
			check(parts.size() == 3, "OUT-03a 250 chars, split length 100: parts " + parts.size());
			check(LONG.equals(text.toString()), "OUT-03b parts put together give the text");
			m = msg(201, "18667991226", DST, LONG);
			g = sent(m, 702);
			System.out.println("INFO nosplit " + show(g) + " count=" + inStreams("stream:mt:routed:702:*", m.getMsgID()).size());
			check(g != null && inStreams("stream:mt:routed:702:*", m.getMsgID()).size() == 1, "OUT-04 customer SupportMsgSplit=no: one message, not split");

			// ---- encoding ----
			m = msg(202, "18667991226", DST, "hello"); g = sent(m, 703); System.out.println("INFO 7bit " + show(g));
			check(g != null && g.isGsm7bit(), "OUT-05a SMSOptionConvert2GSM7bit: ASCII goes out as GSM 7bit");
			m = msg(203, "18667991226", DST, "hello"); m.setGsm7bit(true); g = sent(m, 704); System.out.println("INFO force " + show(g));
			check(g != null && !g.isGsm7bit() && GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(g.getContentType()), "OUT-05b SMSOptionForceTrans2ASCII, channel without 7bit: goes out as ASCII");

			// ---- UCS2 on a channel with the default settings: what Core hands to the Client ----
			m = msg(205, "18667991226", DST, "\u4F60\u597D");
			m.setContentType(GmmsMessage.AIC_CS_UCS2); m.setMessageSize(4);
			g = sent(m, 701);
			System.out.println("INFO ucs2 " + show(g));
			check(g != null && GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(g.getContentType()), "OUT-05c UCS2 stays UCS2 on its way to the Client: " + show(g));

			// ---- UDH ----
			m = msg(204, "18667991226", DST, "hello"); m.setUdh(PORT_UDH); g = sent(m, 705); System.out.println("INFO udh " + show(g));
			check(g != null && g.getUdh() == null, "OUT-06a SmsOptionIsSupportUDH=No: UDH removed");
			m = msg(202, "18667991226", DST, "hello"); m.setUdh(PORT_UDH); g = sent(m, 703);
			check(g != null && Arrays.equals(PORT_UDH, g.getUdh()), "OUT-06b channel without the option: UDH kept: " + show(g));
			m = msg(202, "18667991226", DST, "hello"); m.setUdh(new byte[] { 5, 0, 3, 9, 1, 1 }); g = sent(m, 703);
			check(g != null && g.getUdh() == null, "OUT-06c concatenation header of a single part: removed, not sent as UDH: " + show(g));

			// ---- content replacement ----
			m = msg(205, "18667991226", DST, "foo baz"); g = sent(m, 701); System.out.println("INFO repl " + show(g));
			check(g != null && "bar qux".equals(g.getTextContent()), "OUT-07a prefix=all and prefix=86 both apply: " + show(g));
			m = msg(205, "18667991226", "85212345678", "foo baz"); g = sent(m, 701);
			check(g != null && "bar baz".equals(g.getTextContent()), "OUT-07b other country: only prefix=all: " + show(g));

			// ---- template by routing ----
			m = msg(206, "18667991226", DST, "code 1234 here"); g = sent(m, 701); System.out.println("INFO tpl " + show(g));
			check(g != null && "your code is 1234 ok".equals(g.getTextContent()), "OUT-08a code in the text: template with the code: " + show(g));
			m = msg(206, "18667991226", DST, "no number"); q.produceSubmitMessage(m);
			String d1 = drOf(206, m.getMsgID()); System.out.println("INFO tplfail dr=" + d1);
			check(out(701, m.getMsgID(), 1) == null && !"none".equals(d1) && !String.valueOf(GmmsStatus.DELIVERED.getCode()).equals(d1), "OUT-08b no code: not sent, failure DR: " + d1);
			m = msg(207, "18667991226", DST, "no number"); q.produceSubmitMessage(m);
			d1 = drOf(207, m.getMsgID());
			check(out(701, m.getMsgID(), 1) == null && String.valueOf(GmmsStatus.DELIVERED.getCode()).equals(d1), "OUT-08c DrSuccForTemplateReplaceFailed: not sent, DELIVERED: " + d1);

			// ---- sender replacement ----
			m = msg(211, "OLDID", DST, "hello"); g = sent(m, 701); System.out.println("INFO sender " + show(g));
			check(g != null && "NEWID".equals(g.getSenderAddress()), "OUT-10a SMSOptionAdvancedSenderReplacement: sender replaced: " + show(g));
			m = msg(211, "OTHER", DST, "hello"); g = sent(m, 701);
			check(g != null && "OTHER".equals(g.getSenderAddress()), "OUT-10b other sender unchanged");
			m = msg(205, "OLDID", DST, "hello"); g = sent(m, 701);
			check(g != null && "OLDID".equals(g.getSenderAddress()), "OUT-10c customer without the option: unchanged");

			// ---- HTTP channel ----
			m = msg(212, "18667991226", DST, "hello"); q.produceSubmitMessage(m);
			final String hid = m.getMsgID();
			check(waitFor(() -> !inStreams("stream:http:submit:707:*", hid).isEmpty(), 5000), "OUT-02 HTTP channel: goes to stream:http:submit");
			check(inStreams("stream:mt:routed:707:*", hid).isEmpty(), "OUT-02b not to the SMPP stream");

			// ---- anti-spam ----
			m = msg(208, "18667991226", DST, "this spamword here"); q.produceSubmitMessage(m);
			d1 = drOf(208, m.getMsgID()); System.out.println("INFO spam in dr=" + d1);
			check(out(701, m.getMsgID(), 1) == null && !"none".equals(d1), "CK-16a incoming keyword: not sent, failure DR: " + d1);
			m = msg(208, "18667991226", DST, "clean text"); g = sent(m, 701);
			check(g != null, "CK-16b clean text sent");
			m = msg(209, "18667991226", DST, "this outbad here"); q.produceSubmitMessage(m);
			d1 = drOf(209, m.getMsgID()); System.out.println("INFO spam out dr=" + d1);
			check(out(706, m.getMsgID(), 1) == null && !"none".equals(d1), "CK-16c outgoing keyword of the channel: not sent, failure DR: " + d1);

			// ---- service type ----
			m = msg(205, "18667991226", DST, "hello"); m.setServiceTypeID(5); q.produceSubmitMessage(m);
			d1 = drOf(205, m.getMsgID()); System.out.println("INFO stid dr=" + d1);
			check(out(701, m.getMsgID(), 1) == null && !"none".equals(d1), "CK-17a service type id 5 above MaxServiceTypeID: not sent, failure DR: " + d1);
			m = msg(205, "18667991226", DST, "hello"); m.setServiceTypeID(3); g = sent(m, 701);
			check(g != null, "CK-17b service type id 3: sent");

			// ---- expiry ----
			long gmt = GmmsUtility.getInstance().getGMTTime().getTime();
			m = msg(210, "18667991226", DST, "hello"); m.setExpiryDate(null); g = sent(m, 701);
			long left = g == null || g.getExpiryDate() == null ? -1 : (g.getExpiryDate().getTime() - gmt) / 1000;
			check(left >= 35990 && left <= 36010, "CK-18a no expiry from the customer, ExpireTime 10 (hours): about 36000 s: " + left);
			m = msg(205, "18667991226", DST, "hello"); m.setExpiryDate(null); g = sent(m, 701);
			left = g == null || g.getExpiryDate() == null ? -1 : (g.getExpiryDate().getTime() - gmt) / 60;
			long want = GmmsUtility.getInstance().getExpireTimeInMinute() * 1000L;
			check(Math.abs(left - want) <= 2000, "CK-18b no ExpireTime: the global default: " + left + " vs " + want);

			// ---- scheduled ----
			m = msg(213, "18667991226", DST, "later"); m.setScheduleDeliveryTime(new Date(gmt + 3600 * 1000L)); q.produceSubmitMessage(m);
			check(sqlSeen("INSERT", "SMQ", m.getMsgID(), 1), "CK-20a schedule in the future: kept in SMQ");
			check(out(701, m.getMsgID(), 1) == null, "CK-20b not sent yet");
			m = msg(213, "18667991226", DST, "now"); m.setScheduleDeliveryTime(new Date(gmt - 60000L)); g = sent(m, 701);
			check(g != null, "CK-20c schedule passed: sent");

			// ---- reloads take effect for new messages ----
			conf("contentTemplate.cfg", "[T1]\nEnglish: new text $shortcode$\n");
			reload("RELOAD_CONTENT_TEMPLATE");
			m = msg(206, "18667991226", DST, "code 5678 here"); g = sent(m, 701);
			check(g != null && "new text 5678".equals(g.getTextContent()), "CF-03 RELOAD_CONTENT_TEMPLATE: " + show(g));

			m = msg(205, "18667991226", "85299990000", "hello"); check(sent(m, 701) != null, "CF-05a before the rule: routed");
			conf("recipientRuleFile.cfg", "[\n{\"cc\":\"852\",\"lens\":[11],\"allowPrefixList\":[\"8521\"]}\n]\n");
			reload("RELOAD_RECIPIENT_RULE");
			m = msg(205, "18667991226", "85299990000", "hello"); q.produceSubmitMessage(m);
			d1 = drOf(205, m.getMsgID());
			check(out(701, m.getMsgID(), 1) == null && !"none".equals(d1), "CF-05b RELOAD_RECIPIENT_RULE: rejected by the new rule: " + d1);

			m = msg(208, "18667991226", DST, "newbad text"); check(sent(m, 701) != null, "CF-06a before the keyword: sent");
			conf("antiSpam.cfg", "[A208]\nCCBSSID: 208\nIN_ASCII: newbad\n\n[A706]\nCCBSSID: 706\nOUT_ASCII: outbad\n");
			reload("RELOAD_ANTISPAM");
			m = msg(208, "18667991226", DST, "newbad text"); q.produceSubmitMessage(m);
			d1 = drOf(208, m.getMsgID());
			check(out(701, m.getMsgID(), 1) == null && !"none".equals(d1), "CF-06b RELOAD_ANTISPAM: new keyword rejected: " + d1);

			m = msg(205, "NEWBADSND", DST, "hello"); check(sent(m, 701) != null, "CF-08a before the list: sent");
			conf("senderactiondir/-1_sender_blacklist.cfg", "86:NEWBADSND\n");
			reload("RELOAD_SENDER_BLACKLIST");
			m = msg(205, "NEWBADSND", DST, "hello"); q.produceSubmitMessage(m);
			d1 = drOf(205, m.getMsgID());
			check(String.valueOf(GmmsStatus.REJECTED_SDBL.getCode()).equals(d1), "CF-08b RELOAD_SENDER_BLACKLIST: rejected: " + d1);
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
