package it;

import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Core checks before routing: sender and content lists, numbers, recipient rules, size, per-recipient limit, duplicates. */
public class CoreCheckIT extends CoreBizSupport {
	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder(cust("V401", 401, ""));
		String[][] c = {
			{ "C300", "300", "" }, { "C301", "301", "" }, { "C302", "302", "CustomerTypeBySender: VIP\n" }, { "C303", "303", "" },
			{ "C304", "304", "" }, { "C305", "305", "SMSOptionAllowOriPrefixList: ^1866.*\n" }, { "C306", "306", "SMSOptionOriMinNumberLen: 8\n" },
			{ "C307", "307", "SMSOptionRecipientMaxSendCountIn24H: 2\n" }, { "C308", "308", "DuplicateMessagePeriod: 60\nDuplicateMessageServiceTypeIDList: 0\nCheckDuplicateMessageContent: true\n" },
			{ "C309", "309", "PhonePrefix: (Prefix=86)\nSMSOptionRecNumberLenList: 13\n" }, { "C310", "310", "NeedCheckMsgSize: false\n" }, { "C311", "311", "" },
			{ "C312", "312", "" }, { "C313", "313", "CustomerTypeBySender: PA\n" }, { "C314", "314", "" }, { "C315", "315", "" }, { "C316", "316", "" },
		};
		for (String[] x : c) { cm.append(cust(x[0], Integer.parseInt(x[1]), x[2])); route(x[0], Integer.parseInt(x[1]), "A2PMainRouting: (Priority=30,PhonePrefix=86;852;65,Relay=V401)"); }
		appendCm(cm.toString());
		conf("senderactiondir/-1_sender_blacklist.cfg", "86:GLOBALBAD\n65:SGBAD\n");
		conf("senderactiondir/301_sender_blacklist.cfg", "86:CUSTBAD\n65:CUSTBAD65\n");
		conf("senderactiondir/-1_sender_whitelist.cfg", "65:wl:DEFOK:x\n");
		conf("senderactiondir/312_sender_whitelist.cfg", "65:wl:GOODSG:x\n65:wl:SGBAD:x\n65:wl:CUST312:x\n");
		conf("senderactiondir/312_sender_blacklist.cfg", "65:CUST312\n");
		conf("senderactiondir/313_sender_blacklist.cfg", "65:PABAD\n");
		conf("senderactiondir/314_sender_whitelist.cfg", "86:wl:ONLY86:x\n");
		conf("senderactiondir/315_sender_whitelist.cfg", "65:rp:RPOLD:RPNEW\n");
		conf("senderactiondir/316_sender_whitelist.cfg", "all:wl:ANYCC:x\n");
		conf("senderactiondir/303_sender_whitelist.cfg", "86:wl:GOODONE:x\n65:wl:GOODSG:x\n");
		conf("contentactiondir/-1_content_blacklist.cfg", "86:globalforbidden\n");
		conf("contentactiondir/304_content_blacklist.cfg", "86:custforbidden\n");
		conf("recipientRuleFile.cfg", "[\n{\"cc\":\"86\",\"lens\":[13],\"allowPrefixList\":[\"8613\",\"8615\"]}\n]\n");
		try {
			start();
			Thread.sleep(800);
			Out o;
			o = send(300); check(o.routedTo(401) && o.inCdr == 1 && o.acked, "CK-00 clean MT routed, one IN_SUBMIT CDR, acked: " + o);
			o = send(300, "GLOBALBAD", DST, "x"); check(o.rejected(GmmsStatus.REJECTED_SDBL) && o.inCdr == 1 && o.acked, "CK-01a sender on the global blacklist: rejected DR, CDR, not routed: " + o);
			o = send(300, "GLOBALBAD", "85212345678", "x"); check(o.rssid == 401, "CK-01b list is per country: same sender to 852 routed: " + o);
			o = send(301, "CUSTBAD", DST, "x"); check(o.rejected(GmmsStatus.REJECTED_SDBL), "CK-02a sender on this customer's blacklist: rejected: " + o);
			o = send(300, "CUSTBAD", DST, "x"); check(o.routedTo(401), "CK-02b another customer not affected: " + o);
			o = send(302, "GLOBALBAD", DST, "x"); check(o.routedTo(401), "CK-03 VIP skips the sender lists: " + o);
			o = send(300, "18667991226", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04a NonPA (default), Singapore, no whitelist: rejected: " + o);
			o = send(302, "18667991226", "6591234567", "x"); check(o.routedTo(401), "CK-04b VIP to Singapore: routed: " + o);
			o = send(301, "CUSTBAD65", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04c NonPA to Singapore: customer blacklist not used, whitelist decides: " + o);
			for (String form : new String[] { "+6591234567", "006591234567", "06591234567" }) { o = send(300, "18667991226", form, "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04d NonPA, recipient written " + form + ": same rule: " + o); }
			o = send(300, "18667991226", "8613650000000", "x"); check(o.routedTo(401), "CK-04e 65 inside the number, not at the start: rule not applied: " + o);
			o = send(300, "DEFOK", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04f NonPA without its own whitelist: the default whitelist does not help: " + o);
			o = send(312, "GOODSG", "6591234567", "x"); check(o.routedTo(401), "CK-04g NonPA, sender on its own whitelist for 65: routed: " + o);
			o = send(312, "OTHER", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04h NonPA, whitelist for 65 without this sender: rejected: " + o);
			o = send(312, "CUST312", "6591234567", "x"); check(o.routedTo(401), "CK-04i NonPA to 65: customer blacklist not used, whitelisted sender routed: " + o);
			o = send(312, "SGBAD", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDBL), "CK-04j NonPA to 65: global blacklist wins over the whitelist: " + o);
			o = send(314, "ONLY86", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04k NonPA, whitelist only for 86: 65 rejected: " + o);
			o = send(314, "ONLY86", DST, "x"); check(o.routedTo(401), "CK-04l same customer and sender to 86: routed: " + o);
			o = send(315, "RPOLD", "6591234567", "x"); java.util.List<GmmsMessage> rp = inStreams("stream:mt:routed:401:*", o.id);
			check(o.routedTo(401) && !rp.isEmpty() && "RPNEW".equals(rp.get(0).getSenderAddress()), "CK-04m NonPA, replacement entry for 65: routed, sender replaced: " + o + " sender=" + (rp.isEmpty() ? "none" : rp.get(0).getSenderAddress()));
			o = send(316, "ANYCC", "6591234567", "x"); check(o.routedTo(401), "CK-04n NonPA, whitelist for all countries: routed to 65: " + o);
			o = send(316, "OTHER", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04o NonPA, whitelist for all countries without this sender: rejected: " + o);
			o = send(313, "18667991226", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-04p PA without its own whitelist: default whitelist decides, sender not on it: " + o);
			o = send(313, "DEFOK", "6591234567", "x"); check(o.routedTo(401), "CK-04q PA, sender on the default whitelist: routed: " + o);
			o = send(313, "PABAD", "6591234567", "x"); check(o.rejected(GmmsStatus.REJECTED_SDBL), "CK-04r PA to 65: customer blacklist is used: " + o);
			o = send(303, "GOODSG", "6591234567", "x"); check(o.routedTo(401), "CK-05a whitelisted for Singapore: routed: " + o);
			o = send(303, "GOODONE", DST, "x"); check(o.routedTo(401), "CK-05b sender on the customer's whitelist: routed: " + o);
			o = send(303, "OTHER", DST, "x"); check(o.rejected(GmmsStatus.REJECTED_SDWL), "CK-05c sender not on the whitelist: rejected: " + o);
			o = send(300, "18667991226", DST, "this is globalforbidden text"); check(o.rejected(GmmsStatus.REJECTED_CTBL), "CK-06a content on the global blacklist: rejected: " + o);
			o = send(304, "18667991226", DST, "this is custforbidden text"); check(o.rejected(GmmsStatus.REJECTED_CTBL), "CK-06b content on the customer's blacklist: rejected: " + o);
			o = send(300, "18667991226", DST, "this is custforbidden text"); check(o.routedTo(401), "CK-06c another customer not affected: " + o);
			GmmsMessage nodr = msg(300, "GLOBALBAD", DST, "x"); nodr.setDeliveryReport(false); o = send(nodr); check(o.rssid == -1 && o.dr == null && o.inCdr == 1, "CK-07 rejected, no DR requested: CDR only: " + o);
			o = send(300, "18667991226", "abc123", "x"); check(o.rssid == -1 && o.dr != null, "CK-08 non-numeric recipient: not routed, failure DR: " + o);
			o = send(309, "18667991226", DST, "x"); check(o.routedTo(401), "CK-09a PhonePrefix and allowed length: routed: " + o);
			o = send(309, "18667991226", "86138000", "x"); check(o.rssid == -1 && o.dr != null, "CK-09b recipient length not allowed: not routed: " + o);
			o = send(309, "18667991226", "8521234567890", "x"); check(o.rssid == -1 && o.dr != null, "CK-09c recipient prefix not allowed: not routed: " + o);
			o = send(305, "18667991226", DST, "x"); check(o.routedTo(401), "CK-10a sender matches SMSOptionAllowOriPrefixList: routed: " + o);
			o = send(305, "13900000000", DST, "x"); check(o.rssid == -1 && o.dr != null, "CK-10b sender does not match: not routed: " + o);
			o = send(306, "1234567", DST, "x"); check(o.rssid == -1 && o.dr != null, "CK-10c sender shorter than SMSOptionOriMinNumberLen: not routed: " + o);
			o = send(300, "18667991226", "8619900000000", "x"); check(o.rssid == -1 && o.dr != null && o.dr.getStatusCode() != 10000, "CK-11a recipient prefix outside recipientRuleFile: not routed: " + o);
			o = send(300, "18667991226", "86138000000", "x"); check(o.rssid == -1 && o.dr != null, "CK-11b length outside recipientRuleFile: not routed: " + o);
			GmmsMessage ucs = msg(300, "18667991226", DST, "x"); ucs.setContentType("UnicodeBigUnmarked"); ucs.setMessageSize(141); o = send(ucs); check(o.rssid == -1 && o.dr != null, "CK-12a non-ASCII over 140 bytes: rejected: " + o);
			ucs = msg(300, "18667991226", DST, "x"); ucs.setContentType("UnicodeBigUnmarked"); ucs.setMessageSize(140); o = send(ucs); check(o.routedTo(401), "CK-12b exactly 140 bytes: routed: " + o);
			ucs = msg(310, "18667991226", DST, "x"); ucs.setContentType("UnicodeBigUnmarked"); ucs.setMessageSize(141); o = send(ucs); check(o.routedTo(401), "CK-12c NeedCheckMsgSize=false: routed: " + o);
			String r13 = "8613900000013";
			check(send(307, "18667991226", r13, "1").routedTo(401) && send(307, "18667991226", r13, "2").routedTo(401), "CK-13a limit 2: first two to one number routed");
			o = send(307, "18667991226", r13, "3"); check(o.rssid == -1 && o.dr != null, "CK-13b third refused: " + o);
			o = send(307, "18667991226", "8613900000014", "1"); check(o.routedTo(401), "CK-13c another number not affected: " + o);
			o = send(308, "18667991226", "8613900000020", "same"); check(o.routedTo(401), "CK-14a first routed: " + o);
			o = send(308, "18667991226", "8613900000020", "same"); check(o.rssid == -1, "CK-14b same content within the period: duplicate, not routed: " + o);
			o = send(308, "18667991226", "8613900000020", "different"); check(o.routedTo(401), "CK-15 different content: not a duplicate: " + o);
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
