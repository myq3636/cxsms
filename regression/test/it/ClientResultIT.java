package it;

import java.io.ByteArrayOutputStream;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.client.MultiSmppClient;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.OutboundStreamConsumer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.protocol.smpp.Smpp;
import com.king.gmms.protocol.smpp.pdu.DeliverSM;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.SerializableHandler;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

/** What the vendor answers, as Core gets it: submit_sm_resp codes and message ids, DR receipts (text and TLV), MO; and how a DR status is shown to the customer. */
public class ClientResultIT extends ItSupport {
	static final int PORT = 17793;
	static StreamQueueManager q;
	static int no = 0;

	static String vendor(String name, int ssid, String user, String opts) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nIsInit: true\nReconnectInterval: 1\nEnquireLink: false\nWindowSize: 50\nBufferTimeout: 60\n" + opts
				+ "OutgoingConnection: (ConnName=" + name + "_c1,userName=" + user + ",password=p,URL=127.0.0.1,Port=" + PORT + ",SessionNumber=1,BindMode=TRX,Version=V34,IsInit=true)\n";
	}
	static GmmsMessage submit(FakeSmsc.Account a, int rssid) throws Exception {
		String id = "8888_CR" + (++no);
		GmmsMessage m = mt(id, 8888, rssid, 0);
		q.produceOutboundMessage(m);
		waitFor(() -> !inStreams("stream:core:results*", id).isEmpty(), 8000);
		List<GmmsMessage> l = inStreams("stream:core:results*", id);
		return l.isEmpty() ? null : l.get(0);
	}
	static List<GmmsMessage> all(String pattern) {
		List<GmmsMessage> r = new ArrayList<>();
		for (String k : jedis.keys(pattern))
			for (StreamEntry e : jedis.xrange(k, (StreamEntryID) null, (StreamEntryID) null)) {
				GmmsMessage g = SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
				if (g != null) r.add(g);
			}
		return r;
	}
	/** the newest DR written for Core since n DRs were there */
	static GmmsMessage newDr(int ssid, int before) throws Exception {
		waitFor(() -> all("stream:core:inbound-dr:" + ssid + "*").size() > before, 5000);
		List<GmmsMessage> l = all("stream:core:inbound-dr:" + ssid + "*");
		return l.size() > before ? l.get(l.size() - 1) : null;
	}
	static int drs(int ssid) { return all("stream:core:inbound-dr:" + ssid + "*").size(); }
	static String show(GmmsMessage g) { return g == null ? "none" : "out=" + g.getOutMsgID() + " status=" + g.getStatusCode() + " from=" + g.getSenderAddress() + " to=" + g.getRecipientAddress(); }
	static byte[] receipt(String id, String stat, String err) throws Exception {
		return ("id:" + id + " sub:001 dlvrd:001 submit date:2611031200 done date:2611031201 stat:" + stat + " err:" + err + " text:x").getBytes("US-ASCII");
	}
	static int resp(FakeSmsc.Account a, int seq) throws Exception {
		waitFor(() -> a.deliverResp.containsKey(seq), 5000);
		return a.deliverResp.getOrDefault(seq, -1);
	}
	static GmmsMessage dr(FakeSmsc.Account a, int ssid, byte[] sm, byte[] tlvs) throws Exception {
		int before = drs(ssid);
		int seq = a.deliver("8613800000000", "18667991226", 4, sm, tlvs);
		check(resp(a, seq) == 0, "    deliver_sm_resp ROK, seq " + seq);
		return newDr(ssid, before);
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		StringBuilder cm = new StringBuilder();
		cm.append(vendor("RV401", 401, "u401", "SMSOptionSubmitNotPaidMapping: 88:" + GmmsStatus.SUBMIT_NOTPAID.getCode() + "\n"));
		cm.append(vendor("RV402", 402, "u402", ""));
		cm.append(vendor("RV403", 403, "u403", "SMSOptionSubmitNotPaidMapping: 88:" + GmmsStatus.SUBMIT_NOTPAID.getCode() + "\n"));
		cm.append(vendor("RV404", 404, "u404", "ChlSMPPMsgIDParse: true\n"));
		cm.append(vendor("RV405", 405, "u405", "ChlSMPPMsgIDParse: true\nSMSOptionSMPPIsPadZero4SR: false\n"));
		cm.append(vendor("RV406", 406, "u406", "SMSOptionDRStatusIsOptionPara: true\n"));
		cm.append(vendor("RV407", 407, "u407", "SMSOptionDRNotPaidCode: 088,030001\n"));
		cm.append(vendor("RV408", 408, "u408", "DrSwapAddr: false\n"));
		cm.append(vendor("RV409", 409, "u409", "SMSOptionRemoveUdh: true\n"));
		cm.append("\n[RC410]\nCCBSSID: 410\nA2PPlayerType: operator\nProtocol: SMPP\nSMSOptionDRStatusMapping: DELIVRD:ACCEPTD\n");
		cm.append("\n[RC411]\nCCBSSID: 411\nA2PPlayerType: operator\nProtocol: SMPP\nSMSOptionIsNotSupportUnknowDRStatus: true\n");
		cm.append("\n[RC412]\nCCBSSID: 412\nA2PPlayerType: operator\nProtocol: SMPP\n");
		cm.append("\n[RC413]\nCCBSSID: 413\nA2PPlayerType: operator\nProtocol: SMPP\nSMSOptionSmppMapErrCod4DR: true\n");
		Path p = Paths.get(home + "conf/cm.cfg");
		Files.write(p, (new String(Files.readAllBytes(p), "UTF-8") + cm).getBytes("UTF-8"));
		FakeSmsc smsc = new FakeSmsc(PORT);
		FakeSmsc.Account a401 = smsc.account("u401"), a402 = smsc.account("u402"), a403 = smsc.account("u403"), a404 = smsc.account("u404"), a405 = smsc.account("u405"), a406 = smsc.account("u406"), a407 = smsc.account("u407"), a408 = smsc.account("u408"), a409 = smsc.account("u409");
		try {
			boot();
			q = StreamQueueManager.getInstance();
			new MultiSmppClient().startService();
			OutboundStreamConsumer.getInstance().start();
			check(waitFor(() -> a401.binds.get() == 1 && a402.binds.get() == 1 && a403.binds.get() == 1 && a404.binds.get() == 1 && a405.binds.get() == 1 && a406.binds.get() == 1 && a407.binds.get() == 1 && a408.binds.get() == 1 && a409.binds.get() == 1, 15000), "RS-00 all bound");
			Thread.sleep(300);

			// ---- submit_sm_resp status ----
			Object[][] codes = {
				{ 0x00, GmmsStatus.SUCCESS }, { 0x03, GmmsStatus.OPER_UNAVAILABLE }, { 0x01, GmmsStatus.INVALID_MSG_FIELD }, { 0x45, GmmsStatus.UNDELIVERED },
				{ 0x0A, GmmsStatus.SENDER_ADDR_ERROR }, { 0x0B, GmmsStatus.RECIPIENT_ADDR_ERROR }, { 0x14, GmmsStatus.Throttled }, { 0x58, GmmsStatus.Throttled },
				{ 0x11, GmmsStatus.COMMUNICATION_ERROR }, { 0xFE, GmmsStatus.COMMUNICATION_ERROR }, { 0x08, GmmsStatus.SERVER_ERROR }, { 0x66, GmmsStatus.POLICY_DENIED },
				{ 0x0E, GmmsStatus.AUTHENTICATION_ERROR }, { 0x400, GmmsStatus.UNKNOWN_ERROR } };
			for (Object[] c : codes) {
				a402.submitStatus = (Integer) c[0];
				GmmsMessage r = submit(a402, 402);
				int want = ((GmmsStatus) c[1]).getCode();
				check(r != null && r.getStatusCode() == want, "RS-02 command_status 0x" + Integer.toHexString((Integer) c[0]) + " -> " + want + ": " + (r == null ? "none" : r.getStatusCode()));
			}
			a402.submitStatus = 0;

			// ---- not paid mapping ----
			a403.submitStatus = 88;
			GmmsMessage r = submit(a403, 403);
			check(r != null && r.getStatusCode() == GmmsStatus.SUBMIT_NOTPAID.getCode(), "RS-03a SMSOptionSubmitNotPaidMapping 88 -> 8000: " + (r == null ? "none" : r.getStatusCode()));
			a403.submitStatus = 0x0A;
			r = submit(a403, 403);
			check(r != null && r.getStatusCode() == GmmsStatus.SENDER_ADDR_ERROR.getCode(), "RS-03b code not in the mapping: the normal status");
			a403.submitStatus = 0;

			// ---- message id ----
			a402.submitMessageId = "1A2B"; r = submit(a402, 402);
			check(r != null && "1A2B".equals(r.getOutMsgID()), "RS-04a default: message_id kept: " + (r == null ? "" : r.getOutMsgID()));
			a404.submitMessageId = "1A2B"; r = submit(a404, 404);
			System.out.println("INFO parse " + (r == null ? "none" : r.getOutMsgID()));
			String idParsed = r == null ? null : r.getOutMsgID();
			check("0000006699".equals(idParsed), "RS-04b ChlSMPPMsgIDParse: hex to decimal, and padded (SMSOptionSMPPIsPadZero4SR defaults to it): " + idParsed);
			a405.submitMessageId = "1A2B"; r = submit(a405, 405);
			check(r != null && "6699".equals(r.getOutMsgID()), "RS-04c SMSOptionSMPPIsPadZero4SR false: not padded: " + (r == null ? "" : r.getOutMsgID()));
			GmmsMessage v = dr(a404, 404, receipt("0000006699", "DELIVRD", "000"), null);
			check(v != null && "0000006699".equals(v.getOutMsgID()), "RS-04d the DR carries the same id, so Core can match it: " + show(v));

			// ---- text receipts ----
			Object[][] stats = { { "DELIVRD", 10000 }, { "UNDELIV", 10400 }, { "EXPIRED", 10200 }, { "DELETED", 10300 }, { "REJECTD", 10500 }, { "UNKNOWN", 10900 }, { "ACCEPTD", 10001 } };
			int n = 0;
			for (Object[] c : stats) {
				String id = "MSG" + (++n);
				v = dr(a402, 402, receipt(id, (String) c[0], "000"), null);
				check(v != null && id.equals(v.getOutMsgID()) && v.getStatusCode() == (Integer) c[1], "VD-05a stat:" + c[0] + " -> " + c[1] + ": " + show(v));
			}
			v = dr(a402, 402, "id:1234567890 stat:DELIVRD err:000".getBytes("US-ASCII"), null);
			check(v != null && "1234567890".equals(v.getOutMsgID()) && v.getStatusCode() == 10000, "VD-05b no sub: field: the 10 characters after id: " + show(v));
			check(v != null && "18667991226".equals(v.getSenderAddress()) && "8613800000000".equals(v.getRecipientAddress()), "VD-09a DrSwapAddr default true: source and dest swapped: " + show(v));
			v = dr(a408, 408, receipt("SW1", "DELIVRD", "000"), null);
			check(v != null && "8613800000000".equals(v.getSenderAddress()) && "18667991226".equals(v.getRecipientAddress()), "VD-09b DrSwapAddr false: as the vendor sent them: " + show(v));

			// ---- receipts without text ----
			byte[] id = (("TLV1") + "\0").getBytes("US-ASCII");
			Object[][] states = { { 2, 10000 }, { 6, 10000 }, { 3, GmmsStatus.EXPIRED.getCode() }, { 4, GmmsStatus.DELETED.getCode() }, { 5, GmmsStatus.UNDELIVERABLE.getCode() }, { 8, GmmsStatus.REJECTED.getCode() }, { 9, GmmsStatus.UNKNOWN.getCode() } };
			for (Object[] c : states) {
				ByteArrayOutputStream t = new ByteArrayOutputStream();
				t.write(FakeSmsc.tlv(0x001E, id)); t.write(FakeSmsc.tlv(0x0427, new byte[] { ((Integer) c[0]).byteValue() }));
				v = dr(a406, 406, new byte[0], t.toByteArray());
				check(v != null && "TLV1".equals(v.getOutMsgID()) && v.getStatusCode() == (Integer) c[1], "VD-04a SMSOptionDRStatusIsOptionPara, message_state " + c[0] + " -> " + c[1] + ": " + show(v));
			}
			ByteArrayOutputStream t = new ByteArrayOutputStream();
			t.write(FakeSmsc.tlv(0x001E, id)); t.write(FakeSmsc.tlv(0x0423, new byte[] { 3, 0, 1 }));
			v = dr(a406, 406, new byte[0], t.toByteArray());
			check(v != null && v.getStatusCode() == GmmsStatus.UNDELIVERABLE.getCode(), "VD-04b only network_error_code: UNDELIVERABLE: " + show(v));
			v = dr(a406, 406, new byte[0], FakeSmsc.tlv(0x001E, id));
			check(v != null && v.getStatusCode() == GmmsStatus.DELIVERED.getCode(), "VD-04c only receipted_message_id: DELIVERED: " + show(v));
			t = new ByteArrayOutputStream();
			t.write(FakeSmsc.tlv(0x001E, id)); t.write(FakeSmsc.tlv(0x0427, new byte[] { 5 }));
			v = dr(a402, 402, new byte[0], t.toByteArray());
			check(v != null && "TLV1".equals(v.getOutMsgID()) && v.getStatusCode() == GmmsStatus.UNDELIVERABLE.getCode(), "VD-04d no option, empty text: read from the TLVs too: " + show(v));

			// ---- not paid ----
			v = dr(a407, 407, receipt("NP1", "UNDELIV", "088"), null);
			check(v != null && v.getStatusCode() == GmmsStatus.REJECTED_BYNC.getCode(), "VD-08a SMSOptionDRNotPaidCode, err:088: REJECTED_BYNC: " + show(v));
			v = dr(a407, 407, receipt("NP2", "UNDELIV", "001"), null);
			check(v != null && v.getStatusCode() == 10400, "VD-08b other err: stays UNDELIV: " + show(v));
			v = dr(a407, 407, receipt("NP3", "UNDELIV", "001"), FakeSmsc.tlv(0x0423, new byte[] { 3, 0, 1 }));
			check(v != null && v.getStatusCode() == GmmsStatus.REJECTED_BYNC.getCode(), "VD-08c network_error_code 030001 on the list: REJECTED_BYNC: " + show(v));

			// ---- a receipt that cannot be parsed ----
			int before = drs(402);
			int seq = a402.deliver("8613800000000", "18667991226", 4, "garbage without fields".getBytes("US-ASCII"), null);
			int st = resp(a402, seq);
			Thread.sleep(300);
			System.out.println("INFO garbage resp=0x" + Integer.toHexString(st) + " written=" + (drs(402) - before));
			check(a402.open().size() == 1, "VD-03a receipt with no id: the session survives");
			check(st != -1, "VD-03b it is answered: 0x" + Integer.toHexString(st));
			GmmsMessage g = drs(402) > before ? newDr(402, before) : null;
			check(st == 0 && g != null && g.getOutMsgID() == null, "VD-03c answered ROK and written with no id, Core cannot match it: resp 0x" + Integer.toHexString(st) + " " + show(g));

			// ---- Redis refuses the write ----
			check(drs(402) > 0, "VD-02a the DR stream has entries");
			before = drs(402);
			props.setProperty("RedisStreamBackPressureEnable", "true");
			props.setProperty("RedisStreamMonitor.BlockXLEN", "1");
			seq = a402.deliver("8613800000000", "18667991226", 4, receipt("BP1", "DELIVRD", "000"), null);
			st = resp(a402, seq);
			check(st == 0x64, "VD-02b DR not written: ESME_RX_T_APPN, the vendor sends it again: 0x" + Integer.toHexString(st));
			check(drs(402) == before, "VD-02c nothing written");
			props.remove("RedisStreamBackPressureEnable");
			props.remove("RedisStreamMonitor.BlockXLEN");
			v = dr(a402, 402, receipt("BP1", "DELIVRD", "000"), null);
			check(v != null && "BP1".equals(v.getOutMsgID()), "VD-02d the vendor's next try is accepted: " + show(v));

			// ---- MO ----
			seq = a402.deliver("8613800000000", "18667991226", 0, "mo text".getBytes("US-ASCII"), null);
			check(resp(a402, seq) == 0, "VD-10a MO: deliver_sm_resp ROK");
			waitFor(() -> !all("stream:mt:pending:402:*").isEmpty(), 5000);
			List<GmmsMessage> mo = all("stream:mt:pending:402:*");
			check(mo.size() == 1 && "mo text".equals(mo.get(0).getTextContent()) && mo.get(0).getOSsID() == 402, "VD-10b written to stream:mt:pending with the vendor as ossid: " + (mo.isEmpty() ? "none" : mo.get(0).getTextContent()));
			props.setProperty("RedisStreamBackPressureEnable", "true");
			props.setProperty("RedisStreamMonitor.BlockXLEN", "1");
			seq = a402.deliver("8613800000000", "18667991226", 0, "mo two".getBytes("US-ASCII"), null);
			st = resp(a402, seq);
			check(st == 0x64, "VD-10c MO not written: ESME_RX_T_APPN: 0x" + Integer.toHexString(st));
			props.remove("RedisStreamBackPressureEnable");
			props.remove("RedisStreamMonitor.BlockXLEN");

			// ---- UDH on incoming ----
			byte[] udh = { 6, 5, 4, 0x0B, (byte) 0x84, 0x23, (byte) 0xF0, 'h', 'i' };
			seq = a409.deliver("8613800000000", "18667991226", 0x40, udh, null);
			check(resp(a409, seq) == 0, "VD-11a MO with UDH accepted");
			waitFor(() -> !all("stream:mt:pending:409:*").isEmpty(), 5000);
			List<GmmsMessage> ud = all("stream:mt:pending:409:*");
			check(ud.size() == 1 && ud.get(0).getUdh() == null, "VD-11b SMSOptionRemoveUdh: UDH removed: " + (ud.isEmpty() ? "none" : String.valueOf(ud.get(0).getUdh())));
			seq = a402.deliver("8613800000000", "18667991226", 0x40, udh, null);
			resp(a402, seq);
			waitFor(() -> all("stream:mt:pending:402:*").size() >= 2, 5000);
			List<GmmsMessage> ku = all("stream:mt:pending:402:*");
			GmmsMessage last = ku.get(ku.size() - 1);
			check(last.getUdh() != null && last.getUdh().length == 7, "VD-11c channel without the option: UDH kept: " + (last.getUdh() == null ? "null" : last.getUdh().length));

			// ---- the status shown to the customer (built by the Server when the DR goes out) ----
			Smpp smpp = new Smpp();
			String text = customerDr(smpp, 410, GmmsStatus.DELIVERED);
			check(text.contains("stat:ACCEPTD"), "VD-06a SMSOptionDRStatusMapping DELIVRD:ACCEPTD: the customer sees ACCEPTD: " + text);
			text = customerDr(smpp, 410, GmmsStatus.UNDELIVERABLE);
			check(text.contains("stat:UNDELIV"), "VD-06b status not in the mapping: unchanged");
			text = customerDr(smpp, 411, GmmsStatus.UNKNOWN);
			check(text.contains("stat:UNDELIV"), "VD-07a SMSOptionIsNotSupportUnknowDRStatus: UNKNOWN shown as UNDELIV: " + text);
			text = customerDr(smpp, 411, GmmsStatus.DELETED);
			check(text.contains("stat:UNDELIV"), "VD-07b DELETED shown as UNDELIV");
			text = customerDr(smpp, 412, GmmsStatus.UNKNOWN);
			check(text.contains("stat:UNKNOWN"), "VD-07c customer without the option: UNKNOWN: " + text);
			text = customerDr(smpp, 413, GmmsStatus.UNDELIVERABLE);
			check(text.contains("err:003"), "VD-09d SMSOptionSmppMapErrCod4DR: 10400 shown as err:003: " + text);
			text = customerDr(smpp, 413, GmmsStatus.EXPIRED);
			check(text.contains("err:001"), "VD-09e 10200 shown as err:001: " + text);
			text = customerDr(smpp, 412, GmmsStatus.UNDELIVERABLE);
			check(text.contains("err:000"), "VD-09f customer without the option: err:000: " + text);
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
	static String customerDr(Smpp smpp, int ssid, GmmsStatus st) throws Exception {
		A2PCustomerInfo c = GmmsUtility.getInstance().getCustomerManager().getCustomerBySSID(ssid);
		GmmsMessage d = mt(ssid + "_CDR" + (++no), ssid, 402, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(st);
		d.setInMsgID("1A2B");      // ids of this system are hex: the receipt shows them as decimal
		DeliverSM pdu = smpp.createSmppDeliverSM4dr(d, c);
		return pdu == null ? "null" : pdu.getShortMessage();
	}
}
