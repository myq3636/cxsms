package it;

import java.nio.file.*;

import com.king.gmms.domain.DrErrorConfig;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * DR error codes (docs/DR错误码设计.md): a channel's codes to the internal ones (conf/drErrorMapping.cfg, the
 * built-in conf/drErrorCode.cfg), and the internal ones in the receipt the real Server sends to customers.
 */
public class DrErrorIT extends ItSupport {
	static final int PORT = 17807;
	static StreamQueueManager q;
	static int no = 0;

	static GmmsMessage vendorDr(int rssid, GmmsStatus st, String raw) {
		GmmsMessage m = mt("DE" + (++no), 61, rssid, 0);
		m.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		m.setStatus(st);
		DrErrorConfig.getInstance().applyVendorDr(m, raw);
		return m;
	}
	static String err(String receipt) {
		int i = receipt.indexOf("err:");
		if (i < 0) return "none";
		int e = receipt.indexOf(' ', i);
		return receipt.substring(i + 4, e < 0 ? receipt.length() : e);
	}
	static String stat(String receipt) {
		int i = receipt.indexOf("stat:");
		if (i < 0) return "none";
		int e = receipt.indexOf(' ', i);
		return receipt.substring(i + 5, e < 0 ? receipt.length() : e);
	}
	/** a DR for customer ossid as Core writes it; the receipt the customer gets */
	static String receipt(SmppTestClient c, int ossid, GmmsStatus st, String code, String raw) throws Exception {
		GmmsMessage d = mt("DR" + (++no), ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(st);
		d.setInMsgID("1A2B");
		d.setDrErrorCode(code);
		d.setVendorErrorCode(raw);
		c.deliverByText.clear();
		q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
		waitFor(() -> !c.deliverByText.isEmpty(), 8000);
		return c.deliverByText.isEmpty() ? "none" : c.deliverByText.keySet().iterator().next();
	}
	static String cust(String name, int ssid) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\n"
				+ "IncomingConnection: (ConnName=" + name + "_c1,userName=" + name.toLowerCase() + ",password=p,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n";
	}
	/** the network_error_code TLV (0x0423) of a deliver_sm body, null when none */
	static int[] tlv(byte[] body) {
		if (body == null) return null;
		// service_type; TON, NPI, source; TON, NPI, dest; esm, pid, priority; schedule; validity; 4 bytes; sm
		int i = 0;
		while (body[i] != 0) i++; i++;
		i += 2; while (body[i] != 0) i++; i++;
		i += 2; while (body[i] != 0) i++; i++;
		i += 3; while (body[i] != 0) i++; i++; while (body[i] != 0) i++; i++;
		i += 4;
		int n = body[i++] & 0xff;
		i += n;
		while (i + 4 <= body.length) {
			int tag = ((body[i] & 0xff) << 8) | (body[i + 1] & 0xff), len = ((body[i + 2] & 0xff) << 8) | (body[i + 3] & 0xff);
			if (tag == 0x0423 && len == 3) return new int[] { body[i + 4] & 0xff, ((body[i + 5] & 0xff) << 8) | (body[i + 6] & 0xff) };
			i += 4 + len;
		}
		return null;
	}

	public static void main(String[] args) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg");
		Files.write(cm, (new String(Files.readAllBytes(cm), "UTF-8") + cust("DECUSTA", 61) + cust("DECUSTB", 62) + cust("DECUSTC", 63)
				+ cust("DECUSTD", 64) + cust("DECUSTE", 65) + cust("DECUSTF", 66)
				+ "\n[DENOPAY]\nCCBSSID: 69\nA2PPlayerType: operator\nProtocol: SMPP\nSMSOptionDRNotPaidCode: 077,030001\n").getBytes("UTF-8"));
		Files.write(Paths.get(home + "conf/" + DrErrorConfig.MAPPING_FILE), ("[DEFAULT]\n"
				+ "DRErr: (Stat=UNDELIV,Err=1,Code=101)\n"
				+ "[VENDOR8]\n"
				+ "DRErr: (Stat=UNDELIV,Err=002~009,Code=104);(Err=088,Code=404);(Stat=REJECTD,Code=202)\n"
				+ "DRNetworkErr: (Type=3,Value=001B,Code=103)\n"
				+ "SubmitErr: (Status=0x0B,Code=102);(Status=*,Code=601)\n"
				+ "[DENOPAY]\nDRErr: (Err=077,Code=201);(Stat=UNDELIV,Err=5,Code=104)\nDRNetworkErr: (Type=3,Value=0001,Code=103)\n"
				+ "[DECUSTB]\nDRErrDefault: passthrough\n"
				+ "[DECUSTC]\nCustomerErr: (Code=104,Err=013);(Code=100~199,Err=003)\n"
				+ "[DECUSTD]\nDRErrDefault: vendor\n"
				+ "[DECUSTE]\nDRErrDefault: 000\n"
				+ "[DECUSTF]\nDRErrDefault: passthrough\nDRErrTLV: true\n").getBytes("UTF-8"));
		Path cfg = Paths.get(home + "Gmms/GmmsConfig.properties");
		Files.write(cfg, (new String(Files.readAllBytes(cfg), "UTF-8") + "\nAsyncSqlWriterEnable=false\n").getBytes("UTF-8"));
		msmFactory = CoreRetryIT.RecordingStore::new;
		try {
			boot();
			jedis.set("thcon", "1");

			// ---- the DB: the two columns in the SQL, RDQ retry by the code ----
			com.king.message.gmms.MessageStoreManager store = com.king.gmms.GmmsUtility.getInstance().getMessageStoreManager();
			GmmsMessage q1 = mt("DEQ1", 61, 8, 0);
			q1.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
			q1.setVendorErrorCode("stat=UNDELIV;err=5");
			q1.setDrErrorCode("104");
			props.setProperty("DB.DrErrorCodeColumns", "true");
			store.insertMessageToDB(q1, com.king.message.gmms.MessageStoreManager.RDQ);
			String sql = lastSql("INSERT INTO RDQ", "DEQ1");
			check(sql != null && sql.contains("VendorErrorCode='stat=UNDELIV;err=5'") && sql.contains("DrErrorCode='104'"), "DE-14a DB.DrErrorCodeColumns=true: the INSERT has both columns: " + sql);
			props.setProperty("DB.DrErrorCodeColumns", "false");
			GmmsMessage q2 = new GmmsMessage(q1);
			q2.setMsgID("DEQ2"); q2.setInMsgID("IN-DEQ2");
			store.insertMessageToDB(q2, com.king.message.gmms.MessageStoreManager.RDQ);
			sql = lastSql("INSERT INTO RDQ", "DEQ2");
			check(sql != null && !sql.contains("DrErrorCode"), "DE-14b false: no such columns in the SQL");
			props.setProperty("DB.DrErrorCodeColumns", "auto");
			com.king.message.gmms.MessageStoreManager.recheckDrErrorColumns();
			check(!store.drErrorColumns(), "DE-14c auto, no DB reachable: not used");
			props.remove("DB.DrErrorCodeColumns");
			java.lang.reflect.Method restore = com.king.message.gmms.MessageStoreManager.class.getDeclaredMethod("restoreResendDrStatus", GmmsMessage.class);
			restore.setAccessible(true);
			GmmsMessage r1 = mt("DER1", 61, 8, 0);
			r1.setStatusText(GmmsStatus.UNDELIVERY_NETWORK_ERROR.getText());
			r1.setDrErrorCode("104");
			check((Integer) restore.invoke(store, r1) == 10405, "DE-15a RDQ retry: the text alone is not a DR state, the code 104 gives 10405 (DEF-C5)");
			r1.setStatusText(GmmsStatus.UNDELIVERABLE.getText());
			check((Integer) restore.invoke(store, r1) == 10405, "DE-15b text UNDELIV, code of the same kind: 10405");
			r1.setDrErrorCode("000");
			check((Integer) restore.invoke(store, r1) == 10400, "DE-15c a code of another kind: the text decides: 10400");
			r1.setDrErrorCode(null);
			check((Integer) restore.invoke(store, r1) == 10400, "DE-15d no code: as before, by the text");

			// ---- a channel's codes to internal ones (what Core does with a vendor DR) ----
			GmmsMessage m = vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=5");
			check("104".equals(m.getDrErrorCode()) && m.getStatusCode() == 10405, "DE-01a UNDELIV err 5 in the range 002~009: 104, status 10405: " + m.getDrErrorCode() + " " + m.getStatusCode());
			m = vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=005");
			check("104".equals(m.getDrErrorCode()), "DE-01b 005 is 5: " + m.getDrErrorCode());
			m = vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=5;nec=3:001b");
			check("103".equals(m.getDrErrorCode()), "DE-02 network_error_code 3:001B first: 103: " + m.getDrErrorCode());
			m = vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=77");
			check("200".equals(m.getDrErrorCode()) && m.getStatusCode() == 10400, "DE-03a no rule: status kept, its own code 200: " + m.getDrErrorCode() + " " + m.getStatusCode());
			m = vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=1");
			check("101".equals(m.getDrErrorCode()), "DE-03b no channel rule: the DEFAULT block: 101: " + m.getDrErrorCode());
			m = vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=088");
			check("404".equals(m.getDrErrorCode()) && m.getStatusCode() == GmmsStatus.REJECTED_BYNC.getCode(), "DE-04 err 088 (any state) to 404: no credit, status 10800: " + m.getStatusCode());
			m = vendorDr(8, GmmsStatus.DELIVERED, "stat=DELIVRD;err=088");
			check("000".equals(m.getDrErrorCode()) && m.getStatusCode() == 10000, "DE-04b DELIVRD with err 088: still 000: " + m.getDrErrorCode());
			m = vendorDr(8, GmmsStatus.REJECTED, "stat=REJECTD;err=9");
			check("202".equals(m.getDrErrorCode()), "DE-04c state rule: REJECTD 202: " + m.getDrErrorCode());
			m = vendorDr(8, GmmsStatus.EXPIRED, null);
			check("501".equals(m.getDrErrorCode()) && m.getStatusCode() == 10200, "DE-04d nothing from the channel: the code of the status: 501: " + m.getDrErrorCode());
			GmmsMessage s = mt("DES1", 61, 8, 0);
			s.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			s.setVendorErrorCode(DrErrorConfig.submitRaw(0x0B));
			DrErrorConfig.getInstance().applySubmitResult(s);
			check("102".equals(s.getDrErrorCode()), "DE-08a submit_sm_resp 0x0B: 102: " + s.getDrErrorCode());
			s.setVendorErrorCode(DrErrorConfig.submitRaw(0x58));
			DrErrorConfig.getInstance().applySubmitResult(s);
			check("601".equals(s.getDrErrorCode()), "DE-08b other command_status: 601: " + s.getDrErrorCode());
			check(itLog("[DR-ERR-UNMAPPED]"), "DE-03c the unmapped code is logged");
			// SMSOptionDRNotPaidCode of the channel (cm.cfg) before the mapping rules
			m = vendorDr(69, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=077");
			check(m.getStatusCode() == GmmsStatus.REJECTED_BYNC.getCode() && "404".equals(m.getDrErrorCode()), "DE-04e SMSOptionDRNotPaidCode 077 wins over the DRErr rule: REJECTED_BYNC, 404: " + m.getStatusCode() + " " + m.getDrErrorCode());
			m = vendorDr(69, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=5;nec=3:0001");
			check(m.getStatusCode() == GmmsStatus.REJECTED_BYNC.getCode() && "404".equals(m.getDrErrorCode()), "DE-04f network_error_code 030001 on the list wins over the DRNetworkErr rule: " + m.getStatusCode() + " " + m.getDrErrorCode());
			m = vendorDr(69, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=5");
			check(m.getStatusCode() == 10405 && "104".equals(m.getDrErrorCode()), "DE-04g not on the list: the mapping rules: " + m.getStatusCode() + " " + m.getDrErrorCode());

			// ---- the stream carries both ----
			m = vendorDr(8, GmmsStatus.EXPIRED, null);
			GmmsMessage back = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(com.king.redis.SerializableHandler.convertGmmsMessage2RedisMessage(m));
			check(back != null && "501".equals(back.getDrErrorCode()), "DE-10a drErrorCode in the stream format");
			GmmsMessage v = vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=5;nec=3:001B");
			back = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(com.king.redis.SerializableHandler.convertGmmsMessage2RedisMessage(v));
			check(back != null && "stat=UNDELIV;err=5;nec=3:001B".equals(back.getVendorErrorCode()), "DE-10b vendorErrorCode in the stream format: " + (back == null ? "" : back.getVendorErrorCode()));

			// ---- the receipt the customer gets from the real Server ----
			props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
			MultiSmppServer server = new MultiSmppServer();
			server.startService();
			Thread.sleep(1500);
			q = StreamQueueManager.getInstance();
			String raw = "stat=UNDELIV;err=5";
			String[][] cases = {
				{ "decusta", "61", "016", "DE-05a no customer block: the table customers had (10405 -> 016)" },
				{ "decustb", "62", "104", "DE-05b passthrough: the internal code" },
				{ "decustc", "63", "013", "DE-05c CustomerErr 104 -> 013" },
				{ "decustd", "64", "005", "DE-05d vendor: the channel's err, 3 digits" },
				{ "decuste", "65", "000", "DE-05e 000" } };
			for (String[] x : cases) {
				SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, 5, null);
				check(c.bind(x[0], "p"), x[3] + ": bound");
				String r = receipt(c, Integer.parseInt(x[1]), GmmsStatus.UNDELIVERY_NETWORK_ERROR, "104", raw);
				System.out.println("INFO " + x[0] + " " + r);
				check(x[2].equals(err(r)) && "UNDELIV".equals(stat(r)), x[3] + ": err:" + err(r) + " stat:" + stat(r));
				c.close();
			}
			SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, 5, null);
			c.bind("decustc", "p");
			String r = receipt(c, 63, GmmsStatus.UNDELIVERY_RECIPIENT_ERROR, "101", raw);
			check("003".equals(err(r)), "DE-05f CustomerErr range 100~199 -> 003: " + err(r));
			r = receipt(c, 63, GmmsStatus.DELIVERED, "000", "stat=DELIVRD");
			check("000".equals(err(r)) && "DELIVRD".equals(stat(r)), "DE-05g delivered: 000 and DELIVRD: " + err(r));
			c.close();
			c = new SmppTestClient("127.0.0.1", PORT, 5, null);
			c.bind("decustf", "p");
			r = receipt(c, 66, GmmsStatus.UNDELIVERY_NETWORK_ERROR, "104", raw);
			int[] t = tlv(c.lastDeliverBody);
			check("104".equals(err(r)) && t != null && t[0] == 3 && t[1] == 104, "DE-06 DRErrTLV: network_error_code type 3 value 104: " + (t == null ? "none" : t[0] + "/" + t[1]));
			c.close();
			c = new SmppTestClient("127.0.0.1", PORT, 5, null);
			c.bind("decusta", "p");
			r = receipt(c, 61, GmmsStatus.UNDELIVERABLE, null, null);
			check("003".equals(err(r)), "DE-09 a DR without a code (fake / system DR) to a customer without a block: the table: 003: " + err(r));
			check(tlv(c.lastDeliverBody) == null, "DE-06b no TLV without DRErrTLV");
			c.close();

			// ---- reload ----
			Files.write(Paths.get(home + "conf/" + DrErrorConfig.MAPPING_FILE), "[VENDOR8]\nDRErr: (Stat=UNDELIV,Err=5,Code=201)\n".getBytes("UTF-8"));
			check(DrErrorConfig.getInstance().reload(), "DE-12a reload");
			check("201".equals(vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=5").getDrErrorCode()), "DE-12b the new rule applies");
			Files.write(Paths.get(home + "conf/" + DrErrorConfig.MAPPING_FILE), "[VENDOR8]\nDRErr: (Stat=UNDELIV,Err=5,Code=777)\n".getBytes("UTF-8"));
			check(!DrErrorConfig.getInstance().reload() && "201".equals(vendorDr(8, GmmsStatus.UNDELIVERABLE, "stat=UNDELIV;err=5").getDrErrorCode()),
					"DE-12c a code not in the table: reload refused, the rules in use kept");
			server.stopService();
		} catch (Throwable x) { x.printStackTrace(System.out); failures++; }
		exit();
	}

	/** the newest recorded SQL that starts with head and mentions the msgId */
	static String lastSql(String head, String id) {
		String found = null;
		synchronized (CoreRetryIT.sql) { for (String x : CoreRetryIT.sql) if (x.startsWith(head) && x.contains("IN-" + id)) found = x; }
		return found;
	}
	static boolean itLog(String part) throws Exception {
		for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(part)) return true;
		return false;
	}
}
