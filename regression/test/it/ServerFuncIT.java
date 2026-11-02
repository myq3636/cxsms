package it;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Functional test list of the SMPP server on a real MultiSmppServer. Verdicts follow SMPP 3.4 and the configuration reference; no product code is changed. */
public class ServerFuncIT extends ItSupport {
	static final int PORT = 17776;
	static final String DST = "8613800000000";

	static class Pdu { int cmd, status, seq; byte[] body = new byte[0];
		public String toString() { return String.format("cmd=0x%08x status=0x%02x seq=%d", cmd, status, seq); } }

	/** Raw SMPP client: everything it reads is kept in order; no hidden answers. */
	static class R implements Closeable {
		final Socket s; final DataInputStream in; final OutputStream out; int seq = 100;
		R() throws Exception { this(PORT); }
		R(int port) throws Exception { s = new Socket("127.0.0.1", port); s.setTcpNoDelay(true); s.setSoTimeout(3000); in = new DataInputStream(s.getInputStream()); out = s.getOutputStream(); }
		synchronized void send(int cmd, int sq, byte[] body) throws Exception { ByteBuffer b = ByteBuffer.allocate(16 + body.length); b.putInt(16 + body.length).putInt(cmd).putInt(0).putInt(sq).put(body); out.write(b.array()); out.flush(); }
		/** next PDU, null on timeout; cmd=-1 when closed */
		Pdu read(int ms) throws Exception {
			s.setSoTimeout(ms); Pdu p = new Pdu();
			try { int len = in.readInt(); p.cmd = in.readInt(); p.status = in.readInt(); p.seq = in.readInt(); p.body = new byte[Math.max(0, len - 16)]; in.readFully(p.body); return p; }
			catch (SocketTimeoutException e) { return null; } catch (IOException e) { p.cmd = -1; return p; }
		}
		Pdu read() throws Exception { return read(3000); }
		boolean closedWithin(int ms) throws Exception { long e = System.currentTimeMillis() + ms; while (System.currentTimeMillis() < e) { Pdu p = read(Math.max(50, (int) (e - System.currentTimeMillis()))); if (p != null && p.cmd == -1) return true; } return false; }
		Pdu bind(int cmd, String u, String p, int ver) throws Exception { ByteBuffer b = ByteBuffer.allocate(256); b.put(c(u)).put(c(p)).put(c("")).put((byte) ver).put((byte) 0).put((byte) 0).put(c("")); send(cmd, 1, arr(b)); return read(); }
		Pdu bind(String u, String p) throws Exception { return bind(9, u, p, 0x34); }
		Pdu submit(byte[] body) throws Exception { send(4, ++seq, body); return read(); }
		public void close() { try { s.close(); } catch (Exception e) { } }
	}
	/** a refused bind: this status, or the close that lost the bind_resp (DEF-03, measured by B-05) */
	static boolean rej(Pdu p, int status) { return p != null && (p.cmd == -1 || p.status == status); }
	static boolean rejAny(Pdu p) { return p != null && (p.cmd == -1 || p.status != 0); }
	static byte[] c(String s) { byte[] b = s.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1); return Arrays.copyOf(b, b.length + 1); }
	static byte[] arr(ByteBuffer b) { return Arrays.copyOf(b.array(), b.position()); }
	static String cstr(ByteBuffer b) { StringBuilder sb = new StringBuilder(); byte x; while (b.hasRemaining() && (x = b.get()) != 0) sb.append((char) (x & 0xff)); return sb.toString(); }

	/** submit_sm body; tlvs appended raw */
	static byte[] sub(String src, String dst, int esm, int reg, int dcs, byte[] sm, byte[] tlvs) {
		ByteBuffer b = ByteBuffer.allocate(1024);
		b.put(c("")).put((byte) 5).put((byte) 0).put(c(src)).put((byte) 1).put((byte) 1).put(c(dst)).put((byte) esm).put((byte) 0).put((byte) 0).put(c("")).put(c(""))
				.put((byte) reg).put((byte) 0).put((byte) dcs).put((byte) 0).put((byte) sm.length).put(sm);
		if (tlvs != null) b.put(tlvs);
		return arr(b);
	}
	static byte[] sub(String text) { return sub("SENDER", DST, 0, 1, 0, text.getBytes(), null); }
	static byte[] tlv(int tag, byte[] v) { ByteBuffer b = ByteBuffer.allocate(4 + v.length); b.putShort((short) tag).putShort((short) v.length).put(v); return b.array(); }
	static byte[] cat(byte[]... p) { ByteArrayOutputStream o = new ByteArrayOutputStream(); for (byte[] x : p) o.write(x, 0, x.length); return o.toByteArray(); }
	static String msgId(Pdu p) { return p == null ? null : cstr(ByteBuffer.wrap(p.body)); }

	static GmmsMessage find(int ssid, String inMsgId) throws Exception {
		long end = System.currentTimeMillis() + 2000;
		while (System.currentTimeMillis() < end) {
			for (String k : jedis.keys("stream:mt:pending:" + ssid + ":*"))
				for (redis.clients.jedis.resps.StreamEntry e : jedis.xrange(k, (redis.clients.jedis.StreamEntryID) null, (redis.clients.jedis.StreamEntryID) null)) {
					GmmsMessage m = com.king.redis.SerializableHandler.convertRedisMssage2GmmsMessage(e.getFields().get("body"));
					if (m != null && inMsgId.equals(m.getInMsgID())) return m;
				}
			Thread.sleep(50);
		}
		return null;
	}
	static GmmsMessage dr(String id, int ossid, GmmsStatus st) {
		GmmsMessage d = mt(id, ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(st);
		d.setInMsgID(id);
		return d;
	}
	static void sendDr(GmmsMessage d) throws Exception { StreamQueueManager q = StreamQueueManager.getInstance(); q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d)); }
	/** deliver_sm fields */
	static Map<String, String> deliver(Pdu p) {
		Map<String, String> m = new HashMap<String, String>(); ByteBuffer b = ByteBuffer.wrap(p.body);
		cstr(b); b.get(); b.get(); m.put("src", cstr(b)); b.get(); b.get(); m.put("dst", cstr(b)); m.put("esm", String.valueOf(b.get() & 0xff)); b.get(); b.get(); cstr(b); cstr(b); b.get(); b.get(); b.get(); b.get();
		int l = b.get() & 0xff; byte[] sm = new byte[l]; b.get(sm); m.put("text", new String(sm));
		while (b.remaining() >= 4) { int t = b.getShort() & 0xffff, n = b.getShort() & 0xffff; byte[] v = new byte[n]; b.get(v); m.put("tlv" + Integer.toHexString(t), new String(v).trim()); }
		return m;
	}
	static Pdu nextDeliver(R r, int ms) throws Exception { long e = System.currentTimeMillis() + ms; while (System.currentTimeMillis() < e) { Pdu p = r.read(Math.max(50, (int) (e - System.currentTimeMillis()))); if (p != null && p.cmd == -1) return null; if (p != null && p.cmd == 5) return p; } return null; }
	static String cust(String name, int ssid, String user, String opts, String conn) {
		return "\n[" + name + "]\nCCBSSID: " + ssid + "\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\n" + opts
				+ "IncomingConnection: (ConnName=" + name + "_c1,userName=" + user + ",password=pw" + ssid + ",URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34" + conn + ")\n";
	}
	static long logs(String t) throws Exception { long n = 0; for (String l : Files.readAllLines(Paths.get(home + "log/it.log"))) if (l.contains(t)) n++; return n; }

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg"); byte[] orig = Files.readAllBytes(cm);
		StringBuilder x = new StringBuilder(new String(orig, "UTF-8"));
		x.append(cust("F70", 70, "f70", "", ""));
		x.append(cust("F71", 71, "f71", "SMSOptionIncomingThrottlingNum: 5\n", ""));
		x.append(cust("F72", 72, "f72", "SMSOptionChargeByGateWay: true\n", ""));
		x.append(cust("F73", 73, "f73", "SMSOptionChargeCountryByGateWay: true\n", ""));
		x.append(cust("F74", 74, "f74", "", "").replace("SessionNumber=20", "SessionNumber=2"));
		x.append(cust("F75", 75, "f75", "", "").replace("URL=127.0.0.1", "URL=10.0.0.1"));
		x.append("\n[F90]\nCCBSSID: 90\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\nIncomingConnection: (ConnName=F90_c1,userName=f90,password=pw90,URL=10.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34);(ConnName=F90_c2,userName=f90,password=pw90,URL=10.0.0.2,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n");
		x.append("\n[F91]\nCCBSSID: 91\nA2PPlayerType: operator\nProtocol: SMPP\nConnectionType: MC\nWindowSize: 20\nIncomingConnection: (ConnName=F91_c1,userName=f91,password=pw91,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34);(ConnName=F91_c2,userName=f91,password=pw91,URL=10.0.0.2,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n");
		x.append(cust("F76", 76, "f76", "", "").replace("Version=V34", "Version=V33"));
		x.append(cust("F77", 77, "f77", "", "").replace("BindMode=TRX", "BindMode=R"));
		x.append(cust("F78", 78, "f78", "", "").replace("BindMode=TRX", "BindMode=T"));
		x.append(cust("F79", 79, "f79", "SMSOptionSMPPIsGenHexMsgId: true\n", ""));
		x.append(cust("F80", 80, "f80", "SMSOptionSupportMaxInMsgID: true\n", ""));
		x.append(cust("F81", 81, "f81", "SMSOptionSMPPNeedReceiptedMessageId: true\nDrSwapAddr: false\n", ""));
		x.append(cust("F82", 82, "f82", "SmppServerModules: OtherServer9\n", ""));
		x.append(cust("F83", 83, "f83", "ServerEnquireLink: true\nEnquireLinkTime: 1\nFailEnquireLinkTime: 1\nEnquireLinkFailureNum: 2\n", ""));
		x.append(cust("F84", 84, "f84", "ConnectionSilentTime: 3\n", ""));
		x.append(cust("F85", 85, "f85", "", "").replace("WindowSize: 20", "WindowSize: 2"));
		x.append(cust("F86", 86, "f86", "SMPPDRPendingMode: redis\n", ""));
		x.append(cust("F87", 87, "f87", "BufferTimeout: 2\n", ""));
		x.append(cust("F88", 88, "f88", "", ""));
		x.append(cust("F89", 89, "f89", "", ""));
		Files.write(cm, x.toString().getBytes("UTF-8"));
		try {
			boot();
			jedis.set("thcon", "1");
			props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
			new MultiSmppServer().startService();
			Thread.sleep(1500);
			String[] groups = { "bind", "keep", "submit", "limits", "dr", "proto", "security", "reload" };
			for (String g : groups) {
				try {
					ServerFuncIT.class.getDeclaredMethod(g).invoke(null);
				} catch (java.lang.reflect.InvocationTargetException e) {
					check(false, "ABORT group " + g + ": " + e.getCause());
					e.getCause().printStackTrace(System.out);
				}
			}
		} finally {
			Files.write(cm, orig);
		}
		exit();
	}

	static void bind() throws Exception {
		try (R r = new R()) { Pdu p = r.bind("f70", "pw70");
			check(p != null && p.cmd == 0x80000009 && p.status == 0 && p.seq == 1, "B-01a bind_transceiver accepted: " + p);
			ByteBuffer b = ByteBuffer.wrap(p == null ? new byte[0] : p.body); String sys = cstr(b);
			check("f70".equals(sys), "B-01b resp carries system_id (" + sys + ")");
			boolean ver = false; while (b.remaining() >= 4) { int t = b.getShort() & 0xffff, n = b.getShort() & 0xffff; byte[] v = new byte[n]; b.get(v); if (t == 0x0210 && n == 1 && v[0] == 0x34) ver = true; }
			check(ver, "B-01c resp carries sc_interface_version 0x34");
		}
		try (R r = new R()) { Pdu p = r.bind("f70", "wrong"); check(rej(p, 0x0e), "B-02a wrong password -> ESME_RINVPASWD: " + p); check(r.closedWithin(1500), "B-02b connection closed after a rejected bind"); }
		try (R r = new R()) { Pdu p = r.bind("nobody", "x"); check(rej(p, 0x0f), "B-03 unknown system_id -> ESME_RINVSYSID: " + p); }
		try (R r = new R()) { Pdu p = r.bind("f70", ""); check(rej(p, 0x0e), "B-04 empty password -> ESME_RINVPASWD: " + p); }
		int lost = 0; for (int i = 0; i < 1000; i++) { try (R r = new R()) { Pdu p = r.bind("f70", "bad" + i); if (p == null || p.cmd != 0x80000009) lost++; } }
		check(lost == 0, "B-05 every rejected bind gets its bind_resp before the close (lost " + lost + " of 1000)");
		try (R r = new R()) { Pdu p = r.bind("f70", "password_too_long"); check(p != null && p.cmd == 0x80000009 && p.status != 0, "B-04b password longer than SMPP allows gets an error bind_resp: " + p); }
		try (R r = new R()) { Pdu p = r.bind("system_id_longer_than_sixteen", "x"); check(p != null && p.cmd == 0x80000009 && p.status != 0, "B-04c system_id longer than SMPP allows gets an error bind_resp: " + p); }
		try (R r = new R()) { Pdu p = r.bind("f75", "pw75"); check(rejAny(p), "B-06 client IP not in the customer's URL -> rejected: " + p); }
		try (R r = new R()) { Pdu p = r.bind("f90", "pw90"); check(rejAny(p), "B-06b same user on several connections, none for this IP -> rejected: " + p); }
		try (R r = new R()) { Pdu p = r.bind("f91", "pw91"); check(p != null && p.status == 0, "B-06c same user on several connections, one for this IP -> accepted: " + p); }
		try (R r = new R()) { Pdu p = r.bind(9, "f76", "pw76", 0x33); check(p != null && p.status == 0, "B-07a Version=V33, bind 3.3: accepted " + p); }
		try (R r = new R()) { Pdu p = r.bind(9, "f76", "pw76", 0x34); check(rej(p, 0x0d), "B-07b Version=V33, bind 3.4: ESME_RBINDFAIL " + p); }
		try (R r = new R()) { Pdu p = r.bind(9, "f70", "pw70", 0x33); check(rej(p, 0x0d), "B-07c Version=V34, bind 3.3: ESME_RBINDFAIL " + p); }
		try (R r = new R()) { check(r.bind(2, "f77", "pw77", 0x34).status == 0, "B-08a BindMode=R: bind_transmitter accepted"); }
		try (R r = new R()) { check(rej(r.bind(1, "f77", "pw77", 0x34), 0x0d), "B-08b BindMode=R: bind_receiver rejected"); }
		try (R r = new R()) { check(rej(r.bind(9, "f77", "pw77", 0x34), 0x0d), "B-08c BindMode=R: bind_transceiver rejected"); }
		try (R r = new R()) { check(r.bind(1, "f78", "pw78", 0x34).status == 0, "B-09a BindMode=T: bind_receiver accepted"); }
		try (R r = new R()) { Pdu p = r.bind(2, "f78", "pw78", 0x34); check(rej(p, 0x0d), "B-09b BindMode=T: bind_transmitter rejected: " + p); }
		try (R r = new R()) { check(rej(r.bind(9, "f78", "pw78", 0x34), 0x0d), "B-09c BindMode=T: bind_transceiver rejected"); }
		try (R r = new R()) { check(rej(r.bind(1, "f70", "pw70", 0x34), 0x0d), "B-10a BindMode=TRX: bind_receiver rejected"); }
		try (R r = new R()) { check(rej(r.bind(2, "f70", "pw70", 0x34), 0x0d), "B-10b BindMode=TRX: bind_transmitter rejected"); }
		R s1 = new R(), s2 = new R(), s3 = new R();
		check(s1.bind("f74", "pw74").status == 0 && s2.bind("f74", "pw74").status == 0, "B-11a SessionNumber=2: two binds accepted");
		Pdu third = s3.bind("f74", "pw74"); check(rej(third, 0x0d), "B-11b third bind refused: " + third);
		s1.close(); s3.close(); Thread.sleep(1500);
		try (R r = new R()) { check(r.bind("f74", "pw74").status == 0, "B-11c slot free again after a close"); }
		s2.close();
		try (R r = new R()) { Pdu p = r.bind("f82", "pw82"); check(rejAny(p), "B-12 SmppServerModules excludes this module -> rejected: " + p); }
		try (R r = new R()) { r.bind("f70", "pw70"); Pdu p = r.bind("f70", "pw70"); check(p != null && p.status == 0x05, "B-13 second bind on a bound connection -> ESME_RALYBND: " + p); }
		props.setProperty("SmppServerBindTimeoutMs", "1500");
		try (R r = new R()) { check(r.closedWithin(4000), "B-14 no bind within SmppServerBindTimeoutMs: closed"); }
		props.setProperty("SmppServerBindTimeoutMs", "30000");
		props.setProperty("SmppServerMaxConnectionsPerIp", "1");
		try (R r1 = new R(); R r2 = new R()) { r1.bind("f70", "pw70"); check(r2.closedWithin(2000), "B-15a SmppServerMaxConnectionsPerIp=1: second connection closed"); }
		props.setProperty("SmppServerMaxConnectionsPerIp", "0");
		Thread.sleep(1200);
		props.setProperty("SmppServerMaxConnections", "1");
		try (R r1 = new R(); R r2 = new R()) { r1.bind("f70", "pw70"); check(r2.closedWithin(2000), "B-15b SmppServerMaxConnections=1: second connection closed"); }
		props.setProperty("SmppServerMaxConnections", "0");
		Thread.sleep(1200);
		try (R r = new R()) { check(r.bind("f70", "pw70").status == 0, "B-15c limits removed by reload: bind works"); }
	}

	static void keep() throws Exception {
		try (R r = new R()) { r.bind("f70", "pw70"); r.send(0x15, 55, new byte[0]); Pdu p = r.read(); check(p != null && p.cmd == 0x80000015 && p.status == 0 && p.seq == 55, "K-01 enquire_link answered with the same sequence: " + p);
			r.send(6, 56, new byte[0]); p = r.read(); check(p != null && p.cmd == 0x80000006 && p.status == 0, "K-02a unbind answered: " + p); check(r.closedWithin(1500), "K-02b closed after unbind"); }
		int lost = 0; for (int i = 0; i < 100; i++) { try (R r = new R()) { r.bind("f88", "pw88"); r.send(6, 56, new byte[0]); Pdu p = r.read(); if (p == null || p.cmd != 0x80000006) lost++; } Thread.sleep(5); }
		check(lost == 0, "K-02c every unbind gets its unbind_resp before the close (lost " + lost + " of 100)");
		try (R r = new R()) { r.bind("f84", "pw84"); long t = System.currentTimeMillis(); boolean alive = true;
			for (int i = 0; i < 6 && alive; i++) { r.send(0x15, 60 + i, new byte[0]); Pdu p = r.read(2000); if (p == null || p.cmd == -1) alive = false; Thread.sleep(800); }
			check(alive, "K-03a ConnectionSilentTime=3 with traffic for " + (System.currentTimeMillis() - t) / 1000 + "s: stays open");
			t = System.currentTimeMillis(); boolean c = r.closedWithin(9000); check(c, "K-03b silent: closed after " + (System.currentTimeMillis() - t) + " ms"); }
		try (R r = new R()) { r.bind("f83", "pw83"); int pings = 0; long e = System.currentTimeMillis() + 3500; boolean open = true;
			while (System.currentTimeMillis() < e) { Pdu p = r.read(500); if (p != null && p.cmd == -1) { open = false; break; } if (p != null && p.cmd == 0x15) { pings++; r.send(0x80000015, p.seq, new byte[0]); } }
			check(pings >= 2 && open, "K-04a ServerEnquireLink, EnquireLinkTime=1, answered: " + pings + " pings, stays open"); }
		try (R r = new R()) { r.bind("f83", "pw83"); long t = System.currentTimeMillis(); check(r.closedWithin(9000), "K-04b pings not answered, EnquireLinkFailureNum=2: closed after " + (System.currentTimeMillis() - t) + " ms"); }
		R c = new R(); c.bind("f88", "pw88"); String key = "smpp:server:active:88:MultiSmppServer1";
		check(waitFor(() -> jedis.zcard(key) == 1, 2000), "K-05a bound session registered");
		c.close(); check(waitFor(() -> jedis.zcard(key) == 0, 5000), "K-05b client gone without unbind: entry removed");
		props.setProperty("SmppServerSessionKeeperShared", "false");
		try (R r = new R()) { r.bind("f70", "pw70"); r.send(0x15, 70, new byte[0]); Pdu p = r.read(); check(p != null && p.status == 0, "K-06 SmppServerSessionKeeperShared=false: session works with its own thread"); }
		props.setProperty("SmppServerSessionKeeperShared", "true");
	}

	static void submit() throws Exception {
		try (R r = new R()) { r.bind("f70", "pw70");
			Pdu p = r.submit(sub("hello")); String id = msgId(p);
			check(p != null && p.cmd == 0x80000004 && p.status == 0 && id != null && id.matches("\\d+"), "S-01a submit accepted, decimal message_id: " + p + " id=" + id);
			GmmsMessage m = find(70, id);
			check(m != null && m.getOSsID() == 70 && "SENDER".equals(m.getSenderAddress()) && DST.equals(m.getRecipientAddress()), "S-01b in stream:mt:pending:70 with ssid, sender, recipient: " + (m == null ? null : m.getSenderAddress() + "/" + m.getRecipientAddress()));
			check(m != null && "hello".equals(m.getTextContent()), "S-01c content kept: " + (m == null ? null : m.getTextContent()));
			check(m != null && m.getInnerTransaction() != null && "MultiSmppServer1".equals(m.getInnerTransaction().getModule().getModule()), "S-01d server module recorded (DR route)");
			Set<String> ids = new HashSet<String>(); for (int i = 0; i < 200; i++) ids.add(msgId(r.submit(sub("u" + i))));
			check(ids.size() == 200, "S-02 200 submits: 200 different message_ids (" + ids.size() + ")");
			String ucs = "你好"; Pdu u = r.submit(sub("SENDER", DST, 0, 1, 8, ucs.getBytes("UTF-16BE"), null)); m = find(70, msgId(u));
			check(u.status == 0 && m != null && ucs.equals(m.getTextContent()), "S-03 UCS2 (dcs 8) decoded: " + (m == null ? null : m.getTextContent()));
			Pdu lat = r.submit(sub("SENDER", DST, 0, 1, 3, "café".getBytes("ISO-8859-1"), null)); m = find(70, msgId(lat));
			check(lat.status == 0 && m != null && "café".equals(m.getTextContent()), "S-04 Latin-1 (dcs 3) decoded: " + (m == null ? null : m.getTextContent()));
			Pdu bin = r.submit(sub("SENDER", DST, 0, 1, 4, new byte[] { 1, 2, 3, (byte) 0xff }, null));
			check(bin.status == 0, "S-05 binary (dcs 4) accepted, customer without SMSOptionSupportIncomingBinary: " + bin);
			Pdu pay = r.submit(sub("SENDER", DST, 0, 1, 0, new byte[0], tlv(0x0424, "payload text".getBytes()))); m = find(70, msgId(pay));
			check(pay.status == 0 && m != null && "payload text".equals(m.getTextContent()), "S-06 message_payload TLV with sm_length 0: " + (m == null ? null : m.getTextContent()));
			byte[] udh = { 5, 0, 3, 7, 2, 1 }; Pdu cc = r.submit(sub("SENDER", DST, 0x40, 1, 0, cat(udh, "part1".getBytes()), null)); m = find(70, msgId(cc));
			check(cc.status == 0 && m != null && Arrays.equals(m.getUdh(), udh) && "part1".equals(m.getTextContent()), "S-07 UDH concat (esm 0x40): UDH kept apart from the text: " + (m == null ? null : Arrays.toString(m.getUdh()) + " text=" + m.getTextContent()));
			Pdu sar = r.submit(sub("SENDER", DST, 0, 1, 0, "part2".getBytes(), cat(tlv(0x020C, new byte[] { 0, 9 }), tlv(0x020E, new byte[] { 3 }), tlv(0x020F, new byte[] { 2 })))); m = find(70, msgId(sar));
			check(sar.status == 0 && m != null && m.getSarTotalSeqments() == 3 && m.getSarSegmentSeqNum() == 2, "S-08 SAR TLVs: total/seq " + (m == null ? null : m.getSarTotalSeqments() + "/" + m.getSarSegmentSeqNum()));
			Pdu r0 = r.submit(sub("SENDER", DST, 0, 0, 0, "nodr".getBytes(), null)); GmmsMessage m0 = find(70, msgId(r0));
			Pdu r1 = r.submit(sub("SENDER", DST, 0, 1, 0, "dr".getBytes(), null)); GmmsMessage m1 = find(70, msgId(r1));
			check(m0 != null && m1 != null && !m0.getDeliveryReport() && m1.getDeliveryReport(), "S-09 registered_delivery 0 / 1 kept");
			StringBuilder lg = new StringBuilder(); for (int i = 0; i < 254; i++) lg.append('x'); Pdu lm = r.submit(sub(lg.toString())); m = find(70, msgId(lm));
			check(lm.status == 0 && m != null && m.getTextContent().length() == 254, "S-10 254 bytes accepted intact");
			Pdu empty = r.submit(sub("")); check(empty.status == 0 || empty.status == 0x0e, "S-11 empty text answered: " + empty);
			Pdu nodst = r.submit(sub("SENDER", "", 0, 1, 0, "x".getBytes(), null)); check(nodst != null && nodst.status == 0x0b, "S-12 [PROTOCOL] empty recipient -> ESME_RINVDSTADR: " + nodst);
			Pdu alpha = r.submit(sub("SENDER", "abc", 0, 1, 0, "x".getBytes(), null)); check(alpha != null && alpha.status == 0x0b, "S-13 [PROTOCOL] non-numeric recipient -> ESME_RINVDSTADR: " + alpha);
			Pdu nosrc = r.submit(sub("", DST, 0, 1, 0, "x".getBytes(), null)); check(nosrc != null, "S-14 empty sender answered: " + nosrc);
			byte[] one = sub("split"); ByteBuffer bb = ByteBuffer.allocate(16 + one.length); bb.putInt(16 + one.length).putInt(4).putInt(0).putInt(700).put(one);
			for (byte b : bb.array()) { r.out.write(b); r.out.flush(); Thread.sleep(1); } Pdu sp = r.read(); check(sp != null && sp.seq == 700 && sp.status == 0, "S-15 PDU sent byte by byte: " + sp);
			ByteBuffer tw = ByteBuffer.allocate(3 * (16 + one.length)); for (int i = 0; i < 3; i++) tw.putInt(16 + one.length).putInt(4).putInt(0).putInt(710 + i).put(one); r.out.write(tw.array()); r.out.flush();
			Set<Integer> seqs = new HashSet<Integer>(); for (int i = 0; i < 3; i++) { Pdu q = r.read(); if (q != null) seqs.add(q.seq); } check(seqs.size() == 3, "S-16 three PDUs in one packet: three answers " + seqs);
			ByteBuffer dl = ByteBuffer.allocate(512); dl.put(c("")).put((byte) 1).put((byte) 1).put(c("8613900000000")).put((byte) 1).put((byte) 1).put(c(DST)).put((byte) 0).put((byte) 0).put((byte) 0).put(c("")).put(c("")).put((byte) 0).put((byte) 0).put((byte) 0).put((byte) 0).put((byte) 2).put("mo".getBytes());
			r.send(5, 720, arr(dl)); Pdu mo = r.read(); check(mo != null && mo.cmd == 0x80000005 && mo.status == 0 && mo.seq == 720, "S-17 deliver_sm from the peer answered: " + mo);
		}
		try (R r = new R()) { r.bind("f79", "pw79"); String id = msgId(r.submit(sub("hex"))); check(id != null && id.matches("[0-9a-f]+") && !id.matches("\\d{16,}"), "S-18a SMSOptionSMPPIsGenHexMsgId: hex id " + id); }
		try (R r = new R()) { r.bind("f80", "pw80"); String id = msgId(r.submit(sub("max"))); check(id != null && id.matches("\\d+"), "S-18b SMSOptionSupportMaxInMsgID: id " + id + " (len " + (id == null ? 0 : id.length()) + ")"); }
		try (R r = new R()) { r.bind(9, "f76", "pw76", 0x33); String id = msgId(r.submit(sub("v33"))); check(id != null && id.matches("[0-9a-fA-F]+"), "S-18c SMPP 3.3 customer: hex id " + id); }
	}

	static void limits() throws Exception {
		try (R r = new R()) { r.bind("f71", "pw71"); Thread.sleep(1100); int ok = 0, th = 0, other = 0; for (int i = 0; i < 60; i++) { r.send(4, 800 + i, sub("t" + i)); } for (int i = 0; i < 60; i++) { Pdu p = r.read(); if (p == null) other++; else if (p.status == 0) ok++; else if (p.status == 0x58) th++; else other++; }
			check(th > 0 && ok > 0 && ok < 30 && other == 0, "T-01 SMSOptionIncomingThrottlingNum=5, burst 60: ok=" + ok + " throttled=" + th + " other=" + other); }
		java.lang.reflect.Field tc = com.king.gmms.connectionpool.session.MultiSmppSession.class.getDeclaredField("lastThconCheckTime"); tc.setAccessible(true);
		try (R r = new R()) { r.bind("f70", "pw70"); jedis.del("thcon"); tc.set(null, 0L); Pdu p = r.submit(sub("off")); check(p != null && p.status == 0x58, "T-02a thcon key missing -> all submits throttled: " + p);
			r.send(0x15, 901, new byte[0]); Pdu e = r.read(); check(e != null && e.status == 0, "T-02b enquire_link still answered");
			jedis.set("thcon", "1"); tc.set(null, 0L); Pdu q = r.submit(sub("on")); check(q != null && q.status == 0, "T-02c thcon back: accepted"); }
		try (R r = new R()) { r.bind("f72", "pw72"); jedis.set("stock:72", "2"); int[] st = new int[3]; for (int i = 0; i < 3; i++) st[i] = r.submit(sub("c" + i)).status;
			check(st[0] == 0 && st[1] == 0 && st[2] == 0x59, "T-03a SMSOptionChargeByGateWay, stock 2: " + Arrays.toString(st) + " stock=" + jedis.get("stock:72"));
			jedis.del("stock:72"); Pdu p = r.submit(sub("nostock")); check(p != null && p.status == 0x59, "T-03b no stock key: refused " + p); }
		try (R r = new R()) { r.bind("f73", "pw73"); jedis.set("stock:73:CN", "1"); Pdu a = r.submit(sub("cn1")), b = r.submit(sub("cn2")); check(a.status == 0 && b.status == 0x59, "T-04a SMSOptionChargeCountryByGateWay, CN stock 1: " + a.status + "," + b.status);
			Pdu bad = r.submit(sub("SENDER", "abc", 0, 1, 0, "x".getBytes(), null)); check(bad != null, "T-04b charged customer with an invalid recipient still gets an answer: " + bad); }
		try (R r = new R()) { r.bind("f70", "pw70"); int pre = 0; for (int i = 0; i < 3; i++) if (r.submit(sub("w")).status == 0) pre++;
			new Thread(() -> { try (redis.clients.jedis.Jedis j = new redis.clients.jedis.Jedis("127.0.0.1", 16379)) { j.clientPause(6000); } }).start(); Thread.sleep(100);
			r.send(4, 910, sub("during pause")); Pdu p = r.read(9000); check(p != null && p.status == 0x08 || p != null && p.status == 0, "T-05a Redis not answering: submit still gets an answer: " + p);
			Thread.sleep(6500); Pdu q = r.submit(sub("after pause")); check(q != null && q.status == 0, "T-05b Redis back: accepted, same connection: " + q); }
		props.setProperty("SmppServerInboundPerSsidQueueSize", "20");
		props.setProperty("SmppServerInboundMaxInFlightPerSsid", "1");
		props.setProperty("SmppServerInboundDispatchBatchPerSsid", "1");
		try (R r = new R()) { r.bind("f89", "pw89"); byte[] one = sub("q"); ByteBuffer b = ByteBuffer.allocate(2000 * (16 + one.length)); for (int i = 0; i < 2000; i++) b.putInt(16 + one.length).putInt(4).putInt(0).putInt(1000 + i).put(one); r.out.write(b.array()); r.out.flush();
			int ok = 0, full = 0, other = 0, got = 0; while (got < 2000) { Pdu p = r.read(); if (p == null) break; got++; if (p.status == 0) ok++; else if (p.status == 0x58) full++; else other++; }
			check(got == 2000 && full > 0 && ok > 0, "T-06 PerSsidQueueSize=20, burst 2000: answered " + got + ", ok=" + ok + " full=" + full + " other=" + other); }
		props.setProperty("SmppServerInboundQueueFullCommandStatus", "20");
		try (R r = new R()) { r.bind("f89", "pw89"); byte[] one = sub("q"); ByteBuffer b = ByteBuffer.allocate(2000 * (16 + one.length)); for (int i = 0; i < 2000; i++) b.putInt(16 + one.length).putInt(4).putInt(0).putInt(4000 + i).put(one); r.out.write(b.array()); r.out.flush();
			int q = 0, got = 0; while (got < 2000) { Pdu p = r.read(); if (p == null) break; got++; if (p.status == 0x14) q++; }
			check(q > 0, "T-07 SmppServerInboundQueueFullCommandStatus=20 (0x14) applied: " + q); }
		props.setProperty("SmppServerInboundQueueFullCommandStatus", "ESME_RTHROTTLED");
		props.setProperty("SmppServerInboundPerSsidQueueSize", "10000");
		props.setProperty("SmppServerInboundMaxInFlightPerSsid", "20");
		props.setProperty("SmppServerInboundDispatchBatchPerSsid", "20");
	}

	static void dr() throws Exception {
		try (R r = new R()) { r.bind("f70", "pw70"); sendDr(dr("1000001", 70, GmmsStatus.DELIVERED)); Pdu p = nextDeliver(r, 5000); Map<String, String> d = p == null ? null : deliver(p);
			check(d != null && "4".equals(d.get("esm")), "D-01a DR is deliver_sm with esm_class 4: " + d);
			check(d != null && d.get("text").contains("stat:DELIVRD"), "D-01b receipt text has stat:DELIVRD");
			check(d != null && d.get("text").contains("1000001") , "D-01c receipt text has the message id");
			check(d != null && DST.equals(d.get("src")) && "18667991226".equals(d.get("dst")), "D-01d DrSwapAddr default: src is the recipient, dst the sender (src=" + (d == null ? null : d.get("src")) + ")");
			check(d != null && d.get("tlv1e") == null, "D-01e receipted_message_id absent by default");
			r.send(0x80000005, p == null ? 0 : p.seq, new byte[] { 0 }); }
		Object[][] map = { { GmmsStatus.DELIVERED, "DELIVRD" }, { GmmsStatus.EXPIRED, "EXPIRED" }, { GmmsStatus.UNDELIVERED, "UNDELIV" }, { GmmsStatus.DELETED, "UNDELIV" } };
		try (R r = new R()) { r.bind("f70", "pw70"); int k = 0; for (Object[] m : map) { sendDr(dr("100010" + k++, 70, (GmmsStatus) m[0])); Pdu p = nextDeliver(r, 5000); Map<String, String> d = p == null ? null : deliver(p); check(d != null && d.get("text").contains("stat:" + m[1]), "D-02 status " + m[0] + " -> stat:" + m[1] + ": " + (d == null ? null : d.get("text"))); if (p != null) r.send(0x80000005, p.seq, new byte[] { 0 }); } }
		try (R r = new R()) { r.bind("f81", "pw81"); sendDr(dr("1000200", 81, GmmsStatus.DELIVERED)); Pdu p = nextDeliver(r, 5000); Map<String, String> d = p == null ? null : deliver(p);
			check(d != null && "1000200".equals(d.get("tlv1e")), "D-03 SMSOptionSMPPNeedReceiptedMessageId: receipted_message_id " + (d == null ? null : d.get("tlv1e")));
			check(d != null && "18667991226".equals(d.get("src")) && DST.equals(d.get("dst")), "D-04 DrSwapAddr=false: addresses not swapped (src=" + (d == null ? null : d.get("src")) + ")");
			if (p != null) r.send(0x80000005, p.seq, new byte[] { 0 }); }
		R a = new R(), b = new R(), c = new R(); a.bind("f88", "pw88"); b.bind("f88", "pw88"); c.bind("f88", "pw88"); for (int i = 0; i < 12; i++) sendDr(dr("10003" + (10 + i), 88, GmmsStatus.DELIVERED));
		int n = 0; Set<String> texts = new HashSet<String>(); long e = System.currentTimeMillis() + 6000; int[] per = new int[3]; R[] rs = { a, b, c };
		while (System.currentTimeMillis() < e && n < 12) for (int i = 0; i < 3; i++) { Pdu p = rs[i].read(100); if (p != null && p.cmd == 5) { n++; per[i]++; texts.add(deliver(p).get("text")); rs[i].send(0x80000005, p.seq, new byte[] { 0 }); } }
		check(n == 12 && texts.size() == 12, "D-05 three connections: 12 DRs delivered once each, per connection " + Arrays.toString(per)); a.close(); b.close(); c.close();
		try (R r = new R()) { r.bind("f85", "pw85"); for (int i = 0; i < 6; i++) sendDr(dr("10004" + (10 + i), 85, GmmsStatus.DELIVERED)); List<Pdu> got = new ArrayList<Pdu>(); Pdu p; while ((p = nextDeliver(r, 1500)) != null) got.add(p);
			check(got.size() == 2, "D-06a WindowSize=2, nothing answered: " + got.size() + " DRs on the wire"); int total = got.size(); Set<Integer> s = new HashSet<Integer>();
			for (Pdu g : got) { s.add(g.seq); r.send(0x80000005, g.seq, new byte[] { 0 }); } long end = System.currentTimeMillis() + 8000; while (total < 6 && System.currentTimeMillis() < end) { p = nextDeliver(r, 2000); if (p != null) { total++; s.add(p.seq); r.send(0x80000005, p.seq, new byte[] { 0 }); } }
			check(total == 6, "D-06b answered: all 6 delivered, none twice (" + total + ")"); }
		try (R r = new R()) { r.bind("f86", "pw86"); sendDr(dr("1000500", 86, GmmsStatus.DELIVERED)); Pdu p = nextDeliver(r, 5000); check(p != null, "D-07a SMPPDRPendingMode=redis: DR delivered"); if (p != null) r.send(0x80000005, p.seq, new byte[] { 0 });
			Thread.sleep(800); String keys = String.valueOf(jedis.keys("*pending*dr*86*")); check(p != null, "D-07b deliver_sm_resp accepted in redis mode; pending keys left: " + keys); }
		try (R r = new R()) { r.bind("f87", "pw87"); sendDr(dr("1000600", 87, GmmsStatus.DELIVERED)); Pdu p = nextDeliver(r, 5000); check(p != null, "D-08a BufferTimeout=2: DR sent, not answered"); long l0 = logs("[RESP-TIMEOUT]"); check(waitFor(() -> logs("[RESP-TIMEOUT]") > l0 || logs("1000600") > 3, 8000), "D-08b no deliver_sm_resp: timed out and logged"); }
		Thread.sleep(1500); long sn0 = logs("session not found"); sendDr(dr("1000700", 89, GmmsStatus.DELIVERED));
		check(waitFor(() -> logs("session not found") > sn0, 8000), "D-09a customer not connected: logged session not found");
		check(waitFor(() -> { long pd = 0; for (String k : jedis.keys("stream:dr:pending:89:*")) { try { pd += jedis.xpending(k, "ServerDrToCustomerGroup_1").getTotal(); } catch (Exception x) { } } return pd == 0; }, 6000), "D-09b its stream entry is acked, not left pending");
		try (R r = new R()) { r.bind(1, "f78", "pw78", 0x34); sendDr(dr("1000800", 78, GmmsStatus.DELIVERED)); Pdu p = nextDeliver(r, 5000); check(p != null, "D-10a bind_receiver customer gets the DR"); if (p != null) r.send(0x80000005, p.seq, new byte[] { 0 }); }
		try (R r = new R()) { r.bind(2, "f77", "pw77", 0x34); sendDr(dr("1000900", 77, GmmsStatus.DELIVERED)); Pdu p = nextDeliver(r, 2500); check(p == null, "D-10b bind_transmitter customer gets no DR on that connection (got " + (p == null ? "none" : deliver(p).get("text")) + ")"); }
	}

	static void proto() throws Exception {
		try (R r = new R()) { r.send(4, 5, sub("x")); Pdu p = r.read(); check(p != null && p.status == 0x04, "P-01a submit before bind -> ESME_RINVBNDSTS: " + p); check(r.closedWithin(1500), "P-01b closed"); }
		try (R r = new R()) { r.send(0x15, 5, new byte[0]); Pdu p = r.read(); check(p != null && p.status == 0x04, "P-02 enquire_link before bind -> ESME_RINVBNDSTS: " + p); }
		try (R r = new R()) { r.bind("f70", "pw70");
			r.send(0x99, 11, new byte[0]); Pdu p = r.read(); check(p != null && p.cmd == 0x80000000 && p.status == 0x03, "P-03 unknown command -> generic_nack ESME_RINVCMDID: " + p);
			r.send(4, 12, new byte[] { 1, 2, 3 }); p = r.read(); check(p != null && p.status != 0, "P-04 truncated submit_sm answered with an error: " + p);
			r.send(4, 13, new byte[300]); p = r.read(); check(p != null && p.status != 0, "P-05 300 zero bytes as submit_sm answered with an error: " + p);
			ByteBuffer q1 = ByteBuffer.allocate(64); q1.put(c("m1")).put((byte) 1).put((byte) 1).put(c("S")); r.send(3, 14, arr(q1)); p = r.read(); check(p != null && p.status == 0x03, "P-06a query_sm (not supported) -> ESME_RINVCMDID: " + p);
			ByteBuffer q2 = ByteBuffer.allocate(64); q2.put(c("")).put(c("m1")).put((byte) 1).put((byte) 1).put(c("S")).put((byte) 1).put((byte) 1).put(c(DST)); r.send(8, 15, arr(q2)); p = r.read(); check(p != null && p.status == 0x03, "P-06b cancel_sm (not supported) -> ESME_RINVCMDID: " + p);
			ByteBuffer q3 = ByteBuffer.allocate(64); q3.put(c("m1")).put((byte) 1).put((byte) 1).put(c("S")).put(c("")).put(c("")).put((byte) 0).put((byte) 0).put((byte) 1).put((byte) 1).put((byte) 'x'); r.send(7, 16, arr(q3)); p = r.read(); check(p != null && p.status == 0x03, "P-06c replace_sm (not supported) -> ESME_RINVCMDID: " + p);
			ByteBuffer q4 = ByteBuffer.allocate(64); q4.put(c("")).put((byte) 1).put((byte) 1).put(c("S")).put((byte) 1).put((byte) 1).put((byte) 1).put(c(DST)).put((byte) 0).put((byte) 1).put((byte) 0); r.send(0x103, 17, arr(q4)); p = r.read(); check(p != null && p.status == 0x03, "P-06d data_sm (not supported) -> ESME_RINVCMDID: " + p);
			r.send(0x80000004, 18, new byte[0]); p = r.read(500); check(p == null, "P-07a unexpected submit_sm_resp ignored");
			r.send(0x80000000, 19, new byte[0]); p = r.read(500); check(p == null, "P-07b generic_nack from the peer ignored");
			p = r.submit(sub("still ok")); check(p != null && p.status == 0, "P-07c session still works: " + p); }
		for (int len : new int[] { 8, 0, 15, -1 }) { try (R r = new R()) { r.bind("f70", "pw70"); DataOutputStream d = new DataOutputStream(r.out); d.writeInt(len); d.writeInt(4); d.writeInt(0); d.writeInt(40); d.flush(); Pdu p = r.read(); check(p != null && (p.cmd == 0x80000000 && p.status == 0x02 || p.cmd == -1), "P-08 command_length=" + len + " -> generic_nack ESME_RINVCMDLEN or close: " + p); } }
		try (R r = new R()) { r.bind("f70", "pw70"); System.gc(); long h0 = used(); DataOutputStream d = new DataOutputStream(r.out); d.writeInt(0x7FFFFFFF); d.writeInt(4); d.writeInt(0); d.writeInt(50); byte[] j = new byte[64 * 1024]; try { for (int i = 0; i < 800; i++) d.write(j); } catch (Exception e) { } d.flush(); Thread.sleep(800); System.gc(); long h1 = used();
			check(h1 - h0 < 30, "P-09 command_length=2GB then 50 MB: heap " + h0 + " -> " + h1 + " MB"); }
		try (R r = new R()) { r.bind(1, "f78", "pw78", 0x34); Pdu p = r.submit(sub("rx")); check(p != null && p.status == 0x04, "P-10 submit on a bind_receiver session -> ESME_RINVBNDSTS: " + p); }
	}
	static long used() { Runtime t = Runtime.getRuntime(); return (t.totalMemory() - t.freeMemory()) >> 20; }

	static void security() throws Exception {
		Thread.sleep(300);
		String marker = "Sec98765"; try (R r = new R()) { r.bind("f70", marker); } Thread.sleep(300);
		check(logs(marker) == 0, "SEC-01 bind password not written to the logs (" + logs(marker) + " lines)");
		long t = System.currentTimeMillis(); int refused = 0, tries = 300; for (int i = 0; i < tries; i++) { try (R r = new R()) { Pdu p = r.bind("f70", "g" + i); if (p != null && p.status != 0x0e) refused++; else if (p == null || p.cmd == -1) refused++; } }
		check(refused >= tries / 2, "SEC-02 repeated wrong passwords get limited (" + tries + " in " + (System.currentTimeMillis() - t) + " ms, limited/closed " + refused + ")");
		try (R r = new R()) { Pdu p = r.bind("f70", "pw70"); check(p != null && p.status == 0, "SEC-03 right password still works after the attempts: " + p); }
		long d = logs("[RECV]"); props.setProperty("SmppPduLogEnable", "false"); try (R r = new R()) { r.bind("f70", "pw70"); r.submit(sub("nolog")); } Thread.sleep(300); check(logs("[RECV]") == d, "SEC-04 SmppPduLogEnable=false: no PDU log lines"); props.setProperty("SmppPduLogEnable", "true");
	}

	static void reload() throws Exception {
		Path cm = Paths.get(home + "conf/cm.cfg"); byte[] cur = Files.readAllBytes(cm);
		R live = new R(), other = new R(); check(live.bind("f88", "pw88").status == 0 && other.bind("f89", "pw89").status == 0, "R-01a both customers bound");
		String changed = new String(cur, "UTF-8").replace("password=pw88", "password=new88"); String loaded = jedis.hget("config:files:MultiSmppServer1:1", "cm.loadedAt");
		Files.write(cm, changed.getBytes("UTF-8")); Thread.sleep(600); jedis.publish("channel:gmms:control", "RELOAD_CUSTOMER"); waitFor(() -> !String.valueOf(loaded).equals(jedis.hget("config:files:MultiSmppServer1:1", "cm.loadedAt")), 8000); Thread.sleep(1500);
		try (R r = new R()) { Pdu p = r.bind("f88", "pw88"); check(rej(p, 0x0e), "R-01b password changed by RELOAD_CUSTOMER: old one refused " + p); }
		try (R r = new R()) { Pdu p = r.bind("f88", "new88"); check(p != null && p.status == 0, "R-01c new one accepted " + p); }
		Pdu q = other.submit(sub("unchanged")); check(q != null && q.status == 0, "R-01d other customer's session not touched: " + q);
		Files.write(cm, cur); live.close(); other.close();
	}
}
