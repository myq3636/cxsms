package it;

import java.io.*; import java.net.*; import java.nio.ByteBuffer; import java.nio.file.*; import java.util.*;
import javax.net.ssl.*;
import com.king.gmms.listener.smppserver.SslSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** SMPP over TLS on a real SslSmppServer with a generated certificate. */
public class ServerTlsIT extends ItSupport {
	static final int PORT = 17778;
	static Socket tls() throws Exception {
		SSLContext c = SSLContext.getInstance("TLS");
		c.init(null, new TrustManager[] { new X509TrustManager() { public void checkClientTrusted(java.security.cert.X509Certificate[] a, String b) { } public void checkServerTrusted(java.security.cert.X509Certificate[] a, String b) { } public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; } } }, null);
		SSLSocket s = (SSLSocket) c.getSocketFactory().createSocket("127.0.0.1", PORT); s.setSoTimeout(4000); s.startHandshake(); return s;
	}
	static byte[] c(String s) { byte[] b = s.getBytes(); return Arrays.copyOf(b, b.length + 1); }
	static void send(Socket s, int cmd, int seq, byte[] body) throws Exception { ByteBuffer b = ByteBuffer.allocate(16 + body.length); b.putInt(16 + body.length).putInt(cmd).putInt(0).putInt(seq).put(body); s.getOutputStream().write(b.array()); s.getOutputStream().flush(); }
	static int[] read(Socket s) throws Exception { try { DataInputStream in = new DataInputStream(s.getInputStream()); int l = in.readInt(), cmd = in.readInt(), st = in.readInt(), q = in.readInt(); in.readFully(new byte[Math.max(0, l - 16)]); return new int[] { cmd, st, q }; } catch (SocketTimeoutException e) { return null; } catch (IOException e) { return new int[] { -1, -1, -1 }; } }
	static byte[] bindB(String u, String p) { ByteBuffer b = ByteBuffer.allocate(128); b.put(c(u)).put(c(p)).put(c("")).put((byte) 0x34).put((byte) 0).put((byte) 0).put(c("")); return Arrays.copyOf(b.array(), b.position()); }
	static byte[] sub(String t) { byte[] sm = t.getBytes(); ByteBuffer b = ByteBuffer.allocate(256); b.put(c("")).put((byte) 5).put((byte) 0).put(c("S")).put((byte) 1).put((byte) 1).put(c("8613800000000")).put((byte) 0).put((byte) 0).put((byte) 0).put(c("")).put(c("")).put((byte) 1).put((byte) 0).put((byte) 0).put((byte) 0).put((byte) sm.length).put(sm); return Arrays.copyOf(b.array(), b.position()); }

	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		Path cm = Paths.get(home + "conf/cm.cfg"), cf = Paths.get(home + "Gmms/GmmsConfig.properties");
		byte[] oc = Files.readAllBytes(cm), of = Files.readAllBytes(cf);
		String ks = System.getProperty("ks");
		Files.write(cm, (new String(oc, "UTF-8") + "\n[TLS95]\nCCBSSID: 95\nA2PPlayerType: operator\nProtocol: SSLSMPP\nConnectionType: MC\nWindowSize: 20\nIncomingConnection: (ConnName=TLS95_c1,userName=t95,password=pw95,URL=127.0.0.1,SessionNumber=20,MinReceiverNumber=1,MaxReceiverNumber=5,BindMode=TRX,Version=V34)\n").getBytes("UTF-8"));
		Files.write(cf, (new String(of, "UTF-8") + "\nSsl.KeyStorePath=" + ks + "\nSsl.TrustStorePath=" + ks + "\nSslSmppServer1.Port=" + PORT + "\n").getBytes("UTF-8"));
		try {
			boot();
			jedis.set("thcon", "1");
			props.setProperty("SslSmppServer1.Port", String.valueOf(PORT));
			check(new SslSmppServer().startService(), "TLS-00 SslSmppServer started");
			Thread.sleep(1500);
			Socket s = tls();
			check(((SSLSocket) s).getSession().isValid(), "TLS-01 TLS handshake: " + ((SSLSocket) s).getSession().getProtocol());
			send(s, 9, 1, bindB("t95", "pw95")); int[] r = read(s); check(r != null && r[1] == 0, "TLS-02 bind over TLS: " + Arrays.toString(r));
			send(s, 4, 2, sub("over tls")); r = read(s); check(r != null && r[1] == 0 && r[0] == 0x80000004, "TLS-03 submit over TLS: " + Arrays.toString(r));
			check(waitFor(() -> jedis.zcard("smpp:server:active:95:SslSmppServer1") == 1, 3000), "TLS-04 session registered");
			GmmsMessage d = mt("777001", 95, 8, 0); d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT); d.setStatus(GmmsStatus.DELIVERED); d.setInMsgID("777001");
			d.setInnerTransaction(new com.king.gmms.ha.TransactionURI(new com.king.gmms.ha.ModuleURI("127.0.0.1", "SslSmppServer1"), "TLS95_c1"));
			StreamQueueManager q = StreamQueueManager.getInstance(); q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
			r = read(s); check(r != null && r[0] == 5, "TLS-05 DR over TLS (this module consumes DR streams): " + Arrays.toString(r));
			Socket w = tls(); send(w, 9, 1, bindB("t95", "bad")); r = read(w); check(r != null && (r[0] == -1 || r[1] == 0x0e), "TLS-06 wrong password over TLS: " + Arrays.toString(r));
			try (Socket p = new Socket("127.0.0.1", PORT)) { p.setSoTimeout(3000); send(p, 9, 1, bindB("t95", "pw95")); int[] x = read(p); check(x == null || x[0] != 0x80000009 || x[1] != 0, "TLS-07 plain TCP on the TLS port cannot bind: " + Arrays.toString(x)); }
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; } finally { Files.write(cm, oc); Files.write(cf, of); }
		exit();
	}
}
