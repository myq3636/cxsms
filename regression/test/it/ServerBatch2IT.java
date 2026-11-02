package it;

import java.io.File;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import com.king.gmms.connectionpool.TCPIPConnection;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/** Server batch 2: log chain, shared session keeper, bounded send queue, DR acked on result; not part of the repo. */
public class ServerBatch2IT extends ItSupport {
	static final int PORT = 17778;

	static SmppTestClient client(int cust, int window) throws Exception {
		SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, window, new SmppTestClient.LatencyRecorder());
		if (!c.bind("c" + cust, "p" + cust)) throw new IllegalStateException("bind failed c" + cust);
		return c;
	}

	static List<String> logLines() throws Exception {
		return Files.readAllLines(new File(home + "log/it.log").toPath());
	}

	static int indexOf(List<String> lines, int from, String... parts) {
		for (int i = Math.max(0, from); i < lines.size(); i++) {
			boolean all = true;
			for (String p : parts) all &= lines.get(i).contains(p);
			if (all) return i;
		}
		return -1;
	}

	static int threads(String prefix) {
		int n = 0;
		for (Thread t : Thread.getAllStackTraces().keySet()) if (t.getName().startsWith(prefix)) n++;
		return n;
	}

	static GmmsMessage dr(String id, int ossid) {
		GmmsMessage d = mt(id, ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInMsgID("IN-" + id);
		return d;
	}

	static long pendingDr(int ossid) {
		long n = 0;
		for (String k : jedis.keys("stream:dr:pending:" + ossid + ":*")) {
			try { n += jedis.xpending(k, "ServerDrToCustomerGroup_1").getTotal(); } catch (Exception e) { }
		}
		return n;
	}

	public static void main(String[] args) throws Exception {
		try {
			run();
		} catch (Throwable t) {
			t.printStackTrace();
			failures++;
		}
		exit();
	}

	static void run() throws Exception {
		boot();
		jedis.set("thcon", "1");
		props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
		props.setProperty("SmppServerBindTimeoutMs", "2000");
		MultiSmppServer server = new MultiSmppServer();
		server.startService();
		Thread.sleep(1500);
		StreamQueueManager q = StreamQueueManager.getInstance();

		// ---- S2-01 one INFO line per key point, in order ----
		SmppTestClient a = client(29, 10);
		a.submit("12345", "8613800000029", "s201-submit");
		check(waitFor(() -> a.okResp.get() == 1, 5000), "S2-01 setup: submit accepted");
		GmmsMessage d = dr("S201DR", 29);
		q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
		check(waitFor(() -> pendingDr(29) == 0 && a.delivers.get() == 1, 5000), "S2-01 setup: DR delivered and answered");
		Thread.sleep(500);
		List<String> L = logLines();
		int recv = indexOf(L, 0, "[RECV]", "cmd=submit_sm ", "SMPPCUST29");
		int in = indexOf(L, recv, "[SUBMIT-IN]", "ssid=29");
		int put = indexOf(L, in, "[STREAM-PUT]", "stream:mt:pending:29");
		int toCore = indexOf(L, put, "[SUBMIT-TO-CORE]", "ssid=29", "ESME_ROK");
		check(recv >= 0 && in > recv && put > in && toCore > put,
				"S2-01a submit chain: [RECV] pdu -> [SUBMIT-IN] -> [STREAM-PUT] -> [SUBMIT-TO-CORE]");
		check(indexOf(L, 0, "Smpp receive message") < 0, "S2-01b full message dump no longer at INFO");
		int get = indexOf(L, 0, "S201DR", "[STREAM-GET]");
		int out = indexOf(L, get, "S201DR", "[DR-OUT]");
		int resp = indexOf(L, out, "S201DR", "[DR-RESP]");
		int res = indexOf(L, resp, "S201DR", "[DR-RESULT-TO-CORE]");
		int ack = indexOf(L, out, "S201DR", "[STREAM-ACK]");
		check(get >= 0 && out > get && resp > out && res > resp,
				"S2-01c DR chain: [STREAM-GET] -> [DR-OUT] -> [DR-RESP] -> [DR-RESULT-TO-CORE]");
		check(ack > resp, "S2-01d DR stream entry acked after the customer's deliver_sm_resp ([STREAM-ACK] after [DR-RESP])");
		check(get >= 0 && !L.get(get).contains("Content") && L.get(get).length() < 1200,
				"S2-01e [STREAM-GET] prints the short message form (" + (get >= 0 ? L.get(get).length() : -1) + " chars)");
		check(indexOf(L, 0, "resolved session") < 0, "S2-01f duplicate 'resolved session' line no longer at INFO");
		props.setProperty("SmppPduLogEnable", "false");
		int before = logLines().size();
		a.submit("12345", "8613800000029", "s201-nopdu");
		waitFor(() -> a.okResp.get() == 2, 5000);
		Thread.sleep(300);
		List<String> L2 = logLines();
		check(indexOf(L2, before, "[RECV]") < 0 && indexOf(L2, before, "[SUBMIT-IN]") >= 0,
				"S2-01g SmppPduLogEnable=false (hot): no PDU lines, business lines stay");
		props.remove("SmppPduLogEnable");

		// ---- S2-02 threads per connection ----
		List<SmppTestClient> conns = new ArrayList<SmppTestClient>();
		int recvBefore = threads("Receiver"), sendBefore = threads("Sender");
		for (int i = 0; i < 20; i++) conns.add(client(33, 5));
		Thread.sleep(500);
		check(threads("ServerSessionTrd") == 0 && threads("SmppSessionKeeper") <= 4,
				"S2-02a no ServerSessionTrd per connection; shared SmppSessionKeeper threads: " + threads("SmppSessionKeeper"));
		check(threads("Receiver") - recvBefore == 20 && threads("Sender") - sendBefore == 20,
				"S2-02b 20 more connections = 20 Receiver + 20 Sender threads (2 per connection)");
		for (SmppTestClient c : conns) c.close();

		// ---- S2-03 keeper: bind timeout, silence, enquire_link ----
		Socket idle = new Socket("127.0.0.1", PORT);
		idle.setSoTimeout(6000);
		long t0 = System.currentTimeMillis();
		int r;
		try { r = idle.getInputStream().read(); } catch (Exception x) { r = -2; }
		long closedMs = System.currentTimeMillis() - t0;
		check(r == -1 && closedMs >= 1800 && closedMs < 4500, "S2-03a bind timeout still enforced: closed after " + closedMs + " ms");
		idle.close();
		final SmppTestClient silent = client(30, 5);           // ConnectionSilentTime 3 s
		t0 = System.currentTimeMillis();
		boolean closed = waitFor(() -> silent.isClosedByServer(), 8000);
		long silentMs = System.currentTimeMillis() - t0;
		check(closed && silentMs >= 2500, "S2-03b bound but silent connection closed after " + silentMs + " ms (ConnectionSilentTime 3 s)");
		final SmppTestClient alive = client(32, 5);            // ServerEnquireLink, EnquireLinkTime 1 s
		boolean pinged = waitFor(() -> alive.enquiresFromServer.get() >= 2, 6000);
		check(pinged && !alive.isClosedByServer(), "S2-03c ServerEnquireLink: server sent " + alive.enquiresFromServer.get()
				+ " enquire_link, connection kept while answered");
		alive.close();

		// ---- S2-04 bounded send queue (unit level: a peer that never reads) ----
		ServerSocket ss = new ServerSocket(0);
		Socket peer = new Socket();
		peer.setReceiveBufferSize(4096);
		peer.connect(ss.getLocalSocketAddress());
		Socket srv = ss.accept();
		srv.setSendBufferSize(4096);
		TCPIPConnection conn = new TCPIPConnection(srv);
		conn.setConnectionName("bounded");
		conn.setMaxQueued(200);
		conn.open();
		byte[] pdu = new byte[1024];
		int sent = 0;
		boolean threw = false;
		for (int i = 0; i < 5000 && !threw; i++) {
			try { conn.send(pdu); sent++; } catch (Exception e) { threw = true; }
		}
		check(threw && sent >= 200 && conn.isClosed(),
				"S2-04a peer does not read: after " + sent + " PDUs the queue hit 200, send failed and the connection was closed");
		peer.close();
		ss.close();

		// ---- S2-05 DR acked on result ----
		final SmppTestClient slow = client(31, 5);              // BufferTimeout 3 s
		slow.ackDeliver = false;
		for (int i = 0; i < 5; i++) {
			GmmsMessage x = dr("S205DR" + i, 31);
			q.produceDeliveryReportToQueue(x, q.getDrPendingQueue(x));
		}
		check(waitFor(() -> slow.delivers.get() == 5, 5000) && pendingDr(31) == 5,
				"S2-05a DRs sent, customer silent: the 5 stream entries stay pending (a crash now would leave them for auto-claim)");
		check(waitFor(() -> pendingDr(31) == 0, 8000), "S2-05b BufferTimeout (3 s): failure results written, entries acked");
		check(inStreams("stream:core:results*", "S205DR0").size() == 1, "S2-05c one result per DR (the timeout failure)");
		slow.close();

		final SmppTestClient fast = client(34, 5);
		int n = 300;
		for (int i = 0; i < n; i++) {
			GmmsMessage x = dr("S205F" + i, 34);
			q.produceDeliveryReportToQueue(x, q.getDrPendingQueue(x));
		}
		check(waitFor(() -> fast.delivers.get() >= n && pendingDr(34) == 0, 20000),
				"S2-05d fast customer, " + n + " DRs: all delivered (" + fast.delivers.get() + ") and acked (pending " + pendingDr(34) + ")");
		Thread.sleep(1500);
		check(fast.delivers.get() == n, "S2-05e none sent twice (resp before/after registration both ack)");

		props.setProperty("ServerDrAckOnResp", "false");
		final SmppTestClient legacy = client(35, 5);
		legacy.ackDeliver = false;
		GmmsMessage y = dr("S205L", 35);
		q.produceDeliveryReportToQueue(y, q.getDrPendingQueue(y));
		check(waitFor(() -> legacy.delivers.get() == 1, 5000) && waitFor(() -> pendingDr(35) == 0, 3000),
				"S2-05f ServerDrAckOnResp=false: acked when sent (old behaviour) although no response yet");
		props.remove("ServerDrAckOnResp");
		legacy.close();
		fast.close();
		a.close();
		server.stopService();
	}
}
