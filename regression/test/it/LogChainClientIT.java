package it;

import java.util.List;

import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.MultiSmppSession;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.GmmsUtility;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * Client log chain on a real SMPP link (not part of the repo): a client session of vendor 41 binds
 * to the MultiSmppServer of this process as customer 42; the server plays the vendor.
 */
public class LogChainClientIT extends ItSupport {
	static final int PORT = 17781;

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
		props.setProperty("RedisStreamTraceLogEnable", "true");
		props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
		MultiSmppServer server = new MultiSmppServer();
		server.startService();
		Thread.sleep(1500);
		StreamQueueManager q = StreamQueueManager.getInstance();

		SingleNodeCustomerInfo vendor = (SingleNodeCustomerInfo) GmmsUtility.getInstance().getCustomerManager()
				.getCustomerBySSID(41);
		ConnectionInfo info = vendor.getConnectionMap(false).values().iterator().next();
		info.setUserName("c42");      // cm.cfg has another name: the shared customer manager would map c42 to ssid 41
		info.setPassword("p42");
		final MultiSmppSession cs = new MultiSmppSession(info);
		cs.setKeepRunning(true);
		check(cs.createConnection() && cs.connect(), "L-S0 client session of vendor 41 bound to the local server");
		cs.setStatus(ConnectionStatus.CONNECT);

		// ---- L-S1 MT out: [MT-OUT] -> [SUBMIT-RESP] -> result for Core ----
		GmmsMessage m = mt("8888_LS1", 8888, 41, 0);
		check(cs.submitClientOutbound(m) == com.king.gmms.connectionpool.session.SmppSubmitResult.SENT, "L-S1a submit_sm sent");
		check(waitFor(() -> inStreams("stream:core:results*", "8888_LS1").size() == 1, 8000),
				"L-S1b submit_sm_resp written to stream:core:results");
		List<String> l1 = LogChainCoreIT.lines("8888_LS1");
		LogChainCoreIT.dump(l1);
		check(LogChainCoreIT.inOrder(l1, "[MT-OUT] rssid=41 session=SMPPVEND41_c1_0 seq=", "[SUBMIT-RESP] seq=",
				"pduStatus=0 status=0 outMsgId=", "[STREAM-PUT] flow=result->core"),
				"L-S1c client MT chain: MT-OUT -> SUBMIT-RESP (seq, pdu status, outMsgId) -> STREAM-PUT result");
		check(LogChainCoreIT.count(l1, "WARN") == 0 && LogChainCoreIT.count(l1, "ERROR") == 0
				&& logLine("8888_LS1", "SubmitSM receivies response") == null, "L-S1d no WARN/ERROR, old SubmitSM text gone");
		String outMsgId = inStreams("stream:core:results*", "8888_LS1").get(0).getOutMsgID();

		// ---- L-S2 vendor DR in: [VDR-IN] -> [STREAM-PUT] inbound-dr -> [VDR-TO-CORE] ----
		GmmsMessage d = mt("LSDR1", 42, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatus(GmmsStatus.DELIVERED);
		d.setInMsgID(outMsgId);       // the id the server answered the submit with
		q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
		check(waitFor(() -> streamLen("stream:core:inbound-dr:41*") >= 1, 8000), "L-S2a vendor DR written to stream:core:inbound-dr:41");
		check(waitFor(() -> logLine(outMsgId, "[VDR-TO-CORE]") != null, 3000), "L-S2b [VDR-TO-CORE] logged");
		String in = logLine(outMsgId, "[VDR-IN] outMsgId=" + outMsgId);
		String toCore = logLine(outMsgId, "[VDR-TO-CORE] outMsgId=" + outMsgId);
		System.out.println("    | " + in);
		System.out.println("    | " + toCore);
		check(in != null && in.startsWith("INFO") && toCore != null && toCore.contains("deliver_sm_resp ROK")
				&& logLine(outMsgId, "Received DR,outmsgid") == null && logLine(outMsgId, "send response ok.") == null,
				"L-S2c vendor DR chain: VDR-IN (outMsgId, status) -> VDR-TO-CORE deliver_sm_resp ROK");

		// ---- L-S3 vendor never answers submit_sm: [RESP-TIMEOUT] WARN with session and seq ----
		cs.stop();
		final java.net.ServerSocket mute = new java.net.ServerSocket(PORT + 1);
		Thread vendorThread = new Thread() {
			public void run() {
				try (java.net.Socket s = mute.accept()) {
					java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
					java.io.DataOutputStream out = new java.io.DataOutputStream(new java.io.BufferedOutputStream(s.getOutputStream()));
					int len = in.readInt(), cmd = in.readInt(), st = in.readInt(), seq = in.readInt();
					in.readFully(new byte[len - 16]);
					byte[] sys = "mute\0".getBytes("US-ASCII");
					out.writeInt(16 + sys.length);
					out.writeInt(cmd | 0x80000000);       // bind_*_resp, ROK
					out.writeInt(0);
					out.writeInt(seq);
					out.write(sys);
					out.flush();
					while (in.read() >= 0) { }               // read everything, answer nothing
				} catch (Exception e) { }
			}
		};
		vendorThread.setDaemon(true);
		vendorThread.start();
		info.setPort(PORT + 1);
		final MultiSmppSession cs2 = new MultiSmppSession(info);
		cs2.setKeepRunning(true);
		check(cs2.createConnection() && cs2.connect(), "L-S3a bound to a vendor that never answers submit_sm");
		cs2.setStatus(ConnectionStatus.CONNECT);
		GmmsMessage t = mt("8888_LS3", 8888, 41, 0);
		cs2.submitClientOutbound(t);
		check(waitFor(() -> logLine("8888_LS3", "[RESP-TIMEOUT]") != null, 10000), "L-S3b [RESP-TIMEOUT] after BufferTimeout 3 s");
		String to = logLine("8888_LS3", "[RESP-TIMEOUT]");
		System.out.println("    | " + to);
		check(to != null && to.startsWith("WARN") && to.contains("session=SMPPVEND41_c1_") && to.contains(" seq=1 ")
				&& waitFor(() -> inStreams("stream:core:results*", "8888_LS3").size() == 1, 3000),
				"L-S3c WARN names session and seq; failure result written for Core");
		cs2.stop();
		mute.close();
		server.stopService();
	}
}
