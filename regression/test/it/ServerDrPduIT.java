package it;

import java.util.*;

import com.king.gmms.GmmsUtility;
import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/** The receipt in the deliver_sm the real server sends, on the first try and on a real retry: no answer from the customer, failure result, code restored as getResendDR does. */
public class ServerDrPduIT extends ItSupport {
	static final int PORT = 17785;
	static StreamQueueManager q;
	static int no = 0;

	static GmmsMessage dr(int ossid, int code, String text) {
		String id = "SDP" + (++no);
		GmmsMessage d = mt(id, ossid, 8, 0);
		d.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		d.setStatusCode(code);
		d.setStatusText(text);
		d.setInMsgID("1A2B");
		return d;
	}
	static String stat(String receipt) {
		int i = receipt.indexOf("stat:");
		if (i < 0) return receipt;
		int e = receipt.indexOf(' ', i);
		return receipt.substring(i + 5, e < 0 ? receipt.length() : e);
	}
	/** the receipt of the next deliver_sm the client gets */
	static String pdu(SmppTestClient c, GmmsMessage d) throws Exception {
		c.deliverByText.clear();
		q.produceDeliveryReportToQueue(d, q.getDrPendingQueue(d));
		waitFor(() -> !c.deliverByText.isEmpty(), 8000);
		return c.deliverByText.isEmpty() ? "none" : c.deliverByText.keySet().iterator().next();
	}
	static GmmsMessage fromRdq(GmmsMessage failed) throws Exception {
		// as getResendDR restores it: by the text, the DR error code (kept in RDQ) gives the detail
		java.lang.reflect.Method m = MessageStoreManager.class.getDeclaredMethod("restoreResendDrStatus", GmmsMessage.class);
		m.setAccessible(true);
		GmmsMessage b = new GmmsMessage(failed);
		b.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		b.setStatusCode((Integer) m.invoke(GmmsUtility.getInstance().getMessageStoreManager(), failed));
		b.setOriginalQueue("RDQ");
		b.setRowId(5);
		return b;
	}

	public static void main(String[] args) throws Exception {
		try {
			boot();
			jedis.set("thcon", "1");
			props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
			MultiSmppServer server = new MultiSmppServer();
			server.startService();
			Thread.sleep(1500);
			q = StreamQueueManager.getInstance();
			SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, 5, null);
			check(c.bind("c31", "p31"), "SD-00 customer bound, BufferTimeout 3 s");

			// status, receipt word on the first try, check id of the retry
			Object[][] all = {
				{ GmmsStatus.DELIVERED, "DELIVRD", "SD-02b" }, { GmmsStatus.UNDELIVERABLE, "UNDELIV", "SD-02c" }, { GmmsStatus.EXPIRED, "EXPIRED", "SD-02d" },
				{ GmmsStatus.REJECTED, "REJECTD", "SD-02e" }, { GmmsStatus.UNKNOWN, "UNKNOWN", "SD-02f" },
				{ GmmsStatus.UNDELIVERY_NETWORK_ERROR, "UNDELIV", "SD-03a" }, { GmmsStatus.UNDELIVERY_RECIPIENT_ERROR, "UNDELIV", "SD-03b" }, { GmmsStatus.REJECTED_SDBL, "UNKNOWN", "SD-02g" } };

			// ---- first try ----
			for (Object[] x : all) {
				GmmsStatus st = (GmmsStatus) x[0];
				String r = pdu(c, dr(31, st.getCode(), st.getText()));
				System.out.println("INFO first " + st.getCode() + " " + r);
				check(x[1].equals(stat(r)), "SD-01 first try, status " + st.getCode() + ": stat:" + x[1] + " in the PDU: " + stat(r));
			}

			// ---- real retry ----
			for (Object[] x : all) {
				GmmsStatus st = (GmmsStatus) x[0];
				c.ackDeliver = false;
				GmmsMessage d = dr(31, st.getCode(), st.getText());
				String first = pdu(c, d);
				final String id = d.getMsgID();
				waitFor(() -> !inStreams("stream:core:results*", id).isEmpty(), 10000);
				List<GmmsMessage> l = inStreams("stream:core:results*", id);
				GmmsMessage failed = l.isEmpty() ? null : l.get(0);
				c.ackDeliver = true;
				check(failed != null && failed.getStatusCode() == 11005, "SD-02a customer does not answer: result 11005: " + (failed == null ? "none" : failed.getStatusCode() + " " + failed.getStatusText()));
				if (failed == null) continue;
				GmmsMessage back = fromRdq(failed);
				back.setMsgID(id + "R");
				String r = pdu(c, back);
				System.out.println("INFO retry " + st.getCode() + " text=[" + failed.getStatusText() + "] restored=" + back.getStatusCode() + " " + r);
				check(x[1].equals(stat(r)), x[2] + " retry of status " + st.getCode() + ": stat:" + x[1] + " in the PDU: " + stat(r) + " (restored " + back.getStatusCode() + ")");
			}
			c.close();
			server.stopService();
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
