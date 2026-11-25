package it;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.king.gmms.listener.smppserver.MultiSmppServer;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;

/**
 * An MO at the Server: Core writes it as a Delivery to the customer's stream (stream:dr:pending:{rssid}, CoreMoIT);
 * the real server sends it to the bound customer as a deliver_sm that is not a receipt (esm_class 0) and reports the
 * customer's answer to Core as a submit_resp (Core then writes the OUT_SUBMIT CDR, as for an MT).
 */
public class ServerMoIT extends ItSupport {
	static final int PORT = 17787;
	static int no = 0;

	static GmmsMessage mo(String text) {
		GmmsMessage m = mt("SMO" + (++no), 8, 31, 0);   // from vendor 8 to customer 31 (SMPPCUST31, bound as c31)
		m.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY);
		m.setSenderAddress("8613800000001");
		m.setRecipientAddress("106601");
		m.setTextContent(text);
		m.setMessageSize(text.length());
		return m;
	}

	/** source address, destination address and esm_class of a deliver_sm body */
	static String[] fields(byte[] b) {
		int i = 0;
		while (b[i] != 0) i++; i++;                                   // service_type
		i += 2; int s = i; while (b[i] != 0) i++; String src = new String(b, s, i - s, StandardCharsets.ISO_8859_1); i++;
		i += 2; s = i; while (b[i] != 0) i++; String dst = new String(b, s, i - s, StandardCharsets.ISO_8859_1); i++;
		return new String[] { src, dst, String.valueOf(b[i] & 0xff) };
	}

	static GmmsMessage result(String id) throws Exception {
		waitFor(() -> !inStreams("stream:core:results*", id).isEmpty(), 10000);
		List<GmmsMessage> l = inStreams("stream:core:results*", id);
		return l.isEmpty() ? null : l.get(0);
	}

	public static void main(String[] args) throws Exception {
		try {
			boot();
			jedis.set("thcon", "1");
			props.setProperty("MultiSmppServer1.Port", String.valueOf(PORT));
			MultiSmppServer server = new MultiSmppServer();
			server.startService();
			Thread.sleep(1500);
			StreamQueueManager q = StreamQueueManager.getInstance();
			SmppTestClient c = new SmppTestClient("127.0.0.1", PORT, 5, null);
			check(c.bind("c31", "p31"), "SM-00 customer bound");

			GmmsMessage m = mo("hello from a phone");
			check(q.getServerDeliveryQueue(m).startsWith("stream:dr:pending:31"), "SM-01 the MO's stream is the customer's: " + q.getServerDeliveryQueue(m));
			check(q.produceServerDeliveryMessage(m), "SM-02 written");
			waitFor(() -> c.deliverByText.containsKey("hello from a phone"), 8000);
			check(c.deliverByText.containsKey("hello from a phone"), "SM-03 the customer gets a deliver_sm with the MO text: " + c.deliverByText.keySet());
			if (c.lastDeliverBody != null) {
				String[] f = fields(c.lastDeliverBody);
				check("0".equals(f[2]), "SM-04 esm_class 0 (an MO, not a receipt): " + f[2]);
				check("8613800000001".equals(f[0]) && "106601".equals(f[1]), "SM-05 source = the phone, destination = the customer's number: " + f[0] + " -> " + f[1]);
			}
			GmmsMessage r = result(m.getMsgID());
			check(r != null && GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(r.getMessageType()) && r.getStatusCode() == 0,
					"SM-06 the customer's deliver_sm_resp reported to Core as a successful submit_resp: " + (r == null ? "none" : r.getMessageType() + " " + r.getStatusCode()));

			c.ackDeliver = false;
			GmmsMessage m2 = mo("nobody answers");
			q.produceServerDeliveryMessage(m2);
			GmmsMessage r2 = result(m2.getMsgID());
			check(r2 != null && GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(r2.getMessageType()) && r2.getStatusCode() != 0,
					"SM-07 no deliver_sm_resp (BufferTimeout 3 s): a failed submit_resp to Core: " + (r2 == null ? "none" : r2.getMessageType() + " " + r2.getStatusCode()));
			c.ackDeliver = true;
			c.close();

			GmmsMessage m3 = mo("customer offline");
			Thread.sleep(1500);
			q.produceServerDeliveryMessage(m3);
			GmmsMessage r3 = result(m3.getMsgID());
			check(r3 != null && GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(r3.getMessageType()) && r3.getStatusCode() != 0,
					"SM-08 customer not bound: a failed submit_resp to Core: " + (r3 == null ? "none" : r3.getMessageType() + " " + r3.getStatusCode()));
			server.stopService();
		} catch (Throwable t) { t.printStackTrace(System.out); failures++; }
		exit();
	}
}
