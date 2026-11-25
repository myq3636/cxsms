package it;

import java.util.List;

import com.king.message.gmms.GmmsMessage;

/**
 * An MO through Core: the Client writes a vendor's deliver_sm (not a DR) to stream:mt:pending as a Submit with the
 * vendor as ossid (ClientResultIT VD-10); Core routes it by the vendor's routing to a customer and should write it as
 * a Delivery to the customer's Server stream (stream:dr:pending:{rssid}, [MO-TO-SERVER]), from where the Server sends
 * it to the customer as a deliver_sm. Not to stream:mt:routed (that is the Client's, for a vendor). An MO is a message
 * from a vendor (cm Role: Vendor, as the CDR marks it, ServiceTypeID 3) to a customer (not Role: Vendor).
 */
public class CoreMoIT extends CoreBizSupport {
	public static void main(String[] a) throws Exception {
		home = System.getProperty("a2p_home");
		appendCm(cust("V701", 701, "Role: Vendor\n") + cust("C702", 702, "") + cust("V703", 703, "Role: Vendor\n") + cust("C704", 704, ""));
		// the vendor's MO routing: its long number 106601 to the customer, everything else on to vendor V703 (an MT)
		route("V701", 701, "A2PMainRouting: (Priority=30,PhonePrefix=106601,Relay=C702);(Priority=31,PhonePrefix=86,Relay=V703)");
		// a customer (no Role) routed to another customer: not an MO, as before
		route("C704", 704, "A2PMainRouting: (Priority=30,PhonePrefix=106601,Relay=C702)");
		try {
			start();
			Thread.sleep(800);
			GmmsMessage mo = msg(701, "8613800000001", "106601", "mo text");
			Out o = send(mo);
			check(o.acked, "MO-01 the MO read and acked by Core: " + o);
			List<GmmsMessage> toServer = inStreams("stream:dr:pending:702*", mo.getMsgID());
			check(o.rssid == -1, "MO-02 not written to stream:mt:routed (the Client's): " + o);
			check(toServer.size() == 1, "MO-03 written to the customer's Server stream stream:dr:pending:702: " + toServer.size());
			check(!toServer.isEmpty() && GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(toServer.get(0).getMessageType()),
					"MO-04 as a Delivery: " + (toServer.isEmpty() ? "none" : toServer.get(0).getMessageType()));
			check(!toServer.isEmpty() && "mo text".equals(toServer.get(0).getTextContent()) && "8613800000001".equals(toServer.get(0).getSenderAddress())
					&& toServer.get(0).getOSsID() == 701 && toServer.get(0).getRSsID() == 702, "MO-05 text, sender, ossid, rssid kept");
			check(logLine(mo.getMsgID(), "[MO-TO-SERVER]") != null, "MO-06 [MO-TO-SERVER] logged");
			check(o.inCdr >= 1, "MO-07 IN_SUBMIT CDR of the MO: " + o.inCdr);

			Out mt = send(701, "8613800000001", "8613800000002", "via vendor");
			check(mt.routedTo(703), "MO-08 the same vendor's message to a vendor route stays an MT (stream:mt:routed:703): " + mt);
			GmmsMessage c2c = msg(704, "8613800000003", "106601", "from a customer");
			Out o3 = send(c2c);
			check(o3.routedTo(702) && inStreams("stream:dr:pending:702*", c2c.getMsgID()).isEmpty(),
					"MO-09 a customer's message to a customer is not an MO: stays an MT (stream:mt:routed:702): " + o3);
		} catch (Throwable t) {
			t.printStackTrace(System.out);
			failures++;
		}
		exit();
	}
}
