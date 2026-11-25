package com.king.gmms.metrics;

import com.king.message.gmms.GmmsMessage;

/**
 * MetricsNames.msgType: the message types of GmmsMessage and the short names some callers pass (the HTTP Client:
 * "dr", "submit_resp", "dr_resp") give the same metric type; before 2026-11-26 the short ones all became "other".
 */
public class MetricsNamesCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	public static void main(String[] args) {
		check("submit".equals(MetricsNames.msgType(GmmsMessage.MSG_TYPE_SUBMIT)) && "submit".equals(MetricsNames.msgType(GmmsMessage.MSG_TYPE_DELIVERY)),
				"MN-01 Submit and Delivery (MO): submit");
		check("dr".equals(MetricsNames.msgType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT)), "MN-02 Delivery Report: dr");
		check("dr_resp".equals(MetricsNames.msgType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP))
				&& "submit_resp".equals(MetricsNames.msgType(GmmsMessage.MSG_TYPE_SUBMIT_RESP)), "MN-03 the responses");
		check("dr".equals(MetricsNames.msgType("dr")) && "dr_resp".equals(MetricsNames.msgType("dr_resp"))
				&& "submit_resp".equals(MetricsNames.msgType("submit_resp")) && "submit".equals(MetricsNames.msgType("submit")),
				"MN-04 the short names stay (HTTP Client: business.client.dr.sent_to_customer, not client.other)");
		check("other".equals(MetricsNames.msgType((String) null)) && "other".equals(MetricsNames.msgType("whatever")), "MN-05 unknown: other");
		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
