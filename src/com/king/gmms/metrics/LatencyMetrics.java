package com.king.gmms.metrics;

import com.king.message.gmms.GmmsMessage;

/**
 * The latency timers of docs/性能监控埋点评估与设计.md 3.3 that are measured on a message: the vendor's
 * submit_sm_resp and DR, and end to end. Each costs a clock read and a few additions.
 */
public final class LatencyMetrics {
    private LatencyMetrics() {
    }

    private static MetricsCollector metrics() {
        return MetricsCollector.getInstance();
    }

    /** submit_sm sent to submit_sm_resp received: per channel, and per bind with Metrics.Bind.Enable */
    public static void vendorSubmitRtt(int rssid, String bind, long ms) {
        MetricsCollector m = metrics();
        if (!m.isEnabled() || ms < 0) {
            return;
        }
        m.recordMillis(MetricsNames.metric(MetricsNames.TIMER_VENDOR_SUBMIT_RTT, rssid), ms);
        if (bind != null && MetricsNames.isBindMetricsEnabled()) {
            m.recordMillis(MetricsNames.metric(MetricsNames.TIMER_VENDOR_SUBMIT_RTT + "." + rssid, bind), ms);
        }
    }

    /** the MT entered the gateway (Server) to its submit_sm sent to the vendor (Client) */
    public static void e2eSubmit(GmmsMessage msg) {
        MetricsCollector m = metrics();
        if (msg == null || msg.getInTimeMs() <= 0 || !m.isEnabled()) {
            return;
        }
        m.recordMillis(MetricsNames.metric(MetricsNames.TIMER_E2E_SUBMIT), System.currentTimeMillis() - msg.getInTimeMs());
    }

    /** the vendor accepted the MT to its DR matched in Core, per channel */
    public static void vendorDr(GmmsMessage mt) {
        MetricsCollector m = metrics();
        if (mt == null || mt.getOutTimeMs() <= 0 || !m.isEnabled()) {
            return;
        }
        m.recordMillis(MetricsNames.metric(MetricsNames.TIMER_VENDOR_DR_LATENCY, mt.getRSsID()),
                System.currentTimeMillis() - mt.getOutTimeMs());
    }

    /** the DR entered the gateway (vendor DR at the Client, or a fake DR made) to the customer's answer at the Server */
    public static void e2eDr(GmmsMessage dr) {
        MetricsCollector m = metrics();
        if (dr == null || dr.getInTimeMs() <= 0 || !m.isEnabled()) {
            return;
        }
        m.recordMillis(MetricsNames.metric(MetricsNames.TIMER_E2E_DR), System.currentTimeMillis() - dr.getInTimeMs());
    }
}
