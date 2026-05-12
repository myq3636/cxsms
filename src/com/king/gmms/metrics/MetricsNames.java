package com.king.gmms.metrics;

import com.king.message.gmms.GmmsMessage;

public class MetricsNames {

    public static String counter(String flow, GmmsMessage msg, String metric) {
        return build(flow, "in", msgType(msg), metric);
    }

    public static String timer(String flow, GmmsMessage msg, String metric) {
        return build(flow, "run", msgType(msg), metric);
    }

    public static String counter(String component, String flow, GmmsMessage msg, String action) {
        return build(component, flow, msgType(msg), action);
    }

    public static String timer(String component, String flow, GmmsMessage msg, String action) {
        return build(component, flow, msgType(msg), action);
    }

    public static String gauge(String component, String name) {
        return module() + "." + normalize(component) + "." + normalize(name);
    }

    public static String dbGauge(String dsName, String metric) {
        return module() + ".db." + normalize(dsName) + "." + normalize(metric);
    }

    public static String build(String component, String flow, String messageType, String action) {
        return module() + "." + normalize(component) + "." + normalize(flow)
                + "." + normalize(messageType) + "." + normalize(action);
    }

    public static String module() {
        String module = System.getProperty("module");
        if (module == null || module.trim().length() == 0) {
            return "unknown";
        }
        return normalize(module);
    }

    public static String msgType(GmmsMessage msg) {
        if (msg == null || msg.getMessageType() == null) {
            return "other";
        }
        String type = msg.getMessageType();
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(type)
                || GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(type)) {
            return "submit";
        }
        if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(type)
                || "Submit Resp".equalsIgnoreCase(type)) {
            return "submit_resp";
        }
        if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(type)) {
            return "dr";
        }
        if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(type)
                || "Delivery Report Resp".equalsIgnoreCase(type)) {
            return "dr_resp";
        }
        return "other";
    }

    private static String normalize(String value) {
        if (value == null || value.trim().length() == 0) {
            return "unknown";
        }
        return value.trim().replace(' ', '_').replace('-', '_').toLowerCase();
    }
}
