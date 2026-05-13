package com.king.gmms.metrics;

import com.king.message.gmms.GmmsMessage;

public class MetricsNames {

    public static final String COMPONENT_STREAM = "stream";
    public static final String COMPONENT_CONSUMER = "consumer";
    public static final String COMPONENT_BUSINESS = "business";

    public static final String FLOW_IN = "in";
    public static final String FLOW_OUT = "out";
    public static final String FLOW_PROCESS = "process";
    public static final String FLOW_RUN = "run";
    public static final String FLOW_SERVER = "server";
    public static final String FLOW_CORE = "core";
    public static final String FLOW_CLIENT = "client";
    public static final String STAGE_IN_SUBMIT = "insubmit";
    public static final String STAGE_OUT_SUBMIT = "outsubmit";
    public static final String STAGE_IN_DR = "indr";
    public static final String STAGE_OUT_DR = "outdr";

    public static final String ACTION_WRITE_OK = "write_ok";
    public static final String ACTION_WRITE_FAIL = "write_fail";
    public static final String ACTION_READ = "read";
    public static final String ACTION_ACK = "ack";
    public static final String ACTION_ACK_FAIL = "ack_fail";
    public static final String ACTION_FAIL = "fail";
    public static final String ACTION_LATENCY = "latency";

    public static final String ACTION_RECEIVED = "received";
    public static final String ACTION_ACCEPTED_TO_REDIS = "accepted_to_redis";
    public static final String ACTION_REJECTED_BEFORE_REDIS = "rejected_before_redis";
    public static final String ACTION_RECEIVED_FROM_STREAM = "received_from_stream";
    public static final String ACTION_ROUTING_SUCCESS = "routing_success";
    public static final String ACTION_ROUTING_FAILED = "routing_failed";
    public static final String ACTION_ROUTED_TO_REDIS = "routed_to_redis";
    public static final String ACTION_SMPP_SENT = "smpp_sent";
    public static final String ACTION_HTTP_SENT = "http_sent";
    public static final String ACTION_FAILED_BEFORE_RESPONSE = "failed_before_response";
    public static final String ACTION_RESPONSE_RECEIVED = "response_received";
    public static final String ACTION_RESULT_WRITTEN = "result_written";
    public static final String ACTION_WRITTEN_TO_REDIS = "written_to_redis";
    public static final String ACTION_PROCESSED = "processed";
    public static final String ACTION_SENT_TO_CUSTOMER = "sent_to_customer";
    public static final String ACTION_FAILED_TO_CUSTOMER = "failed_to_customer";

    private static volatile boolean ssidMetricsEnabled = false;

    public static String counter(String flow, GmmsMessage msg, String metric) {
        return build(flow, FLOW_IN, msgType(msg), metric);
    }

    public static String timer(String flow, GmmsMessage msg, String metric) {
        return build(flow, FLOW_RUN, msgType(msg), metric);
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
        return module() + ".db.pool." + normalize(dsName) + "." + normalize(metric);
    }

    public static String business(String flow, GmmsMessage msg, String action) {
        return build(COMPONENT_BUSINESS, flow, msgType(msg), action);
    }

    public static String business(String flow, String messageType, String action) {
        return build(COMPONENT_BUSINESS, flow, msgType(messageType), action);
    }

    public static String ssid(String stage, int ssid, String action) {
        if (!ssidMetricsEnabled || ssid < 0) {
            return null;
        }
        return module() + ".ssid." + ssid + "." + normalize(stage) + "." + normalize(action);
    }

    public static String ssid(String stage, GmmsMessage msg, boolean useRssid, String action) {
        if (msg == null) {
            return null;
        }
        return ssid(stage, useRssid ? msg.getRSsID() : msg.getOSsID(), action);
    }

    public static void configureSsidMetrics(boolean enabled) {
        ssidMetricsEnabled = enabled;
    }

    public static boolean isSsidMetricsEnabled() {
        return ssidMetricsEnabled;
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
        return msgType(msg.getMessageType());
    }

    public static String msgType(String type) {
        if (type == null) {
            return "other";
        }
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
