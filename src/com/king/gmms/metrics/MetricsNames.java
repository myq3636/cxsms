package com.king.gmms.metrics;

import java.util.concurrent.ConcurrentHashMap;

import com.king.message.gmms.GmmsMessage;

public class MetricsNames {

    // 性能④: 缓存 module（启动后不变）避免每条消息多次 System.getProperty(同步 Hashtable)；
    //         memo normalize（输入均为常量/小集合）避免每次 trim/replace/toLowerCase 的多次 String 分配。
    private static volatile String cachedModule = null;
    private static final ConcurrentHashMap<String, String> NORMALIZE_CACHE = new ConcurrentHashMap<String, String>();

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
    public static final String ACTION_DEAD_LETTER = "dead_letter";
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
    public static final String ACTION_WINDOW_FULL = "window_full";
    public static final String ACTION_BIND_THROTTLED = "bind_throttled";
    public static final String ACTION_RETRY_CLAIM = "retry_claim";
    public static final String ACTION_TIMEOUT_PROCESSED = "timeout_processed";
    public static final String ACTION_TIMEOUT_SKIPPED = "timeout_skipped";
    public static final String ACTION_REGISTRY_FALLBACK_CONFIGURED_SERVER = "registry_fallback_configured_server";
    public static final String ACTION_REGISTRY_NO_SERVER_MODULE = "registry_no_server_module";
    public static final String ACTION_RDQ_RETRY_POLICY = "rdq_retry_policy";

    // latency timers (docs/性能监控埋点评估与设计.md 3.3), each under {module}.
    public static final String TIMER_STREAM_LAG = "stream.lag";
    public static final String TIMER_VENDOR_SUBMIT_RTT = "client.vendor.submit_rtt";
    public static final String TIMER_VENDOR_DR_LATENCY = "core.vendor.dr_latency";
    public static final String TIMER_E2E_SUBMIT = "e2e.submit";
    public static final String TIMER_E2E_DR = "e2e.dr";
    public static final String TIMER_REDIS_OP = "redis.op";
    public static final String TIMER_DB_OP = "db.op";
    public static final String TIMER_DEFER_WAIT = "defer.wait";
    /** consumer processing time per consumer: {module}.consumer.{name} */
    public static final String CONSUMER_MT = "mt";
    public static final String CONSUMER_MQM = "mqm";
    public static final String CONSUMER_RESULT = "result";
    public static final String CONSUMER_INBOUND_DR = "inbound_dr";
    public static final String CONSUMER_SERVER_DR = "server_dr";
    public static final String CONSUMER_HTTP_SUBMIT = "http_submit";
    public static final String CONSUMER_CLIENT_OUT = "client_out";

    private static volatile boolean ssidMetricsEnabled = false;
    private static volatile boolean bindMetricsEnabled = false;

    /** build(component, flow, type, action) by its four parts, so a name is concatenated once */
    private static final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>>> BUILD_CACHE =
            new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>>>();
    /** metric(base) and metric(base, part) */
    private static final ConcurrentHashMap<String, String> METRIC_CACHE = new ConcurrentHashMap<String, String>();
    private static final ConcurrentHashMap<String, ConcurrentHashMap<Object, String>> PART_CACHE =
            new ConcurrentHashMap<String, ConcurrentHashMap<Object, String>>();
    /** a bound on the cached names: ssids, binds and streams are many but not unbounded; past it names are built each time */
    private static final int MAX_CACHED = 50000;

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
        if (cachedModule == null || component == null || flow == null || messageType == null || action == null) {
            return buildNow(component, flow, messageType, action);
        }
        ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>> byFlow = BUILD_CACHE.get(component);
        if (byFlow == null) {
            byFlow = BUILD_CACHE.computeIfAbsent(component, k -> new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>>());
        }
        ConcurrentHashMap<String, ConcurrentHashMap<String, String>> byType = byFlow.get(flow);
        if (byType == null) {
            byType = byFlow.computeIfAbsent(flow, k -> new ConcurrentHashMap<String, ConcurrentHashMap<String, String>>());
        }
        ConcurrentHashMap<String, String> byAction = byType.get(messageType);
        if (byAction == null) {
            byAction = byType.computeIfAbsent(messageType, k -> new ConcurrentHashMap<String, String>());
        }
        String name = byAction.get(action);
        if (name == null) {
            name = buildNow(component, flow, messageType, action);
            byAction.putIfAbsent(action, name);
        }
        return name;
    }

    private static String buildNow(String component, String flow, String messageType, String action) {
        return module() + "." + normalize(component) + "." + normalize(flow)
                + "." + normalize(messageType) + "." + normalize(action);
    }

    /** {module}.{base} */
    public static String metric(String base) {
        if (cachedModule == null) {
            return module() + "." + base;
        }
        String name = METRIC_CACHE.get(base);
        if (name == null) {
            name = module() + "." + base;
            if (METRIC_CACHE.size() < MAX_CACHED) {
                METRIC_CACHE.putIfAbsent(base, name);
            }
        }
        return name;
    }

    /** {module}.{base}.{part}, e.g. the timer of one channel: part is the ssid, a bind or a stream flow */
    public static String metric(String base, Object part) {
        if (part == null) {
            return metric(base);
        }
        if (cachedModule == null) {
            return module() + "." + base + "." + normalize(String.valueOf(part));
        }
        ConcurrentHashMap<Object, String> byPart = PART_CACHE.get(base);
        if (byPart == null) {
            byPart = PART_CACHE.computeIfAbsent(base, k -> new ConcurrentHashMap<Object, String>());
        }
        String name = byPart.get(part);
        if (name == null) {
            name = module() + "." + base + "." + normalize(String.valueOf(part));
            if (byPart.size() < MAX_CACHED) {
                byPart.putIfAbsent(part, name);
            }
        }
        return name;
    }

    /** the processing time of one consumer: {module}.consumer.{consumer} */
    public static String consumer(String consumer) {
        return metric(COMPONENT_CONSUMER, consumer);
    }

    public static void configureBindMetrics(boolean enabled) {
        bindMetricsEnabled = enabled;
    }

    public static boolean isBindMetricsEnabled() {
        return bindMetricsEnabled;
    }

    public static String module() {
        // 性能④: 一次性缓存（module 在启动设定、运行期不变）。未设置时返回 "unknown" 且不缓存，
        // 以便后续真正设置后能被采用。
        String cached = cachedModule;
        if (cached != null) {
            return cached;
        }
        String module = System.getProperty("module");
        if (module == null || module.trim().length() == 0) {
            return "unknown";
        }
        cached = normalize(module);
        cachedModule = cached;
        return cached;
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
        // 性能④: memo —— 输入为常量/小集合(component/flow/action/msgType)，缓存后命中即免去 trim/replace×2/toLowerCase 的分配。
        String cached = NORMALIZE_CACHE.get(value);
        if (cached != null) {
            return cached;
        }
        cached = value.trim().replace(' ', '_').replace('-', '_').toLowerCase();
        if (NORMALIZE_CACHE.size() < MAX_CACHED) {
            NORMALIZE_CACHE.put(value, cached);
        }
        return cached;
    }
}
