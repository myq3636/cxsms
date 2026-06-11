package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

/**
 * Legacy HTTP client stream consumer.
 */
public class HttpClientConsumer extends Thread {
	private static final SystemLogger logger = SystemLogger.getSystemLogger(HttpClientConsumer.class);
	private static final String GROUP_NAME = "cg_http_clients";

	private final StreamQueueManager queueManager;
	private final CommonHttpClientFactory clientFactory;
	private final String consumerName;
	private volatile boolean running = true;
	private long lastAutoClaimMs = 0L;

	public HttpClientConsumer() {
		this.queueManager = StreamQueueManager.getInstance();
		this.clientFactory = CommonHttpClientFactory.getInstance();
		this.consumerName = "http_client_" + System.currentTimeMillis();
		setName("HttpClientConsumer-" + consumerName);
	}

	@Override
	public void run() {
		logger.info("HttpClientConsumer started. Group: {}, Consumer: {}", GROUP_NAME, consumerName);
		Map<String, StreamEntryID> streamsMap = new HashMap<String, StreamEntryID>();
		streamsMap.put(StreamQueueManager.STR_OUTBOUND_HTTP, StreamEntryID.UNRECEIVED_ENTRY);

		while (running) {
			try {
				maybeProcessAutoClaim();
				List<GmmsMessage> tasks = queueManager.consumeBatch(
						GROUP_NAME, consumerName, 10, streamsMap, true);
				if (tasks == null || tasks.isEmpty()) {
					Thread.sleep(100L);
					continue;
				}
				for (GmmsMessage msg : tasks) {
					dispatchTask(StreamQueueManager.STR_OUTBOUND_HTTP, msg);
				}
			} catch (InterruptedException e) {
				logger.warn("HttpClientConsumer interrupted");
				running = false;
			} catch (Exception e) {
				logger.error("Error in HttpClientConsumer loop", e);
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException ignored) {
					running = false;
				}
			}
		}
	}

	private void maybeProcessAutoClaim() {
		long now = System.currentTimeMillis();
		if (!queueManager.isAutoClaimEnabled() || now - lastAutoClaimMs < queueManager.getAutoClaimIntervalMs()) {
			return;
		}
		lastAutoClaimMs = now;
		processAutoClaim();
	}

	private void processAutoClaim() {
		List<GmmsMessage> orphans = queueManager.autoClaimBatch(
				StreamQueueManager.STR_OUTBOUND_HTTP, GROUP_NAME, consumerName,
				queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimMaxMessagesPerRun(), true);
		if (orphans == null || orphans.isEmpty()) {
			return;
		}
		for (GmmsMessage msg : orphans) {
			dispatchTask(StreamQueueManager.STR_OUTBOUND_HTTP, msg);
		}
	}

	private void dispatchTask(String streamKey, GmmsMessage msg) {
		int ssid = msg.getRSsID();
		if (ssid <= 0) {
			ssid = msg.getOSsID();
		}

		OperatorMessageQueue queue = clientFactory.getOperatorMessageQueue(ssid);
		if (queue == null) {
			logger.info(msg, "SSID {} queue not found, attempting lazy initialization", ssid);
			failToCoreAndAck(streamKey, msg, "queue not found for ssid=" + ssid);
			return;
		}

		if (!queue.putMsg(msg)) {
			failToCoreAndAck(streamKey, msg, "queue put failed for ssid=" + ssid);
		}
	}

	private void failToCoreAndAck(String streamKey, GmmsMessage msg, String reason) {
		String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
		markFailureResult(msg);
		if (queueManager.produceResult(msg)) {
			queueManager.ackById(streamKey, GROUP_NAME, sourceRedisId, msg, true);
			logger.warn(msg, "HTTP task failed, result produced and acked. streamKey={}, reason={}",
					streamKey, reason);
			return;
		}
		if (msg != null) {
			msg.setRedisStreamID(sourceRedisId);
		}
		logger.error(msg, "HTTP task failed and result was not produced. Keep pending for auto-claim. streamKey={}, reason={}",
				streamKey, reason);
		queueManager.logNack(streamKey, GROUP_NAME, consumerName, msg, true,
				"failure_result_not_produced:" + reason);
	}

	private void markFailureResult(GmmsMessage msg) {
		if (msg == null) {
			return;
		}
		String messageType = msg.getMessageType();
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)) {
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(messageType)) {
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
			msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(messageType)) {
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
			msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
		} else {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
	}

	public void stopConsumer() {
		this.running = false;
		this.interrupt();
	}
}
