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
 * V4.1 HTTP Client 任务消费者
 * 采用共享消费组模式，从 stream:outbound:http:tasks 认领外发任务
 */
public class HttpClientConsumer extends Thread {
	private static final SystemLogger logger = SystemLogger.getSystemLogger(HttpClientConsumer.class);
	
	private final StreamQueueManager queueManager;
	private final CommonHttpClientFactory clientFactory;
	private volatile boolean running = true;
	
	private static final String GROUP_NAME = "cg_http_clients";
	private final String consumerName;
	
	public HttpClientConsumer() {
		this.queueManager = StreamQueueManager.getInstance();
		this.clientFactory = CommonHttpClientFactory.getInstance();
		this.consumerName = "http_client_" + System.currentTimeMillis();
		setName("HttpClientConsumer-" + consumerName);
	}

	@Override
	public void run() {
		logger.info("HttpClientConsumer started. Group: {}, Consumer: {}", GROUP_NAME, consumerName);
		
		// 1. 尝试初始化消费组
		//queueManager.createGroup(StreamQueueManager.STR_OUTBOUND_HTTP, GROUP_NAME);
		
		Map<String, StreamEntryID> streamsMap = new HashMap<>();
		streamsMap.put(StreamQueueManager.STR_OUTBOUND_HTTP, StreamEntryID.UNRECEIVED_ENTRY);
		
		while (running) {
			try {
				// 2. 先认领超时未确认的消息 (PEL)
				processAutoClaim();
				
				// 3. 读取新消息
				List<GmmsMessage> tasks = queueManager.consumeBatch(GROUP_NAME, consumerName, 10, streamsMap, true);
				if (tasks == null || tasks.isEmpty()) {
					Thread.sleep(100);
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
				try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
			}
		}
	}

	private void processAutoClaim() {
		List<GmmsMessage> orphans = queueManager.autoClaimBatch(
				StreamQueueManager.STR_OUTBOUND_HTTP, GROUP_NAME, consumerName,
				queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimBatchSize(), true);
		for (GmmsMessage msg : orphans) {
			dispatchTask(StreamQueueManager.STR_OUTBOUND_HTTP, msg);
		}
	}

	/**
	 * 将任务分发到对应的 SSID 内存队列线程池中执行
	 */
	private void dispatchTask(String streamKey, GmmsMessage msg) {
		int ssid = msg.getRSsID();
		if (ssid <= 0) {
			ssid = msg.getOSsID(); // 对于 DR 回调，目标 SSID 可能是 OSsID
		}
		
		OperatorMessageQueue queue = clientFactory.getOperatorMessageQueue(ssid);
		if (queue == null) {
			// 延迟初始化：如果该 SSID 尚未在当前节点开启过队列，则尝试启动
			logger.info(msg, "SSID {} queue not found, attempting lazy initialization", ssid);
			failToCoreAndAck(streamKey, msg, "queue not found for ssid=" + ssid);
			// 注意：这里需要确保 CommonHttpClientFactory 能够动态创建并启动队列
			// 在此系统中，通常由 initQueryMsgThread 或启动配置触发
			// 如果没找到，可能该节点未配置此 SSID 的外发
			return;
		}
		
		// 注入发送队列，异步执行发送并将结果上报 Core
		if (!queue.putMsg(msg)) {
			failToCoreAndAck(streamKey, msg, "queue put failed for ssid=" + ssid);
		}
	}

	private void failToCoreAndAck(String streamKey, GmmsMessage msg, String reason) {
		markFailureResult(msg);
		if (queueManager.produceResult(msg)) {
			queueManager.ack(streamKey, GROUP_NAME, msg, true);
			logger.warn(msg, "HTTP task failed, result produced and acked. streamKey={}, reason={}",
					streamKey, reason);
			return;
		}
		logger.error(msg, "HTTP task failed and result was not produced. Keep pending for auto-claim. streamKey={}, reason={}",
				streamKey, reason);
		queueManager.logNack(streamKey, GROUP_NAME, consumerName, msg, true, "failure_result_not_produced:" + reason);
	}

	private void markFailureResult(GmmsMessage msg) {
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
