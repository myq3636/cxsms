package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageStoreManager;
import redis.clients.jedis.StreamEntryID;

/**
 * V4.1 Core 结果流消费者
 * 监听 stream:core:results，将客户端回传的 SUBMIT_RESP/DR_RESP 提交给 MessageStoreManager 处理
 */
public class ResultStreamConsumer extends Thread {
	private static final SystemLogger logger = SystemLogger.getSystemLogger(ResultStreamConsumer.class);
	
	private final StreamQueueManager queueManager;
	private final MessageStoreManager msm;
	private volatile boolean running = true;
	
	private static final String GROUP_NAME = "cg_core_results";
	private final String consumerName;
	
	public ResultStreamConsumer() {
		this.queueManager = StreamQueueManager.getInstance();
		this.msm = GmmsUtility.getInstance().getMessageStoreManager();
		this.consumerName = "core_result_" + System.currentTimeMillis();
		setName("ResultStreamConsumer-" + consumerName);
	}

	@Override
	public void run() {
		logger.info("ResultStreamConsumer started. Group: {}, Consumer: {}", GROUP_NAME, consumerName);
		
		// 1. 初始化消费组
		queueManager.createGroup(StreamQueueManager.STR_CORE_RESULTS, GROUP_NAME);
		
		Map<String, StreamEntryID> streamsMap = new HashMap<>();
		streamsMap.put(StreamQueueManager.STR_CORE_RESULTS, StreamEntryID.UNRECEIVED_ENTRY);
		
		while (running) {
			try {
				// 2. 自动认领过期的 Pending 消息
				processAutoClaim();
				
				// 3. 认领反馈结果
				List<GmmsMessage> results = queueManager.consumeBatch(GROUP_NAME, consumerName, 20, streamsMap, false);
				if (results == null || results.isEmpty()) {
					Thread.sleep(100);
					continue;
				}
				
				for (GmmsMessage msg : results) {
					processResult(msg);
				}
				
			} catch (InterruptedException e) {
				logger.warn("ResultStreamConsumer interrupted");
				running = false;
			} catch (Exception e) {
				logger.error("Error in ResultStreamConsumer loop", e);
				try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
			}
		}
	}

	private void processAutoClaim() {
		List<GmmsMessage> orphans = queueManager.autoClaimBatch(
				StreamQueueManager.STR_CORE_RESULTS, GROUP_NAME, consumerName, 60000, 10, false);
		for (GmmsMessage msg : orphans) {
			logger.info(msg, "Reclaimed orphan Result from PEL");
			processResult(msg);
		}
	}

	private void processResult(GmmsMessage msg) {
		try {
			// 将反馈结果分发给 MessageStoreManager 进行业务闭环
			// 这会触发 CDR 记录、计费更新、以及状态匹配逻辑
			msm.handleOutSubmitRes(msg);
			
			// 只有核心逻辑执行完毕后才 ACK
			queueManager.ack(StreamQueueManager.STR_CORE_RESULTS, GROUP_NAME, msg, false);
			
		} catch (Exception e) {
			logger.error(msg, "Failed to process result in Core", e);
		}
	}
	
	public void stopConsumer() {
		this.running = false;
		this.interrupt();
	}
}
