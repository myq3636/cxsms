package com.king.gmms.messagequeue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

import redis.clients.jedis.StreamEntryID;

public class HttpClientStreamConsumer {
	private static final SystemLogger logger = SystemLogger.getSystemLogger(HttpClientStreamConsumer.class);
	private static final HttpClientStreamConsumer instance = new HttpClientStreamConsumer();

	private final HttpFlowConsumer submitConsumer;
	private final HttpFlowConsumer drConsumer;

	private HttpClientStreamConsumer() {
		this.submitConsumer = new HttpFlowConsumer("HttpClientSubmit", "HttpClientSubmitGroup",
				"client-http-submit", StreamQueueManager.ZSET_HTTP_SUBMIT_ACTIVE,
				StreamQueueManager.STR_HTTP_SUBMIT_PREFIX, true);
		this.drConsumer = new HttpFlowConsumer("HttpClientDR", "HttpClientDRGroup",
				"client-http-dr", StreamQueueManager.ZSET_HTTP_DR_ACTIVE,
				StreamQueueManager.STR_HTTP_DR_PREFIX, false);
	}

	public static HttpClientStreamConsumer getInstance() {
		return instance;
	}

	public synchronized void start() {
		submitConsumer.start();
		drConsumer.start();
	}

	public synchronized void stop() {
		submitConsumer.stop();
		drConsumer.stop();
	}

	private static final class HttpFlowConsumer {
		private final StreamQueueManager queueManager = StreamQueueManager.getInstance();
		private final CommonHttpClientFactory clientFactory = CommonHttpClientFactory.getInstance();
		private final RedisStreamConsumerConfig consumerConfig;
		private final String configPrefix;
		private final String groupName;
		private final String consumerName;
		private final String zsetKey;
		private final String streamPrefix;
		private final boolean submitMq;
		private final ConcurrentHashMap<String, Boolean> processingMap = new ConcurrentHashMap<String, Boolean>();
		private final ConcurrentHashMap<String, Boolean> pendingDoorbellMap = new ConcurrentHashMap<String, Boolean>();
		private volatile boolean running = false;
		private volatile long lastPoolConfigRefreshMs = 0L;
		private ExecutorService dispatcherThread;
		private ThreadPoolExecutor workerPool;

		private HttpFlowConsumer(String configPrefix, String groupName, String consumerBaseName,
				String zsetKey, String streamPrefix, boolean submitMq) {
			String nodeId = System.getProperty("NodeID", "0");
			this.configPrefix = configPrefix;
			this.groupName = groupName;
			this.consumerName = consumerBaseName + "-" + nodeId;
			this.zsetKey = zsetKey;
			this.streamPrefix = streamPrefix;
			this.submitMq = submitMq;
			this.consumerConfig = RedisStreamConsumerConfig.load("Client", configPrefix,
					submitMq ? 50 : 20, submitMq ? 100 : 50, 5000, 100, 50);
		}

		private synchronized void start() {
			if (running) {
				return;
			}
			running = true;
			workerPool = new ThreadPoolExecutor(
					consumerConfig.workerCorePoolSize(), consumerConfig.workerMaxPoolSize(),
					60L, TimeUnit.SECONDS,
					new LinkedBlockingQueue<Runnable>(consumerConfig.workerQueueSize()),
					new ThreadPoolExecutor.CallerRunsPolicy());
			dispatcherThread = Executors.newSingleThreadExecutor();
			dispatcherThread.execute(new Runnable() {
				public void run() {
					dispatcherLoop();
				}
			});
			queueManager.reRingExistingStreams(streamPrefix + "*", submitMq);
			logger.info("{} consumer started. Group: {}, Consumer: {}, Config: {}",
					configPrefix, groupName, consumerName, consumerConfig.summary());
		}

		private synchronized void stop() {
			running = false;
			if (dispatcherThread != null) {
				dispatcherThread.shutdownNow();
			}
			if (workerPool != null) {
				workerPool.shutdown();
			}
			logger.info("{} consumer stopped. Consumer: {}", configPrefix, consumerName);
		}

		private void dispatcherLoop() {
			while (running) {
				try {
					refreshWorkerPoolConfig();
					String streamKey = queueManager.popResponsibleDoorbell(zsetKey, submitMq);
					if (streamKey != null) {
						if (processingMap.putIfAbsent(streamKey, Boolean.TRUE) == null) {
							workerPool.submit(new FetchAndProcessTask(streamKey));
						} else {
							pendingDoorbellMap.put(streamKey, Boolean.TRUE);
						}
					} else {
						Thread.sleep(consumerConfig.dispatcherIdleSleepMs());
					}
				} catch (InterruptedException e) {
					break;
				} catch (Exception e) {
					logger.error(configPrefix + " dispatcher loop error", e);
					try {
						Thread.sleep(1000L);
					} catch (InterruptedException ignored) {
						break;
					}
				}
			}
		}

		private final class FetchAndProcessTask implements Runnable {
			private final String streamKey;

			private FetchAndProcessTask(String streamKey) {
				this.streamKey = streamKey;
			}

			public void run() {
				try {
					queueManager.createGroup(streamKey, groupName, submitMq);
					while (running) {
						processAutoClaim(streamKey);
						Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
						query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
						List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName,
								consumerConfig.batchSize(), query, submitMq);
						if (messages == null || messages.isEmpty()) {
							break;
						}
						for (GmmsMessage msg : messages) {
							processMessage(streamKey, msg);
						}
					}
				} catch (Exception e) {
					logger.error("Error processing HTTP client stream: " + streamKey, e);
				} finally {
					processingMap.remove(streamKey);
					if (pendingDoorbellMap.remove(streamKey) != null
							&& queueManager.streamLength(streamKey, submitMq) > 0) {
						queueManager.triggerDoorbell(streamKey, submitMq);
					}
				}
			}
		}

		private void refreshWorkerPoolConfig() {
			long now = System.currentTimeMillis();
			if (now - lastPoolConfigRefreshMs < 5000L) {
				return;
			}
			lastPoolConfigRefreshMs = now;
			consumerConfig.applyWorkerPool(workerPool);
		}

		private void processAutoClaim(String streamKey) {
			List<GmmsMessage> messages = queueManager.autoClaimBatch(streamKey, groupName, consumerName,
					queueManager.getAutoClaimIdleMs(), queueManager.getAutoClaimBatchSize(), submitMq);
			if (messages == null || messages.isEmpty()) {
				return;
			}
			for (GmmsMessage msg : messages) {
				processMessage(streamKey, msg);
			}
		}

		private void processMessage(String streamKey, GmmsMessage msg) {
			long start = System.nanoTime();
			String processMessageType = submitMq ? "submit" : "dr";
			int targetSsid = -1;
			try {
				targetSsid = resolveTargetSsid(msg);
				incrementReceivedFromStreamMetric(targetSsid);
				Session session = clientFactory.getSession(targetSsid);
				if (session == null) {
					failToCoreAndAck(streamKey, msg, targetSsid, "http session not found, ssid=" + targetSsid);
					return;
				}
				boolean success = session.submit(msg);
				if (success) {
					incrementSuccessMetric(targetSsid);
					queueManager.ack(streamKey, groupName, msg, submitMq);
					return;
				}
				failToCoreAndAck(streamKey, msg, targetSsid, "http submit returned false, ssid=" + targetSsid);
			} catch (Exception e) {
				logger.error(msg, "HTTP client stream message processing failed", e);
				failToCoreAndAck(streamKey, msg, targetSsid, "exception:" + e.getClass().getSimpleName());
			} finally {
				MetricsCollector.getInstance().recordTime(MetricsNames.build(MetricsNames.COMPONENT_CONSUMER,
						MetricsNames.FLOW_PROCESS, processMessageType, MetricsNames.ACTION_LATENCY),
						System.nanoTime() - start);
			}
		}

		private int resolveTargetSsid(GmmsMessage msg) {
			if (submitMq) {
				return msg.getRSsID();
			}
			return msg.getOSsID() > 0 ? msg.getOSsID() : msg.getRSsID();
		}

		private void incrementReceivedFromStreamMetric(int targetSsid) {
			if (submitMq) {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "submit", MetricsNames.ACTION_RECEIVED_FROM_STREAM));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_SUBMIT,
						targetSsid, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
			} else {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "dr", MetricsNames.ACTION_RECEIVED_FROM_STREAM));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_DR,
						targetSsid, MetricsNames.ACTION_RECEIVED_FROM_STREAM));
			}
		}

		private void incrementSuccessMetric(int targetSsid) {
			if (submitMq) {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "submit", MetricsNames.ACTION_HTTP_SENT));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_SUBMIT,
						targetSsid, MetricsNames.ACTION_HTTP_SENT));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "submit_resp", MetricsNames.ACTION_RESPONSE_RECEIVED));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "submit_resp", MetricsNames.ACTION_RESULT_WRITTEN));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_SUBMIT,
						targetSsid, MetricsNames.ACTION_RESPONSE_RECEIVED));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_SUBMIT,
						targetSsid, MetricsNames.ACTION_RESULT_WRITTEN));
			} else {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "dr", MetricsNames.ACTION_SENT_TO_CUSTOMER));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "dr_resp", MetricsNames.ACTION_RESULT_WRITTEN));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_DR,
						targetSsid, MetricsNames.ACTION_SENT_TO_CUSTOMER));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_DR,
						targetSsid, MetricsNames.ACTION_RESULT_WRITTEN));
			}
		}

		private void failToCoreAndAck(String streamKey, GmmsMessage msg, int targetSsid, String reason) {
			boolean resultProduced = false;
			boolean alreadyResponse = isResponseMessage(msg);
			try {
				logger.warn(msg, "{} failed before HTTP response result. streamKey={}, reason={}",
						configPrefix, streamKey, reason);
				incrementFailureMetric(targetSsid, alreadyResponse);
				if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())) {
					if (msg.getStatusCode() < 0) {
						msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
					}
				} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
					msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
					msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
				} else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType())) {
					if (msg.getStatusCode() < 0) {
						msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					}
				} else {
					msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				}
				resultProduced = queueManager.produceResult(msg);
				if (resultProduced) {
					incrementResultWrittenMetric(targetSsid);
				}
			} catch (Exception e) {
				logger.error(msg, "Failed to produce HTTP failure result", e);
			} finally {
				if (resultProduced) {
					queueManager.ack(streamKey, groupName, msg, submitMq);
				} else {
					queueManager.logNack(streamKey, groupName, consumerName, msg, submitMq,
							"failure_result_not_produced:" + reason);
				}
			}
		}

		private boolean isResponseMessage(GmmsMessage msg) {
			if (msg == null || msg.getMessageType() == null) {
				return false;
			}
			return GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType())
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType());
		}

		private void incrementFailureMetric(int targetSsid, boolean alreadyResponse) {
			String messageType = submitMq ? "submit" : "dr";
			MetricsCollector.getInstance().incrementCounter(MetricsNames.build(MetricsNames.COMPONENT_CONSUMER,
					MetricsNames.FLOW_PROCESS, messageType, MetricsNames.ACTION_FAIL));
			if (!alreadyResponse) {
				String stage = submitMq ? MetricsNames.STAGE_OUT_SUBMIT : MetricsNames.STAGE_OUT_DR;
				String action = submitMq ? MetricsNames.ACTION_FAILED_BEFORE_RESPONSE : MetricsNames.ACTION_FAILED_TO_CUSTOMER;
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, messageType, action));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(stage, targetSsid, action));
			}
		}

		private void incrementResultWrittenMetric(int targetSsid) {
			if (submitMq) {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "submit_resp", MetricsNames.ACTION_RESULT_WRITTEN));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_SUBMIT,
						targetSsid, MetricsNames.ACTION_RESULT_WRITTEN));
			} else {
				MetricsCollector.getInstance().incrementCounter(MetricsNames.business(
						MetricsNames.FLOW_CLIENT, "dr_resp", MetricsNames.ACTION_RESULT_WRITTEN));
				MetricsCollector.getInstance().incrementCounter(MetricsNames.ssid(MetricsNames.STAGE_OUT_DR,
						targetSsid, MetricsNames.ACTION_RESULT_WRITTEN));
			}
		}
	}
}
