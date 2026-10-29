package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.throttle.DistributedThrottlingManager;
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
		private ExecutorService recoveryThread;
		private ScheduledExecutorService deferThread;
		/** vendor submits put off for want of a rate permit (OutgoingThrottlingNum) */
		private StreamDeferrals deferrals;
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
			// HTTP sends are synchronous (up to connect + read timeout each): small batches, so the
			// entries read with it are not idle long enough for auto-claim to hand them out again
			this.consumerConfig = RedisStreamConsumerConfig.load("Client", configPrefix,
					submitMq ? 50 : 20, submitMq ? 100 : 50, 5000, 10, 50);
		}

		private synchronized void start() {
			if (running) {
				return;
			}
			running = true;
			workerPool = consumerConfig.newElasticWorkerPool();
			deferThread = Executors.newSingleThreadScheduledExecutor();
			deferrals = new StreamDeferrals(deferThread, new StreamDeferrals.Resume() {
				public void resume(String streamKey) {
					resumeDeferred(streamKey);
				}
			});
			dispatcherThread = Executors.newSingleThreadExecutor();
			dispatcherThread.execute(new Runnable() {
				public void run() {
					dispatcherLoop();
				}
			});
			recoveryThread = Executors.newSingleThreadExecutor();
			recoveryThread.execute(new RedisStreamAutoClaimRunner(configPrefix,
					streamPrefix + "*", groupName, consumerName, submitMq, queueManager,
					new RedisStreamAutoClaimRunner.MessageProcessor() {
						public void process(String streamKey, GmmsMessage msg) {
							if (!processMessage(streamKey, msg)) {
								deferrals.defer(streamKey, java.util.Collections.singletonList(msg),
										DistributedThrottlingManager.getInstance().retryAfterMs(msg.getRSsID()));
							}
						}
					}));
			queueManager.reRingExistingStreams(streamPrefix + "*", submitMq);
			logger.info("{} consumer started. Group: {}, Consumer: {}, Config: {}",
					configPrefix, groupName, consumerName, consumerConfig.summary());
		}

		private synchronized void stop() {
			running = false;
			if (dispatcherThread != null) {
				dispatcherThread.shutdownNow();
			}
			if (recoveryThread != null) {
				recoveryThread.shutdownNow();
			}
			if (deferThread != null) {
				deferThread.shutdownNow();
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
						if (deferrals.isDeferred(streamKey)) {
							// its put-off entries go first; the task started for them re-rings afterwards
							pendingDoorbellMap.put(streamKey, Boolean.TRUE);
						} else if (processingMap.putIfAbsent(streamKey, Boolean.TRUE) == null) {
							try {
								workerPool.submit(new FetchAndProcessTask(streamKey, null));
							} catch (RejectedExecutionException full) {
								// all workers busy: give the stream back instead of running it on the dispatcher
								processingMap.remove(streamKey);
								queueManager.triggerDoorbell(streamKey, submitMq);
								Thread.sleep(consumerConfig.dispatcherIdleSleepMs());
							}
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
			/** entries of this stream put off earlier, processed before reading new ones */
			private final List<GmmsMessage> carried;

			private FetchAndProcessTask(String streamKey, List<GmmsMessage> carried) {
				this.streamKey = streamKey;
				this.carried = carried;
			}

			public void run() {
				boolean yielded = false;
				try {
					queueManager.createGroup(streamKey, groupName, submitMq);
					if (carried != null && !processCarried()) {
						return;
					}
					StreamTaskBudget budget = new StreamTaskBudget("Client");
					while (running) {
						Map<String, StreamEntryID> query = new HashMap<String, StreamEntryID>();
						query.put(streamKey, StreamEntryID.UNRECEIVED_ENTRY);
						List<GmmsMessage> messages = queueManager.consumeBatch(groupName, consumerName,
								batchSize(streamKey), query, submitMq);
						if (messages == null || messages.isEmpty()) {
							break;
						}
						if (!processAll(messages)) {
							return;
						}
						if (budget.spent()) {
							yielded = true;
							break;
						}
					}
				} catch (Exception e) {
					logger.error("Error processing HTTP client stream: " + streamKey, e);
				} finally {
					processingMap.remove(streamKey);
					boolean rung = pendingDoorbellMap.remove(streamKey) != null;
					if ((rung || yielded) && !deferrals.isDeferred(streamKey)
							&& queueManager.streamLength(streamKey, submitMq) > 0) {
						queueManager.triggerDoorbell(streamKey, submitMq);
					}
				}
			}

			/** Put-off entries: keep the ones still pending (idle reset) and process them. */
			private boolean processCarried() {
				List<String> ids = new ArrayList<String>();
				for (GmmsMessage m : carried) {
					ids.add(m.getRedisStreamID());
				}
				Set<String> still;
				try {
					still = new HashSet<String>(queueManager.keepPending(streamKey, groupName, consumerName, ids, submitMq));
				} catch (Exception e) {
					logger.warn("HTTP put-off entries not reclaimed, retried later. streamKey={}", streamKey, e);
					deferrals.defer(streamKey, carried, 1000L);
					return false;
				}
				List<GmmsMessage> live = new ArrayList<GmmsMessage>();
				for (GmmsMessage m : carried) {
					if (still.contains(m.getRedisStreamID())) {
						live.add(m);
					}
				}
				return processAll(live);
			}

			/** false when the rest was put off (no rate permit): the task ends. */
			private boolean processAll(List<GmmsMessage> messages) {
				for (int i = 0; i < messages.size(); i++) {
					GmmsMessage msg = messages.get(i);
					if (!processMessage(streamKey, msg)) {
						deferrals.defer(streamKey, new ArrayList<GmmsMessage>(messages.subList(i, messages.size())),
								DistributedThrottlingManager.getInstance().retryAfterMs(msg.getRSsID()));
						return false;
					}
				}
				return true;
			}
		}

		/** Delay over: a task processes the put-off entries, then goes on reading the stream. */
		private void resumeDeferred(String streamKey) {
			if (processingMap.putIfAbsent(streamKey, Boolean.TRUE) != null) {
				deferrals.schedule(streamKey, 20L);    // the task that put them off is still finishing
				return;
			}
			List<GmmsMessage> list = deferrals.take(streamKey);
			if (list == null) {
				processingMap.remove(streamKey);
				return;
			}
			try {
				workerPool.submit(new FetchAndProcessTask(streamKey, list));
			} catch (RejectedExecutionException full) {
				processingMap.remove(streamKey);
				deferrals.defer(streamKey, list, 100L);
			}
		}

		/** Vendor submits of a rate limited channel: read at most about one second of its rate. */
		private int batchSize(String streamKey) {
			int batch = consumerConfig.batchSize();
			if (!submitMq) {
				return batch;
			}
			int tps = DistributedThrottlingManager.getInstance().tps(rssidOf(streamKey));
			return tps > 0 ? Math.max(1, Math.min(batch, tps)) : batch;
		}

		/** stream:http:submit:{rssid}:{shard} */
		private int rssidOf(String streamKey) {
			try {
				String rest = streamKey.substring(streamPrefix.length());
				return Integer.parseInt(rest.substring(0, rest.indexOf(':')));
			} catch (Exception e) {
				return -1;
			}
		}

		private void refreshWorkerPoolConfig() {
			long now = System.currentTimeMillis();
			if (now - lastPoolConfigRefreshMs < 5000L) {
				return;
			}
			lastPoolConfigRefreshMs = now;
			consumerConfig.applyElasticWorkerPool(workerPool);
		}

		/**
		 * @return false when a vendor submit got no rate permit (OutgoingThrottlingNum, per Client
		 *         node): nothing done, the caller puts it off
		 */
		private boolean processMessage(String streamKey, GmmsMessage msg) {
			long start = System.nanoTime();
			String processMessageType = submitMq ? "submit" : "dr";
			int targetSsid = -1;
			String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
			// vendor submits are sent at most once (ClientSendMarker); DRs pushed to HTTP customers are not
			boolean useMarker = submitMq && ClientSendMarker.enabled();
			boolean markerHeld = false;
			try {
				targetSsid = resolveTargetSsid(msg);
				if (submitMq && !DistributedThrottlingManager.getInstance().tryAcquire(targetSsid)) {
					return false;
				}
				incrementReceivedFromStreamMetric(targetSsid);
				if (useMarker) {
					ClientSendMarker.Claim c;
					try {
						c = ClientSendMarker.claim(msg, ClientSendMarker.owner(consumerName));
					} catch (Exception e) {
						logger.error(msg, "HTTP send marker not available, left pending", e);
						queueManager.logNack(streamKey, groupName, consumerName, msg, submitMq, "send_marker_error");
						return true;
					}
					if (c == ClientSendMarker.Claim.ACK_ONLY) {
						ClientSendMarker.ackFailed(streamKey, groupName, sourceRedisId, msg);
					}
					if (c != ClientSendMarker.Claim.SEND) {
						return true;
					}
					markerHeld = true;
				}
				Session session = clientFactory.getSession(targetSsid);
				if (session == null) {
					failToCoreAndAck(streamKey, msg, targetSsid, "http session not found, ssid=" + targetSsid, markerHeld);
					return true;
				}
				boolean success = session.submit(msg);
				if (success) {
					incrementSuccessMetric(targetSsid);
					markerHeld = false;
					boolean acked = useMarker ? ClientSendMarker.ackSent(streamKey, groupName, sourceRedisId, msg)
							: queueManager.ackById(streamKey, groupName, sourceRedisId, msg, submitMq);
					if (!acked) {
						logger.warn(msg,
								"HTTP client stream ack failed after send success. streamKey={}, sourceRedisId={}, currentRedisId={}",
								streamKey, sourceRedisId, msg == null ? null : msg.getRedisStreamID());
					}
					return true;
				}
				failToCoreAndAck(streamKey, msg, targetSsid, "http submit returned false, ssid=" + targetSsid, markerHeld);
			} catch (Exception e) {
				logger.error(msg, "HTTP client stream message processing failed", e);
				if (useMarker && !markerHeld) {
					// sent (only the ack failed) or the marker was settled: the redelivery only acks
					return true;
				}
				failToCoreAndAck(streamKey, msg, targetSsid, "exception:" + e.getClass().getSimpleName(), markerHeld);
			} finally {
				MetricsCollector.getInstance().recordTime(MetricsNames.build(MetricsNames.COMPONENT_CONSUMER,
						MetricsNames.FLOW_PROCESS, processMessageType, MetricsNames.ACTION_LATENCY),
						System.nanoTime() - start);
			}
			return true;
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

		private void failToCoreAndAck(String streamKey, GmmsMessage msg, int targetSsid, String reason,
				boolean markerHeld) {
			boolean resultProduced = false;
			String sourceRedisId = msg == null ? null : msg.getRedisStreamID();
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
					if (markerHeld) {
						ClientSendMarker.ackFailed(streamKey, groupName, sourceRedisId, msg);
					} else {
						queueManager.ackById(streamKey, groupName, sourceRedisId, msg, submitMq);
					}
				} else {
					if (msg != null) {
						msg.setRedisStreamID(sourceRedisId);
					}
					if (markerHeld) {
						ClientSendMarker.release(msg);
					}
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
