package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/**
 * Core-side direct IN_DR dispatcher used by auto generated delayed DR.
 */
public class DirectInDRDispatchService {
	private static final SystemLogger log = SystemLogger.getSystemLogger(DirectInDRDispatchService.class);
	private static final DirectInDRDispatchService instance = new DirectInDRDispatchService();

	private final GmmsUtility gmmsUtility;
	private final A2PCustomerManager ctm;
	private final MessageStoreManager msm;
	private final ModuleManager moduleManager;

	private DirectInDRDispatchService() {
		gmmsUtility = GmmsUtility.getInstance();
		ctm = gmmsUtility.getCustomerManager();
		msm = gmmsUtility.getMessageStoreManager();
		moduleManager = ModuleManager.getInstance();
	}

	public static DirectInDRDispatchService getInstance() {
		return instance;
	}

	public boolean isEnabled() {
		return Boolean.parseBoolean(gmmsUtility.getCommonProperty("AutoInDR.DirectStreamEnable", "false"));
	}

	public boolean dispatchOrDelay(GmmsMessage message, int delaySeconds) {
		if (message == null) {
			return true;
		}
		if (message.getSarMsgRefNum() != null && message.getSarMsgRefNum().trim().length() > 0) {
			List<GmmsMessage> msgList = new ArrayList<GmmsMessage>();
			CsmUtility.splitCsmDr(message, msgList);
			boolean success = true;
			for (GmmsMessage msg : msgList) {
				success = dispatchOneOrDelay(msg, delaySeconds) && success;
			}
			return success;
		}
		return dispatchOneOrDelay(message, delaySeconds);
	}

	public boolean dispatchSingleOrDelay(GmmsMessage message, int delaySeconds) {
		return dispatchOneOrDelay(message, delaySeconds);
	}

	private boolean dispatchOneOrDelay(GmmsMessage message, int delaySeconds) {
		if (delaySeconds <= 0) {
			return dispatchNow(message);
		}
		if (!isDelayDispatcherEnabled()) {
			return false;
		}
		return DelayedInDRDispatcher.getInstance().schedule(message, delaySeconds);
	}

	public boolean dispatchNow(GmmsMessage message) {
		if (message == null) {
			return true;
		}
		try {
			A2PCustomerInfo server = ctm.getCustomerBySSID(message.getOSsID());
			if (server == null) {
				log.warn(message, "Auto IN_DR direct dispatch failed: customer not found. ossid={}", message.getOSsID());
				return false;
			}
			if (ctm.isNotDRSupport(message.getOSsID())) {
				log.info(message, "Auto IN_DR direct dispatch skipped: customer does not support DR. ossid={}", message.getOSsID());
				return true;
			}
			if (message.getInClientPull() == 2) {
				msm.updateMsgForDRQuery(message);
				return true;
			}
			if (ctm.isInDRStoreMode(message.getRSsID())) {
				msm.sendDRMessage(message);
				return true;
			}

			restoreOriginalAddress(message);
			String moduleName = resolveModuleName(message, server);
			String streamKey = null;
			boolean produced;
			if ("HTTP".equalsIgnoreCase(server.getProtocol())) {
				message.setDeliveryChannel(moduleName);
				produced = StreamQueueManager.getInstance().produceHttpDeliveryReport(message);
			} else {
				streamKey = StreamQueueManager.getInstance().getDrPendingQueue(message);
				if (isBlank(streamKey)) {
					message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
					MetricsCollector.getInstance().incrementCounter(MetricsNames.business(MetricsNames.FLOW_CORE,
							message, MetricsNames.ACTION_RDQ_RETRY_POLICY));
					log.warn(message, "Auto IN_DR direct dispatch has no SMPP server module, hand over to DR retry policy. reason=no_server_module, action=rdq_retry_policy, protocol={}, moduleName={}",
							server.getProtocol(), moduleName);
					msm.handleInDeliveryReportRes(message);
					return true;
				}
				preserveDrTargetModule(message, streamKey);
				produced = StreamQueueManager.getInstance().produceDeliveryReportToQueue(message, streamKey);
			}
			if (!produced) {
				log.warn(message, "Auto IN_DR direct dispatch failed to produce stream. protocol={}, moduleName={}, stream={}",
						server.getProtocol(), moduleName, streamKey);
				return false;
			}
			log.debug(message, "Auto IN_DR direct dispatch produced stream. protocol={}, moduleName={}, stream={}",
					server.getProtocol(), moduleName, streamKey);
			return true;
		} catch (Exception e) {
			log.warn(message, "Auto IN_DR direct dispatch error", e);
			return false;
		}
	}

	private boolean isDelayDispatcherEnabled() {
		return Boolean.parseBoolean(gmmsUtility.getCommonProperty("AutoInDR.DelayDispatcherEnable", "true"));
	}

	private void restoreOriginalAddress(GmmsMessage message) {
		String origSender = message.getOriginalSenderAddr();
		if (origSender != null && origSender.length() > 0) {
			message.setSenderAddress(origSender);
		}
		String origRecipient = message.getOriginalRecipientAddr();
		if (origRecipient != null && origRecipient.length() > 0) {
			message.setRecipientAddress(origRecipient);
		}
	}

	private String resolveModuleName(GmmsMessage message, A2PCustomerInfo server) {
		String moduleName = extractModuleName(message.getDeliveryChannel());
		if (!isBlank(moduleName)) {
			return moduleName;
		}
		String queue = message.getInClientPull() == 1 ? server.getReceiverQueue() : server.getChlQueue();
		moduleName = moduleManager.selectChannel(queue);
		return moduleName;
	}

	private String extractModuleName(String deliveryChannel) {
		if (isBlank(deliveryChannel)) {
			return null;
		}
		int idx = deliveryChannel.lastIndexOf(':');
		if (idx >= 0 && idx + 1 < deliveryChannel.length()) {
			return deliveryChannel.substring(idx + 1);
		}
		return deliveryChannel;
	}

	private boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}

	private void preserveDrTargetModule(GmmsMessage message, String streamKey) {
		if (message == null || isBlank(streamKey)) {
			return;
		}
		String targetModule = extractDrTargetModule(streamKey);
		if (isBlank(targetModule)) {
			return;
		}
		String currentModule = extractModuleName(message.getDeliveryChannel());
		String localModule = System.getProperty("module");
		if (isBlank(currentModule) || localModule != null && localModule.equalsIgnoreCase(currentModule)) {
			message.setDeliveryChannel(targetModule);
		}
	}

	private String extractDrTargetModule(String streamKey) {
		if (isBlank(streamKey)) {
			return null;
		}
		String prefix = "stream:dr:pending:";
		if (!streamKey.startsWith(prefix)) {
			return null;
		}
		String rest = streamKey.substring(prefix.length());
		int firstColon = rest.indexOf(':');
		if (firstColon < 0 || firstColon + 1 >= rest.length()) {
			return null;
		}
		String moduleAndShard = rest.substring(firstColon + 1);
		int lastColon = moduleAndShard.lastIndexOf(':');
		if (lastColon > 0) {
			return moduleAndShard.substring(0, lastColon);
		}
		return moduleAndShard;
	}

	public GmmsMessage buildAutoInDR(GmmsMessage source) {
		if (source == null) {
			return null;
		}
		A2PCustomerInfo oInfo = ctm.getCustomerBySSID(source.getOSsID());
		GmmsMessage drMsg = new GmmsMessage(source);
		drMsg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		if (oInfo != null && GmmsUtility.isModifySuccessDR(oInfo.getSSID(), oInfo.getDrSucRatio(), oInfo.getDrBiasRatio())) {
			drMsg.setStatus(GmmsStatus.DELIVERED);
		} else {
			drMsg.setStatus(GmmsStatus.UNDELIVERABLE);
		}
		return drMsg;
	}
}
