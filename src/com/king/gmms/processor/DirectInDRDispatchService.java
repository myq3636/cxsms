package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.messagequeue.StreamQueueManager;
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
			boolean produced;
			if ("HTTP".equalsIgnoreCase(server.getProtocol())) {
				message.setDeliveryChannel(moduleName);
				produced = StreamQueueManager.getInstance().produceHttpDeliveryReport(message);
			} else {
				produced = StreamQueueManager.getInstance().produceDeliveryReportToModule(message, moduleName);
			}
			if (!produced) {
				log.warn(message, "Auto IN_DR direct dispatch failed to produce stream. moduleName={}", moduleName);
				return false;
			}
			log.debug(message, "Auto IN_DR direct dispatch produced stream. moduleName={}", moduleName);
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
