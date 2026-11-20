package com.king.gmms.mqm;

import java.util.ArrayList;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.gmms.util.ExpiredMessageQueueWithSafeExit;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class MQMMessageSender implements Runnable {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MQMMessageSender.class);
	private ExpiredMessageQueueWithSafeExit messages;
	private GmmsUtility gmmsUtility = null;
	private MessageStoreManager msm = null;

	public MQMMessageSender(ExpiredMessageQueueWithSafeExit messages) {
		gmmsUtility = GmmsUtility.getInstance();
		msm = gmmsUtility.getMessageStoreManager();
		this.messages = messages;		
	}

	/** the earliest time (System.nanoTime) of the next send, shared by all the senders */
	private static final java.util.concurrent.atomic.AtomicLong nextSendAt = new java.util.concurrent.atomic.AtomicLong();

	/**
	 * ResendDRThrottle: at most perSecond messages a second from all the MQM senders together (all the MQM resends:
	 * DRs, MTs, expired and query DRs); 0 or less: no limit. Each send takes the next slot and waits for it.
	 */
	static void pace(int perSecond) throws InterruptedException {
		if (perSecond <= 0) {
			return;
		}
		long interval = 1000000000L / perSecond;
		long now = System.nanoTime(), slot;
		while (true) {
			long next = nextSendAt.get();
			slot = Math.max(now, next);
			if (nextSendAt.compareAndSet(next, slot + interval)) {
				break;
			}
		}
		long waitNs = slot - now;
		if (waitNs > 0) {
			Thread.sleep(waitNs / 1000000L, (int) (waitNs % 1000000L));
		}
	}

	public void putMsg(GmmsMessage msg) {
		messages.put(msg);
	}

	public void putMsg(ArrayList<GmmsMessage> msgList) {
		messages.addAll(msgList);
	}

	public void run() {
		log.info("MQMMessageSender Thread start!");
		while (true) {
			try {
				if (!MqmActiveState.isActive()) {
					Thread.sleep(1000L);
					continue;
				}
				GmmsMessage msg = (GmmsMessage) messages.get(1000L);
				if (msg != null) {
					pace(gmmsUtility.getResendDRThrottle());
					boolean success = false;
					String type = msg.getMessageType();

					// V4.1 Async Distribution via Redis Streams
					if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(type)
							|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(type)) {
						success = StreamQueueManager.getInstance().produceMqmSubmitMessage(msg);
					} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(type)
							|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(type)) {
						success = StreamQueueManager.getInstance().produceMqmReportMessage(msg);
					} else {
						log.warn(msg, "Unknown message type {} in MQM, handle as message error", type);
						handleMessageError(msg);
						success = true;
					}

					if (!success) {
						handleMessageError(msg);
					} else {
						if (log.isInfoEnabled()) {
							log.info(msg, "One message [{}] sent to Redis Stream successfully.", type);
						}
					}
				}
			} catch (Exception ex) {
				log.error("Error accessing messages from messagestore", ex);
			}
		}
		
	}
		


	/**
	 * handleMessageError
	 * 
	 * @param msg
	 */
	private void handleMessageError(GmmsMessage msg) {
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg
						.getMessageType())) {
			msg.setStatus(GmmsStatus.SERVER_ERROR);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(msg.getMessageType())) {
			msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())) {
			msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
		}
		msm.handleMessageError(msg);
	}
}
