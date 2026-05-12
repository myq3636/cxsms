package com.king.gmms.mqm;

import java.util.ArrayList;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.messagequeue.StreamQueueManager;
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
				// Throttling for resends
				try {
					int resendThrottle = gmmsUtility.getResendDRThrottle();
					if (resendThrottle != 0 && 1000 / resendThrottle > 0) {
						Thread.sleep(1000 / resendThrottle);
					}
				} catch (Exception e) {
					log.info("MQM Throttling sleep error.", e);
				}

				GmmsMessage msg = (GmmsMessage) messages.get(1000L);
				if (msg != null) {
					boolean success = false;
					String type = msg.getMessageType();

					// V4.1 Async Distribution via Redis Streams
					if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(type)
							|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(type)) {
						// Submit types (MT) go to pending stream for Core processing
						success = StreamQueueManager.getInstance().produceSubmitMessage(msg);
					} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(type)
							|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(type)) {
						// Report types (DR feedback) go to results stream for Core processing
						success = StreamQueueManager.getInstance().produceResult(msg);
					} else {
						log.warn(msg, "Unknown message type {} in MQM, attempting default result stream", type);
						success = StreamQueueManager.getInstance().produceResult(msg);
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
