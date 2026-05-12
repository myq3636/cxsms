/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.UUID;

import com.king.framework.SystemLogger;
import com.king.gmms.util.QueueTimeoutInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * Use to change thread pool behavior
 * And with task expire mechanism
 * @author bensonchen
 * @version 1.0.0
 * @param <E>
 * @see com.king.gmms.threadpool.impl.DynamicBlockingQueue
 */
public class ExpiredDynamicBlockingQueue<E> extends DynamicBlockingQueue<E> {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(ExpiredDynamicBlockingQueue.class);
	
	private static final long serialVersionUID = 1L;
	
	private UUID uuid = UUID.randomUUID();

	private QueueTimeoutInterface listener;

	private long timeoutMillis;
	
	public ExpiredDynamicBlockingQueue() {
		super();
	}

	/**
	 * Creates a TaskQueue with the given (fixed) capacity.
	 * 
	 @param capacity the capacity of this queue.
	 */
	public ExpiredDynamicBlockingQueue(int capacity) {
		super(capacity);
	}

	public void setListener(QueueTimeoutInterface listener) {
		this.listener = listener;
	}

	public QueueTimeoutInterface getListener() {
		return listener;
	}
	
	public void handleTimeoutMessage(Object msg) {
		listener.timeout(msg);
	}

	public long getTimeoutMillis() {
		return timeoutMillis;
	}

	public void setTimeoutMillis(long timeoutMillis) {
		this.timeoutMillis = timeoutMillis;
	}

	public boolean isTimeout(long timeStamp) {
		return System.currentTimeMillis() - timeStamp > (long) timeoutMillis;
	}

	public UUID getUuid() {
		return uuid;
	}
	
	public void procesExpiredMsg(GmmsMessage msg) {
		if (msg != null) {
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType()) || GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())
					|| GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType()) || GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msg.getMessageType())) {
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType()) || GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())) {
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(msg.getMessageType())) {
				msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
			} else { // invalid message type
				log.warn(msg, "Unknown Message Type! when update the fail status" + msg.getMessageType());
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
				return;
			}
			listener.timeout(msg);
		}
	}

}