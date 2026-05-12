package com.king.gmms.util;

import java.util.Map;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBackupWriter;
import com.king.message.gmms.MessageStoreManager;

public class BufferMonitorWithSafeExit extends BufferMonitor implements
		LifecycleListener, SignalHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(BufferMonitorWithSafeExit.class);
	protected LifecycleSupport lifecycle;
	private GmmsUtility gmmsUtility = null;
	private MessageStoreManager mqm = null;

	public BufferMonitorWithSafeExit(int bufferCapacity) {
		super(bufferCapacity);
		lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
		lifecycle.addListener(Event.TYPE_SHUTDOWN, this, 1);
		gmmsUtility = GmmsUtility.getInstance();
		mqm = gmmsUtility.getMessageStoreManager();
	}

	public BufferMonitorWithSafeExit() {
		this(0);
	}

	public int OnEvent(Event event) {
		log.trace("Event Received. Type: {}", event.getEventType());
		if (event.getEventType() == Event.TYPE_SHUTDOWN) {
			isAlow.set(false);
			backupMessage();
		}
		return 1;
	}

	private void backupMessage() {
		MessageBackupWriter writer = MessageBackupWriter.getInstance();
		Thread.currentThread().interrupt();
		try {
			// 已完全去除所有 readLock() 阻塞并改用值抽取（避免 keySet 双重寻找）
			backupBufferValues(oneBuffer, writer);
			backupBufferValues(twoBuffer, writer);
			backupBufferValues(threeBuffer, writer);
			backupBufferValues(fourBuffer, writer);
		} catch (Exception e) {
			log.warn(e, e);
		}
	}

	private void backupBufferValues(Map<Object, GmmsMessage> buffer, MessageBackupWriter writer) {
		if (buffer == null) return;
		for (GmmsMessage msg : buffer.values()) {
			if (msg == null) continue;
			if(log.isInfoEnabled()){
				log.info(msg, "{} backup message!", bufferName);
			}
			writer.backupMessage(msg);
		}
	}

	public void writeAllToDB() {
		try {
			writeToDB(oneBuffer);
			writeToDB(twoBuffer);
			writeToDB(threeBuffer);
			writeToDB(fourBuffer);
		} catch (Exception e) {
			log.warn(e, e);
		}
	}

	private void writeToDB(Map<Object, GmmsMessage> buffer) {
		if (buffer == null) return;
		// 直接提取 values，免去耗时的 key 查找
		for (GmmsMessage message : buffer.values()) {
			if (message == null) continue;
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType())
					|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(message.getMessageType())) {
				
				message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				mqm.handleOutSubmitRes(message);
				
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(message.getMessageType())) {
				
				message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
				mqm.handleInDeliveryReportRes(message);
				
			}
		}// end of for loop
	}

	/**
	 * @Override handle kill signal
	 */
	public void handle(Signal signal) {
		if (SolarisSignal.needHandle(signal)) {
			isAlow.set(false);
			backupMessage();
		}
	}
	
	public int getFullSize(){
		// 使用基类极速 $O(1)$ 取值器，消除多重 size() 慢速累加和阻塞
		return super.size();
	}
}
