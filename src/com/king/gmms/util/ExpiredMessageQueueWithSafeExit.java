package com.king.gmms.util;

import java.util.concurrent.TimeUnit;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageBackupWriter;

public class ExpiredMessageQueueWithSafeExit extends ExpiredMessageQueue implements LifecycleListener,SignalHandler {
	private static SystemLogger log = SystemLogger.getSystemLogger(ExpiredMessageQueueWithSafeExit.class);
	protected LifecycleSupport lifecycle;
	
	public ExpiredMessageQueueWithSafeExit(int limit, int timeout, boolean isServer,String name){
		super(limit,timeout,isServer,name);
		lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
		lifecycle.addListener(Event.TYPE_SHUTDOWN, this, 1);
	}
	
	public ExpiredMessageQueueWithSafeExit(int timeout, boolean isServer, String name) {
		this(100000, timeout, isServer, name);
	}

	public ExpiredMessageQueueWithSafeExit(boolean isServer, String name) {
		this(100000, -1, isServer, name);
	}
	
	public int OnEvent(Event event) {
		if (event.getEventType() == Event.TYPE_SHUTDOWN) {
			isAlow.set(false);
			sendbackMessage();
		}
		return 1;
	}
	

	protected void sendbackMessage() {
		MessageBackupWriter writer = MessageBackupWriter.getInstance();
		MessagePair messagePair = messageQueue.poll();
		
		while (messagePair != null) {
			writer.backupMessage((GmmsMessage)messagePair.getMessage());
			messagePair = messageQueue.poll();
		}
		
		messageQueue.clear();
		
		try{
			GmmsMessage message = backupMessageQueue.poll(200L, TimeUnit.MILLISECONDS);
			while (message != null) {
				writer.backupMessage(message);
				message = backupMessageQueue.poll(200L, TimeUnit.MILLISECONDS);
			}
			
			backupMessageQueue.clear();
		}catch(Exception e){
			log.warn("Failed to backup message");
		}
	}
	/**
	 * @Override
	 * handle kill signal 
	 */
	public void handle(Signal signal) {
		if(SolarisSignal.needHandle(signal)){
			isAlow.set(false);
			sendbackMessage();
		}
	}
	
}
