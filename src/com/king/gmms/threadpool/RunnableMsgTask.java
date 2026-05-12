/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool;

import com.king.message.gmms.GmmsMessage;

/**
 * Wrapper for thread pool task of GmmsMessage
 * @author bensonchen
 * @version 1.0.0
 */
public abstract class RunnableMsgTask implements Runnable {
	
	protected long timeStamp = System.currentTimeMillis();
    protected GmmsMessage message = null;
	public long getTimeStamp() {
		return timeStamp;
	}
	public GmmsMessage getMsg() {
		return message;
	}
    
}
