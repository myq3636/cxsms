/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool;

import java.util.concurrent.ExecutorService;

import com.king.gmms.util.QueueTimeoutInterface;

/**
 * abstract class for CoreEninge handlers
 * @author bensonchen
 * @version 1.0.0
 */
public abstract class ProcessorHandler implements QueueTimeoutInterface {
	
	protected ExecutorService handlerThreadPool;

}
