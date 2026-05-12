/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public interface ExpiredMsgManager {
	public boolean register(Object obj);
	public boolean deregister(Object obj);

}
