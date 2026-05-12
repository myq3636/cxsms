/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.concurrent.ThreadFactory;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;

/**
 * Thread factory which creates threads supporting a naming pattern.
 */
public final class A2PThreadFactory implements ThreadFactory {
	private static SystemLogger log = SystemLogger.getSystemLogger(A2PThreadFactory.class);

    private String name;

    public A2PThreadFactory(String name) {
        this.name = name;
    }

    public Thread newThread(Runnable runnable) {
        String threadName = ThreadHelper.resolveThreadName(name);
        Thread answer = new Thread(A2PThreadGroup.getInstance(), runnable, threadName);
        if (answer.isDaemon()) {
        	answer.setDaemon(false);
        }

        if (log.isDebugEnabled()) {
        	log.debug("Create thread[{}]", threadName);
        }
        
        return answer;
    }

    public String getName() {
        return name;
    }

    public String toString() {
        return "A2PThreadFactory[" + name + "]";
    }

	public void setName(String name) {
		this.name = name;
	}
}