/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.threadpool.ThreadPoolFactory;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.util.QueueTimeoutInterface;

/**
 * Factory for thread pools that uses the JDK {@link Executors} for creating the thread pools.
 */
public class DefaultThreadPoolFactory implements ThreadPoolFactory {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(DefaultThreadPoolFactory.class);

    @Override
    public ExecutorService newThreadPool(ThreadPoolProfile profile, ThreadFactory factory) {
        return newThreadPool(profile.getPoolSize(), 
                             profile.getMaxPoolSize(), 
                             profile.getKeepAliveTime(),
                             profile.getTimeUnit(),
                             profile.getMaxQueueSize(), 
                             profile.getRejectedPolicy(),
                             profile.getNeedSafeExit(),
                             factory, profile.getWorkQueueName());
    }

    public ExecutorService newThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, TimeUnit timeUnit,
                                         int maxQueueSize, RejectedExecutionHandler rejectedExecutionHandler, boolean needSafeExit,
                                         ThreadFactory threadFactory, String queueName) throws IllegalArgumentException {

        // the core pool size must be higher than 0
        if (corePoolSize < 1) {
            log.warn("CorePoolSize must be >= 1, was {}, set to 1", corePoolSize);
        	corePoolSize = 1;
        }

        // validate max >= core
        if (maxPoolSize < corePoolSize) {
            log.warn("maxPoolSize must be >= corePoolSize, was {}, set to corePoolSize {}", maxPoolSize, corePoolSize);
        	maxPoolSize = corePoolSize;
        }

        DynamicBlockingQueue<Runnable> workQueue = new DynamicBlockingQueue<Runnable>(maxQueueSize);
        workQueue.setQueueName(queueName);
        
        A2PThreadPoolExecutor answer = new A2PThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, timeUnit, workQueue);
        workQueue.setThreadPoolExecutor(answer);
        answer.setThreadFactory(threadFactory);
        answer.setRejectedExecutionHandler(rejectedExecutionHandler);
        
        if (needSafeExit) {
        	answer.setNeedSafeExit(needSafeExit);
        }
       
        return answer;
    }

	/** 
	 * @param profile
	 * @param threadFactory
	 * @return
	 * @see com.king.gmms.threadpool.ThreadPoolFactory#newScheduledThreadPool(com.king.gmms.threadpool.ThreadPoolProfile, java.util.concurrent.ThreadFactory)
	 */
	@Override
	public ScheduledExecutorService newScheduledThreadPool(ThreadPoolProfile profile, ThreadFactory threadFactory) {
        RejectedExecutionHandler rejectedExecutionHandler = profile.getRejectedPolicy();

        ScheduledThreadPoolExecutor answer = new ScheduledThreadPoolExecutor(profile.getPoolSize(), threadFactory, rejectedExecutionHandler);

        // need to wrap the thread pool in a sized to guard against the problem that the
        // JDK created thread pool has an unbounded queue (see class javadoc), which mean
        // we could potentially keep adding tasks, and run out of memory.
        if (profile.getMaxPoolSize() > 0) {
            return new SizedScheduledExecutorService(answer, profile.getMaxQueueSize());
        } else {
            return answer;
        }
	}

	/** 
	 * @param profile
	 * @param threadFactory
	 * @param timeoutHandler
	 * @param timeoutMillis
	 * @return
	 * @see com.king.gmms.threadpool.ThreadPoolFactory#newThreadPool(com.king.gmms.threadpool.ThreadPoolProfile, java.util.concurrent.ThreadFactory, com.king.gmms.util.QueueTimeoutInterface, long)
	 */
	@Override
	public ExecutorService newThreadPool(ThreadPoolProfile profile, ThreadFactory threadFactory, QueueTimeoutInterface timeoutHandler, long timeoutMillis) {
		return null;
	}

}
