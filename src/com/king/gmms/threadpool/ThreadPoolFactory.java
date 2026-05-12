/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.king.gmms.util.QueueTimeoutInterface;

/**
 * Creates ExecutorService and ScheduledExecutorService objects that work with a thread pool for a given ThreadPoolProfile and ThreadFactory.
 * 
 * This interface allows to customize the creation of these objects to adapt camel for application servers and other environments where thread pools
 * should not be created with the jdk methods
 */
public interface ThreadPoolFactory {
    
    /**
     * Create a thread pool using the given thread pool profile
     * 
     * @param profile parameters of the thread pool
     * @param threadFactory factory for creating threads
     * @return the created thread pool
     */
    ExecutorService newThreadPool(ThreadPoolProfile profile, ThreadFactory threadFactory);
    
    /**
     * Create a thread pool with task expired mechanism
     * @param profile
     * @param threadFactory
     * @param timeoutHandler
     * @param timeoutMillis
     * @return
     */
    ExecutorService newThreadPool(ThreadPoolProfile profile, ThreadFactory threadFactory, QueueTimeoutInterface timeoutHandler, long timeoutMillis);
    
    /**
     * Create a scheduled thread pool using the given thread pool profile
     * @param profile parameters of the thread pool
     * @param threadFactory factory for creating threads
     * @return the created thread pool
     */
    ScheduledExecutorService newScheduledThreadPool(ThreadPoolProfile profile, ThreadFactory threadFactory);
}