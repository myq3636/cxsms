/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import com.king.gmms.util.QueueTimeoutInterface;


/**
 * Strategy to create thread pools.
 * This manager has fine grained methods for creating various thread pools, however custom strategies
 * do not have to exactly create those kind of pools. Feel free to return a shared or different kind of pool.
 * <p/>
 * If you use the <tt>newXXX</tt> methods to create thread pools, then GmmsUtility will by default take care of
 * shutting down those created pools when A2P is shutting down.
 * <p/>
 * For more information about shutting down thread pools see the {@link #shutdown(java.util.concurrent.ExecutorService)}
 * and {@link #shutdownNow(java.util.concurrent.ExecutorService)}, and {@link #getShutdownAwaitTermination()} methods.
 * Notice the details about using a graceful shutdown at fist, and then falling back to aggressive shutdown in case
 * of await termination timeout occurred.
 *
 * @see ThreadPoolFactory
 */
public interface ExecutorServiceManager {

    /**
     * Creates a new thread pool using the default thread pool profile.
     *
     * @param source the source object, usually it should be <tt>this</tt> passed in as parameter
     * @param name   name which is appended to the thread name
     * @return the created thread pool
     */
    ExecutorService newDefaultThreadPool(Object source, String name);

    /**
     * Creates a new scheduled thread pool using the default thread pool profile.
     *
     * @param source the source object, usually it should be <tt>this</tt> passed in as parameter
     * @param name   name which is appended to the thread name
     * @return the created thread pool
     */
    ScheduledExecutorService newDefaultScheduledThreadPool(Object source, String name);

    /**
     * Creates a new thread pool using the given profile
     *
     * @param source   the source object, usually it should be <tt>this</tt> passed in as parameter
     * @param name     name which is appended to the thread name
     * @param profile the profile with the thread pool settings to use
     * @return the created thread pool
     */
    ExecutorService newThreadPool(Object source, String name, ThreadPoolProfile profile);

    /**
     * Creates a new scheduled thread pool using a profile
     *
     * @param source      the source object, usually it should be <tt>this</tt> passed in as parameter
     * @param name        name which is appended to the thread name
     * @param profile     the profile with the thread pool settings to use
     * @return created thread pool
     */
    ScheduledExecutorService newScheduledThreadPool(Object source, String name, ThreadPoolProfile profile);

    /**
     * Creates a default expired thread pool
     * @param source      the source object, usually it should be <tt>this</tt> passed in as parameter
     * @param name        name which is appended to the thread name
     * @param timeoutHandler
     * @param timeoutMillis
     * @return created thread pool
     */
    ExecutorService newDefaultExpiredThreadPool(Object source, String name, QueueTimeoutInterface timeoutHandler, long timeoutMillis);
	
    /**
     * 
     * @param source      the source object, usually it should be <tt>this</tt> passed in as parameter
     * @param name        name which is appended to the thread name
     * @param profile     the profile with the thread pool settings to use
     * @param timeoutHandler
     * @param timeoutMillis
     * @return created thread pool
     */
	ExecutorService newExpiredThreadPool(Object source, String name, ThreadPoolProfile profile, QueueTimeoutInterface timeoutHandler, long timeoutMillis);

    /**
     * Shutdown the given executor service graceful at first, and then aggressively
     * if the await termination timeout was hit.
     * <p/>
     * Will try to perform an orderly shutdown by giving the running threads
     * time to complete tasks, before going more aggressively by doing a
     * {@link #shutdownNow(java.util.concurrent.ExecutorService)} which
     * forces a shutdown. 
     *
     * @param executorService the executor service to shutdown
     * @see java.util.concurrent.ExecutorService#shutdown()
     */
    void shutdown(ExecutorService executorService);

    /**
     * Shutdown all thread pools which created by ExecutorServiceManager
	 * @throws Exception
	 */
	void shutdownAll() throws Exception;

	/**
	 * Update thread pool profile after pool created
	 * Used in receiver thread pool etc which need update after connection established.
	 * @param pool
	 * @param profile
	 * @return
	 */
	boolean updateThreadPoolProfile(ThreadPoolExecutor pool, ThreadPoolProfile profile, String name);

	/**
	 * @param source
	 * @param name
	 * @param poolSize
	 * @return
	 */
	ExecutorService newFixedThreadPool(Object source, String name, int poolSize);

}
