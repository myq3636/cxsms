/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * To create a ThreadPoolExecutor instance that scales the number of threads up and down, 
 * the BlockingQueue needs to have a reference to the ThreadPoolExecutor.
 * Use to change thread pool behavior
 * a)	If the number of threads is less than the corePoolSize, create a new Thread to run a new task.
 * b)	If more than corePoolSize but less than maximumPoolSize threads are running, prefers adding a new thread over queuing, 
 *      and using an idle thread over adding a new thread 
 * c)	If the number of threads is equal (or greater than) the maximumPoolSize, put the task into the queue.
 * d)	If the queue is full, and the number of threads is greater than or equal to maxPoolSize, reject the task.
 * <p/>
 * @author bensonchen
 * @version 1.0.0
 * @param <E>
 */
public class DynamicBlockingQueue<E> extends A2PLinkedBlockingQueue<E> {

	private static final long serialVersionUID = 1L;
	
	/**
	 * The executor this Queue belongs to
	 */
	protected ThreadPoolExecutor executor;
	
	protected String queueName;

	public DynamicBlockingQueue() {
		super();
	}

	/**
	 * Creates a TaskQueue with the given (fixed) capacity.
	 * 
	 @param capacity the capacity of this queue.
	 */
	public DynamicBlockingQueue(int capacity) {
		super(capacity);
	}

	/**
	 * Sets the executor this queue belongs to.
	 */
	public void setThreadPoolExecutor(ThreadPoolExecutor executor) {
		this.executor = executor;
	}

	/**
	 * the BlockingQueue should return false to indicate that 
	 * a new thread should be added to the pool.
	 */
	@Override
	public boolean offer(E o) {
		if (executor == null) {
			return super.offer(o);
		}
		
		if (super.size() < executor.getCorePoolSize() 
				|| executor.getPoolSize() >= executor.getMaximumPoolSize()) {
			return super.offer(o);
		} 

		return false;
	}

	public ThreadPoolExecutor getExecutor() {
		return executor;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

}