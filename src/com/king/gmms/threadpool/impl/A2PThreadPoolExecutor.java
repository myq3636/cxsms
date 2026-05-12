/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;

/**
 * Thread pool executor
 *
 */
public class A2PThreadPoolExecutor extends ThreadPoolExecutor {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(A2PThreadPoolExecutor.class);
	
	private boolean needSafeExit = false;

    public A2PThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                        BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public A2PThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                        BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public A2PThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                        BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public A2PThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
                                        BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }
    
    /**
     * A handler for rejected tasks that runs the rejected task
     * directly in the calling thread of the <tt>execute</tt> method,
     * unless the executor has been shut down, in which case 
     * throws RejectedExecutionException.
     */
    public static class A2PCallerRunsPolicy implements RejectedExecutionHandler {
        /**
         * Creates a <tt>CallerRunsPolicy</tt>.
         */
        public A2PCallerRunsPolicy() { }

        /**
         * Executes task r in the caller's thread, unless the executor
         * has been shut down, in which case throws RejectedExecutionException.
         * @param r the runnable task requested to be executed
         * @param e the executor attempting to execute this task
         */
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            if (!e.isShutdown()) {
            	if (log.isInfoEnabled()) {
            		log.info(e.toString() + " run A2PCallerRunsPolicy");
            	}
                r.run();
            } else {
            	throw new RejectedExecutionException();
            }
        }
    }


    @Override
    public String toString() {
        // the thread factory often have more precise details what the thread pool is used for
        if (getThreadFactory() instanceof A2PThreadFactory) {
            String name = ((A2PThreadFactory) getThreadFactory()).getName();
            return "ThreadPool: " + name;
        } else {
            return super.toString();
        }
    }

	public boolean isNeedSafeExit() {
		return needSafeExit;
	}

	public interface BatchProcessor {
		void process(java.util.List<redis.clients.jedis.StreamEntry> batch);
	}

	/**
	 * Redis Stream V4.0 批量任务提交适配。
	 * 将“阻塞等待单条消费”改写为“批量拉取多条”。
	 * 能够有效减少线程上下文切换开销。
	 */
	public void executeBatch(final java.util.List<redis.clients.jedis.StreamEntry> batch, final BatchProcessor processor) {
		if (batch == null || batch.isEmpty() || processor == null) {
			return;
		}
		this.execute(new Runnable() {
			@Override
			public void run() {
				processor.process(batch);
			}
		});
	}

	public void setNeedSafeExit(boolean needSafeExit) {
		this.needSafeExit = needSafeExit;
	}

}
