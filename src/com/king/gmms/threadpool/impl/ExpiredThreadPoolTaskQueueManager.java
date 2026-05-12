/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.ExpiredMsgManager;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;

/**
 * Expired check and process for Thread Pool Task
 * @author bensonchen
 * @version 1.0.0
 */
public class ExpiredThreadPoolTaskQueueManager implements ExpiredMsgManager{

	private static SystemLogger log = SystemLogger.getSystemLogger(ExpiredThreadPoolTaskQueueManager.class);

	private static ExpiredThreadPoolTaskQueueManager instance = new ExpiredThreadPoolTaskQueueManager();

	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	private ScheduledExecutorService scheduler = null;
	private ExecutorService expiredProcessThreadPool = null;

	private ConcurrentMap<String, ScheduledFuture<?>> checkTimeoutScheduledFutreMap = null;

	private ExpiredThreadPoolTaskQueueManager() {
		try {
			checkTimeoutScheduledFutreMap = new ConcurrentHashMap<String, ScheduledFuture<?>>();
			
			// scheduled thread pool for CheckTimeoutTask
			ExecutorServiceManager manager = gmmsUtility.getExecutorServiceManager();
			// scheduled thread pool only use corePoolSize, no min and max
			int schedulePoolSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MessageQueue.ScheduleProcessorNumber", "1").trim());
			ThreadPoolProfile scheduleProfile = new ThreadPoolProfileBuilder("ExpTaskQueueScheduleCheckPool").poolSize(schedulePoolSize).build();
			scheduler = manager.newScheduledThreadPool(this, "ExpTaskQueScheduleCheck", scheduleProfile);
			
			// thread pool for ExpiredMsgProcessCall
			int minPoolSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MessageQueue.MinExpiredMsgProcessorNumber", "1").trim());
			int maxPoolSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MessageQueue.MaxExpiredMsgProcessorNumber", "5").trim());
			ThreadPoolProfile profile = new ThreadPoolProfileBuilder("ExpiredMsgProcessPool").poolSize(minPoolSize).maxPoolSize(maxPoolSize).build();
			expiredProcessThreadPool = manager.newThreadPool(this, "ExpiredMsgProcess", profile);
		} catch (Exception e) {
			log.error(e, e);
		}
		
	}

	public static ExpiredThreadPoolTaskQueueManager getInstance() {
		return instance;
	}

	@Override
	public boolean register(Object obj) {
		try {
			ExpiredDynamicBlockingQueue<?> queue = (ExpiredDynamicBlockingQueue<?>)obj;
			if (log.isDebugEnabled()) {
				log.debug("{} register to ExpiredTaskQueueManager with uuid {}, timeout={}", queue.getQueueName(), queue.getUuid(), queue.getTimeoutMillis());
			}
			
			Runnable checkTimeoutTask = new CheckTimeoutTask(queue);
			long timeout = queue.getTimeoutMillis();
			final ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(checkTimeoutTask, timeout/3, timeout/3, TimeUnit.MILLISECONDS);
			checkTimeoutScheduledFutreMap.put(queue.getUuid().toString(), scheduledFuture);
		} catch (Exception e) {
			log.warn(e, e);
			return false;
		}
		
		return true;
	}

	/**
	 * @return
	 */
	@Override
	public boolean deregister(Object obj) { 
		try {
			ExpiredDynamicBlockingQueue<?> queue = (ExpiredDynamicBlockingQueue<?>)obj;
			
			if (log.isDebugEnabled()) {
				log.debug("{} deregister to ExpiredTaskQueueManager with uuid {}", queue.getQueueName(), queue.getUuid());
			}
			
			ScheduledFuture<?> scheduledFutre = checkTimeoutScheduledFutreMap.remove(queue.getUuid().toString());
			if (scheduledFutre != null) {
				scheduledFutre.cancel(true);
			}
			
		} catch (Exception e) {
			log.error(e, e);
			return false;
		}
		
		return true;
	}

	private final class CheckTimeoutTask implements Runnable {
		private final ExpiredDynamicBlockingQueue<?> queue;

		public CheckTimeoutTask(ExpiredDynamicBlockingQueue<?> queue) {
			this.queue = queue;
		}

		/**
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			while (queue.size() > 0) {
				
				if (log.isTraceEnabled()) {
					log.trace("{} queue currentSize={}, completedTaskCount={}", queue.getQueueName(), queue.size(), queue.getExecutor().getCompletedTaskCount());
				}
				
				final ReentrantLock takeLock = queue.getTakeLock();
				takeLock.lock();
				try {
					// get head element of queue
					Object temp = queue.peek();
					if (temp == null) {
						return;
					}
					if (temp instanceof RunnableMsgTask) {
						RunnableMsgTask task = (RunnableMsgTask) temp;
						if (queue.isTimeout(task.getTimeStamp())) {
							queue.getExecutor().remove(task);
							GmmsMessage message = task.getMsg();
							if (message != null) {
								if (log.isInfoEnabled()) {
									log.info(message, "The message is timeout and backup from expired message queue");
								}
								try {
									expiredProcessThreadPool.execute(new ExpiredMsgProcessCall(message, queue));
								} catch (Exception e) {
									if (log.isInfoEnabled()) {
										log.info("submit ExpiredMsgProcessCall exception: {}", e);
									}
								}
								
							}
							continue;
							
						} else {
							// since head of the queue is that element that has been on the queue the longest time
							// if head is not expired, can ignore the remainder check
							break;
						}
					} else {
						break;
					}
					
				} catch (Exception e) {
					log.warn("ExpiredThreadPoolTaskQueueManager run CheckTimeoutTask error", e);
				} finally {
					takeLock.unlock();
				}
			}
		}
	}
	

	private final class ExpiredMsgProcessCall implements Runnable {
		private final GmmsMessage msg;
		private final ExpiredDynamicBlockingQueue<?> queue;

		public ExpiredMsgProcessCall(GmmsMessage msg, ExpiredDynamicBlockingQueue<?> queue) {
			this.msg = msg;
			this.queue = queue;
		}

		@Override
		public void run() {
			queue.procesExpiredMsg(msg);
		}
	}

}
