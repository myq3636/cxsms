/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.threadpool.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.ExpiredMsgManager;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.gmms.util.BufferMonitor;

/**
 * Expired check and process for BufferMonitor
 * @author bensonchen
 * @version 1.0.0
 */
public class BufferMonitorManager implements ExpiredMsgManager {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(BufferMonitorManager.class);
	
	private static BufferMonitorManager instance = new BufferMonitorManager();
	
	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	ScheduledExecutorService scheduler = null;
	
	private ConcurrentMap<String, ScheduledFuture<?>> checkTimeoutScheduledFutreMap = null;
	private ConcurrentMap<String, RegisteredBufferMonitor> registeredMonitorMap = null;
	private volatile ScheduledFuture<?> globalScanFuture = null;
	private boolean globalScanEnabled = true;
	private long globalScanIntervalMs = 1000L;
	private long minCheckIntervalMs = 1000L;
	
	private BufferMonitorManager() {
		try {
			checkTimeoutScheduledFutreMap = new ConcurrentHashMap<String, ScheduledFuture<?>>();
			registeredMonitorMap = new ConcurrentHashMap<String, RegisteredBufferMonitor>();
			globalScanEnabled = Boolean.parseBoolean(gmmsUtility.getFullModuleTypeProperty(
					"BufferMonitor.GlobalScanEnable", "true").trim());
			globalScanIntervalMs = longProperty("BufferMonitor.GlobalScanIntervalMs", 1000L);
			minCheckIntervalMs = longProperty("BufferMonitor.MinCheckIntervalMs", 1000L);
			
			// scheduled thread pool for CheckTimeoutTask
			ExecutorServiceManager manager = gmmsUtility.getExecutorServiceManager();
			int poolSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("BufferMonitor.ScheduleProcessorNumber", "1").trim());
			// scheduled thread pool only use corePoolSize, no min and max
			ThreadPoolProfile profile = new ThreadPoolProfileBuilder("BufMonScheduleCheckPool").poolSize(poolSize).build();
	    	scheduler  = manager.newScheduledThreadPool(this, "BufMonScheduleCheck", profile);
		} catch (Exception e) {
			log.warn(e, e);
		}
	}
	
	public static BufferMonitorManager getInstance() {
		return instance;
	}
	
	/**
	 * 获取 Redis Stream 的积压长度 (代替本地队列的监控)
	 * @param streamKey 队列的 key 名称
	 * @return 积压长度，若失败返回 -1
	 */
	public long getStreamBacklogSize(String streamKey) {
		com.king.redis.RedisConnection redis = com.king.redis.RedisClient.getInstance().getSubmitMqRedis();
		if (redis != null) {
			try (redis.clients.jedis.Jedis jedis = redis.getJedis()) {
				return jedis.xlen(streamKey);
			} catch (Exception e) {
				log.error("Failed to fetch xlen for " + streamKey, e);
			}
		}
		return -1;
	}

	/** 
	 * @param queue
	 * @return
	 * @deprecated Redis 重构阶段一中降级对本地队列的监控，新架构下请使用 getStreamBacklogSize 监控 MQ 积压。
	 */
	@Override
	@Deprecated
	public boolean register(Object obj) {
		try {
			BufferMonitor bufferMonitor = (BufferMonitor)obj;
			
			if (log.isDebugEnabled()) {
				log.debug("{} register to BufferMonitorManager with uuid {}, timeout={}", bufferMonitor.getBufferName(), bufferMonitor.getUuid(), bufferMonitor.getTimeout(TimeUnit.MILLISECONDS));
			}
			
			if (globalScanEnabled) {
				long intervalMs = Math.max(bufferMonitor.getTimeout(TimeUnit.MILLISECONDS) / 2, minCheckIntervalMs);
				registeredMonitorMap.put(bufferMonitor.getUuid().toString(),
						new RegisteredBufferMonitor(bufferMonitor, intervalMs));
				ensureGlobalScannerStarted();
			} else {
				Runnable checkTimeoutTask = new CheckTimeoutTask(bufferMonitor);
				long delay = Math.max(bufferMonitor.getTimeout(TimeUnit.MILLISECONDS) / 2, minCheckIntervalMs);
				final ScheduledFuture<?>  scheduledFuture = scheduler.scheduleAtFixedRate(checkTimeoutTask, delay, delay, TimeUnit.MILLISECONDS);
		    	checkTimeoutScheduledFutreMap.put(bufferMonitor.getUuid().toString(), scheduledFuture);
			}
		} catch (Exception e) {
			log.error(e, e);
			return false;
		}
		return true;
	}
	
	/** 
	 * @param queue
	 * @return
	 * @deprecated Redis 重构阶段一中降级对本地队列的监控，新架构下请使用 getStreamBacklogSize 监控 MQ 积压。
	 */
	@Override
	@Deprecated
	public boolean deregister(Object obj) {
		try {
			BufferMonitor bufferMonitor = (BufferMonitor)obj;
			if (log.isDebugEnabled()) {
				log.debug("{} deregister to BufferMonitorManager with uuid {}", bufferMonitor.getBufferName(), bufferMonitor.getUuid());
			}
			
			if (registeredMonitorMap != null) {
				registeredMonitorMap.remove(bufferMonitor.getUuid().toString());
			}
			ScheduledFuture<?> scheduledFutre = checkTimeoutScheduledFutreMap.remove(bufferMonitor.getUuid().toString());
			if (scheduledFutre != null){
				scheduledFutre.cancel(true);
			}
			
		} catch (Exception e) {
			log.error(e, e);
			return false;
		}
		
		return true;
	}
	
	private long longProperty(String key, long defaultValue) {
		try {
			return Long.parseLong(gmmsUtility.getFullModuleTypeProperty(key, Long.toString(defaultValue)).trim());
		} catch (Exception e) {
			log.warn("Invalid property {}, use default {}", key, defaultValue, e);
			return defaultValue;
		}
	}
	
	private synchronized void ensureGlobalScannerStarted() {
		if (!globalScanEnabled || scheduler == null) {
			return;
		}
		if (globalScanFuture != null && !globalScanFuture.isCancelled() && !globalScanFuture.isDone()) {
			return;
		}
		long intervalMs = Math.max(globalScanIntervalMs, 100L);
		globalScanFuture = scheduler.scheduleWithFixedDelay(new GlobalCheckTimeoutTask(),
				intervalMs, intervalMs, TimeUnit.MILLISECONDS);
		log.info("BufferMonitor global scanner started. intervalMs={}, minCheckIntervalMs={}",
				intervalMs, minCheckIntervalMs);
	}
	
	private final class CheckTimeoutTask implements Runnable {
		private final BufferMonitor bufferMonitor;
		public CheckTimeoutTask(BufferMonitor bufferMonitor){
			this.bufferMonitor = bufferMonitor;
		}

		/** 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			bufferMonitor.checkTimeoutProcess();
		}
	}
	
	private final class GlobalCheckTimeoutTask implements Runnable {
		@Override
		public void run() {
			if (registeredMonitorMap == null || registeredMonitorMap.isEmpty()) {
				return;
			}
			long now = System.currentTimeMillis();
			for (RegisteredBufferMonitor entry : registeredMonitorMap.values()) {
				if (entry == null || !entry.shouldCheck(now)) {
					continue;
				}
				try {
					entry.markChecked(now);
					entry.bufferMonitor.checkTimeoutProcess();
				} catch (Exception e) {
					log.error("BufferMonitor global check failed. bufferName="
							+ entry.bufferMonitor.getBufferName() + ", uuid="
							+ entry.bufferMonitor.getUuid(), e);
				}
			}
		}
	}
	
	private static final class RegisteredBufferMonitor {
		private final BufferMonitor bufferMonitor;
		private final long intervalMs;
		private volatile long nextCheckAtMs;
		
		private RegisteredBufferMonitor(BufferMonitor bufferMonitor, long intervalMs) {
			this.bufferMonitor = bufferMonitor;
			this.intervalMs = intervalMs;
			this.nextCheckAtMs = System.currentTimeMillis() + intervalMs;
		}
		
		private boolean shouldCheck(long now) {
			return now >= nextCheckAtMs;
		}
		
		private void markChecked(long now) {
			nextCheckAtMs = now + intervalMs;
		}
	}
}
