package com.king.gmms.throttle;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;

/**
 * <p>
 * Title: ThrottlingControl
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p>
 * Company: King
 * </p>
 * 
 * @version 6.1
 * @author: Neal
 */
public class ThrottlingControl {
	private static SystemLogger log = SystemLogger.getSystemLogger(ThrottlingControl.class);
	private AtomicInteger moduleIncomingMsgCount = new AtomicInteger();
	
    private boolean canHandover = false;
	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();;
	private A2PCustomerManager custManager = null;
	
	/**
	 * After apply quota from SYS for some time, 
	 * customer applied incoming throttle will expire.
	 * due to ResetCustIncomingThresholdInterval
	 * </p> key: ssid
	 * </p> value: new quota start time
	 */
	private ConcurrentMap<Integer, Long> expireInThottleQuotaMap;

	private static ThrottlingControl instance = new ThrottlingControl();

	/** the customers' incoming counts, in memory and synced to Redis every ThrottleInSyncIntervalMs */
	private final ClusterSecondCounter incomingCounter = new ClusterSecondCounter("throttle:in:");

	/**
	 * constructor
	 */
	private ThrottlingControl() {

		
		expireInThottleQuotaMap = new ConcurrentHashMap<Integer, Long>();
		
		canHandover = gmmsUtility.isDBHandover();
		custManager = gmmsUtility.getCustomerManager();
	}

	/**
	 * instance of ThrottlingControl
	 * 
	 * @return ThrottlingControl
	 */
	public static ThrottlingControl getInstance() {
		return instance;
	}

	/**
	 * judge if the message is permit by throttling
	 * 
	 * @param ssid
	 *            int
	 * @return boolean
	 */
	public boolean isAllowedToReceive(int ssid) {
		try {
			A2PCustomerInfo server = custManager.getCustomerBySSID(ssid);
			// 收侧 0=不限(与发侧/核心处理一致)：用原始配置值判定，不再回退 DefaultCustIncomingThreshold。
			int throttlingNum = server.getIncomingThrottlingNum();
			if (throttlingNum <= 0) {
				return true;
			}
			
			Long currentCount = 1L;
			if (ClusterSecondCounter.syncIntervalMs() > 0) {
				// counted in memory, all nodes' counts synced every ThrottleInSyncIntervalMs (docs/Redis消息操作次数统计.md)
				currentCount = incomingCounter.acquire(ssid, throttlingNum, System.currentTimeMillis());
			} else if (gmmsUtility.getRedisClient() != null && gmmsUtility.getRedisClient().getStateRedis() != null) {
				long currentSecond = System.currentTimeMillis() / 1000;
				String limitKey = "throttle:in:" + ssid + ":" + currentSecond;
				// H-6: 原子 INCR+EXPIRE，避免计数 key 丢失 TTL 导致该 ssid:秒 桶永久限流。
				currentCount = gmmsUtility.getRedisClient().getStateRedis().incrWithExpire(limitKey, 2);
			}

			// not pass throttling control
			if (currentCount != null && currentCount > throttlingNum) {
				A2PCustomerInfo cust = server;
				// apply quota
				
				
				return false;
			} else {
				// counter, just count the msg which will be delivered
				increaseModuleIncomingMsgCount();
			}
		} catch (Exception e) {
			log.warn(e, e);
		}

		return true;
	}
	

	public boolean isAllowedToSend(int ssid) {
		try {
			A2PCustomerInfo server = custManager.getCustomerBySSID(ssid);
			int throttlingNum = server.getOutgoingThrottlingNum();
			if (throttlingNum <= 0) {
				return true;
			}
			
			long currentSecond = System.currentTimeMillis() / 1000;
			String limitKey = "throttle:out:" + ssid + ":" + currentSecond;
			Long currentCount = 1L;
			if (gmmsUtility.getRedisClient() != null && gmmsUtility.getRedisClient().getStateRedis() != null) {
				// H-6: 原子 INCR+EXPIRE，避免计数 key 丢失 TTL 导致该 ssid:秒 桶永久限流。
				currentCount = gmmsUtility.getRedisClient().getStateRedis().incrWithExpire(limitKey, 2);
			}

			// not pass throttling control
			if (currentCount != null && currentCount > throttlingNum) {
				return false;
			}
		} catch (Exception e) {
			log.warn(e, e);
		}
		return true;
	}
	
	public boolean isAllowedToHander(int ssid) {
		try {
			A2PCustomerInfo server = custManager.getCustomerBySSID(ssid);
			int throttlingNum = server.getCoreProcessorThrottlingNum();
			if (throttlingNum <= 0) {
				return true;
			}
			
			long currentSecond = System.currentTimeMillis() / 1000;
			String limitKey = "throttle:core:" + ssid + ":" + currentSecond;
			Long currentCount = 1L;
			if (gmmsUtility.getRedisClient() != null && gmmsUtility.getRedisClient().getStateRedis() != null) {
				// H-6: 原子 INCR+EXPIRE，避免计数 key 丢失 TTL 导致该 ssid:秒 桶永久限流。
				currentCount = gmmsUtility.getRedisClient().getStateRedis().incrWithExpire(limitKey, 2);
			}

			// not pass throttling control
			if (currentCount != null && currentCount > throttlingNum) {
				return false;
			}
		} catch (Exception e) {
			log.warn(e, e);
		}
		return true;
	}

	public void increaseModuleIncomingMsgCount() {
		int count = moduleIncomingMsgCount.incrementAndGet();
		if (log.isTraceEnabled()) {
			log.trace("moduleIncomingMsgCount={}", count);
		}
	}

	public int getModuleIncomingMsgCount() {
		return moduleIncomingMsgCount.get();
	}
	
	public void resetModuleIncomingMsgCount() {
		moduleIncomingMsgCount.set(0);
	}

	public ConcurrentMap<Integer, Long> getExpireThottleQuotaMap() {
		return expireInThottleQuotaMap;
	}


	
	/**
	 * clearThrottlingControlCache
	 * @param ssid
	 */
	public void clearThrottlingControlCache(int ssid){
		expireInThottleQuotaMap.remove(ssid);
	}
}
