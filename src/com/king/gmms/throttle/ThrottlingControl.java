package com.king.gmms.throttle;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.SystemSessionFactory;
import com.king.gmms.ha.systemmanagement.pdu.ApplyInThrottleQuota;

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

	private ConcurrentMap<Integer, ThrottlingAlertMark> incomingThrottlingAlertCache;
	private ConcurrentMap<Integer, ThrottlingAlertMark> outgoingThrottlingAlertCache;
	
	private AtomicInteger moduleIncomingMsgCount = new AtomicInteger();
	
	private boolean isEnableSysMgt = false;
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

	/**
	 * constructor
	 */
	private ThrottlingControl() {

		incomingThrottlingAlertCache = new ConcurrentHashMap<Integer, ThrottlingAlertMark>();
		outgoingThrottlingAlertCache = new ConcurrentHashMap<Integer, ThrottlingAlertMark>();
		
		expireInThottleQuotaMap = new ConcurrentHashMap<Integer, Long>();
		
		isEnableSysMgt = gmmsUtility.isSystemManageEnable();
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
			int throttlingNum = server.getConfigedIncomingThrottlingNum();
			if (throttlingNum <= 0) {
				return true;
			}
			
			long currentSecond = System.currentTimeMillis() / 1000;
			String limitKey = "throttle:in:" + ssid + ":" + currentSecond;
			Long currentCount = 1L;
			if (gmmsUtility.getRedisClient() != null && gmmsUtility.getRedisClient().getStateRedis() != null) {
				currentCount = gmmsUtility.getRedisClient().getStateRedis().incrString(limitKey);
				if (currentCount != null && currentCount == 1L) {
					gmmsUtility.getRedisClient().getStateRedis().setExpired(limitKey, 2);
				}
			}

			// not pass throttling control
			if (currentCount != null && currentCount > throttlingNum) {
				A2PCustomerInfo cust = server;
				// apply quota
				if ((canHandover || isEnableSysMgt) && cust.isApplyInThrottleFlag()) {
					// set flag, avoid apply msg flood
					cust.setApplyInThrottleFlag(false);
					
					int currentThrottlingNum = currentCount.intValue();
					int maxThrottlingNum = gmmsUtility.getMaxCustIncomingThresholdMagnification() * cust.getConfigedIncomingThrottlingNum();
					int sysIncomingThreshold = gmmsUtility.getSystemIncomingThreshold();
					if (currentThrottlingNum < sysIncomingThreshold && currentThrottlingNum < maxThrottlingNum) {
						try {
							boolean isSucessSend = applyInThrottleQuata(ssid);
							if (!isSucessSend) {
								// reset flag, enable to re-apply if exception
								cust.setApplyInThrottleFlag(true);
							}
						} catch(Exception e) {
							log.warn(e, e);
							// reset flag, enable to re-apply if exception
							cust.setApplyInThrottleFlag(true);
						}
					}
				}
				
				// process alert mail
				ThrottlingAlertMark throttlingAlertMark = incomingThrottlingAlertCache.get(ssid);
				if (throttlingAlertMark == null) {
					incomingThrottlingAlertCache.putIfAbsent(ssid, new ThrottlingAlertMark());
					throttlingAlertMark = incomingThrottlingAlertCache.get(ssid);
				} 
				throttlingAlertMark.processAlertMail(ssid, true, System.currentTimeMillis(), throttlingNum);
				
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
	
	/**
	 * customer apply incoming throttling quota
	 * </p> from protocol module to SYS
	 * @param cust
	 * @param currentThrottlingNum
	 * @throws IOException 
	 */
	private boolean applyInThrottleQuata(int ssid) throws IOException {
		SystemSession systemSession = SystemSessionFactory.getInstance().getSystemSessionForFunction();
		if(systemSession == null){
			return false;
		}
		ApplyInThrottleQuota message = new ApplyInThrottleQuota();
		message.setSsid(ssid);
		return systemSession.send(message);
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
				currentCount = gmmsUtility.getRedisClient().getStateRedis().incrString(limitKey);
				if (currentCount != null && currentCount == 1L) {
					gmmsUtility.getRedisClient().getStateRedis().setExpired(limitKey, 2);
				}
			}

			// not pass throttling control, process alert mail
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
				currentCount = gmmsUtility.getRedisClient().getStateRedis().incrString(limitKey);
				if (currentCount != null && currentCount == 1L) {
					gmmsUtility.getRedisClient().getStateRedis().setExpired(limitKey, 2);
				}
			}

			// not pass throttling control, process alert mail
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
		incomingThrottlingAlertCache.remove(ssid);
		outgoingThrottlingAlertCache.remove(ssid);
		expireInThottleQuotaMap.remove(ssid);
	}
}
