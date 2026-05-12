/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.throttle;

import java.util.concurrent.ConcurrentMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.util.AbstractTimer;

/**
 * Protocol module reset customer dynamicIncomingThrottlingNum.
 * After DynamicCustIncomingThresholdExipreTime, 
 * the customer incoming dynamic throttle will be reset.
 * @author bensonchen
 * @version 1.0.0
 */
public class ResetDynamicCustInThresholdTimer extends AbstractTimer {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(ResetDynamicCustInThresholdTimer.class);

	private long dynamicCustInThresholdExipreTime;
	private GmmsUtility gmmsUtility;
	private A2PCustomerManager custManager;
	
	private ThrottlingControl throttlingControl = ThrottlingControl.getInstance();
	
	/**
	 * @param wakeupTime
	 */
	public ResetDynamicCustInThresholdTimer(long wakeupTime) {
		super(wakeupTime);
		gmmsUtility = GmmsUtility.getInstance();
		custManager = gmmsUtility.getCustomerManager();
		dynamicCustInThresholdExipreTime = gmmsUtility.getDynamicCustInThresholdExipreTime();
	}

	/** 
	 * 
	 * @see com.king.gmms.util.AbstractTimer#excute()
	 */
	@Override
	public void excute() {
		ConcurrentMap<Integer, Long> expireInThottleQuotaMap = throttlingControl.getExpireThottleQuotaMap();
		
		for (Integer ssid: expireInThottleQuotaMap.keySet()) {
			try {
				long dynamicThresholdStartTime = expireInThottleQuotaMap.get(ssid);
				long space = System.currentTimeMillis() - dynamicThresholdStartTime;
				
				if (space > dynamicCustInThresholdExipreTime) {
					A2PCustomerInfo cust = custManager.getCustomerBySSID(ssid);
					int confThrottlingNum = cust.getConfigedIncomingThrottlingNum();
					ConcurrentMap<Integer, ThrottlingTimemark> incomingThrottleCache = throttlingControl.getIncomingThrottlingControlCache();
					ThrottlingTimemark oldThrottlingTimemark = incomingThrottleCache.get(ssid);
					// in case of reload customer info
					if (oldThrottlingTimemark != null) {
						int currentThrottlingNum = oldThrottlingTimemark.getDateArraySize();
						if (currentThrottlingNum != confThrottlingNum) {
							// reset throttlingNum to conf value
							incomingThrottleCache.put(ssid, new ThrottlingTimemark(confThrottlingNum));
							
							// don't remove record in expireInThottleQuotaMap, avoid sync, and the later one would override it
							if (log.isInfoEnabled()) {
								log.info("Ssid: {} incoming throttlingNum is reset to configed value: {}", ssid, confThrottlingNum);
							}
						} 
						cust.setApplyInThrottleFlag(true);
					}
				}
			}catch (Exception e) {
				log.warn(e, e);
			}
		}
	}
}
