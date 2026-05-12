/**
 */
package com.king.gmms;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.message.gmms.GmmsMessage;

/**
 * Duplicate message check
 * Since the check occurs in CoreEngine, 
 * you must guarantee that messages with same recipient address go to the same CoreEngine
 * 
 * @author bensonchen
 * @version 1.0.0
 */
public class DuplicateMessageCheck {
	private static SystemLogger log = SystemLogger.getSystemLogger(DuplicateMessageCheck.class);
	
	private static DuplicateMessageCheck instance = new DuplicateMessageCheck();
	public static DuplicateMessageCheck getInstance() {
		return instance;
	}
	
	/**
	 * key: ssid <br>
	 * value: inner Map <p>
	 * inner Map:<br> key: R_Addr + O_SSID + ServiceTypeID or MD5(R_Addr + O_SSID + ServiceTypeID + msgContent)<br>
	 * value: no meaning
	 */
	private ConcurrentMap<Integer, ConcurrentMap<String, Integer>> cache;
	
	private GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	private A2PCustomerManager custManager = null;
	
	/**
	 *  MaxDuplicateMessageCacheSize for each customer
	 */
	int maxDuplicateMessageCacheSize;
	
	private static String SEPERATOR = "_";
	
	private DuplicateMessageCheck() {
		try {
			cache = new ConcurrentHashMap<Integer, ConcurrentMap<String, Integer>>();
			custManager = gmmsUtility.getCustomerManager();
			maxDuplicateMessageCacheSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MaxDuplicateMessageCacheSize", "200000").trim());
		} catch (Exception e) {
			log.error("Init DuplicateMessageCheck error", e);
		}
		
	}
	
	public boolean isDuplicateMsg(GmmsMessage msg) {
		boolean result = false;
		try {
			// don't check duplicate other than submit message
			if (!(GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
					|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType()))) {
				return result;
			}
			
			// don't check duplicate for CSMS
			if (msg.isInCsm()) {
				return result;
			}
			
			// don't check duplicate twice
			if (msg.hasCheckDuplicate()) {
				return result;
			} else {
				msg.setCheckDuplicate(true);
			}
			
			// check configurations
			int ossid = msg.getOSsID();
			A2PCustomerInfo cust = custManager.getCustomerBySSID(ossid);
			List<Integer> serviceTypeIDList = cust.getDuplicateMsgServiceTypeIDList();
			int duplicateMsgPeriod = cust.getDuplicateMsgPeriod();
			if (serviceTypeIDList == null || serviceTypeIDList.size()<1
				|| (duplicateMsgPeriod<=0)) {
				return result;
			}
			
			int msgServiceTypeID = msg.getServiceTypeID();
			if (serviceTypeIDList.indexOf(msgServiceTypeID)<0) {
				return result;
			}
			
			// get duplicateMessageCache
			ConcurrentMap<String, Integer> duplicateMessageCache = cache.get(ossid);
			if (duplicateMessageCache == null) {
				Cache<String, Integer> cachex = CacheBuilder.newBuilder()
				                                    .maximumSize(maxDuplicateMessageCacheSize)
				                                    .expireAfterWrite(duplicateMsgPeriod, TimeUnit.MILLISECONDS)
				                                    .build();
				cache.putIfAbsent(ossid, cachex.asMap());
				duplicateMessageCache = cache.get(ossid);
			} 
			
			// make key
			String duplicateKey = msg.getRecipientAddress() + SEPERATOR + msg.getOSsID() + SEPERATOR + msg.getServiceTypeID();
			if (cust.isCheckDuplicateMsgContent()) {
				if (GmmsMessage.AIC_MSG_TYPE_TEXT.equalsIgnoreCase(msg.getGmmsMsgType())) {
					duplicateKey = duplicateKey + HttpUtils.encrypt(msg.getTextContent(), "MD5");
				} else if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg.getGmmsMsgType())) {
					duplicateKey = duplicateKey + HttpUtils.encrypt(msg.getMimeMultiPartData(), "MD5");
				}
			} 
			
			// check duplicate
			if (duplicateMessageCache.putIfAbsent(duplicateKey, 1) != null) {
				result = true;
			} 
			
		} catch (Exception e) {
			log.warn("DuplicateMessageCheck failed", e);
			result = false;
		}
		return result;
	}
	
	public void clearCache(int ssid) {
		cache.remove(ssid);
	}

}
