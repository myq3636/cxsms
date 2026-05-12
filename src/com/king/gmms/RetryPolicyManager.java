package com.king.gmms;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.*;
import com.king.message.gmms.*;

/**
 * <p>
 * Title:
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * <p>
 * Company:
 * </p>
 * Next retry time is local time, expiry date is GMT time.
 * 
 * @version 1.0
 */
public class RetryPolicyManager {
	// private static Logger log = Logger.getLogger(RetryPolicyManager.class);
	private static SystemLogger log = SystemLogger
			.getSystemLogger(RetryPolicyManager.class);
	private static RetryPolicyManager instance = new RetryPolicyManager();
	private GmmsUtility gmmsUtility;
	private Map<Integer,RetryPolicyInfo> ssid_policy_map = new ConcurrentHashMap<Integer, RetryPolicyInfo>();

	private List<Integer> defaultPolicy;
	private final static String DEFAULT_RETRY_POLICY = "10M*144";
	private TimeZone local;

	// private Date expire = null;

	private RetryPolicyManager() {
		gmmsUtility = GmmsUtility.getInstance();
		local = TimeZone.getDefault();
		try {
			defaultPolicy = parse(gmmsUtility.getCommonProperty(
					"DefaultRetryPolicy", DEFAULT_RETRY_POLICY));
		} catch (Exception ex) {
			log
					.error("The configuration for DefaultRetryPolicy is not correct, use 10M*144.");
			defaultPolicy = parse("10M*144");
		}

	}

	public static RetryPolicyManager getInstance() {
		return instance;
	}
	
	public Map<Integer, RetryPolicyInfo> getSsid_policy_map() {
		return ssid_policy_map;
	}

	public void setSsid_policy_map(Map<Integer, RetryPolicyInfo> ssidPolicyMap) {
		ssid_policy_map = ssidPolicyMap;
	}

	public void doRetryPolicy(GmmsMessage gmmsMessage) {
		List<Integer> policy = getRetryPolicy(gmmsMessage);
		int retriedNumber = gmmsMessage.getRetriedNumber();
		gmmsMessage.setRetriedNumber(retriedNumber + 1);
		String messageType = gmmsMessage.getMessageType();

		if (retriedNumber < policy.size()) {
			Date now = new Date();
			long diff = local.getRawOffset();
			if (local.inDaylightTime(now)) {
				diff += local.getDSTSavings();
			}
			long gmtNow = now.getTime() - diff;

			if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
					.equalsIgnoreCase(messageType)) { // IN_DR
				Date nextRetryTime = new Date(now.getTime()
						+ policy.get(retriedNumber) * 1000);
				gmmsMessage.setNextRetryTime(nextRetryTime);

			} else {
				int defaultExpire = gmmsUtility.getExpireTimeInMinute();
				Date expire = gmmsMessage.getExpiryDate();
				if (expire == null) {
					expire = new Date(gmtNow + defaultExpire * 60 * 1000);
				}

				if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)
						|| GmmsMessage.MSG_TYPE_SUBMIT
								.equalsIgnoreCase(messageType)
						|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
								.equalsIgnoreCase(messageType)) {
					if (expire.getTime() < gmmsMessage.getTimeStamp().getTime()
							+ policy.get(retriedNumber) * 1000
							&& gmmsMessage.getDeliveryReport()) {
						// do nothing, handle it in
						// handleOutSubmitRes/handleOutDeliveryReportRes/handleMessageError
					} else {
						Date nextRetryTime = new Date(now.getTime()
								+ policy.get(retriedNumber) * 1000);
						gmmsMessage.setNextRetryTime(nextRetryTime);
					}
				} else {
					log.warn(gmmsMessage, "Unsupport message type:"
							+ messageType);
				}
			}

			if(log.isInfoEnabled()){
				log.info(gmmsMessage, "Disposed by RetryPolicyManager, next retry time: {}, retry number: {}"
					,gmmsMessage.getNextRetryTime(),gmmsMessage.getRetriedNumber());
			}
		} else {// expired or final expired
			// do nothing, handle it in
			// handleOutSubmitRes/handleOutDeliveryReportRes/handleInDeliveryReportRes/handleMessageError
		}
	}

	private List<Integer> getRetryPolicy(GmmsMessage gmmsMessage) {
		List<Integer> policy = null;
	     A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
		try {
			String messageType = gmmsMessage.getMessageType();
			int oA2P = gmmsMessage.getOA2P();
			int cA2P = gmmsMessage.getCurrentA2P();
			int rA2P = gmmsMessage.getRA2P();
			A2PCustomerInfo ocustomer = ctm.getCustomerBySSID(gmmsMessage.getOSsID());
			if ((GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_SUBMIT
							.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
							.equalsIgnoreCase(messageType))
					&&!ocustomer.isSmsOptionOnlySupportDRRetry()) {
				if (rA2P == 0) {
					policy = getOssidRetryPolicy(gmmsMessage);
				} else if (cA2P == rA2P || gmmsUtility.getCustomerManager().vpOnSameA2P(cA2P, rA2P)) {
					policy = this.getMTRetryPolicy(gmmsMessage.getRSsID());
					if (policy == null) {
						policy = getOssidRetryPolicy(gmmsMessage);
					}
				} else {
					policy = getMTRetryPolicy(rA2P);
				}
				if (policy == null) {
					policy = defaultPolicy;
				}
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
					.equalsIgnoreCase(messageType)) {
				if (cA2P == oA2P || ctm.vpOnSameA2P(cA2P, oA2P)) {
					policy = getOssidRetryPolicy(gmmsMessage);
					if (policy == null) {
						policy = this.getMTRetryPolicy(gmmsMessage.getRSsID());
					}
				} else {
					policy = getMORetryPolicy(oA2P);
				}
				if (policy == null) {
					policy = defaultPolicy;
				}
			} else {
				log.warn(gmmsMessage, "Unsupport message type:{}" , messageType);
				policy = defaultPolicy;
			}
		} catch (Exception ex) {
			log.error(gmmsMessage,
					"The configuration of RetryPolicy is error, use default");
			policy = defaultPolicy;
		}
		return policy;
	}


	private List<Integer> getOssidRetryPolicy(GmmsMessage msg) {
		int ossid = msg.getOSsID();		
		Map<Integer, RetryPolicyInfo> map = this.getSsid_policy_map();
		RetryPolicyInfo policyInfo = null;
		List<Integer> list = null;
		if(map.containsKey(ossid)){
			policyInfo = map.get(ossid);
			list = policyInfo.getServiceTypeIDPolicyMap().get(msg.getServiceTypeID());
			
			if(list == null){
				Iterator<Map.Entry<Pattern, List<Integer>>> it = policyInfo.getSenderPrefixPolicyMap().entrySet().iterator();
				String sender = msg.getSenderAddress();
				while(it.hasNext()){
					Map.Entry<Pattern, List<Integer>> entry = it.next();
					if(entry.getKey().matcher(sender).matches()){
						list = entry.getValue();
						return list;//sender prefix policy
					}
				}
				if(list == null){
					return policyInfo.getMoPolicyList(); //MO policy
				}				
			}else{
				return list; //service type id policy
			}
		}
		return list;
	}
	
	private List<Integer> getMORetryPolicy(int ssid){
		return getMTorMoRetryPolicy(ssid,false);
	}
	
	private List<Integer> getMTRetryPolicy(int ssid){
		return getMTorMoRetryPolicy(ssid,true);
	}
	
	private List<Integer> getMTorMoRetryPolicy(int ssid,boolean isMt) {
		Map<Integer, RetryPolicyInfo> map = this.getSsid_policy_map();
		RetryPolicyInfo policyInfo = null;
		if(map.containsKey(ssid)){
			policyInfo = map.get(ssid);
			if(isMt){
				return policyInfo.getMtPolicyList();
			}else{
				return policyInfo.getMoPolicyList();
			}			
		}else{
			return null;
		}
	}
	
	public List<Integer> parse(String policy) {
		StringBuilder sb = new StringBuilder();
		int current = 0, index4Star = 0;
		while ((index4Star = policy.indexOf("*", current)) > 0) {
			// expand all the time points, get rid of the * sign.
			String multiplicand = null;
			if (policy.charAt(index4Star - 1) != ')') {
				// The multiplicand is a time point
				// append all the time points before current multiplicand
				int index4Comma = policy.lastIndexOf(",", index4Star);
				sb.append(policy.substring(current, index4Comma + 1));
				// separate the multiplicand
				multiplicand = policy.substring(index4Comma + 1, index4Star)
						+ ",";
			} else {
				// The multiplicand is ()
				// append all the time points before current multiplicand
				int index4Bracket = policy.lastIndexOf("(", index4Star);
				int index4Comma = policy.lastIndexOf(",", index4Bracket);
				sb.append(policy.substring(current, index4Comma + 1));
				// separate the multiplicand
				multiplicand = policy.substring(index4Bracket + 1,
						index4Star - 1)
						+ ",";
			}
			int multiplicator = 1;
			// forward the "current" index
			current = policy.indexOf(",", index4Star);
			if (current == -1) {
				// the end of the expression is reached
				current = policy.length();
				// separate the multiplicator
				multiplicator = Integer.parseInt(policy.substring(
						index4Star + 1, current));
			} else {
				// separate the multiplicator
				multiplicator = Integer.parseInt(policy.substring(
						index4Star + 1, current));
				// because the "current" points to the "," right after the
				// multiplicator, so move forward it
				// to the next character
				current++;
			}
			for (int i = 0; i < multiplicator; i++) {
				// do the multiplication
				sb.append(multiplicand);
			}
		}
		// append all the rest time points
		sb.append(policy.substring(current));
		// split the StringBuild to strings
		String[] timePoints = sb.toString().split(",");
		List<Integer> result = new LinkedList<Integer>();
		// do the time unit conversion
		for (String timePoint : timePoints) {
			int timeValue = Integer.parseInt(timePoint.substring(0, timePoint
					.length() - 1));
			switch (timePoint.toUpperCase().charAt(timePoint.length() - 1)) {
			case 'D': {
				result.add(timeValue * 60 * 60 * 24);
				break;
			}
			case 'H': {
				result.add(timeValue * 60 * 60);
				break;
			}
			case 'M': {
				result.add(timeValue * 60);
				break;
			}
			case 'S': {
				result.add(timeValue);
				break;
			}
			}
		}
		return result;
	}

	public boolean isNextExpired(GmmsMessage msg) {
		boolean nextIsExpired = true;
		try {
			List<Integer> policy = getRetryPolicy(msg);
			int retriedNumber = msg.getRetriedNumber();

			Date now = new Date();
			long diff = local.getRawOffset();
			if (local.inDaylightTime(now)) {
				diff += local.getDSTSavings();
			}
			long gmtNow = now.getTime() - diff;

			int defaultExpire = gmmsUtility.getExpireTimeInMinute();
			Date expire = msg.getExpiryDate();
			if (expire == null) {
				expire = new Date(gmtNow + defaultExpire * 60 * 1000);
			}

			nextIsExpired = (expire.getTime() < msg.getTimeStamp().getTime()
					+ policy.get(retriedNumber - 1) * 1000);
		} catch (Exception ex) {
			log.error(ex, ex);
		}
		return nextIsExpired;
	}

	public boolean isLastRetry(GmmsMessage msg) {
		boolean isLastRetry = true;
		try {
			List<Integer> policy = getRetryPolicy(msg);
			int retriedNumber = msg.getRetriedNumber();
			isLastRetry = retriedNumber >= policy.size();
		} catch (Exception ex) {
			log.error(ex, ex);
		}
		return isLastRetry;
	}

	public static void main(String[] args) {
		String po = "10M*144";
		RetryPolicyManager rp = new RetryPolicyManager();
		;
		List<Integer> a = rp.parse(po);
		System.out.println(a.size());
		Iterator<Integer> b = a.iterator();
		while (b.hasNext()) {
			System.out.println(b.next());
		}

	}
}
