/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.routing.nmg;

import java.util.List;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.RentAddrCondition;
import com.king.message.gmms.GmmsMessage;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class NMGUtility {
	
//	private static SystemLogger log = SystemLogger.getLogger(NMGUtility.class);
	
	/** 
	 * The lookup was successful. 
	 */
	public static final int NMG_OK = 0;

	/**
	 * Invalid PDU Parameter Value
	 * <br> Need query to ads
	 */
	public static final int NMG_INVALID_PARAM_VALUE = 1;

	/**
	 * Incorrect Operator Partner 
	 * <br> Need query to ads
	 */
	public static final int NMG_OP_PARTNER_ERROR = 2;

	/** 
	 * No Available Number 
	 * <br> Retry
	 */
	public static final int NMG_NUMBER_UNAVALABLE = 3;

	/** 
	 * Expired MT Transaction
	 * <br> Need query to ads
	 */
	public static final int NMG_EXPIRED_MT_TRANSACTION = 4;

	/** 
	 * The number is not provisioned
	 * <br> Retry
	 */
	public static final int NMG_NUMBER_NOT_PROVISIONED = 5;
	
	/** 
	 * System Failure
	 * <br> Retry
	 */
	public static final int NMG_SYSTEM_FAIL = 6;
	
	/** 
	 * Unknown Error
	 * <br> Need query to ads
	 */
	public static final int NMG_UNKNOW_ERROR = 7;
	
	/**
	 * If ossid configured SMSOptionRentAddrPrefix 
	 * then need goto NMG process
	 */
	public static boolean needNmgRoute(GmmsMessage message) {
		boolean ret = false;

		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		List<String> rentAddrPrefixList = null;
		A2PCustomerInfo ossidCustInfo = ctm.getCustomerBySSID(message.getOSsID());
		if (ossidCustInfo != null) {
			rentAddrPrefixList = ossidCustInfo.getRentAddrPrefixList();
		}
		
		// ossid configured SMSOptionRentAddrPrefix 
		if (rentAddrPrefixList != null && rentAddrPrefixList.size()>0) {
			String recipientAddr = message.getRecipientAddress();
			// recipient address matches rent prefix
			for (String prefix : rentAddrPrefixList) {
				if (recipientAddr.startsWith(prefix)) {
					ret = true;
					break;
				}
			}
		}

		return ret;
	}
	
	public static boolean needRentAddr(GmmsMessage message) {
		boolean ret = false;
		
		// apply addr only once,
		if (alreadyRentAddr(message)) {
			return false;
		}
		
		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		List<RentAddrCondition> rentAddrConditionList = null;
		A2PCustomerInfo oopCustInfo = ctm.getCustomerBySSID(message.getOoperator());
		if (oopCustInfo != null) {
			rentAddrConditionList = oopCustInfo.getRentAddrConditionList();
		}
		
		// oop configured SMSOptionConditionToRentAddr
		if (rentAddrConditionList != null && rentAddrConditionList.size()>0) {
			String recipientAddr = message.getRecipientAddress();
			// recipient address matches rent prefix
			for (RentAddrCondition condition : rentAddrConditionList) {
				if (recipientAddr.startsWith(condition.getrAddrPrefix())) {
					ret = true;
					break;
				}
			}
		}

		return ret;
	}
	
	private static boolean alreadyRentAddr(GmmsMessage message) {
		String originalSenderAddr = message.getOriginalSenderAddr();
		// apply addr only once, 
    	// NMG should guarantee the address should not expired before message expired
    	if (originalSenderAddr != null && originalSenderAddr.trim().length()>0) {
    		return true;
    	}
    	return false;
	}
	
	public static int getPartnerCustId(GmmsMessage message) {

		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		List<RentAddrCondition> rentAddrConditionList = null;
		A2PCustomerInfo oopCustInfo = ctm.getCustomerBySSID(message.getOoperator());
		if (oopCustInfo != null) {
			rentAddrConditionList = oopCustInfo.getRentAddrConditionList();
		}
		
		// oop configured SMSOptionConditionToRentAddr
		if (rentAddrConditionList != null && rentAddrConditionList.size()>0) {
			String recipientAddr = message.getRecipientAddress();
			// recipient address matches rent prefix
			for (RentAddrCondition condition : rentAddrConditionList) {
				if (recipientAddr.startsWith(condition.getrAddrPrefix())) {
					return ctm.getCustIdbyCustomerNameshort(getShortNameByValue(condition.getPartner()));
				}
			}
		}

		return -1;
	}
	
	public static boolean isRentAddrMsg(GmmsMessage message) {
		boolean ret = false;

		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		List<RentAddrCondition> rentAddrConditionList = null;
		A2PCustomerInfo ropCustInfo = ctm.getCustomerBySSID(message.getRoperator());
		if (ropCustInfo != null) {
			rentAddrConditionList = ropCustInfo.getRentAddrConditionList();
		}
		
		// rop configured SMSOptionConditionToRentAddr
		if (rentAddrConditionList != null && rentAddrConditionList.size()>0) {
			String senderAddr = message.getSenderAddress();
			// recipient address matches rent prefix
			for (RentAddrCondition condition : rentAddrConditionList) {
				if (senderAddr.startsWith(condition.getrAddrPrefix())) {
					ret = true;
					break;
				}
			}
		}

		return ret;
	}
	
	public static String getShortNameByValue(String value) {
		if (value == null)
			return null;

		int c = value.lastIndexOf("_");
		if (c > 0)
			return value.substring(0, c);
		else
			return value;
	}

}
