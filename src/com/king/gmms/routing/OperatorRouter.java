package com.king.gmms.routing;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.king.db.DataManagerException;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>
 * Title: OperatorRouter
 * </p>
 * <p>
 * Description: It is a base class to get Operator's information
 * </p>
 * <p>
 * Copyright: Copyright (c) 2001-2010
 * </p>
 * <p>
 * Company: King.Inc
 * </p>
 * 
 * @version 6.1
 * @author: Sam Hao
 */
public abstract class OperatorRouter {
	// DNSClient to query operator's information
	private static SystemLogger log = SystemLogger
			.getSystemLogger(OperatorRouter.class);
	// protected DNSClient dns;
	protected GmmsUtility gmmsUtility;
	protected A2PCustomerManager gmmsCustomerManager;
	protected Pattern pattern = null;

	protected String defaultSuffix = null;
	protected String localSuffix = null;

	/**
	 * Constructor
	 * 
	 */
	public OperatorRouter() {
		gmmsUtility = GmmsUtility.getInstance();
		gmmsCustomerManager = gmmsUtility.getCustomerManager();
		defaultSuffix = gmmsUtility.getCommonProperty("DefaultSuffix", "gprs");
		localSuffix = gmmsUtility.getCommonProperty("LocalSuffix", "local");
	}

	public boolean routeToOperator(GmmsMessage msg) {
		int oOperator = getOoperator(msg);
		if (oOperator <= 0) {
			if(log.isInfoEnabled()){
				log.info(msg, "Route to oOperator error:{}" , oOperator);
			}
			return false;
		}
		int rOperator = getRoperator(msg);
		if (rOperator <= 0) {
			if(log.isInfoEnabled()){
				log.info(msg, "Route to rOperator error:{}" , rOperator);
			}
			return false;
		}
		return true;
	}

	public abstract int getOoperator(GmmsMessage msg);

	public abstract int getOoperatorSYN(GmmsMessage msg);

	public abstract int getRoperatorSYN(GmmsMessage msg);

	public abstract int getRoperator(GmmsMessage msg);

	public int getOperatorByDNS(String[] mncmcc, GmmsMessage msg)
			throws DataManagerException {

		switch (Integer.parseInt(mncmcc[0])) {
		case -1:
		case -3:
		case -4: {
			return -4;
		}
		case -2: {
			return -1;
		}
		case -5: {
			return -2;
		}
		case -6: {// asy query
			return 0;
		}
		default: {
			String mncAndMcc = mncmcc[0] + "_" + mncmcc[1];
			if(log.isDebugEnabled()){
				log.debug(msg, "MNC_MCC:{}", mncAndMcc);
			}
			return getSsidByMncMcc(mncAndMcc);
		}
		}
	}

	private int getSsidByMncMcc(String mncMcc) {
		int ssid = -1;

		ssid = gmmsCustomerManager.getSmsSsidByMncMcc(mncMcc);
		if (ssid > 0)
			return ssid;
		else
			return -3;
	}

	/**
	 * get the suffix by ossid and rcpt
	 * 
	 * @param ossid
	 *            int
	 * @param rcpt
	 *            String
	 * @return String
	 */
	protected String getDNSSuffix(int ossid, String rcpt) {
		String[] prefix = null;
		Map<Integer, String[]> customerLocalDNSPrefix = gmmsCustomerManager
				.getCustomerLocalDNSPrefix();
		if (!customerLocalDNSPrefix.containsKey(ossid))
			return this.defaultSuffix;
		prefix = customerLocalDNSPrefix.get(ossid);
		if (prefix != null && prefix.length > 0) {
			for (int i = 0; i < prefix.length; i++) {
				if (rcpt.startsWith(prefix[i])) {
					return this.localSuffix;
				}
			}
		}
		return this.defaultSuffix;
	}
}
