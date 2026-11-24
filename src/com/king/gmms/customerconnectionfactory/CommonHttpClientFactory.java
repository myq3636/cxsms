package com.king.gmms.customerconnectionfactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.sessionfactory.CommonHttpSessionFactory;
import com.king.gmms.connectionpool.sessionfactory.SessionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.messagequeue.CustomerMessageQueue;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.messagequeue.ShortConnectionCustomerMessageQueue;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.GmmsMessage;

public class CommonHttpClientFactory extends ShortConnectionFactory {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonHttpClientFactory.class);
	private static CommonHttpClientFactory instance = new CommonHttpClientFactory();
	
	private ConcurrentHashMap<Integer, Boolean> initializedHttpSsids = null;
	private CommonHttpClientFactory() {
		super();
		initializedHttpSsids = new ConcurrentHashMap<Integer, Boolean>();
		isServer = false;
	}

	protected void startOperatorMessageQueue(OperatorMessageQueue queue,
			int ssid) {

		A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
		if (info != null) {
			try {
				SessionFactory factory = new CommonHttpSessionFactory(info);
				((ShortConnectionCustomerMessageQueue) queue)
						.startMessageQueue(factory);
			} catch (Exception ex) {
				log.debug(ex, ex);
			}
		} else {

			log.debug("get not get customer by ssid:{}", ssid);

		}

	}

	public void initializeAllHttpCustomers() {
		int count = 0;
		for (A2PCustomerInfo info : cim.getAllCustomerInfos()) {
			if (info == null) {
				continue;
			}
			if (!SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE.equalsIgnoreCase(info.getChlQueue())) {
				continue;
			}
			if (info instanceof A2PSingleConnectionInfo
					&& !((A2PSingleConnectionInfo) info).isChlInit()) {
				continue;
			}
			initializedHttpSsids.put(info.getSSID(), Boolean.TRUE);
			count++;
		}
		log.info("CommonHttpClient initialized HTTP customer metadata. count={}", count);
	}

	public Session getSession(int ssid) {
		A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
		// configuration problems: they repeat for every message of the channel, one line per 10 min
		if (info == null) {
			long n = com.king.gmms.util.LogRateLimiter.allow("http-no-customer:" + ssid, 600000L);
			if (n > 0) {
				log.warn("HTTP customer not found. ssid={} ({} message(s) in 10 min)", ssid, n);
			}
			return null;
		}
		if (!SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE.equalsIgnoreCase(info.getChlQueue())) {
			long n = com.king.gmms.util.LogRateLimiter.allow("http-not-client:" + ssid, 600000L);
			if (n > 0) {
				log.warn("Customer is not configured for CommonHttpClient. ssid={}, chlQueue={} ({} message(s) in 10 min)",
						ssid, info.getChlQueue(), n);
			}
			return null;
		}
		if (info instanceof A2PSingleConnectionInfo
				&& !((A2PSingleConnectionInfo) info).isChlInit()) {
			long n = com.king.gmms.util.LogRateLimiter.allow("http-not-init:" + ssid, 600000L);
			if (n > 0) {
				log.warn("HTTP customer is not initialized (ChlInit). ssid={} ({} message(s) in 10 min)", ssid, n);
			}
			return null;
		}
		initializedHttpSsids.put(ssid, Boolean.TRUE);
		return new CommonHttpSessionFactory(info).getSession();
	}
	
	
    
    
    

	public static CommonHttpClientFactory getInstance() {
		return instance;
	}

}
