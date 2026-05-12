package com.king.gmms.customerconnectionfactory;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.sessionfactory.SessionFactory;
import com.king.gmms.connectionpool.sessionfactory.SpringSessionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.CustomerMessageQueue;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.messagequeue.ShortConnectionCustomerMessageQueue;

public class SpringClientFactory extends ShortConnectionFactory {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SpringClientFactory.class);
	private static SpringClientFactory instance = new SpringClientFactory();

	private SpringClientFactory() {
		super();
		isServer = false;
	}

	protected void startOperatorMessageQueue(OperatorMessageQueue queue,
			int ssid) {
		A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
		if (info != null) {
			try {
				SessionFactory factory = new SpringSessionFactory(info);
				((ShortConnectionCustomerMessageQueue) queue)
						.startMessageQueue(factory);
			} catch (Exception ex) {
				log.debug(ex, ex);
			}
		} else {
			log.debug("get not get customer by ssid:{}", ssid);

		}

	}

	public static SpringClientFactory getInstance() {
		return instance;
	}

}
