package com.king.gmms.customerconnectionfactory;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.sessionfactory.ClickatellSessionFactory;
import com.king.gmms.connectionpool.sessionfactory.SessionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.messagequeue.ShortConnectionCustomerMessageQueue;

public class ClickatellClientFactory extends ShortConnectionFactory {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ClickatellClientFactory.class);
	private static ClickatellClientFactory instance = new ClickatellClientFactory();

	private ClickatellClientFactory() {
		super();
		isServer = false;
	}

	protected void startOperatorMessageQueue(OperatorMessageQueue queue,
			int ssid) {

		A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
		if (info != null) {
			try {
				SessionFactory factory = new ClickatellSessionFactory(info);
				((ShortConnectionCustomerMessageQueue) queue)
						.startMessageQueue(factory);
			} catch (Exception ex) {

				log.debug(ex, ex);

			}
		} else {

			log.debug("get not get customer by ssid:{}", ssid);

		}

	}

	public static ClickatellClientFactory getInstance() {
		return instance;
	}
}
