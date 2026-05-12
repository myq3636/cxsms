package com.king.gmms.customerconnectionfactory;

/**
 * <p>Title: AcceletClientFactory </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2006</p>
 * <p>Company: King</p>
 * @author: Tommy
 * @version 6.1
 */

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.sessionfactory.*;
import com.king.gmms.domain.*;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.messagequeue.ShortConnectionCustomerMessageQueue;

public class AcceletClientFactory extends ShortConnectionFactory {

	private static AcceletClientFactory instance = new AcceletClientFactory();
	private static SystemLogger log = SystemLogger
			.getSystemLogger(AcceletClientFactory.class);

	private AcceletClientFactory() {
		super();
		isServer = false;
	}

	protected void startOperatorMessageQueue(OperatorMessageQueue queue,
			int ssid) {

		A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
		if (info != null) {
			try {
				SessionFactory factory = new AcceletSessionFactory(info);
				((ShortConnectionCustomerMessageQueue) queue)
						.startMessageQueue(factory);
			} catch (Exception ex) {

				log.debug(ex, ex);

			}
		} else {

			log.debug("get not get customer by ssid:{}", ssid);

		}

	}

	public static AcceletClientFactory getInstance() {
		return instance;
	}

}
