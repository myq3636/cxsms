package com.king.gmms.customerconnectionfactory;

import java.util.Iterator;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.ConnectionManagerImplWithBindMode;
import com.king.gmms.connectionpool.connection.MultiServerConnectionImpl;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.messagequeue.CustomerMessageQueue;
import com.king.gmms.messagequeue.LongConnectionCustomerMessageQueue;

/**
 * <p>
 * Title: PeeringTcpServerFactory
 * </p>
 * <p/>
 * <p>
 * Description:
 * </p>
 * <p/>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p/>
 * <p>
 * Company: King
 * </p>
 * 
 * @version 7.0
 * @author: Neal
 */
public class PeeringTcp2ServerFactory extends AbstractConnectionFactory {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(PeeringTcp2ServerFactory.class);
	private static PeeringTcp2ServerFactory instance = new PeeringTcp2ServerFactory();

	public static PeeringTcp2ServerFactory getInstance() {
		return instance;
	}

	private PeeringTcp2ServerFactory() {
		isServer = true;
	}

	/**
	 * initConnectionFactory
	 * 
	 * @param ssid
	 *            int
	 * @param type
	 *            int
	 * @todo Implement this
	 *       com.king.gmms.customerconnectionfactory.AbstractConnectionFactory
	 *       method
	 */
	public void initConnectionFactory(int ssid, int type) {
		log.trace("initConnectionFactory for ssid:{},type:{}", ssid, type);
		this.initSingleConnection(ssid);
	}

	/**
	 * clearConnectionFactory
	 * 
	 * @param ssid
	 *            int
	 * @todo Implement this
	 *       com.king.gmms.customerconnectionfactory.AbstractConnectionFactory
	 *       method
	 */
	public void clearConnectionFactory(A2PCustomerInfo cust) {
		int ssid = cust.getSSID();
		if(log.isInfoEnabled()){
			log.info("breakConnection for ssid:{}", ssid);
		}
		try {
				this.clearSingleConnectionInfo(cust);
		} catch (Exception ex) {
			log.error("initConnectionFactory exception for ssid:" + ssid, ex);
		}
	}
	/**
	 * initSingleConnection
	 * 
	 * @param ssid
	 */
	private void initSingleConnection(int ssid) {
		boolean result = initSingleConnectionInfo(ssid);
		if (!result) {
			log.warn("convert4Type1 failed for ssid:{}", ssid);
			return;
		}
		SingleNodeCustomerInfo ci = (SingleNodeCustomerInfo) cim
				.getCustomerBySSID(ssid);
		Map<String, ConnectionInfo> connectionMap = ci
				.getConnectionMap(isServer);

		Iterator itConnection = connectionMap.values().iterator();
		ConnectionInfo connInfo;

		ConnectionManagerImplWithBindMode connManager = new ConnectionManagerImplWithBindMode();

		while (itConnection.hasNext()) { // while has connection
			connInfo = (ConnectionInfo) itConnection.next();
			Connection conn = new MultiServerConnectionImpl(true);
			conn.initialize(connInfo);
			putConnectionManager(ssid, connInfo.getConnectionName(),
					connManager);
			connManager.insertConnection(conn);
		} // end has connection

		int minSenderNum = ci.getMinSenderNumber();
		int maxSenderNum = ci.getMaxSenderNumber();
		CustomerMessageQueue operatorMessageQueue = new LongConnectionCustomerMessageQueue(
				ci, connManager, minSenderNum, maxSenderNum, isServer);
		ssid2messageQueues.put(ssid, operatorMessageQueue);
	}
}
