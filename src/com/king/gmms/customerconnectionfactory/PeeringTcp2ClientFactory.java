package com.king.gmms.customerconnectionfactory;

import java.util.Iterator;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.PeeringTcp2Session;
import com.king.gmms.domain.*;
import com.king.gmms.messagequeue.CustomerMessageQueue;
import com.king.gmms.messagequeue.LongConnectionCustomerMessageQueue;
import com.king.gmms.strategy.IndexBalanceStrategy;

public class PeeringTcp2ClientFactory extends AbstractConnectionFactory {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(PeeringTcp2ClientFactory.class);
	private static PeeringTcp2ClientFactory instance = new PeeringTcp2ClientFactory();

	private PeeringTcp2ClientFactory() {
		isServer = false;
	}

	public static PeeringTcp2ClientFactory getInstance() {
		return instance;
	}

	/**
	 * initConnectionFactory
	 * 
	 * @param ssid
	 *            int
	 * @param type
	 *            int
	 * 
	 * @todo Implement this
	 *       com.king.gmms.customerconnectionfactory.AbstractConnectionFactory
	 *       method
	 */
	public void initConnectionFactory(int ssid, int type) {
		if(log.isInfoEnabled()){
			log.info("initConnectionFactory for ssid:{},type:{}", ssid, type);
		}
		try {
			this.initSingleConnection(ssid);
		} catch (Exception ex) {
			log.error("initConnectionFactory exception for ssid:" + ssid, ex);
		}
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
		boolean result = super.initSingleConnectionInfo(ssid);
		if (!result) {
			log.warn("convert4Type1 failed for ssid:{}", ssid);
			return;
		}
		initMultiConnection(ssid);
	}

	/**
	 * initMultiConnection
	 * 
	 * @param ssid
	 */
	private void initMultiConnection(int ssid) {
		boolean result = super.initMultiConnectionInfo(ssid);
		if (!result) {
			log.warn("convert4Type2 failed for ssid:{}", ssid);
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
			if (cim.inCurrentA2P(connInfo.getSsid())) {
				continue;
			}
			Connection conn = new MultiClientConnectionImpl(
					new IndexBalanceStrategy(), true);
			conn.initialize(connInfo);
			// add the connection into NodeConnectionManager
			connManager.insertConnection(conn);
			for (int i = 0; i < connInfo.getSessionNum(); i++) {
				PeeringTcp2Session cs = new PeeringTcp2Session(connInfo);

				cs.setConnectionManager(connManager);
				cs.start();

				// add the session into NodeConnectionManager
				connManager.insertSession(conn.getConnectioName(), cs);
				try {
					Thread.sleep(10L);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			putConnectionManager(ssid, connInfo.getConnectionName(),
					connManager);
		} // end has connection


		int minSenderNum = ci.getMinSenderNumber();
		int maxSenderNum = ci.getMaxSenderNumber();
		
		CustomerMessageQueue operatorMessageQueue = new LongConnectionCustomerMessageQueue(
				ci, connManager, minSenderNum, maxSenderNum, isServer);
		ssid2messageQueues.put(ssid, operatorMessageQueue);
	}
}
