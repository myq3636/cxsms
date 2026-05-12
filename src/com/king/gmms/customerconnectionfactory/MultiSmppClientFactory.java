package com.king.gmms.customerconnectionfactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.ConnectionManagerImplWithBindMode;
import com.king.gmms.connectionpool.connection.MultiClientConnectionImpl;
import com.king.gmms.connectionpool.connection.NodeClientConnection;
import com.king.gmms.connectionpool.connection.NodeConnectionManagerWithBindMode;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.connectionpool.sessionthread.*;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.domain.NodeInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.messagequeue.*;
import com.king.gmms.strategy.IndexBalanceStrategy;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class MultiSmppClientFactory extends AbstractConnectionFactory {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(MultiSmppClientFactory.class);
	private static MultiSmppClientFactory instance = new MultiSmppClientFactory();
	private ModuleManager moduleManager = null;
	private List<MultiSmppSession> sessionList = null;

	private MultiSmppClientFactory() {
		isServer = false;
		moduleManager = ModuleManager.getInstance();
		sessionList = new ArrayList<MultiSmppSession>();
	}

	public static MultiSmppClientFactory getInstance() {
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
			switch (type) {
			case 1: {
				this.initSingleConnection(ssid);
				break;
			}
			case 2: {
				this.initMultiConnection(ssid);
				break;
			}
			case 3: {
				this.initMultiNodeConnection(ssid);
				break;
			}
			}
		} catch (Exception ex) {
			log.error("initConnectionFactory exception for ssid:" + ssid, ex);
		}
	}
	/**
	 * clearConnectionFactory
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
	public void clearConnectionFactory(A2PCustomerInfo cust) {
		int ssid = cust.getSSID();
		int connectionType = cust.getConnectionType();
		if(log.isInfoEnabled()){
			log.info("breakConnection for ssid:{},type:{}", ssid, connectionType);
		}
		try {
			switch (connectionType) {
				case 1: {
					this.clearSingleConnectionInfo(cust);
					break;
				}
				case 2: {
					this.clearMultiConnectionInfo(cust);
					break;
				}
				case 3: {
					this.clearMultiNodeConnection(cust);
					break;
				}
			}
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
			Connection conn = new MultiClientConnectionImpl(
					new IndexBalanceStrategy(), true);
			conn.initialize(connInfo);
			// add the connection into NodeConnectionManager
			connManager.insertConnection(conn);
			int sessionLimit = moduleManager.getSessionNumberOfSelf(connInfo
					.getSessionNum());
			for (int i = 0; i < sessionLimit; i++) {
				MultiSmppSession cs = new MultiSmppSession(connInfo);

				cs.setConnectionManager(connManager);
				SessionThread thread = new ClientSessionThread(cs, ci
						.getReconnectInterval(), ci.getEnquireLinkFailureNum());
				cs.setSessionThread(thread);
				sessionList.add(cs);

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

	/**
	 * initMultiNodeConnection
	 * 
	 * @param ssid
	 */
	private void initMultiNodeConnection(int ssid) {
		MultiNodeCustomerInfo ci = (MultiNodeCustomerInfo) cim
				.getCustomerBySSID(ssid);

		int minSenderNum = ci.getMinSenderNumber();
		int maxSenderNum = ci.getMaxSenderNumber();

		NodeManager nm = new NodeManager(ci, minSenderNum, maxSenderNum, false);
		nm.init();
		Map nodeMap = ci.getNodeMap();
		Iterator itNode = nodeMap.values().iterator();
		NodeInfo ni;
		Map connectionMap;
		while (itNode.hasNext()) { // while has node
			ni = (NodeInfo) itNode.next();
			// set the node into NodeConnectionManager
			Node node = nm.getNode(ni.getNodeName());
			// one node has one NodeConnectionManager
			NodeConnectionManagerWithBindMode ncm = new NodeConnectionManagerWithBindMode();
			ncm.setNode(node);
			ncm.addObserver(nm);

			node.init(ci, ncm);

			connectionMap = ni.getConnectionMap(isServer);
			Iterator itConnection = connectionMap.values().iterator();
			ConnectionInfo connInfo;
			while (itConnection.hasNext()) { // while has connection
				connInfo = (ConnectionInfo) itConnection.next();
				Connection conn = new NodeClientConnection(true);
				conn.initialize(connInfo);
				// add the connection into NodeConnectionManager
				ncm.insertConnection(conn);
				int sessionLimit = moduleManager
						.getSessionNumberOfSelf(connInfo.getSessionNum());
				for (int i = 0; i < sessionLimit; i++) {
					MultiSmppSession cs = new MultiSmppSession(connInfo);
					cs.setConnectionManager(ncm);
					SessionThread thread = new NodeClientSessionThread(cs, ci
							.getNodeRecoveryTime(), ci.getReconnectInterval(),
							ci.getEnquireLinkFailureNum());
					cs.setSessionThread(thread);
				    sessionList.add(cs);

					// add the session into NodeConnectionManager
					ncm.insertSession(conn.getConnectioName(), cs);
				}
			} // end has connection
			putConnectionManager(ssid, ni.getNodeName(), ncm);
		} // end has node

		ssid2messageQueues.put(ssid, nm);
	}
	
	public void initializeSession(){
		for(MultiSmppSession session: sessionList){
			session.start();
		}
		sessionList.clear();
	}
}
