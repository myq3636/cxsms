package com.king.gmms.customerconnectionfactory;

import java.util.Map;
import java.util.Iterator;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.domain.NodeInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.messagequeue.*;

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
public class MultiSmppServerFactory extends AbstractConnectionFactory {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(MultiSmppServerFactory.class);
	private static MultiSmppServerFactory instance = new MultiSmppServerFactory();
	private int processorNum = 5;
	private int queueNum = 1;

	private MultiSmppServerFactory() {
		isServer = true;
		processorNum = Integer.parseInt(gmmsUtility.getModuleProperty(
				"SenderNumber", "5").trim());
		queueNum = Integer.parseInt(gmmsUtility.getModuleProperty(
				"SenderQueueNumber", "1").trim());
	}

	public static MultiSmppServerFactory getInstance() {
		return instance;
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

			Connection conn = new MultiServerConnectionImpl(true);

			conn.initialize(connInfo);
			putConnectionManager(ssid, connInfo.getConnectionName(),
					connManager);
			// add the connection into NodeConnectionManager
			connManager.insertConnection(conn);

		} // end has connection

		int minSenderNum = ci.getMinSenderNumber();
		int maxSenderNum = ci.getMaxSenderNumber();
		
		CustomerMessageQueue operatorMessageQueue = new LongConnectionCustomerMessageQueue(
				ci, connManager, minSenderNum, maxSenderNum, isServer);
		ssid2messageQueues.put(ssid, operatorMessageQueue);
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
		

		NodeManager nm = new NodeManager(ci, minSenderNum, maxSenderNum, true);
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

			putConnectionManager(ssid, ni.getNodeName(), ncm);

			connectionMap = ni.getConnectionMap(isServer);
			Iterator itConnection = connectionMap.values().iterator();
			ConnectionInfo connInfo;
			while (itConnection.hasNext()) { // while has connection
				connInfo = (ConnectionInfo) itConnection.next();
				Connection conn = new NodeServerConnection(true);
				conn.initialize(connInfo);
				// add the connection into NodeConnectionManager
				ncm.insertConnection(conn);
			}
		} // end has node

		ssid2messageQueues.put(ssid, nm);
	}

}
