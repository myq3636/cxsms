package com.king.gmms.messagequeue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.BindMode;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.ConnectionStatusInfo;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.connectionpool.node.NodeStatus;
import com.king.gmms.connectionpool.node.NodeType;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.domain.NodeInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.sender.CustomerMessageSender;
import com.king.gmms.sender.CustomerNodeMessageSender;
import com.king.gmms.strategy.NodeLoadBalanceStrategy;
import com.king.gmms.strategy.NodeManualSwitchStrategy;
import com.king.gmms.strategy.NodeOriginalWayStrategy;
import com.king.gmms.strategy.NodePrimaryStrategy;
import com.king.gmms.strategy.NodeRandomStrategy;
import com.king.gmms.strategy.NodeSameSessionStrategy;
import com.king.gmms.strategy.NodeStrategy;
import com.king.gmms.strategy.StrategyType;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

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
 * Copyright: Copyright (c) 2007
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class NodeManager extends CustomerMessageQueue implements Observer {
	private static SystemLogger log = SystemLogger.getSystemLogger(NodeManager.class);
	private int ssid = 0;
	private Map<String, HashMap<String, ConnectionStatus>> node2ConnectionsMap = new HashMap<String, HashMap<String, ConnectionStatus>>();
	private Map<String, Node> initialNodeMap = new HashMap<String, Node>();
	private Map<String, Node> activeNodeMap = new HashMap<String, Node>();
	private Map<String, Node> inactiveNodeMap = new HashMap<String, Node>();
	private Node manualSwitchNode = null;
	private Map<String, Node> connetion2NodeMap = new HashMap<String, Node>();
	private StrategyType submitStrategyType = StrategyType.Random;
	private StrategyType drStrategyType = StrategyType.Random;
	private StrategyType respStrategyType = StrategyType.SameSession;

	private String submitPrimaryNodeName = null;
	private String drPrimaryNodeName = null;
	private NodeStrategy submitNodeStrategy = null;
	private NodeStrategy drNodeStrategy = null;
	private NodeStrategy respNodeStrategy = null;
	protected boolean isEnableSysMgt = false;
	private Object mutex = new Object();
	private MultiNodeCustomerInfo customerInfo = null;
	private GmmsUtility gmmsUtility = null;
	
	private ExecutorService respSenderThreadPool;

	public NodeManager(MultiNodeCustomerInfo cst, int minSenderNum, int maxSenderNum,
			boolean isServer) {
		super((A2PCustomerInfo) cst, minSenderNum, maxSenderNum, isServer, "NodeManager_"+cst.getSSID());
		gmmsUtility = GmmsUtility.getInstance();
		customerInfo = cst;
		ssid = cst.getSSID();
		
		// response sender thread pool
        ThreadPoolProfile profile = new ThreadPoolProfileBuilder(this.queueName + "_Resp")
                .poolSize(minSenderNum).maxPoolSize(maxSenderNum).build();
        respSenderThreadPool = executorServiceManager.newExpiredThreadPool(this, "NodeManagerRespSender_" + cst.getSSID(), profile, this, queueTimeout);
	}

	public boolean init() {
		boolean result = true;
		try {
			if(log.isDebugEnabled()){
				log.debug("nodeManager init begin, ssid: {}", ssid);
			}
			isEnableSysMgt = GmmsUtility.getInstance().isSystemManageEnable();
			Map<String, NodeInfo> tempNodeMap = customerInfo.getNodeMap();
			if(log.isTraceEnabled()){
				log.trace("nodes size: {}", tempNodeMap.size());
			}
			Iterator<Map.Entry<String, NodeInfo>> iterNode = tempNodeMap
					.entrySet().iterator();
			NodeInfo nodeInfo;
			ConnectionInfo connInfo;
			while (iterNode.hasNext()) {
				nodeInfo = iterNode.next().getValue();
				log.trace("node name: {}, node type: {}", nodeInfo
						.getNodeName(), nodeInfo.getNodeType());
				Node node = new Node(nodeInfo, isServer);
				initialNodeMap.put(node.getNodeName(), node);

				node2ConnectionsMap.put(node.getNodeName(),
						new HashMap<String, ConnectionStatus>());
				Map<String, ConnectionInfo> tempConnMap = nodeInfo
						.getConnectionMap(isServer);
				Iterator<Map.Entry<String, ConnectionInfo>> iterConn = tempConnMap
						.entrySet().iterator();
				log.trace("{} connections size: {}", node.getNodeName(),
						tempConnMap.size());
				ArrayList<ConnectionInfo> connList = null;
				while (iterConn.hasNext()) {
					connInfo = iterConn.next().getValue();
					node2ConnectionsMap.get(node.getNodeName()).put(
							connInfo.getConnectionName(),
							ConnectionStatus.INITIAL);
					log.trace("{} node2ConnectionsMap put connection: {}", node
							.getNodeName(), connInfo.getConnectionName());
					connetion2NodeMap.put(connInfo.getConnectionName(), node);
					if (connInfo.getBindMode() == BindMode.Transmitter
							&& connInfo.isCreateReviver() == true) {

						ConnectionInfo newConn = new ConnectionInfo(connInfo);
						newConn.setBindMode(BindMode.Receiver);

						String name = newConn.getConnectionName();
						newConn.setConnectionName(name + "_R");

						if (isServer) {
							gmmsUtility.getCustomerManager().addServerInfoMap(
									newConn);
						}

						node2ConnectionsMap.get(node.getNodeName()).put(
								newConn.getConnectionName(),
								ConnectionStatus.INITIAL);
						log
								.trace(
										"{} node2ConnectionsMap put connection: {}",
										node.getNodeName(), newConn
												.getConnectionName());
						connetion2NodeMap
								.put(newConn.getConnectionName(), node);

						if (connList == null) {
							connList = new ArrayList<ConnectionInfo>();
						}
						connList.add(newConn);
						gmmsUtility.getCustomerManager().addConnNodeMapping(
								newConn.getConnectionName(),
								nodeInfo.getNodeName());
						gmmsUtility.getCustomerManager().addConnIPMapping(
								newConn.getConnectionName(), newConn.getURL());

					}
				}
				if (connList != null) {
					for (ConnectionInfo temp : connList) {
						tempConnMap.put(temp.getConnectionName(), temp);
					}
				}
			}

			if (customerInfo.isAutoMode()) {
				submitStrategyType = StrategyType.getStrategyType(customerInfo
						.getSubmitNodePolicy());
				if (StrategyType.Primary == submitStrategyType) {
					submitPrimaryNodeName = customerInfo.getPrimarySubmitNode();
				}
			} else {
				String manualNodeName = customerInfo.getManualNodeName();
				Node manualNode = getNode(manualNodeName);
				if (null == manualNode) {
					log.error("can not find manualSwitchNode: {}",
							manualNodeName);
					System.exit(-1);
				} else {
					manualSwitchNode = manualNode;
					submitStrategyType = StrategyType.ManualSwitch;
				}
			}

			drStrategyType = StrategyType.getStrategyType(customerInfo
					.getDrNodePolicy());
			if (StrategyType.Primary == drStrategyType) {
				drPrimaryNodeName = customerInfo.getPrimaryDRNode();
			}

		} catch (Exception ex) {
			log.error(ex, ex);
			result = false;
		}
		return result;
	}

	public Map<String, Node> getActiveNodeMap() {
		return activeNodeMap;
	}

	public Map<String, Node> getInitialNodeMap() {
		return initialNodeMap;
	}

	public Node getNode(String nodeName) {
		Node node = activeNodeMap.get(nodeName);
		if (null == node) {
			node = inactiveNodeMap.get(nodeName);
		}
		if (null == node) {
			node = initialNodeMap.get(nodeName);
		}
		return node;
	}

	public Node getManualSwitchNode() {
		return manualSwitchNode;
	}

	public String getSubmitPrimaryNodeName() {
		return submitPrimaryNodeName;
	}

	public String getDrPrimaryNodeName() {
		return drPrimaryNodeName;
	}

	public Node getAvailableNode(String nodeName, boolean needActiveNode) {
		synchronized (mutex) {
			if (activeNodeMap.containsKey(nodeName)) {
				return activeNodeMap.get(nodeName);
			} else if (initialNodeMap.containsKey(nodeName)) {
				return null;
			}

			if (needActiveNode) {
				Iterator<Map.Entry<String, Node>> it = activeNodeMap.entrySet()
						.iterator();
				while (it.hasNext()) {
					Node node = it.next().getValue();
					if (!node.getNodeName().equalsIgnoreCase(nodeName)) {
						return node;
					}
				}
			}
			return null;
		}
	}

	public Node getAvailableNode(int nodePosition) {
		synchronized (mutex) {
			Iterator<Map.Entry<String, Node>> iterNode = activeNodeMap
					.entrySet().iterator();
			Node node = null;
			int i = 1;
			while (iterNode.hasNext()) {
				node = iterNode.next().getValue();
				if (i >= nodePosition) {
					return node;
				}
				i++;
			}
			return node;
		}
	}

	public void update(Observable o, Object arg) {
		if (arg instanceof ConnectionStatusInfo) {
			ConnectionStatusInfo connStatusInfo = (ConnectionStatusInfo) arg;
			Node node = connetion2NodeMap.get(connStatusInfo.connectionName);
			if (node != null) {
				Map<String, ConnectionStatus> connectionsMap = node2ConnectionsMap
						.get(node.getNodeName());
				if (null == connectionsMap) {
					log.error("can not find connectionsMap for node: {}", node
							.getNodeName());
					return;
				}
				ConnectionStatus status = connectionsMap
						.get(connStatusInfo.connectionName);
				if (null == status) {
					log.error(
							"can not find connectionStatus for connection: {}",
							connStatusInfo.connectionName);
					return;
				} else if (connStatusInfo.status == status) {
					log.warn("the status of connection: {} is already {}",
							connStatusInfo.connectionName,
							connStatusInfo.status);
					return;
				} else if (connStatusInfo.status == ConnectionStatus.CONNECT) {
					handleConnectionConnect(node, connectionsMap,
							connStatusInfo.connectionName);
					return;
				} else if (connStatusInfo.status == ConnectionStatus.DISCONNECT) {
					handleConnectionDisconnect(node, connectionsMap,
							connStatusInfo.connectionName);
					return;
				}
			} else {
				log
						.error(
								"connection status update, can not find node, connection name: {}",
								connStatusInfo.connectionName);
			}
		}
	}

	private void handleConnectionConnect(Node node,
			Map<String, ConnectionStatus> connsMap, String connName) {
		synchronized (mutex) {
			connsMap.put(connName, ConnectionStatus.CONNECT);
			if(log.isInfoEnabled()){
				log.info("{} set connetion {} status to CONNECT", node
					.getNodeName(), connName);
			}
			if (node.getStatus() == NodeStatus.initial) {
				node.setStatus(NodeStatus.up);
				if(log.isInfoEnabled()){
					log.info("set node {} status is up, original status is initial",
						node.getNodeName());
				}
				if (node.getType() != NodeType.R) {
					initialNodeMap.remove(node.getNodeName());
					activeNodeMap.put(node.getNodeName(), node);
				}
			} else if (node.getStatus() == NodeStatus.down
					&& isAllConnectionsConnect(connsMap)) {
				node.setStatus(NodeStatus.up);
				if(log.isInfoEnabled()){
					log.info("set node {} status is up, original status is down",
						node.getNodeName());
				}
				if (node.getType() != NodeType.R) {
					inactiveNodeMap.remove(node.getNodeName());
					activeNodeMap.put(node.getNodeName(), node);
				}
			}
		}
		return;
	}

	private void handleConnectionDisconnect(Node node,
			Map<String, ConnectionStatus> connsMap, String connName) {
		synchronized (mutex) {
			connsMap.put(connName, ConnectionStatus.DISCONNECT);
			log.info("{} set connetion {} status to DISCONNECT", node
					.getNodeName(), connName);
			if (node.getStatus() == NodeStatus.initial
					&& isAllConnectionsDisconnect(connsMap)) {
				node.setStatus(NodeStatus.down);
				log
						.info(
								"set node {} status is down, original status is initial",
								node.getNodeName());
				if (node.getType() != NodeType.R) {
					initialNodeMap.remove(node.getNodeName());
					inactiveNodeMap.put(node.getNodeName(), node);
				}
			} else if (node.getStatus() == NodeStatus.up
					&& isAllConnectionsDisconnect(connsMap)) {
				node.setStatus(NodeStatus.down);
					log
							.info(
									"set node {} status is down, original status is up",
									node.getNodeName());
				if (node.getType() != NodeType.R) {
					activeNodeMap.remove(node.getNodeName());
					inactiveNodeMap.put(node.getNodeName(), node);
				}
			}
		}
		return;
	}

	private boolean isAllConnectionsConnect(
			Map<String, ConnectionStatus> connsMap) {
		Iterator<Map.Entry<String, ConnectionStatus>> iter = connsMap
				.entrySet().iterator();
		while (iter.hasNext()) {
			if (iter.next().getValue() != ConnectionStatus.CONNECT) {
				return false;
			}
		}
		return true;
	}

	private boolean isAllConnectionsDisconnect(
			Map<String, ConnectionStatus> connsMap) {
		Iterator<Map.Entry<String, ConnectionStatus>> iter = connsMap
				.entrySet().iterator();
		while (iter.hasNext()) {
			if (iter.next().getValue() != ConnectionStatus.DISCONNECT) {
				return false;
			}
		}
		return true;
	}

	public Node getNode(GmmsMessage msg) {
		String messageType = msg.getMessageType();
		Object node = null;
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)) {
			if (null == submitNodeStrategy) {
				submitNodeStrategy = createNodeStrategy(submitStrategyType,
						true);
			}
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					String connection = transaction.getConnectionName();
					if(connection != null && connetion2NodeMap.containsKey(connection)){
						node = connetion2NodeMap.get(connection);
					}else{
						node = submitNodeStrategy.execute(msg);
					}
				}else{
					node = submitNodeStrategy.execute(msg);
				}
			}else{
				node = submitNodeStrategy.execute(msg);
			}
			if (node != null)
				return (Node) node;
		} else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
						.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msg
						.getMessageType())) {

			if (null == respNodeStrategy) {
				respNodeStrategy = createNodeStrategy(respStrategyType, false);
			}
			node = respNodeStrategy.execute(msg);
			if (node != null)
				return (Node) node;
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())) {
			if (null == drNodeStrategy) {
				drNodeStrategy = createNodeStrategy(drStrategyType, false);
			}
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					String connection = transaction.getConnectionName();
					if(connection != null && connetion2NodeMap.containsKey(connection)){
						node = connetion2NodeMap.get(connection);
					}else{
						node = drNodeStrategy.execute(msg);
					}
				}else{
					node = drNodeStrategy.execute(msg);
				}
			}else{
				node = drNodeStrategy.execute(msg);
			}
			if (node != null)
				return (Node) node;
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(msg.getMessageType())) {
			if (null == drNodeStrategy) {
				drNodeStrategy = createNodeStrategy(drStrategyType, false);
			}
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					String connection = transaction.getConnectionName();
					if(connection != null && connetion2NodeMap.containsKey(connection)){
						node = connetion2NodeMap.get(connection);
					}else{
						node = drNodeStrategy.execute(msg);
					}
				}else{
					node = drNodeStrategy.execute(msg);
				}
			}else{
				node = drNodeStrategy.execute(msg);
			}
			if (node != null)
				return (Node) node;
		} else {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
		return null;
	}

	private NodeStrategy createNodeStrategy(StrategyType strategyType,
			boolean isNewMessage) {
		if (StrategyType.Primary == strategyType) {
			return new NodePrimaryStrategy(this, isNewMessage);
		} else if (StrategyType.LoadBalance == strategyType) {
			return new NodeLoadBalanceStrategy(this);
		} else if (StrategyType.OriginalWay == strategyType) {
			return new NodeOriginalWayStrategy(this);
		} else if (StrategyType.Random == strategyType) {
			return new NodeRandomStrategy(this);
		} else if (StrategyType.ManualSwitch == strategyType) {
			return new NodeManualSwitchStrategy(this);
		} else if (StrategyType.SameSession == strategyType) {
			return new NodeSameSessionStrategy(this);
		} else {
			log.error("Unexpected StrategyType: {}", strategyType);
			return new NodeRandomStrategy(this);
		}
	}

	public void setCustomerInfo(MultiNodeCustomerInfo customerInfo) {
		this.customerInfo = customerInfo;
	}

	public void timeout(Object message) {
		GmmsMessage msg = (GmmsMessage) message;
		ModuleManager moduleManager = ModuleManager.getInstance();
		String routerQueue = null;
        if(log.isInfoEnabled()){
        	log.info(msg,"{} is timeout in customer message queue",msg.getMessageType());
        }
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg
						.getMessageType())) {
			msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
			TransactionURI transaction = msg.getInnerTransaction();
			routerQueue = transaction.getConnectionName();
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())) {
			msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
			TransactionURI transaction = msg.getInnerTransaction();
			routerQueue = transaction.getConnectionName();
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(msg.getMessageType())) {
			msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
			TransactionURI transaction = msg.getInnerTransaction();
			routerQueue = transaction.getConnectionName();
		} else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
						.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP
						.equalsIgnoreCase(msg.getMessageType())) {
			msg.setStatusCode(1);
			msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
			TransactionURI innerTransaction = msg.getInnerTransaction();
			routerQueue = innerTransaction.getConnectionName();
		} else {
			routerQueue = moduleManager.selectRouter(msg);
		}
		InternalAgentConnectionFactory factory = InternalAgentConnectionFactory
				.getInstance();
		OperatorMessageQueue msgQueue = factory.getMessageQueue(msg,
				routerQueue);
		if (msgQueue != null) {
			msgQueue.putMsg(msg);
		}
	}

	public Node getConn2Node(String connectionName) {
		if (connectionName != null) {
			return connetion2NodeMap.get(connectionName);
		}
		return null;
	}

	/** 
	 * @param msg
	 * @return
	 * @see com.king.gmms.messagequeue.CustomerMessageQueue#putMsg(com.king.message.gmms.GmmsMessage)
	 */
	@Override
	public boolean putMsg(GmmsMessage msg) {
		if(msg == null){
			return false;
		}
		if (log.isTraceEnabled()) {
			log.trace(msg, "submit to CustomerNodeMessageSender thread pool");
		}
		
		try {
			String msgType = msg.getMessageType();
			if(GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msgType)
	    			||GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msgType)
	    			||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msgType)
	    			||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(msgType)){
				respSenderThreadPool.execute(new CustomerNodeMessageSender(msg, this, cst));
			}else{
				senderThreadPool.execute(new CustomerNodeMessageSender(msg, this, cst));
			}
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info(msg, e, e);
			}
			return false;
		}
		
		return true;
	}

    public void stopMessageQueue() {
	    super.stopMessageQueue();
    	if (executorServiceManager != null) {
    		executorServiceManager.shutdown(respSenderThreadPool);
    	}
     }
	
}
