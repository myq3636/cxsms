package com.king.gmms.connectionpool.node;

import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.connection.NodeConnectionManagerInterface;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.domain.NodeInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.strategy.ConnectionLoadBalanceStrategy;
import com.king.gmms.strategy.ConnectionOriginalWayStrategy;
import com.king.gmms.strategy.ConnectionPrimaryStrategy;
import com.king.gmms.strategy.ConnectionRandomStrategy;
import com.king.gmms.strategy.ConnectionSameIPStrategy;
import com.king.gmms.strategy.ConnectionSameSessionStrategy;
import com.king.gmms.strategy.ConnectionStrategy;
import com.king.gmms.strategy.StrategyType;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class Node {
    protected String nodeName;
    private NodeStatus status = NodeStatus.initial;
    private NodeType type = NodeType.TR;
    private NodeConnectionManagerInterface nodeConnectionManager = null;
    protected ConnectionStrategy newMsgStrategy;
    protected ConnectionStrategy drStrategy;
    protected ConnectionStrategy responseStrategy;
    protected ConnectionStrategy sameSessionStrategy;
    private MultiNodeCustomerInfo cInfo  = null;
    private boolean isServer;
    private NodeInfo nodeInfo;
    private Map connectionMap ;
	protected boolean isEnableSysMgt = false;


	public void init(MultiNodeCustomerInfo cInfo, NodeConnectionManagerInterface ncm) {
    	this.cInfo = cInfo;
    	this.nodeConnectionManager = ncm;
    	connectionMap = nodeInfo.getConnectionMap(isServer);
        String newMsgPolicy = cInfo.getSubmitConnectionPolicy();
        String newMsgOpt = cInfo.getPrimarySubmitConnection();
        String drPolicy = cInfo.getDrConnectionPolicy();
        String drOpt = cInfo.getPrimaryDRConnection();
        newMsgStrategy = getStrategy(newMsgPolicy, nodeConnectionManager, newMsgOpt);
        newMsgStrategy.setConnectionMap(connectionMap);
        drStrategy = getStrategy(drPolicy, nodeConnectionManager, drOpt);
        sameSessionStrategy = getStrategy("SameSession", nodeConnectionManager, null);
        drStrategy.setConnectionMap(connectionMap);
        String responsePolicy = cInfo.getResponseConnectionPolicy();
        responseStrategy = getStrategy(responsePolicy, nodeConnectionManager, null);
		isEnableSysMgt = GmmsUtility.getInstance().isSystemManageEnable();
    }
    
    public Session getSession(GmmsMessage msg){
    	Session session = null;
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())||
                GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
            if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					session = sameSessionStrategy.execute(msg);
					if (session == null) {
						session = newMsgStrategy.execute(msg);
					}
				} else {
					session = newMsgStrategy.execute(msg);
				}
			} else {
				session = newMsgStrategy.execute(msg);
			}
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					session = sameSessionStrategy.execute(msg);
					if (session == null) {
						session = drStrategy.execute(msg);
					}
				} else {
					session = drStrategy.execute(msg);
				}
			} else {
				session = drStrategy.execute(msg);
			}
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())) {
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					session = sameSessionStrategy.execute(msg);
					if (session == null) {
						session = drStrategy.execute(msg);
					}
				} else {
					session = drStrategy.execute(msg);
				}
			} else {
				session = drStrategy.execute(msg);
			}
        }
        else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType()) ||
                 GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType()) ||
                 GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msg.getMessageType())) {
            session = responseStrategy.execute(msg);
        }
        else {
            msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
            return null;
        }
        return session;
    }

    public Node(NodeInfo nodeInfo, boolean isServer) {
    	this.nodeInfo = nodeInfo;
    	this.nodeName = nodeInfo.getNodeName();
    	this.type = NodeType.getNodeType(nodeInfo.getNodeType());
    	this.isServer = isServer;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public NodeType getType() {
        return type;
    }

    public String getNodeName() {
        return nodeName;
    }
    
    /***
     *get strategy by policy name
     *Policy: configurable policy
     * connMan: connection manager
     * optionValue: primary connection if exist
     *     *
     */
    public ConnectionStrategy getStrategy(String policy, ConnectionManager connMan, String optionValue){
        StrategyType type = StrategyType.getStrategyType(policy);
        ConnectionStrategy strategy = null;
        switch(type){
            case Primary:
                strategy = new ConnectionPrimaryStrategy(connMan, optionValue);
                break;
            case OriginalWay:
                strategy = new ConnectionOriginalWayStrategy(connMan);
                break;
            case LoadBalance:
                strategy = new ConnectionLoadBalanceStrategy(connMan);
                break;
            case SameIP:
                strategy = new ConnectionSameIPStrategy(connMan);
                break;
            case Random:
                strategy = new ConnectionRandomStrategy(connMan);
                break;
            case SameSession:
                strategy = new ConnectionSameSessionStrategy(connMan);
        }
        return strategy;
    }
    
    
    public Map getConnectionMap() {
		return connectionMap;
	}
}

