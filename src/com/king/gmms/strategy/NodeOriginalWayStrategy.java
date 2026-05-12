package com.king.gmms.strategy;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.messagequeue.NodeManager;
import com.king.message.gmms.GmmsMessage;

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
public class NodeOriginalWayStrategy
    extends NodeStrategy {
    private static SystemLogger log = SystemLogger.getSystemLogger( NodeOriginalWayStrategy.class);

    public NodeOriginalWayStrategy(NodeManager nodeManager) {
        this.strategyType = StrategyType.OriginalWay;
        this.nodeManager = nodeManager;
    }

    public Object execute(GmmsMessage msg) {

        String originalConnection = msg.getConnectionID();

    	try {

            String originalNodeName = GmmsUtility.getInstance().
                getCustomerManager().getNodeIDByConnectionID(originalConnection);
            Node tempNode;

            if (null == originalNodeName) {
                log.error(msg,
                          "can not find node by originalConnection: " +
                          originalConnection);
                return null;
            }
            else {
                tempNode = nodeManager.getAvailableNode(originalNodeName, false);
            }

            if (null == tempNode) {
                return null;
            }
            else if (tempNode.getNodeName().equalsIgnoreCase(originalNodeName)) {
                return tempNode;
            }
            else {
                log.warn(msg,
                         "originalNode is not active, put message into database, originalNodeName: " +
                         originalNodeName);
                return null;
            }
    	} catch (Exception e) {
    		log.warn(msg, "NodeOriginalWayStrategy excute error.", e);
    		return null;
    	}

    }
}
