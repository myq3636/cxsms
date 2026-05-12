package com.king.gmms.strategy;

import java.util.HashMap.*;

import com.king.framework.SystemLogger;
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
public class NodePrimaryStrategy
    extends NodeStrategy {
    private static SystemLogger log = SystemLogger.getSystemLogger(
        NodePrimaryStrategy.class);
    String primaryNodeName;

    public NodePrimaryStrategy(NodeManager nodeManager, boolean isNewMessage) {
        this.strategyType = StrategyType.Primary;
        this.nodeManager = nodeManager;
        if (isNewMessage) {
            this.primaryNodeName = nodeManager.getSubmitPrimaryNodeName();
        }
        else {
            this.primaryNodeName = nodeManager.getDrPrimaryNodeName();
        }
    }

    public Object execute(GmmsMessage msg) {
        Node tempNode = nodeManager.getAvailableNode(primaryNodeName, true);

        if (null == tempNode) {
            return null;
        }
        else if (tempNode.getNodeName().equalsIgnoreCase(primaryNodeName)) {
            return tempNode;
        }
        else {
            log.warn(msg,
                     "primary node is inactive, send to backup node: " +
                     tempNode.getNodeName());
            return tempNode;
        }
    }
}
