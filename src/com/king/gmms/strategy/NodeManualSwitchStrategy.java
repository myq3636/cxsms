package com.king.gmms.strategy;

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
public class NodeManualSwitchStrategy
    extends NodeStrategy {
    private static SystemLogger log = SystemLogger.getSystemLogger(
        NodeManualSwitchStrategy.class);
    private Node manualSwitchNode;

    public NodeManualSwitchStrategy(NodeManager nodeManager) {
        this.strategyType = StrategyType.ManualSwitch;
        this.manualSwitchNode = nodeManager.getManualSwitchNode();
        this.nodeManager = nodeManager;
    }

    public Object execute(GmmsMessage msg) {
        Node tempNode;

        if (null == manualSwitchNode) {
            log.error(msg,
                      "manualSwitchNode is null, put message into database");
            return null;
        }
        else {
            tempNode = nodeManager.getAvailableNode(manualSwitchNode.
                getNodeName(), false);
        }

        if (null == tempNode) {
            return null;
        }
        else if (tempNode.getNodeName().equalsIgnoreCase(manualSwitchNode.getNodeName())) {
            return tempNode;
        }
        else {
            log.error(msg,
                      "manualSwitchNode is not active, put message into database, manualSwitchNode: " +
                      manualSwitchNode.getNodeName());
            return null;
        }
    }
}
