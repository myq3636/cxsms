package com.king.gmms.strategy;

import java.util.Map;

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
public class NodeLoadBalanceStrategy
    extends NodeStrategy {
    private static SystemLogger log = SystemLogger.getSystemLogger(
        NodeLoadBalanceStrategy.class);
    private int lastNodePosistion = 0;
    private Object mutex = new Object();
    private Map<String, Node> initialNodeMap;
    private Map<String, Node> activeNodeMap;

    public NodeLoadBalanceStrategy(NodeManager nodeManager) {
        this.strategyType = StrategyType.LoadBalance;
        this.nodeManager = nodeManager;
        this.activeNodeMap = nodeManager.getActiveNodeMap();
        this.initialNodeMap = nodeManager.getInitialNodeMap();
    }

    public Object execute(GmmsMessage msg) {
        int nodePosition = 0;
        int nodesNum = activeNodeMap.size();

        if (nodesNum <= 0) {
            nodesNum = initialNodeMap.size();
            if(nodesNum <= 0) {
                log.warn(msg, "all nodes are inactive, send to deadNode.");
                return null;
            }
        }
        else if (1 == nodesNum) {
            nodePosition = 1;
            lastNodePosistion = 1;
        }
        else {
            synchronized (mutex) {
                nodePosition = (lastNodePosistion) % nodesNum + 1;
                lastNodePosistion = nodePosition;
            }
        }

        return nodeManager.getAvailableNode(nodePosition);
    }
}
