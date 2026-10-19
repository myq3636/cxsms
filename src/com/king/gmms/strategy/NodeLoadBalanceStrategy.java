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
    private Map<String, Node> activeNodeMap;

    public NodeLoadBalanceStrategy(NodeManager nodeManager) {
        this.strategyType = StrategyType.LoadBalance;
        this.nodeManager = nodeManager;
        this.activeNodeMap = nodeManager.getActiveNodeMap();
    }

    public Object execute(GmmsMessage msg) {
        int nodePosition;
        int nodesNum = activeNodeMap.size();

        // H-4: active 为空时不再走 initialNodeMap 死逻辑(getAvailableNode 只迭代 activeNodeMap，
        // 无法服务 initial 节点)，直接返回 null 落 deadNode，避免用 nodePosition=0 调用导致歧义。
        if (nodesNum <= 0) {
            log.warn(msg, "no active node available, send to deadNode.");
            return null;
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
