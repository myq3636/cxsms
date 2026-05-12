package com.king.gmms.strategy;

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
public abstract class NodeStrategy
    implements Strategy {

    protected NodeManager nodeManager;
    protected StrategyType strategyType;

    public abstract Object execute(GmmsMessage msg);

    public StrategyType getStrategyType(){
        return strategyType;
    }
}
