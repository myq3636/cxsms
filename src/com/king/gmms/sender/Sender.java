package com.king.gmms.sender;

import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.strategy.ConnectionInternalSameSessionStrategy;
import com.king.gmms.strategy.ConnectionLoadBalanceStrategy;
import com.king.gmms.strategy.ConnectionOriginalWayStrategy;
import com.king.gmms.strategy.ConnectionPrimaryStrategy;
import com.king.gmms.strategy.ConnectionRandomStrategy;
import com.king.gmms.strategy.ConnectionSameIPStrategy;
import com.king.gmms.strategy.ConnectionSameSessionStrategy;
import com.king.gmms.strategy.ConnectionStrategy;
import com.king.gmms.strategy.StrategyType;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.message.gmms.GmmsMessage;

public abstract class Sender extends RunnableMsgTask{
    protected boolean isServer = false;

    /***
     *get strategy by policy name
     *Policy: configurable policy
     * connMan: connection manager
     * optionValue: primary connection if exist
     * 
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
                break;
            case InternalSameSession:
                strategy = new ConnectionInternalSameSessionStrategy(connMan);
                break;
        }
        return strategy;
    }

    public abstract void deliver(GmmsMessage msg);

    public boolean isIsServer() {
        return isServer;
    }

    public void setIsServer(boolean isServer) {
        this.isServer = isServer;
    }

}
