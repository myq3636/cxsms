package com.king.gmms.connectionpool.systemmanagement.strategy;

import java.util.Map;


import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.sender.CustomerMessageSender;
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

public class CustomerStrategy implements SessionStrategyInterface{
	private static SystemLogger log = SystemLogger.getSystemLogger(CustomerStrategy.class);
    private ConnectionManager connManager = null;
    private A2PMultiConnectionInfo cInfo  = null;
    protected ConnectionStrategy newMsgStrategy;
    protected ConnectionStrategy drStrategy;
    protected ConnectionStrategy responseStrategy;
    private int ssid = -1;
    private Map connectionMap;
    
    public CustomerStrategy(A2PMultiConnectionInfo ci, Map connectionMap, ConnectionManager connManager,boolean isServer) {
    	cInfo = ci;
    	this.connectionMap = connectionMap;
    	this.connManager = connManager;
        String newMsgPolicy = cInfo.getSubmitConnectionPolicy();
        log.info("customer the submit policy is {}, ssid is:{}",newMsgPolicy, cInfo.getSSID());
        String newMsgOpt = cInfo.getPrimarySubmitConnection();
        String drPolicy = cInfo.getDrConnectionPolicy();
        String drOpt = cInfo.getPrimaryDRConnection();
        SingleNodeCustomerInfo nInfo = (SingleNodeCustomerInfo)cInfo;
        connectionMap = nInfo.getConnectionMap(isServer);
        newMsgStrategy = getStrategy(newMsgPolicy, connManager, newMsgOpt);
        newMsgStrategy.setConnectionMap(connectionMap);
        drStrategy = getStrategy(drPolicy, connManager, drOpt);
        drStrategy.setConnectionMap(connectionMap);
        String responsePolicy = cInfo.getResponseConnectionPolicy();
        responseStrategy = getStrategy(responsePolicy, connManager, null);
        ssid = ci.getSSID();
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


	public Session getSession(GmmsMessage msg) {
		
        Session session = null;
        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())||
            GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
        	
            session = newMsgStrategy.execute(msg);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
            session = drStrategy.execute(msg);
        }
        else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())) {
            session = drStrategy.execute(msg);
        }
        else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType()) ||
                 GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType()) ||
                 GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msg.getMessageType())) {
            session = responseStrategy.execute(msg);
        }

		return session;
	}
}
