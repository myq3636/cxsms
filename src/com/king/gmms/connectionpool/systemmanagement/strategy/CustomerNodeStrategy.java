package com.king.gmms.connectionpool.systemmanagement.strategy;

import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.messagequeue.NodeManager;
import com.king.message.gmms.GmmsMessage;

public class CustomerNodeStrategy implements SessionStrategyInterface{
	
    private NodeManager nodeManager = null;
    private A2PMultiConnectionInfo cInfo  = null;
    
    
    public CustomerNodeStrategy(A2PMultiConnectionInfo cInfo,NodeManager nodeManager) {
		this.nodeManager = nodeManager;
		this.cInfo = cInfo;
	}

	public Session getSession(GmmsMessage msg) {
        Session session = null;
        Node node = null;
        node = nodeManager.getNode(msg);
        
        if(node != null){
        	session = node.getSession(msg);
        }
        
		return session;
	}   
    
    
    
    
}
