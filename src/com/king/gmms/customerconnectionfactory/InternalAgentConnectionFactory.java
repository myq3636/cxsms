package com.king.gmms.customerconnectionfactory;

import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.connection.ConnectionManagerImpl;
import com.king.gmms.connectionpool.connection.MultiClientConnectionImpl;
import com.king.gmms.connectionpool.connection.MultiServerConnectionImpl;
import com.king.gmms.connectionpool.session.InternalAgentSession;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;


public class InternalAgentConnectionFactory extends AbstractInternalConnectionFactory{
 	
	private static InternalAgentConnectionFactory instance = new InternalAgentConnectionFactory();
    private ModuleManager moduleManager = null;
    private CustomerConnectionFactory customerFactory = null;
	private InternalAgentConnectionFactory(){
		super();
		moduleManager = ModuleManager.getInstance();
	}
	
	public static synchronized InternalAgentConnectionFactory getInstance(){
		return instance;
	}

	public CustomerConnectionFactory getCustomerFactory() {
		return customerFactory;
	}

	public void setCustomerFactory(CustomerConnectionFactory customerFactory) {
		this.customerFactory = customerFactory;
	}

	/**
	 * start message queue to DeliveryRouter
	 */
	public boolean initInternalConnectionFactory(String coreModuleName) {
		if(coreModuleName == null){
			return false;
		}
		ConnectionManager connectionManager = new ConnectionManagerImpl();
		
		ModuleConnectionInfo selfConnInfo = moduleManager.getServiceConnectionInfo(selfModule);
		ModuleConnectionInfo connInfo = moduleManager.getRouterConnectionInfo(coreModuleName);
		if(selfConnInfo.getOutSessionNum()>0){
	    	//Client
			Connection clientConnection = new MultiClientConnectionImpl(true);
	        clientConnection.initialize(connInfo);
	        //add the connection into ConnectionManager
	        connectionManager.insertConnection(clientConnection);
	        
	        for (int i = 0; i < selfConnInfo.getOutSessionNum(); i++) {
	            InternalAgentSession session = new InternalAgentSession(connInfo);
	            session.setConnectionManager(connectionManager);
	            //add the session into ConnectionManager
	            connectionManager.insertSession(clientConnection.getConnectioName(), session);
	        }

			module2ClientConnectionManagers.put(coreModuleName, connectionManager);
		}
		
		//Server
		ConnectionManager serverconnectionManager = new ConnectionManagerImpl();
		Connection serverConn = new MultiServerConnectionImpl(true);
		connInfo.setSessionNum(-1);//cancel session limit
		serverConn.initialize(connInfo);
        //add the connection into ConnectionManager
		serverconnectionManager.insertConnection(serverConn);
        
		module2ServerConnectionManagers.put(coreModuleName, serverconnectionManager);
		
		return true;
	}
}
