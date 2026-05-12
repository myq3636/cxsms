package com.king.gmms.customerconnectionfactory;

import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.connection.ConnectionManagerImpl;
import com.king.gmms.connectionpool.connection.MultiClientConnectionImpl;
import com.king.gmms.connectionpool.connection.MultiServerConnectionImpl;
import com.king.gmms.connectionpool.session.InternalMQMSession;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;


public class InternalMQMConnectionFactory extends AbstractInternalConnectionFactory{
 	
	private static InternalMQMConnectionFactory instance = null;
    private ModuleManager moduleManager = null;

	private InternalMQMConnectionFactory(){
		super();
		moduleManager = ModuleManager.getInstance();
	}
	
	public static synchronized InternalMQMConnectionFactory getInstance(){
		if(null == instance){
			instance = new InternalMQMConnectionFactory();
		}
		return instance;
	}
	/**
	 * initInternalConnectionFactory
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
	            InternalMQMSession session = new InternalMQMSession(connInfo);
	            session.setConnectionManager(connectionManager);
	            //add the session into ConnectionManager
	            connectionManager.insertSession(clientConnection.getConnectioName(), session);
	        }

			module2ClientConnectionManagers.put(connInfo.getModuleName(), connectionManager);
		}
		
		//Server
		ConnectionManager serverconnectionManager = new ConnectionManagerImpl();
		Connection serverConn = new MultiServerConnectionImpl(true);
		connInfo.setSessionNum(-1);//cancel session limit
		serverConn.initialize(connInfo);
        //add the connection into ConnectionManager
		serverconnectionManager.insertConnection(serverConn);
        
		module2ServerConnectionManagers.put(connInfo.getModuleName(), serverconnectionManager);
		
		return true;
	}
}
