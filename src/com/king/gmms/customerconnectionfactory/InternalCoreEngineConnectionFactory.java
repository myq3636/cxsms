package com.king.gmms.customerconnectionfactory;

import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.connection.ConnectionManagerImpl;
import com.king.gmms.connectionpool.connection.MultiClientConnectionImpl;
import com.king.gmms.connectionpool.connection.MultiServerConnectionImpl;
import com.king.gmms.connectionpool.session.InternalCoreEngineSession;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;

public class InternalCoreEngineConnectionFactory extends AbstractInternalConnectionFactory{

	private static InternalCoreEngineConnectionFactory instance =  new InternalCoreEngineConnectionFactory();
	private ModuleManager moduleManager = null;
	
	private InternalCoreEngineConnectionFactory(){
		super();
		moduleManager = ModuleManager.getInstance();
	}
	
	public static InternalCoreEngineConnectionFactory getInstance(){
		return instance;
	}
	

	public boolean initInternalConnectionFactory(String moduleName) {
		if(moduleName == null){
			return false;
		}
		ConnectionManager connectionManager = new ConnectionManagerImpl();
		ModuleConnectionInfo connInfo = moduleManager.getServiceConnectionInfo(moduleName);	
		if(connInfo.getInSessionNum()>0){
	    	//Client
			Connection clientConnection = new MultiClientConnectionImpl(true);
	        clientConnection.initialize(connInfo);
	        //add the connection into ConnectionManager
	        connectionManager.insertConnection(clientConnection);
	        
	        for (int i = 0; i < connInfo.getInSessionNum(); i++) {
	            InternalCoreEngineSession session = new InternalCoreEngineSession(connInfo);
	            session.setConnectionManager(connectionManager);
	            //add the session into ConnectionManager
	            connectionManager.insertSession(clientConnection.getConnectioName(), session);
	        }
			module2ClientConnectionManagers.put(moduleName, connectionManager);
		}
		
		//Server
		connectionManager = new ConnectionManagerImpl();
		Connection serverConn = new MultiServerConnectionImpl(true);
		connInfo.setSessionNum(-1);//cancel session limit
		serverConn.initialize(connInfo);
        //add the connection into ConnectionManager
        connectionManager.insertConnection(serverConn);
		module2ServerConnectionManagers.put(moduleName, connectionManager);
		
		return true;
	}
	
}
