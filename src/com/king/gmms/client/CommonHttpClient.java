package com.king.gmms.client;

import java.util.ArrayList;
import java.util.List;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.message.gmms.GmmsMessage;

public class CommonHttpClient extends AbstractClient {
	private static SystemLogger log = SystemLogger.getSystemLogger(CommonHttpClient.class);	
    private InternalAgentConnectionFactory agentFactory = null;
    private CommonHttpClientFactory customerFactory = null;
    
	public CommonHttpClient(){		 
			 customerFactory = CommonHttpClientFactory.getInstance();
			 customerFactory.initQueryMsgThread();	           		 
	}
	 /**
     * start agent message queue and listener
     */
    private void startAgentConnection(){
    	 //start MessageQueue of InternalAgent
        agentFactory = InternalAgentConnectionFactory.getInstance();
        agentFactory.setCustomerFactory(customerFactory);
        ModuleManager moduleManager = ModuleManager.getInstance();
        List<String> moduleNameList = moduleManager.getRouterModules();
        if(moduleNameList != null){
        	for(String routerModuleName:moduleNameList){
        		agentFactory.initInternalConnectionFactory(routerModuleName);
        	}
        }
    }

	public boolean startService() {
		if(!initSystemManagement()){
	   		  log.warn("module register failed!");
	    }
		startAgentConnection();
        agentListener.start();
		return true;
	}
    
	public boolean stopService() {
		if(canHandover || isEnableSysMgt){
			systemSession.moduleStop();
	        systemListener.stop();
	        if(systemSession!=null){
	        	systemSession.shutdown();
	        }
        }
		agentListener.stop();
		return false;
	}
}
