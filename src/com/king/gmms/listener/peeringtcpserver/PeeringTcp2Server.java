package com.king.gmms.listener.peeringtcpserver;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.PeeringTcp2Session;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.customerconnectionfactory.MultiSmppServerFactory;
import com.king.gmms.customerconnectionfactory.PeeringTcp2ServerFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.listener.AbstractServer;

/**
 * <p>Title: </p>
 * <p/>
 * <p>Description: </p>
 * <p/>
 * Common Frameworked
 * Common Logged
 * <p>Copyright: Copyright (c) 2001-2010</p>
 * <p/>
 * <p>Company: King</p>
 *
 * @version 6.1
 */
public class PeeringTcp2Server extends AbstractServer {
    private static SystemLogger log = SystemLogger.getSystemLogger(PeeringTcp2Server.class);
    private final static String PROTOCOL_NAME = "Peering2";
    private InternalAgentConnectionFactory agentFactory = null;
    private PeeringTcp2ServerFactory serverFactory = null;
    public PeeringTcp2Server() {
    	this.port = Integer.parseInt(gmmsUtility.getModuleProperty("Port","3203"));
        serverFactory = PeeringTcp2ServerFactory.getInstance();
    }
    /**
     * initConnectionFactory
     */
    private void initConnectionFactory(){
    	try {
            ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager().
                getSsidByProtocol(PROTOCOL_NAME);

            if (alSsid != null) {
            	A2PCustomerInfo ci;
                for (int i = 0; i < alSsid.size(); i++) {
                    int ssid = alSsid.get(i);
                    ci = gmmsUtility.getCustomerManager().
                        getCustomerBySSID(ssid);
                    serverFactory.initConnectionFactory(ssid, ci.getConnectionType());
                }
            } //end of alSsid != null
        }
        catch (Exception e) {
            log.error(e, e);
            System.exit( -1);
        }
    }
    /**
     * start the to listen the port
     *
     * @param clientSocket Socket
     * @throws IOException
     */
    protected void createSession(Socket clientSocket) throws IOException {
        if(clientSocket != null) {
            new PeeringTcp2Session(clientSocket);
        }
    }

    public boolean startService() {
        try {
        	 boolean isRegister = super.startService();
             if(!isRegister){
             	log.warn("module register failed!");
             }
             startAgentConnection();    
             initConnectionFactory();
            return isRegister;
        }catch(Exception ex) {
            System.exit(-1);
            return false;
        }
    }
    /**
     * stopService
     *
     * @return boolean
     * @todo Implement.framework.A2PService method
     */
    public boolean stopService() {
    	super.stopService();
        try {
            agentListener.stop();
        }
        catch (Exception ioe) {
            log.error("Error occur while attempt to stop SMPP Server.", ioe);
        }
        finally {
            running = false;
        }
        return true;
    }
    /**
     * start agent message queue and listener
     */
    private void startAgentConnection(){
    	 //start MessageQueue of InternalAgent
        agentFactory = InternalAgentConnectionFactory.getInstance();
        agentFactory.setCustomerFactory(serverFactory);
        ModuleManager moduleManager = ModuleManager.getInstance();
        List<String> moduleNameList = moduleManager.getRouterModules();
        if(moduleNameList != null){
        	for(String routerModuleName:moduleNameList){
        		agentFactory.initInternalConnectionFactory(routerModuleName);
        	}
        }
        agentListener.start();
    }
}
