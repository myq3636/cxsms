package com.king.gmms.listener.smppserver;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.Socket;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.customerconnectionfactory.*;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.listener.*;
import com.king.gmms.messagequeue.DRStreamConsumer;
import com.king.gmms.routing.ADSServerMonitor;

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
public class MultiSmppServer
    extends AbstractServer {

    private final static String PROTOCOL_NAME = "SMPP";
    private static SystemLogger log = SystemLogger.getSystemLogger(MultiSmppServer.class);
    private boolean smppAsync;

    private MultiSmppServerFactory serverFactory = null;
    private InternalAgentConnectionFactory agentFactory = null;
    public MultiSmppServer() {
    	this.port = Integer.parseInt(gmmsUtility.getModuleProperty("Port","16272"));
        serverFactory = MultiSmppServerFactory.getInstance();
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
            new MultiSmppSession(clientSocket);
        }
    }


    /**
     * startService
     *
     * @return boolean
     * @todo Implement.framework.A2PService method
     */
    public boolean startService() {
        try {
            initConnectionFactory();
            //ADSServerMonitor.getInstance().start();//start thread to monitor the DNS server connection
            boolean isRegister = super.startService();
            if(!isRegister){
            	log.warn("module register failed!");
            }
            startAgentConnection();    
            smppAsync = Boolean.parseBoolean(gmmsUtility.getModuleProperty(
                "Asynchronous", "true").toLowerCase().trim());
            if (smppAsync) {
            		log.info("starting listener in separate thread.");
                new Thread(A2PThreadGroup.getInstance(), this,
                           "MultiSmppServer").start();
            } else {
            		log.info("going to listen in the context of current thread.");
                run();
            }
            log.info( "{} starting...",module);
            // V4.0 Start DR Consumer
            DRStreamConsumer.getInstance().start();
            
            // V4.0 Register node for failover discovery
            try {
                gmmsUtility.getRedisClient().getStateRedis().sadd("system:server:nodes", gmmsUtility.getNodeId());
            } catch (Exception e) {
                log.warn("Fail to register node in Redis: {}", e.getMessage());
            }
            return true;
        }
        catch (Exception ex) {
            log.fatal("startService initialize fail!", ex);
            return false;
        }
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
            // V4.0 Stop DR Consumer
            DRStreamConsumer.getInstance().stop();
        }
        return true;
    }
}
