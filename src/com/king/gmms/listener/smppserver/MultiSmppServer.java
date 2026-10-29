package com.king.gmms.listener.smppserver;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.customerconnectionfactory.*;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.listener.*;
import com.king.gmms.messagequeue.DRStreamConsumer;
import com.king.gmms.routing.ADSServerMonitor;
import com.king.gmms.ha.SmppServerAssignmentResolver;
import com.king.gmms.ha.SmppServerSessionRegistry;

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
    private ScheduledExecutorService sessionRegistryReconcileExecutor = null;
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
            int total = 0;
            int accepted = 0;
            int skipped = 0;
            String currentModule = System.getProperty("module");
            SmppServerAssignmentResolver assignmentResolver = SmppServerAssignmentResolver.getInstance();

            if (alSsid != null) {
            	A2PCustomerInfo ci;
                for (int i = 0; i < alSsid.size(); i++) {
                    total++;
                    int ssid = alSsid.get(i);
                    ci = gmmsUtility.getCustomerManager().
                        getCustomerBySSID(ssid);
                    if (!assignmentResolver.shouldLoadOnModule(ci, currentModule)) {
                        skipped++;
                        log.info("Skip SMPP server customer by assignment. module={}, ssid={}, assigned={}",
                                currentModule, ssid, assignmentResolver.assignedModules(ci));
                        continue;
                    }
                    accepted++;
                    serverFactory.initConnectionFactory(ssid, ci.getConnectionType());
                }
            } //end of alSsid != null
            log.info("SMPP server assignment init summary. module={}, totalSMPP={}, accepted={}, skipped={}",
                    currentModule, total, accepted, skipped);
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
        if (clientSocket == null || !SmppServerConnectionLimiter.getInstance().admit(clientSocket, module)) {
            return;
        }
        try {
            new MultiSmppSession(clientSocket);
        } catch (Throwable t) {
            SmppServerConnectionLimiter.getInstance().failed(clientSocket);
            throw t;
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
            //startAgentConnection();    
            smppAsync = Boolean.parseBoolean(gmmsUtility.getModuleProperty(
                "Asynchronous", "true").toLowerCase().trim());
            log.info("listener started by AbstractServer. asynchronous={}", smppAsync);
            log.info( "{} starting...",module);
            // V5.0 Start DR Consumer (Doorbell pattern, no registerSSID needed)
            DRStreamConsumer.getInstance().start();
            startSessionRegistryReconcile();
            
            return true;
        }
        catch (Exception ex) {
            log.fatal("startService initialize fail!", ex);
            return false;
        }
    }

    private void startSessionRegistryReconcile() {
        final SmppServerSessionRegistry registry = SmppServerSessionRegistry.getInstance();
        if (!registry.isEnabled() || sessionRegistryReconcileExecutor != null) {
            return;
        }
        sessionRegistryReconcileExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(A2PThreadGroup.getInstance(), r, "SmppServerSessionRegistryReconcile");
                t.setDaemon(true);
                return t;
            }
        });
        long intervalMs = registry.reconcileIntervalMs();
        sessionRegistryReconcileExecutor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    serverFactory.reconcileServerSessionRegistry();
                } catch (Exception e) {
                    log.warn("SMPP server session registry reconcile failed.", e);
                }
            }
        }, Math.min(5000L, intervalMs), intervalMs, TimeUnit.MILLISECONDS);
        log.info("SMPP server session registry reconcile started. intervalMs={}", intervalMs);
    }

    private void stopSessionRegistryReconcile() {
        if (sessionRegistryReconcileExecutor != null) {
            sessionRegistryReconcileExecutor.shutdownNow();
            sessionRegistryReconcileExecutor = null;
        }
    }
    /**
     * start agent message queue and listener
     */
    private void startAgentConnection(){
    	 //start MessageQueue of InternalAgent
			/*
			 * agentFactory = InternalAgentConnectionFactory.getInstance();
			 * agentFactory.setCustomerFactory(serverFactory); ModuleManager moduleManager =
			 * ModuleManager.getInstance(); List<String> moduleNameList =
			 * moduleManager.getRouterModules(); if(moduleNameList != null){ for(String
			 * routerModuleName:moduleNameList){
			 * agentFactory.initInternalConnectionFactory(routerModuleName); } }
			 */
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
           // agentListener.stop();
        }
        catch (Exception ioe) {
            log.error("Error occur while attempt to stop SMPP Server.", ioe);
        }
        finally {
            running = false;
            // V5.0 Stop DR Consumer
            DRStreamConsumer.getInstance().stop();
            stopSessionRegistryReconcile();
        }
        return true;
    }
}
