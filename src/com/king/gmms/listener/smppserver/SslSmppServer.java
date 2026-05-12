package com.king.gmms.listener.smppserver;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.SslSmppSession;
import com.king.gmms.connectionpool.ssl.SslConfiguration;
import com.king.gmms.connectionpool.ssl.SslContextFactory;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.customerconnectionfactory.MultiSmppServerFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.listener.AbstractSslServer;
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
public class SslSmppServer extends AbstractSslServer {

    private final static String PROTOCOL_NAME = "SSLSMPP";
    private static SystemLogger log = SystemLogger.getSystemLogger(SslSmppServer.class);
    private boolean smppAsync;

    private MultiSmppServerFactory serverFactory = null;
    private InternalAgentConnectionFactory agentFactory = null;
    private SslContextFactory sslContextFactory = null;
    
    public SslSmppServer() {
    	this.port = Integer.parseInt(gmmsUtility.getModuleProperty("Port","16282"));
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
        	new SslSmppSession(clientSocket,sslContextFactory);
        }
    }

	/**
	 * start the thread
	 */
	public void run() {
		try {
			if (serverSocket == null) {

				if (port <= 0) {
					throw new IOException("Port number is:" + port);
				}
				try{
					SslConfiguration config = gmmsUtility.getSslConfiguration();
					sslContextFactory = new SslContextFactory(config);
					serverSocket = sslContextFactory.newSslServerSocket(port);
				}catch(Exception e){
					log.error("Can not create the TLS server socket, and excpeiton is {}",e.getMessage());
					return;
				}
				serverSocket.setReuseAddress(true);
				String info = module + " began to listen on port:" + port;
				log.info(info);
				serverSocket.setSoTimeout(10 * 1000);
			}
			while (isRunning()) {
				Socket nextClient = null;
				try {
					nextClient = serverSocket.accept();
					String host = nextClient.getInetAddress().getHostAddress();
					if (nextClient != null) {
						if (gmmsUtility.isAddressScreened(host)) {
							nextClient.close();
							continue;
						}
						log.info("{} accept a connection on port {} from host {}",
										module, host, port);
						createSession(nextClient);
					}
				} catch (IOException e) {
					// NO log needed here because the
					// java.net.SocketTimeoutException is regularly thrown
				}
			}
		} catch (IOException e) {
			log.error(e, e);
		} finally {
			running = false;
			try {
				if (serverSocket != null) {
					serverSocket.close();
					serverSocket = null;
				}
			} catch (IOException e1) {
				log.warn(e1, e1);
			}
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
                           "SslSmppServer").start();
            } else {
            		log.info("going to listen in the context of current thread.");
                run();
            }
                log.info( "{} starting...",module);
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
        }
        return true;
    }
    
    
}
