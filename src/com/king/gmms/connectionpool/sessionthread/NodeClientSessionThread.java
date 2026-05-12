package com.king.gmms.connectionpool.sessionthread;

import java.io.IOException;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.connection.NodeConnectionManager;
import com.king.gmms.connectionpool.connection.NodeConnectionManagerInterface;
import com.king.gmms.connectionpool.session.AbstractSession;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.strategy.StrategyType;
import com.king.gmms.util.AbstractTimer;

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
public class NodeClientSessionThread extends AbstractTimer implements SessionThread {
        private static SystemLogger log = SystemLogger.getSystemLogger(NodeClientSessionThread.class);
        private int connectionInterval = 10000;
        private int enquireLinkFailCount = 3;
        private int failCount = 1;
        private AbstractSession session = null;
        private ConnectionInfo connectionInfo = null;
        private A2PMultiConnectionInfo customerInfo = null;
        private ConnectionManager connectionManager = null;
        private GmmsUtility gmmsUtility = null;
        private Thread thread = null;

        public NodeClientSessionThread(AbstractSession session,long recoverTime,int connectionInterval,int enquireLinkFailCount) {
            super(recoverTime);
            this.session = session;
            this.connectionInfo = session.getConnectionInfo();
            this.customerInfo = session.getCustomerInfo();
            this.connectionInterval = connectionInterval;
            this.enquireLinkFailCount = enquireLinkFailCount;
            gmmsUtility = GmmsUtility.getInstance();
        }

        public void start() {
            thread = new Thread(A2PThreadGroup.getInstance(), this,
                                       "NodeClientSessionTrd " +  + session.getSessionNum());
            thread.start();
        }

        public void run() {
            connectionManager = session.getConnectionManager();
            while (session.isKeepRunning()) {
                try {
                    switch(session.getStatus()){
                        case DISCONNECT:
                        	try{
                        		Thread.sleep(connectionInterval);
                        	} catch (Exception e) {
                        		
                        	}
                        	if (!session.isKeepRunning()) {
	                        	continue;
	                        }
                        	
                            failCount = 1;
	                    	if(!session.createConnection()){
	                    		session.stop();
	                    		continue;
	                    	}
	                    	else if (session.connect()) {
                                String type = ((MultiNodeCustomerInfo)customerInfo).getSubmitNodePolicy();
                                if (! ( (NodeConnectionManagerInterface)connectionManager).getNodeStatus(connectionInfo.getConnectionName())
                                    && StrategyType.getStrategyType(type).equals(StrategyType.Primary)) {
                                        log.warn("The session({}) status is set:  RECOVER and the original status is DISCONNECT",session.getSessionName());
	                                    session.setStatus(ConnectionStatus.RECOVER);
	                                    startTimer();
                                }
                                else {
                                    log.warn("The session({}) status is set:  CONNECT and the original status is DISCONNECT",session.getSessionName());
                                    session.setStatus(ConnectionStatus.CONNECT);
                                }
                                break;
                            }else{
                                session.stop();
                                continue;
                            }
                        case RETRY:
                        	try {
                        		Thread.sleep(connectionInterval);
                        	} catch (Exception e) {
                        		
                        	}
                        	
                        	if (!session.isKeepRunning()) {
	                        	continue;
	                        }
                            
                			log.warn("The retry number of session({}) is {}",session.getSessionName(), failCount);
	                    	if(!session.createConnection()){
	                    		session.stop();
	                    		continue;
	                    	}
	                    	else if (session.connect()) {
                                log.warn("The session({}) status is set:  CONNECT and the original status is RETRY",session.getSessionName());
                                session.setStatus(ConnectionStatus.CONNECT);
                                failCount = 1;
                                break;
                            }else{
                                session.stop();
                                continue;
                            }
                        case INITIAL:
	                    	if(!session.createConnection()){
	                    		session.stop();
	                    		continue;
	                    	}
	                    	else if (session.connect()) {
                                log.warn("The session({}) status is set:  CONNECT and the original status is INITIAL",session.getSessionName());
                                session.setStatus(ConnectionStatus.CONNECT);
                                failCount = 1;
                                break;
                            }
                            else {
                                session.stop();
                                continue;
                            }
                    }
                    if (customerInfo.isEnquireLink()) {
                        if (System.currentTimeMillis() -  session.getLastActivity() >
                            customerInfo.getConnectionSilentTime()) {
                            session.stop();
                        }
                        else {
                            int count = session.enquireLink();
                            if(count >= enquireLinkFailCount){
                                session.stop();
                            }
                            else if(count <= 1){
                                Thread.sleep(customerInfo.getEnquireLinkTime() + gmmsUtility.getEnquireLinkResponseTiem());
                            }else {
                                if (session.getStatus().equals(ConnectionStatus.RECOVER)) {
                                    restartTimer();
                                }
                                Thread.sleep(customerInfo.getFailEnquireLinkTime() + gmmsUtility.getEnquireLinkResponseTiem());
                            }
                        }
                    }
                    else if (System.currentTimeMillis() - session.getLastActivity() >
                             customerInfo.getConnectionSilentTime()) {
                        session.stop();
                    }
                    else {
                    	Thread.sleep(customerInfo.getConnectionSilentTime() - 100);
                    }
                }
                catch(IOException e){
                    log.error(e, e);
                    session.stop();
                }
                catch (Exception e) {
                    log.error(e, e);
                }
            }
        }

        public void stopThread() {
            switch (session.getStatus()) {
                case INITIAL:
                	log.warn("The session({}) status is set:  RETRY and the original status is INITIAL",session.getSessionName());
                    session.setStatus(ConnectionStatus.RETRY);
                    break;
                case CONNECT:
                	log.warn("The session({}) status is set:  RETRY and the original status is CONNECT",session.getSessionName());
                    session.setStatus(ConnectionStatus.RETRY);
                    break;
                case RETRY:
                    if (customerInfo.getMaxRetryNum() <= failCount) {
                    	log.warn("The session({}) status is set:  DISCONNECT and the original status is RETRY",session.getSessionName());
                        session.setStatus(ConnectionStatus.DISCONNECT);
                    }
                    else {
                        failCount++;
                    }
                    break;
                case RECOVER:
                	log.warn("The session({}) status is set:  DISCONNECT and the original status is RECOVER",session.getSessionName());
                    session.setStatus(ConnectionStatus.DISCONNECT);
                    stopTimer();
                    break;
                case DISCONNECT:
                default:
                    break;
            }
        }

        public void excute() {
        	log.warn("The session({}) status is set:  CONNECT and the original status is RECOVER",session.getSessionName());
            session.setStatus(ConnectionStatus.CONNECT);
            stopTimer();
        }
        
        public void interrupt(){
        	if(thread != null){
        		try{
        			thread.interrupt();
        		}catch(Exception ex){
        			if(log.isDebugEnabled()){
        				log.debug(ex.getMessage());
        			}
        		}
        	}
        	if(isRunning()){
        		stopTimer();
        	}
        }
    }
