package com.king.gmms.connectionpool.sessionthread;

import java.io.IOException;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.AbstractSession;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;


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
public class ClientSessionThread implements SessionThread {
        private static SystemLogger log = SystemLogger.getSystemLogger(ClientSessionThread.class);
        private int connectionInterval = 10000;
        private int enquireLinkFailCount = 3;
        private AbstractSession session = null;
        private ConnectionInfo connectionInfo = null;
        private A2PMultiConnectionInfo customerInfo = null;
        private GmmsUtility gmmsUtility = null;
        private Thread thread = null;
        
        public ClientSessionThread(AbstractSession session, int connectionInterval, int enquireLinkFailCount) {
            this.session = session;
            this.connectionInfo = session.getConnectionInfo();
            this.customerInfo = session.getCustomerInfo();
            this.connectionInterval = connectionInterval;
            this.enquireLinkFailCount = enquireLinkFailCount;
            gmmsUtility = GmmsUtility.getInstance();
        }

        public void start() {
            thread = new Thread(A2PThreadGroup.getInstance(), this,
                                       "ClientSessionTrd_" + customerInfo.getSSID() + "_" + session.getSessionNum());
            thread.start();
        }

        public void run() {
            while (session.isKeepRunning()){
            	try {
            		switch (session.getStatus()) {
	                    case DISCONNECT:
	                    	
	                    	try{
	                    		Thread.sleep(connectionInterval);
	                        }catch(Exception e){
	                        	
	                        }
	                        
	                        if (!session.isKeepRunning()) {
	                        	continue;
	                        }
	                    	if(!session.createConnection()){
	                    		session.stop();
	                    		continue;
	                    	}
	                    	else if (session.connect()) {
	                    		log.warn("The session({}) status is set:  CONNECT and the original status is DISCONNECT",session.getSessionName());
                                session.setStatus(ConnectionStatus.CONNECT);
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
                                break;
	                        }else{
	                            session.stop();
	                            continue;
	                        }
	                    case CONNECT:
	                    default:
	                        break;
	             }
            	
            	if (customerInfo.isEnquireLink()) {
                    if (System.currentTimeMillis() - session.getLastActivity() >
                        customerInfo.getConnectionSilentTime()) {
                        session.stop();
                    }
                    else {
                        int count = session.enquireLink();
                        if(count >= enquireLinkFailCount){
                            log.warn("{} alive requests didn't receive response, so close the connection.",count);
                            session.stop();
                        }else {
                        	try{                        		
                        		Thread.sleep(customerInfo.getEnquireLinkTime() + gmmsUtility.getEnquireLinkResponseTiem());
	                        }catch(Exception e){
	                        	
	                        }
                        	
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
            } catch (IOException e) {
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
                case CONNECT:
                	log.warn("The session({}) status is set:  DISCONNECT and the original status is CONNECT",session.getSessionName());
	                session.setStatus(ConnectionStatus.DISCONNECT);
                    break;
                case INITIAL:
                	log.warn("The session({}) status is set:  DISCONNECT and the original status is INITIAL",session.getSessionName());
	                session.setStatus(ConnectionStatus.DISCONNECT);
                    break;
                case DISCONNECT:
                default:
                    break;
            }
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
        }
    }
