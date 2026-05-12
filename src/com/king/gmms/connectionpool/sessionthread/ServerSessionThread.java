package com.king.gmms.connectionpool.sessionthread;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.AbstractSession;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.util.AbstractTimer;
import com.king.gmms.util.BufferMonitor;

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
public class ServerSessionThread extends AbstractTimer implements SessionThread {
        private static SystemLogger log = SystemLogger.getSystemLogger(ServerSessionThread.class);
        private AbstractSession session = null;
        private int connectionSilentTime = 60000;
        private ConnectionInfo connectionInfo = null;
        private A2PMultiConnectionInfo customerInfo = null;
//        private ConnectionManager connectionManager = null;
        private Thread thread = null;
        
        public ServerSessionThread(AbstractSession session,int connectionSilentTime) {
            super(900*1000);
            this.session = session;
            this.connectionSilentTime = connectionSilentTime;
        }

        private void initSessionThread(){
            connectionInfo = session.getConnectionInfo();
            customerInfo = session.getCustomerInfo();
            thread.setName("ServerSessionTrd_" + customerInfo.getSSID() + "_" + session.getSessionNum());
        }

        public void run() {
//            session.initReceivers(1);
//            Object obj = null;
            try {
                /**
                 * just wait if session status is disconnect or inital
                 */
                while (session.isKeepRunning() &&
                       (session.getStatus().equals(ConnectionStatus.DISCONNECT)||
                        session.getStatus().equals(ConnectionStatus.INITIAL))) {

                    if(session.getStatus().equals(ConnectionStatus.DISCONNECT)){
                        log.warn("Session from initial dump to disconnect, direct stop session!");                			
                        session.stop();
                        return;
                    }
                    else if ( (System.currentTimeMillis() - session.getLastActivity()) >
                        connectionSilentTime) {
                        log.warn("Session is stopped since reach to Max Silent time! And last time is {}",session.getLastActivity());
                        session.stop();
                        return;
                    }
                    else {
                        Thread.sleep(session.getWaitingTime());
                    }
                }

                if ( (session.getStatus().equals(ConnectionStatus.CONNECT) ||
                      session.getStatus().equals(ConnectionStatus.RECOVER)) &&
                    session.getConnectionInfo() != null) {

                    initSessionThread();
                    if(session.getStatus().equals(ConnectionStatus.RECOVER)){
                        this.setWakeupTime(( (MultiNodeCustomerInfo)
                                                     customerInfo).getNodeRecoveryTime());
                        startTimer();
                    }
                }
                else {
                    log.error("The server session is initialized unsuccessfully");
                    session.stop();
                    return;
                }

                while (session.isKeepRunning()) {
                    if (customerInfo.isServerEnquireLink()) {
                        if (System.currentTimeMillis() - session.getLastActivity() >
                            customerInfo.getConnectionSilentTime()) {
                            session.stop();
                        }
                        else {
                            int count = session.enquireLink();
                            if(count >= customerInfo.getEnquireLinkFailureNum()){
                                session.stop();
                            } else if(count >= 1){

                                if (session.getStatus().equals(ConnectionStatus.
                                    RECOVER)) {
                                    restartTimer();
                                }
                                Thread.sleep(customerInfo.getFailEnquireLinkTime());

                            }else{
                                Thread.sleep(customerInfo.getEnquireLinkTime());
                            }
                        }
                    }
                    else if (System.currentTimeMillis() - session.getLastActivity() >
                             customerInfo.getConnectionSilentTime()) {
                        session.stop();
                    }
                    else {
                        try {
                        	Thread.sleep(customerInfo.getConnectionSilentTime() - 100);
                        }
                        catch (Exception e) {
//                            log.info(e, e);
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error(e, e);
            }
            finally {
                session.stop();
                log.warn("ServerSessionThread stop, session number: {}", session.getSessionNum());
            }
        }

        public void stopThread() {

            session.setKeepRunning(false);
//            BufferMonitor bufferMonitor = session.getBufferMonitor();
//            if (bufferMonitor != null) {
//                bufferMonitor.sendbackBuffer();
//                bufferMonitor.stopMonitor();
//                bufferMonitor = null;
//            }
            switch (session.getStatus()) {
                case CONNECT:
                    log.warn("The session({}) status is set:  DISCONNECT and the original status is CONNECT",
                    		session.getSessionName());
                    session.setStatus(ConnectionStatus.DISCONNECT);
                    break;
                case INITIAL:
                    log.warn("The session({}) status is set:  DISCONNECT and the original status is INITIAL",
                    		session.getSessionName());
                    session.setStatus(ConnectionStatus.DISCONNECT);
                    break;
                case RECOVER:
                    log.warn("The session({}) status is set:  DISCONNECT and the original status is RECOVER",
                    		session.getSessionName());
                    session.setStatus(ConnectionStatus.DISCONNECT);
                    break;
                case DISCONNECT:
                default:
                    break;
            }
        }

        public void start() {
            thread = new Thread(A2PThreadGroup.getInstance(), this,
                                       "ServerSessionTrd " + session.getSessionNum());
            thread.start();
        }

        public void excute() {

            if(session.getStatus().equals(ConnectionStatus.RECOVER)){
                log.warn("The session() status is set:  CONNECT and the original status is RECOVER",
                		session.getSessionName());
                session.setStatus(ConnectionStatus.CONNECT);
            }
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
