package com.king.gmms.connectionpool.sessionthread;

import java.io.IOException;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.ConnectionInfo;

public class InternalClientSessionThread implements SessionThread {
    private static SystemLogger log = SystemLogger.getSystemLogger(InternalClientSessionThread.class);
    private Session session = null;
    private ConnectionInfo connectionInfo = null;
    private int connectionSilentTime = 5*60*1000;
    private int enquireLinkTime = 30*1000;
    private int enquireLinkFailCount = 50;
    private int connectionInterval = 1*1000;
    private GmmsUtility gmmsUtility = null;
    private Thread thread = null;

    public InternalClientSessionThread(Session session, int connectionSilentTime) {
        this.session = session;
        this.connectionInfo = session.getConnectionInfo();
        this.connectionSilentTime = connectionSilentTime;
        gmmsUtility = GmmsUtility.getInstance();
    }

    public void start() {
       thread = new Thread(A2PThreadGroup.getInstance(), this,
                                   "IntClientSessionTrd " + session.getSessionNum());
        thread.start();
    }

    public void run() {
        while (session.isKeepRunning()) {
            try {
                switch (session.getStatus()) {
                    case DISCONNECT:
                        Thread.sleep(connectionInterval);
                    	if(!session.createConnection()){
                    		session.stop();
                    		continue;
                    	}
                        if (session.connect()) {
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
                        if (session.connect()) {
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
	
	            if (System.currentTimeMillis() - session.getLastActivity() > connectionSilentTime) {	              
	            	log.warn("connectionSilentTime is come,so stop session {}",connectionSilentTime);
	            	session.stop();
	            }
	            else {
	                int count = session.enquireLink();
	                if(count >= enquireLinkFailCount){
	                    log.warn("{} alive requests didn't receive response, so close the connection.",count);
	                    session.stop();
	                }else {
	                    Thread.sleep(enquireLinkTime + gmmsUtility.getEnquireLinkResponseTiem());
	                }
	            }
            }
            catch (IOException e) {
                log.error(e.getMessage());
                session.stop();
            }
            catch (Exception e) {
                log.error(e.getMessage());
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

