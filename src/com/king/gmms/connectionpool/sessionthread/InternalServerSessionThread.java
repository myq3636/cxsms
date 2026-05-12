package com.king.gmms.connectionpool.sessionthread;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.ConnectionInfo;

public class InternalServerSessionThread implements SessionThread{
    private static SystemLogger log = SystemLogger.getSystemLogger(InternalServerSessionThread.class);
    private Session session = null;
    private int connectionSilentTime = 5*60*1000;
    private long waitingTime = 1000;
    private ConnectionInfo connectionInfo = null;
    private Thread thread = null;
    
    public InternalServerSessionThread(Session session,int connectionSilentTime) {
        this.session = session;
        this.connectionSilentTime = connectionSilentTime;
    }

    private void initSessionThread(){
        connectionInfo = session.getConnectionInfo();
    }
    
    public void run() {
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
                	log.warn("Max Silent time! The last time is {}",session.getLastActivity());
                    session.stop();
                    return;
                }
                else {
                    Thread.sleep(waitingTime);
                }
            }

            if ( (session.getStatus().equals(ConnectionStatus.CONNECT)) &&
                session.getConnectionInfo() != null) {
                initSessionThread();
            }
            else {
                log.error("The server session is initialized unsuccessfully");
                session.stop();
                return;
            }

            while (session.isKeepRunning()) {
                if (System.currentTimeMillis() - session.getLastActivity() >connectionSilentTime) {
                    session.stop();
                }
                else {
                    try {
                        Thread.sleep(connectionSilentTime - 500);
                    }
                    catch (InterruptedException e) {
//                        log.error(e, e);
                    }
                }
            }
        }
        catch (Exception e) {
            log.error(e, e);
        }
        finally {
            session.stop();
            log.warn("ServerSessionThread stop, session number: {}" , session.getSessionNum());
        }
    }

    public void stopThread() {

        session.setKeepRunning(false);
      
        switch (session.getStatus()) {
            case CONNECT:
            	log.warn("The session({}) status is set:  DISCONNECT and the original status is CONNECT",session.getSessionName());
                session.setStatus(ConnectionStatus.DISCONNECT);
                break;
            case INITIAL:
            	log.warn("The session({}) status is set:  DISCONNECT and the original status is INITIAL",session.getSessionName());
                session.setStatus(ConnectionStatus.DISCONNECT);
                break;
            case RECOVER:
            	log.warn("The session({}) status is set:  DISCONNECT and the original status is RECOVER",session.getSessionName());
                session.setStatus(ConnectionStatus.DISCONNECT);
                break;
            case DISCONNECT:
            default:
                break;
        }
        interrupt();
    }

    public void start() {
        thread = new Thread(A2PThreadGroup.getInstance(), this,
                                   "IntServerSessionTrd " + session.getSessionNum());
        thread.start();
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
