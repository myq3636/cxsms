package com.king.gmms.ha.systemmanagement;

import java.io.IOException;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.sessionthread.SessionThread;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.util.SystemConstants;

import java.util.Observable;
import java.util.concurrent.LinkedBlockingQueue;

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
public class SystemServerSessionThread implements Runnable,SessionThread {
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemServerSessionThread.class);
    private SystemSession systemSession = null;
    private static int maxRetryNumber = 3;
    private volatile int retryNum = 0;
    private static int maxEnquireLinkNum = 3;
    private static long connectionInterval = 30000L;
    private static long enquireLinkTime = 40000L;
    private static long maxConnectionSilentTime = 100000L;
    private static long recoveryTime = 60000L;
    private String name = "";
    private int failedAddressNum = 0;
    private ModuleManager moduleManager = null;

    public SystemServerSessionThread(String name, SystemSession systemSession) {
        if (name != null && name.length() > 0) {
            this.name = name;
        }
        if (systemSession != null) {
            this.systemSession = systemSession;
        }
        moduleManager = ModuleManager.getInstance();
        init();
    }

    private void init() {
        maxRetryNumber = Integer.parseInt(moduleManager.getClusterProperty(
            "MGM.MaxRetryTimes", "3"));
        maxRetryNumber = maxRetryNumber < 3? 3 : maxRetryNumber;
        maxEnquireLinkNum = Integer.parseInt(moduleManager.getClusterProperty(
            "MGM.MaxEnquireLinkNumber", "3"));
        maxEnquireLinkNum = maxEnquireLinkNum < 3? 3 : maxEnquireLinkNum;
        connectionInterval = Long.parseLong(moduleManager.getClusterProperty(
            "MGM.ReconnectInterval", "10")) * 1000;
        connectionInterval = connectionInterval < 10 * 1000? 10 * 1000 : connectionInterval;
        enquireLinkTime = Long.parseLong(moduleManager.getClusterProperty(
            "MGM.EnquireLinkTime", "10")) * 1000;
        enquireLinkTime = enquireLinkTime < 10 * 1000? 10 * 1000 : enquireLinkTime;
        maxConnectionSilentTime = Long.parseLong(moduleManager.getClusterProperty(
            "MGM.ConnectionSilentTime", "100")) * 1000;
        maxConnectionSilentTime = maxConnectionSilentTime < 100 * 1000? 100 * 1000 : maxConnectionSilentTime;
        recoveryTime = Long.parseLong(moduleManager.getClusterProperty(
            "MGM.RecoveryTime", "60")) * 1000;
        recoveryTime = recoveryTime < 60 * 1000? 60 * 1000 : recoveryTime;
    }

    public void start() {
        Thread thread = new Thread(A2PThreadGroup.getInstance(), this,
                                   name + "_SerSessionTd");
        thread.start();
    }

    public void run() {
        try {
            /**
             * just wait if session status is disconnect or inital
             */
            while (systemSession.isKeepRunning() &&
                   (systemSession.getStatus().equals(ConnectionStatus.DISCONNECT)||
                    systemSession.getStatus().equals(ConnectionStatus.INITIAL))) {

                if(systemSession.getStatus().equals(ConnectionStatus.DISCONNECT)){
            		log.warn("Session from initial dump to disconnect, direct stop session!");
                    systemSession.stop();
                    return;
                } else if ( (System.currentTimeMillis() - systemSession.getLastActivity()) >
                    maxConnectionSilentTime) {
            		log.warn("Max Silent time! and last time is {}",systemSession.getLastActivity());
                    systemSession.stop();
                    return;
                }
                else {
                    Thread.sleep(100L);
                }
            }

            if ( (systemSession.getStatus().equals(ConnectionStatus.CONNECT)) &&
                systemSession.getConnection() != null) {
//                systemSession.initReceivers(3);
//                systemSession.initSenders(3);
            }
            else {
                log.error("The server session is initialized unsuccessfully");
                systemSession.stop();
                return;
            }

            while (systemSession.isKeepRunning()) {

                if (System.currentTimeMillis() - systemSession.getLastActivity() >maxConnectionSilentTime) {
                    systemSession.stop();
                }
                else {
                    try {
                        Thread.sleep(maxConnectionSilentTime - 500);
                    }
                    catch (Exception e) {
                        log.error(e, e);
                    }
                }
            }
        }
        catch (Exception e) {
            log.error(e, e);
        }
        finally {
            systemSession.stop();
                log.info("ServerSessionThread stop!");
        }
    }

    public void stopThread() {
        switch (systemSession.getStatus()) {
            case INITIAL:
        		log.warn("The session({}) status is set:  RETRY and the original status is INITIAL",name);
                systemSession.setStatus(ConnectionStatus.RETRY);
                break;
            case CONNECT:
        		log.warn("The session({}) status is set:  RETRY and the original status is CONNECT",name);
                systemSession.setStatus(ConnectionStatus.RETRY);
                break;
            case RECOVER:
        		 log.warn("The session({}) status is set:  DISCONNECT and the original status is RECOVER",name);
                systemSession.setStatus(ConnectionStatus.DISCONNECT);
                break;
            case RETRY:
                if (retryNum >= maxRetryNumber) {
                    retryNextAddress();
                    if (failedAddressNum >= 2) {
                		log.warn("The session({}) status is set:  DISCONNECT and the original status is RETRY",name);
                        systemSession.setStatus(ConnectionStatus.DISCONNECT);
                    }
                }
                break;
            case DISCONNECT:
                if (retryNum >= maxRetryNumber) {
                    retryNextAddress();
                }
                break;
            default:
                break;
        }
    }

    public void retryNextAddress(){
    }
    
	public void interrupt(){
		
	}
}
