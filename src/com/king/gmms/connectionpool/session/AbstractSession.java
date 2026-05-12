package com.king.gmms.connectionpool.session;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.GuavaCache;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.sessionthread.SessionThread;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.util.BufferMonitor;
import com.king.gmms.util.BufferTimeoutInterface;

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
public abstract class AbstractSession
    extends Session implements  BufferTimeoutInterface {

    private static SystemLogger log = SystemLogger.getSystemLogger(AbstractSession.class);
    private long waitingTime = 1000;
    protected boolean isServer = false;

    protected String sessionName = "";
    protected int sessionNum = 0;

    protected volatile long lastActivity = System.currentTimeMillis();
    protected volatile long eqLastActivity = System.currentTimeMillis();
    protected GmmsUtility gmmsUtility = null;
    protected ConnectionInfo connectionInfo = null;
    protected A2PMultiConnectionInfo customerInfo = null;
    protected ConnectionManager connectionManager = null;
    protected volatile boolean keepRunning = false;
    protected Object mutex = new Object();
    protected ConnectionStatus status = ConnectionStatus.INITIAL;
    protected BufferMonitor bufferMonitor = null;
    protected TransactionURI transaction;
    protected SessionThread sessionThread = null;
    
    protected ExecutorService receiverThreadPool;
    protected ExecutorServiceManager executorServiceManager;
    protected GuavaCache guavaCache = null;
    
    public AbstractSession() {
    	gmmsUtility = GmmsUtility.getInstance();
        sessionNum = gmmsUtility.assignUniqueNumber();
        sessionName = Integer.toString(sessionNum);
        executorServiceManager = gmmsUtility.getExecutorServiceManager();
    }

    public boolean startBufferMonitor(A2PMultiConnectionInfo customerInfo) {
        if (customerInfo == null) {
            log.warn("customerInfo is null");
            return false;
        }
        bufferMonitor = new BufferMonitor(customerInfo.getWindowSize());
        bufferMonitor.setListener(this);
        bufferMonitor.setWaitTime(200, TimeUnit.MILLISECONDS);
        bufferMonitor.setTimeout(customerInfo.getBufferTimeout(),
                                 TimeUnit.MILLISECONDS);
        bufferMonitor.startMonitor("AbstractSessionBuffer_" + customerInfo.getSSID());
        return true;
    }
    
    public boolean startGuavaCache(A2PMultiConnectionInfo customerInfo) {
        if (customerInfo == null) {
            log.warn("customerInfo is null");
            return false;
        }        
        guavaCache = new GuavaCache(customerInfo.getBufferTimeout(),customerInfo.getWindowSize(),this);
        //guavaCache.setListener(this);
        //guavaCache.setTimeout(customerInfo.getBufferTimeout());
        //guavaCache.setBufferCapacity(customerInfo.getWindowSize());
        return true;
    }

    public BufferMonitor getBufferMonitor() {
        return bufferMonitor;
    }

    public boolean isServer() {
        return isServer;
    }

    public String getSessionName() {
        return sessionName;
    }

    public int getSessionNum() {
        return sessionNum;
    }

    public void setWaitingTime(long waitingTime) {
        this.waitingTime = waitingTime;
    }

    public long getWaitingTime() {
        return waitingTime;
    }


    public long getLastActivity() {
        return lastActivity;
    }

    public void setSessionName(String connectionName) {
        this.sessionName = connectionName + "_" + sessionNum;
    }

    public ConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public boolean intialize(ConnectionInfo connectionInfo,
    		A2PMultiConnectionInfo customerInfo) {
        if (connectionInfo == null || customerInfo == null) {
            log.warn("connectionInfo or customerInfo is null");
            return false;
        }
        this.connectionInfo = connectionInfo;
        this.customerInfo = customerInfo;
        
//        int minSenderNum = customerInfo.getMinReceiverNumber();
//		int maxSenderNum = customerInfo.getMaxReceiverNumber();
//        // thread pool profile
//        ThreadPoolProfile profile = new ThreadPoolProfileBuilder(connectionInfo.getConnectionName())
//                                            .poolSize(minSenderNum).maxPoolSize(maxSenderNum).build();
//        if (receiverThreadPool != null) {
//        	gmmsUtility.getExecutorServiceManager().updateThreadPoolProfile((ThreadPoolExecutor)receiverThreadPool, profile, "CustomerMessageReceiver " + customerInfo.getSSID());
//        } else {
//        	receiverThreadPool = gmmsUtility.getExecutorServiceManager().newThreadPool(this, "CustomerMessageReceiver " + customerInfo.getSSID(), profile);
//        }
        
        return true;
    }

    public A2PMultiConnectionInfo getCustomerInfo() {
        return customerInfo;
    }

    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public void start() {
        synchronized (mutex) {
            if (keepRunning == true) {
                return;
            }
            keepRunning = true;
            if (sessionThread != null) {
                sessionThread.start();
            }
        }
    }


    public synchronized void setKeepRunning(boolean keepRunning) {
        this.keepRunning = keepRunning;
    }

    public boolean isKeepRunning() {
        return keepRunning;
    }

    public ConnectionStatus getStatus() {
        return status;
    }

    public TransactionURI getTransactionURI() {
        return transaction;
    }


    public void setStatus(ConnectionStatus status) {
        synchronized (mutex) {
            this.status = status;
            setChanged();
            notifyObservers(status);
        }
    }
    
    public boolean isFakeSession(){
    	return false;
    }

	public long getEqLastActivity() {
		return eqLastActivity;
	}

	public void setEqLastActivity(long eqLastActivity) {
		this.eqLastActivity = eqLastActivity;
	}
    
    
}
