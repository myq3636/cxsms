package com.king.gmms.messagequeue;

import java.util.concurrent.ExecutorService;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.gmms.util.QueueTimeoutInterface;
import com.king.message.gmms.GmmsMessage;

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
 * @deprecated Phase 2 refactoring: Local memory queues are deprecated in favor of Redis Stream event-driven architecture.
 */
@Deprecated
public abstract class CustomerMessageQueue implements OperatorMessageQueue,QueueTimeoutInterface {
	
	protected A2PCustomerInfo cst = null;
    protected boolean isServer;
    protected GmmsUtility gmmsUtility;
    protected String queueName = "CustomerMessageQueue";
    private static SystemLogger log = SystemLogger.getSystemLogger(CustomerMessageQueue.class);
    protected int queueTimeout = 3 * 60 * 1000;
    
    protected ExecutorServiceManager executorServiceManager;
    
    /**
     * thread pool for CustomerMessageSender/CustomerNodeMessageSender
     * not safe exit
     */
    protected ExecutorService senderThreadPool;

    public CustomerMessageQueue(A2PCustomerInfo cst, int minSenderNum, int maxSenderNum,
    		                    boolean isServer, String queueName) {
        this.cst = cst;
        this.isServer = isServer;
        if(queueName != null){
        	this.queueName = queueName;
        }
        
        
        gmmsUtility = GmmsUtility.getInstance();
        executorServiceManager = gmmsUtility.getExecutorServiceManager();
        queueTimeout = gmmsUtility.getCacheMsgTimeout();
		
		// sender thread pool
        ThreadPoolProfile profile = new ThreadPoolProfileBuilder(this.queueName)
                .poolSize(minSenderNum).maxPoolSize(maxSenderNum).build();
        senderThreadPool = executorServiceManager.newExpiredThreadPool(this, "CustSender_" + cst.getSSID(), profile, this, queueTimeout);
    }
    
    /**
     * put msg to queue
     */
    public abstract boolean putMsg(GmmsMessage msg); 
    
    @Override
    public void stopMessageQueue() {
    	if (executorServiceManager != null) {
    		executorServiceManager.shutdown(senderThreadPool);
    	}
    }

	public ExecutorService getSenderThreadPool() {
		return senderThreadPool;
	}

	public void setSenderThreadPool(ExecutorService senderThreadPool) {
		this.senderThreadPool = senderThreadPool;
	}    
	
}
