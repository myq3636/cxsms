package com.king.gmms.messagequeue;

import java.util.concurrent.ExecutorService;

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
 * @version 1.0
 */
public abstract class ReceiverMessageQueue implements OperatorMessageQueue,QueueTimeoutInterface{
	
	protected A2PCustomerInfo cst = null;
    protected boolean isServer;
    protected GmmsUtility gmmsUtility;
    protected String queueName = "ReceiverMessageQueue";
    
    protected ExecutorService receiverThreadPool;
    protected ExecutorServiceManager executorServiceManager;
    /**
     * 
     * @param processorNum
     * @param maxReceiverNum
     * @param queueHandler
     * @param timeoutHandler
     */
    public ReceiverMessageQueue(A2PCustomerInfo cst,int minReceiverNum, 
    		                    int maxReceiverNum, String queueName) {
        this.cst = cst;
        if(queueName != null){
        	this.queueName = queueName;
        }
        
        gmmsUtility = GmmsUtility.getInstance();
        executorServiceManager = gmmsUtility.getExecutorServiceManager();
        int queueTimeout = gmmsUtility.getCacheMsgTimeout();
        	
        // create the thread pool using a builder
        ThreadPoolProfile profile = new ThreadPoolProfileBuilder(this.queueName)
                                           .poolSize(minReceiverNum).maxPoolSize(maxReceiverNum).build();
        receiverThreadPool = executorServiceManager.newExpiredThreadPool(this, "CustShortReceiver_" + cst.getSSID(), 
        		                           profile, this, queueTimeout);
    }
    
    /**
     * put msg to queue
     */
    public abstract boolean putMsg(GmmsMessage msg);
    
   
    public void stopMessageQueue() {
    	if (executorServiceManager != null) {
    		executorServiceManager.shutdown(receiverThreadPool);
    	}
    }
    
}
