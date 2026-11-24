package com.king.gmms.messagequeue;

import java.util.concurrent.ExecutorService;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.sessionfactory.SessionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.sender.ShortConnectionSender;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.gmms.util.QueueTimeoutInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class ShortConnectionCustomerMessageQueue implements OperatorMessageQueue,QueueTimeoutInterface{
	protected A2PCustomerInfo cst = null;
    protected boolean isServer;
    protected GmmsUtility gmmsUtility;
    private static SystemLogger log = SystemLogger.getSystemLogger(ShortConnectionCustomerMessageQueue.class);
    private SessionFactory sessionFactory = null;
    
    protected ExecutorService senderThreadPool;
    protected ExecutorServiceManager executorServiceManager;

    public ShortConnectionCustomerMessageQueue(A2PCustomerInfo cst, boolean isServer) {
        this.cst = cst;
        this.isServer = isServer;
        
        int minSenderNum = cst.getMinSenderNumber();
		int maxSenderNum = cst.getMaxSenderNumber();
		
		gmmsUtility = GmmsUtility.getInstance();
		executorServiceManager = gmmsUtility.getExecutorServiceManager();
		int queueTimeout = gmmsUtility.getCacheMsgTimeout();
		
        // create the thread pool using a builder
        ThreadPoolProfile profile = new ThreadPoolProfileBuilder("ShortConnectionMessageQueue_" + cst.getSSID())
                .poolSize(minSenderNum).maxPoolSize(maxSenderNum).build();
        senderThreadPool = executorServiceManager.newExpiredThreadPool(this, "CustShortSender_" + cst.getSSID(), profile, this, queueTimeout);
        
    }
   
    public boolean startMessageQueue(SessionFactory sessionFactory){
    	this.sessionFactory = sessionFactory;
		return true;
    }
    /**
     * put msg to queue
     */
    @Override
    public boolean putMsg(GmmsMessage msg) {
		if(msg == null){
			return false;
		}

		if (log.isTraceEnabled()) {
			log.trace(msg, "submit to ShortConnectionCustomerMessageQueue thread pool");
		}
		 
		try {
			senderThreadPool.execute(new ShortConnectionSender(msg, sessionFactory, cst.getSSID()));
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info(msg, e, e);
			}
			
			return false;
		}
		
		return true;
    }

	
	public void timeout(Object message) {
		GmmsMessage msg = (GmmsMessage)message;
		try {
	    	String routerQueue = null;
	        if(log.isInfoEnabled()){
	        	log.info(msg,"{} is timeout in customer queue",msg.getMessageType());
	        }
	        if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
					|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType())) {
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
		        msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
	    		TransactionURI transaction = msg.getInnerTransaction();
	    		routerQueue = transaction.getConnectionName();
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
		        msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
	    		TransactionURI transaction = msg.getInnerTransaction();
	    		routerQueue = transaction.getConnectionName();
			}else if(GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType()) || GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())){
				msg.setStatusCode(1);
		        msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
	    		TransactionURI innerTransaction = msg.getInnerTransaction();
	    		routerQueue = innerTransaction.getConnectionName();
			}
	    	// the failure went back over the internal connection to Core, which stream mode does not have: Core's outbox
	    	// times out an MT without a result (docs/流模式下系统管理和模块间连接代码清理分析.md 3.4)
	    	if (log.isInfoEnabled()) {
	    		log.info(msg, "{} for {} not sent back: no internal connection in stream mode", msg.getMessageType(), routerQueue);
	    	}
		} catch (Exception e) {
			log.warn(msg, "ShortConnectionCustomerMessageQueue exception:" + e);
		}
    	
	}

	/** 
	 * 
	 */
	@Override
	public void stopMessageQueue() {
		if (executorServiceManager != null) {
			executorServiceManager.shutdown(senderThreadPool);
		}
	}

}
