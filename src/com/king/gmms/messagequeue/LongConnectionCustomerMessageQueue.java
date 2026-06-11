package com.king.gmms.messagequeue;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.sender.CustomerMessageSender;
import com.king.gmms.sender.PrioritrySender;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.redis.SerializableHandler;

public class LongConnectionCustomerMessageQueue extends CustomerMessageQueue{
	private static SystemLogger log = SystemLogger.getSystemLogger(LongConnectionCustomerMessageQueue.class);
	private ConnectionManager connectionManager = null;
	private ExecutorService respSenderThreadPool;
	private ExecutorService priorityThreadPool;
	private GmmsUtility gmmsUtility;
	private String moduleName;
	private boolean sharedServerQueueEnabled;

	public LongConnectionCustomerMessageQueue(A2PCustomerInfo cst, ConnectionManager connectionManager, 
			                                  int minSenderNum, int maxSenderNum,boolean isServer){
		super(cst, minSenderNum, maxSenderNum, isServer, "LongConnectionCustomerMessageQueue_" + cst.getSSID(),
				!isSharedServerQueueEnabled(isServer));
		this.connectionManager = connectionManager;
        gmmsUtility = GmmsUtility.getInstance();
        moduleName = System.getProperty("module");
        sharedServerQueueEnabled = isSharedServerQueueEnabled(isServer);
        if (sharedServerQueueEnabled) {
        	log.info("Skip per-SSID LongConnectionCustomerMessageQueue pools because MultiSmppServer shared customer queue is enabled. ssid={}",
        			cst.getSSID());
        } else {
    		// response sender thread pool
            ThreadPoolProfile profile = new ThreadPoolProfileBuilder(this.queueName + "_Resp")
                    .poolSize(minSenderNum).maxPoolSize(maxSenderNum).build();
            respSenderThreadPool = executorServiceManager.newExpiredThreadPool(this, "CustRespSender_" + cst.getSSID(), profile, this, queueTimeout);
            int processNum = cst.getPrioritryHanderNumber();
            priorityThreadPool = executorServiceManager.newFixedThreadPool(this, "CustPriority_" + queueName, processNum);
        }
	}
	
	public boolean putMsg(GmmsMessage msg) {
		if(msg == null){
			return false;
		}
		if (sharedServerQueueEnabled) {
			return LongConnectionCustomerSharedQueueManager.getInstance().submit(this, msg, connectionManager, cst);
		}
		if (log.isTraceEnabled()) {
			log.trace(msg, "submit to CustomerMessageSender thread pool");
		}
		
		try {
			String msgType = msg.getMessageType();
			if(GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msgType)
	    			||GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msgType)
	    			||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msgType)
	    			||GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(msgType)){
				respSenderThreadPool.execute(new CustomerMessageSender(msg, connectionManager, cst));
			}else{
				A2PCustomerInfo oInfo = gmmsUtility.getCustomerManager().getCustomerBySSID(msg.getOSsID());
				int priority = oInfo.getPriority();
				Map map = cst.getPriorityPercentMap();				
				if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msgType) 
						&& map!= null 
						&& !map.isEmpty() 
						&& cst.getOutgoingThrottlingNum() > 0
						&& ModuleManager.getInstance().getClientModules().contains(moduleName) ) {
					// V4.1 迁移：不再使用 Priority List 轮询，转而投递到针对此节点的输出流
					//boolean produceSuccess = gmmsUtility.getStreamQueueManager().produceOutboundMessage(msg);
					if (!false) {
						log.error(msg, "Failed to produce message to Outbound Stream in putMsg!");
					} else {
						log.trace(msg, "put message to outbound stream");
					}
				}else {
					senderThreadPool.execute(new CustomerMessageSender(msg, connectionManager, cst));
				}
				
				//senderThreadPool.execute(new CustomerMessageSender(msg, connectionManager, cst));
			}
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
    	ModuleManager moduleManager = ModuleManager.getInstance();
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
		}else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg.getMessageType())) {
			msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
	        msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
    		TransactionURI transaction = msg.getInnerTransaction();
    		routerQueue = transaction.getConnectionName();
		}else if(GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg.getMessageType()) 
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP.equalsIgnoreCase(msg.getMessageType())){
			msg.setStatusCode(1);
	        msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
    		TransactionURI innerTransaction = msg.getInnerTransaction();
    		routerQueue = innerTransaction.getConnectionName();
		}else{
			routerQueue = moduleManager.selectRouter(msg);
		}
    	InternalAgentConnectionFactory factory = InternalAgentConnectionFactory.getInstance();
    	OperatorMessageQueue msgQueue = factory.getMessageQueue(msg, routerQueue);
    	if(msgQueue != null){
    		msgQueue.putMsg(msg);
    	}
	}
	
    public void stopMessageQueue() {
	    super.stopMessageQueue();
    	if (executorServiceManager != null) {
    		if (respSenderThreadPool != null) {
    			executorServiceManager.shutdown(respSenderThreadPool);
    		}
    		if (priorityThreadPool != null) {
    			executorServiceManager.shutdown(priorityThreadPool);
    		}
    	}
     }

	private static boolean isSharedServerQueueEnabled(boolean isServer) {
		if (!isServer) {
			return false;
		}
		String module = System.getProperty("module");
		if (module == null || !module.startsWith("MultiSmppServer")) {
			return false;
		}
		try {
			String moduleKey = module + ".LongConnectionCustomerSharedQueueEnable";
			String moduleValue = GmmsUtility.getInstance().getCommonProperty(moduleKey);
			if (moduleValue != null) {
				return Boolean.parseBoolean(moduleValue.trim());
			}
			return Boolean.parseBoolean(GmmsUtility.getInstance()
					.getCommonProperty("MultiSmppServer.LongConnectionCustomerSharedQueueEnable", "false").trim());
		} catch (Exception e) {
			return false;
		}
	}

}
