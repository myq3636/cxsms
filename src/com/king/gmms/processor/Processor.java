package com.king.gmms.processor;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;


public class Processor {
    private static SystemLogger log = SystemLogger.getSystemLogger(Processor.class);
    protected GmmsUtility gmmsUtility;
    protected InternalAgentConnectionFactory factory = null;

    public Processor() {
        gmmsUtility = GmmsUtility.getInstance();
        factory = InternalAgentConnectionFactory.getInstance();
    }

   public void logFail(GmmsMessage message) {
       String messageType = message.getMessageType();
       if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)){
    	   message.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
           message.setStatus(GmmsStatus.COMMUNICATION_ERROR); 
       }else if(GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(messageType)|| 
   		    GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(messageType)) {
    	   message.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
           message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
       }
       else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(messageType)) { //if delivery report query
           message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
    	   message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
       }
       else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(messageType)) { //if delivery report query
    	   message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
           message.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
       }
       else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(messageType)) {
    	   message.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
           message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
       }
       else { //invalid message type
           log.warn(message,"Unknown Message Type when update the fail status");
           message.setStatus(GmmsStatus.UNKNOWN_ERROR);
       }
       if(log.isInfoEnabled()){
			log.info(message,"Log failed, statuscode:{}",message.getStatus());
       }
	   	TransactionURI transaction = message.getInnerTransaction();
	   	String routerQueue = null;
	   	if(transaction != null){
	   		routerQueue = transaction.getConnectionName();
			OperatorMessageQueue msgQueue = factory.getMessageQueue(message, routerQueue);
			if(msgQueue != null){
				msgQueue.putMsg(message);
			}
	   	}else{
	   		log.warn(message,"Can not find the transaction");
	   	}

  }

}
