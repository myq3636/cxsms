package com.king.gmms.processor;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.util.ExpiredMessageQueueWithHandlerThreadAndSafeExit;
import com.king.gmms.util.QueueTimeoutInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * 
 * @author levens
 * @version 1.0.0
 */
public class SenderRentHandler  implements QueueTimeoutInterface{

	private static SenderRentHandler instance = new SenderRentHandler();
    private static SystemLogger log = SystemLogger.getSystemLogger(SenderRentHandler.class);
	private ExpiredMessageQueueWithHandlerThreadAndSafeExit messageQueue;
    private int queueTimeout = 10*60*1000;
    private GmmsUtility gmmsUtility = null;
    private volatile boolean initialization = false;
    private int processorNum = 10;
    private int queueNum = 5;
    
	private SenderRentHandler(){
		gmmsUtility = GmmsUtility.getInstance();
        queueTimeout = gmmsUtility.getCacheMsgTimeout();
		messageQueue = new ExpiredMessageQueueWithHandlerThreadAndSafeExit(queueTimeout, false,"SenderRentThread");
        processorNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("SenderRentThreadNumber", "1").trim());
        queueNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("SenderRentQueueNumber", "1").trim());
	}
    
	private boolean init() throws Exception{
		if(initialization){
			return initialization;
		}
        
        messageQueue.init(SenderRentThread.class, this, queueNum, processorNum);
        
        messageQueue.startMessageQueue();
        
        initialization = true;
        return initialization;
	}
	
	public  synchronized static SenderRentHandler getInstance(){
		if(!instance.initialization){
			try{
				instance.init();
			}catch(Exception e){
				log.error("Got Error when to nitialize MessageProcessorThread and error is {}", e.getMessage());
			}
		}
		return instance;
	}
	
	public boolean putMsg(GmmsMessage message){
		if(message == null){
			return false;
		}
		return messageQueue.put(message);
	}
	
	/**
	 * buffer timeout
	 */
	public void timeout(Object msg) {
		GmmsMessage bufferedMsg = (GmmsMessage)msg;
        if(log.isInfoEnabled()){
        	log.info(bufferedMsg,"{} is timeout in Sender Rent Queue",bufferedMsg.getMessageType());
        }
        try {
            if (bufferedMsg != null) {
                if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(
                    bufferedMsg.getMessageType())) {
                    bufferedMsg.setStatusCode(GmmsStatus.
                                              FAIL_SENDOUT_DELIVERYREPORT.
                                              getCode());
                    gmmsUtility.getMessageStoreManager().handleInDeliveryReportRes(bufferedMsg);
                }else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(
                    bufferedMsg.getMessageType())) {
                    bufferedMsg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
                    gmmsUtility.getMessageStoreManager().handleOutDeliveryReportRes(bufferedMsg);
                }
                else {
                    bufferedMsg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
                    gmmsUtility.getMessageStoreManager().handleOutSubmitRes(bufferedMsg);
                }
            }
        }
        catch (Exception ex) {
            log.error(bufferedMsg,ex, ex);
        }
    }
}
