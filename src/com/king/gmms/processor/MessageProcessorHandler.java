package com.king.gmms.processor;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.threadpool.ProcessorHandler;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class MessageProcessorHandler extends ProcessorHandler{
	
	private static MessageProcessorHandler instance = new MessageProcessorHandler();
	
    private static SystemLogger log = SystemLogger.getSystemLogger(MessageProcessorHandler.class);
    private GmmsUtility gmmsUtility = null;
    private volatile boolean initialization = false;
	private String moduleName; 

	private MessageProcessorHandler(){
        try {
			gmmsUtility = GmmsUtility.getInstance();
			
			// processor thread pool
	        int queueTimeout = GmmsUtility.getInstance().getCacheMsgTimeout();
	        int minProcessorNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MinMessageProcessorNumber", "1").trim());
	        int maxProcessorNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MaxMessageProcessorNumber", "5").trim());
	        int workQueueSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("ProcessorWorkQueueSize", "10000").trim());
	        ThreadPoolProfile profile = new ThreadPoolProfileBuilder("MessageProcessorHandler")
	                                         .poolSize(minProcessorNum).maxPoolSize(maxProcessorNum)
	                                         .maxQueueSize(workQueueSize).needSafeExit(true).build();
	        handlerThreadPool = gmmsUtility.getExecutorServiceManager().newExpiredThreadPool(this, "MessageProcessorThread", profile, this, queueTimeout);
	        moduleName = System.getProperty("module");
	        
		} catch (Exception e) {
			log.error(e, e);
		}
	}
	
	private boolean init() throws Exception{
		if(initialization){
			return initialization;
		}
        
        initialization = true;
        return initialization;
	}
	
	public  synchronized static MessageProcessorHandler getInstance(){
		if(!instance.initialization){
			try{
				instance.init();
			}catch(Exception e){
				log.error("Got Error when to nitialize MessageProcessorThread and error is {}", e.getMessage());
			}
		}
		return instance;
	}

	public ExecutorService getHandlerThreadPool() {
		return handlerThreadPool;
	}
	
	public boolean putMsg(GmmsMessage message){
		boolean ret = false;
		if(message == null){
			return ret;
		}
		try {
			A2PCustomerInfo cst = gmmsUtility.getCustomerManager().getCustomerBySSID(message.getRSsID());
			A2PCustomerInfo oInfo = gmmsUtility.getCustomerManager().getCustomerBySSID(message.getOSsID());
			int priority = oInfo.getPriority();
			if (cst == null ) {				
				if(GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType()) 
						&& (oInfo.isSmsOptionSendDRByInSubmitInCU()||oInfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress()))
						&& message.getRetriedNumber()<1	){	
					//backup routing didn't need to send indr at 2024.09.14
					boolean indrFlag = true;
					String rssids = message.getRoutingSsIDs();
					if(rssids!=null) {
						int oldLen = rssids.length();
						int newLen = rssids.replaceAll(",", "").length();
						if(oldLen-newLen>2) {
							indrFlag = false;
						}
					}
					if(indrFlag) {
						handlerThreadPool.execute(new AutoSendInDRProcessorThread(message));
						message.setFakeDR(false);
					}
				}
				handlerThreadPool.execute(new MessageProcessorThread(message));
			}else{
				Map map = cst.getPriorityPercentMap();
				if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType()) 					
						&& cst.getCoreProcessorThrottlingNum() > 0
						&& map!= null 
						&& !map.isEmpty() 
						&& ModuleManager.getInstance().getRouterModules().contains(moduleName) ) {
					if((oInfo.isSmsOptionSendDRByInSubmitInCU()||oInfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress()))
							&& message.getRetriedNumber()<1){
						//backup routing didn't need to send indr at 2024.09.14
						boolean indrFlag = true;
						String rssids = message.getRoutingSsIDs();
						if(rssids!=null) {
							int oldLen = rssids.length();
							int newLen = rssids.replaceAll(",", "").length();
							if(oldLen-newLen>2) {
								indrFlag = false;
							}
						}
						if(indrFlag) {
							handlerThreadPool.execute(new AutoSendInDRProcessorThread(message));
							message.setFakeDR(false);
						}
					}					
					handlerThreadPool.execute(new MessageProcessorThread(message));
					log.debug(message, "core priority list bypassed, put message to MessageProcessorThread");
				}else {
					if(GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(message.getMessageType()) 
							&& (oInfo.isSmsOptionSendDRByInSubmitInCU()||oInfo.isNeedFakeDRByInSubmitRecPrefixInCU(message.getRecipientAddress()))
							&& message.getRetriedNumber()<1){	
						//backup routing didn't need to send indr at 2024.09.14
						boolean indrFlag = true;
						String rssids = message.getRoutingSsIDs();
						if(rssids!=null) {
							int oldLen = rssids.length();
							int newLen = rssids.replaceAll(",", "").length();
							if(oldLen-newLen>2) {
								indrFlag = false;
							}
						}
						if(indrFlag) {
							handlerThreadPool.execute(new AutoSendInDRProcessorThread(message));
							message.setFakeDR(false);
						}
					}
					handlerThreadPool.execute(new MessageProcessorThread(message));
				}
			}									
			ret = true;
		} catch(Exception e) {
			if (log.isInfoEnabled()) {
				log.info("putMsg exception: ", e);
			}
			ret = false;
		}
		return ret;
	}
	/**
	 * buffer timeout
	 */
	public void timeout(Object msg) {
		GmmsMessage bufferedMsg = (GmmsMessage)msg;
        if(log.isInfoEnabled()){
        	log.info(bufferedMsg,"{} is timeout in Message Processer Queue",bufferedMsg.getMessageType());
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
