package com.king.gmms.processor;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.threadpool.ProcessorHandler;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;

public class CsmProcessorHandler extends ProcessorHandler {
	private static CsmProcessorHandler instance = new CsmProcessorHandler();
	
    private static SystemLogger log = SystemLogger.getSystemLogger(CsmProcessorHandler.class);
    private GmmsUtility gmmsUtility = null;
    private volatile boolean initialization = false;
    private MessageStoreManager msm;
    
    /**
     * use to query whether receive all concatenated messages of the group
     */
    private CsmIntegrityCache csmIntegrityCache;
    
	private CsmProcessorHandler(){
		try {
			gmmsUtility = GmmsUtility.getInstance();
			
			// processor thread pool
	        int queueTimeout = gmmsUtility.getCacheMsgTimeout();
	        int minProcessorNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MinCSMMessageProcessorNumber", "1").trim());
	        int maxProcessorNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MaxCSMMessageProcessorNumber", "5").trim());
	        int workQueueSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("ProcessorWorkQueueSize", "10000").trim());
	        ThreadPoolProfile profile = new ThreadPoolProfileBuilder("CsmProcessorHandler")
	                                         .poolSize(minProcessorNum).maxPoolSize(maxProcessorNum)
	                                         .maxQueueSize(workQueueSize).needSafeExit(true).build();
	        handlerThreadPool = gmmsUtility.getExecutorServiceManager().newExpiredThreadPool(this, "CsmProcessorThread", profile, this, queueTimeout);
	        
	        msm = gmmsUtility.getMessageStoreManager();
		} catch (Exception e) {
			log.error(e, e);
		}
	}
	
	public  synchronized static CsmProcessorHandler getInstance(){
		if(!instance.initialization){
			try{
				instance.init();
			}catch(Exception e){
				log.error("Got Error when to nitialize MessageProcessorThread and error is {}", e.getMessage());
			}
		}
		return instance;
	}

	private boolean init() throws Exception{
		if(initialization){
			return initialization;
		}
        startCSMCache();
        
        initialization = true;
        return initialization;
	}
	
	public boolean putMsg(GmmsMessage message){
		boolean ret = false;
		if(message == null){
			return ret;
		}
		try {
			handlerThreadPool.execute(new CsmProcessorThread(message));
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
	 * queue timeout
	 */
	public void timeout(Object msg) {
		GmmsMessage bufferedMsg = (GmmsMessage)msg;
        if(log.isInfoEnabled()){
        	log.info(bufferedMsg,"{} is timeout in CSM Processer Queue",bufferedMsg.getMessageType());
        }
        try {
            if (bufferedMsg != null) {
                if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(
                    bufferedMsg.getMessageType())) {
                    bufferedMsg.setStatusCode(GmmsStatus.
                                              FAIL_SENDOUT_DELIVERYREPORT.getCode());
                    msm.handleInDeliveryReportRes(bufferedMsg);
                }
                else {
                    bufferedMsg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
                    msm.handleOutSubmitRes(bufferedMsg);
                }
            }
        }
        catch (Exception ex) {
            log.error(bufferedMsg,ex, ex);
        }
    }
	
    private void startCSMCache(){
        int csmIntegrityCacheTimeout = gmmsUtility.getCsmIntegrityCacheTimeout();
        int csmIntegrityCacheCapacity = gmmsUtility.getCsmIntegrityCacheCapacity();
        csmIntegrityCache = new CsmIntegrityCache(csmIntegrityCacheCapacity);
        csmIntegrityCache.setTimeout(csmIntegrityCacheTimeout);
        csmIntegrityCache.startMonitor();
    }
    
    public CsmIntegrityCache getCSMCache(){
    	return csmIntegrityCache;
    }
    
}
