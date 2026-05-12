package com.king.gmms.processor;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.threadpool.ProcessorHandler;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageStoreManager;
import com.king.gmms.metrics.MetricsCollector;

public class DBBackupHandler extends ProcessorHandler{
	
	private static DBBackupHandler instance = new DBBackupHandler();
    private static SystemLogger log = SystemLogger.getSystemLogger(DBBackupHandler.class);
    private GmmsUtility gmmsUtility = null;
    private volatile boolean initialization = false;
    private MessageStoreManager msm;
    
	private DBBackupHandler(){
        try {
			gmmsUtility = GmmsUtility.getInstance();
			
			// processor thread pool
	        int queueTimeout = GmmsUtility.getInstance().getCacheMsgTimeout();
	        int minProcessorNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MinDBBackupHandlerNumber", "1").trim());
	        int maxProcessorNum = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("MaxDBBackupHandlerNumber", "5").trim());
	        int workQueueSize = Integer.parseInt(gmmsUtility.getFullModuleTypeProperty("ProcessorWorkQueueSize", "10000").trim());
	        ThreadPoolProfile profile = new ThreadPoolProfileBuilder("DBBackupHandler")
	                                         .poolSize(minProcessorNum).maxPoolSize(maxProcessorNum)
	                                         .maxQueueSize(workQueueSize).needSafeExit(true).build();
	        handlerThreadPool = gmmsUtility.getExecutorServiceManager().newExpiredThreadPool(this, "DBBackupThread", profile, this, queueTimeout);
	        
	        msm = GmmsUtility.getInstance().getMessageStoreManager();
	        
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
	
	public  synchronized static DBBackupHandler getInstance(){
		if(!instance.initialization){
			try{
				instance.init();
			}catch(Exception e){
				log.error("Got Error when to nitialize DeliveryRouterThread and error is {}", e.getMessage());
			}
		}
		return instance;
	}

	
	public boolean putMsg(GmmsMessage message){
		boolean ret = false;
		if(message == null){
			return ret;
		}
		try {
			handlerThreadPool.execute(new DBBackupThread(message));
			MetricsCollector.getInstance().incrementCounter("dbbackup.putMsg");
			ret = true;
		} catch(Exception e) {
			if (log.isInfoEnabled()) {
				log.info("putMsg exception: ", e);
			}
			msm.handleMessageError(message);
			ret = false;
		}
		return ret;
		
	}
	/**
	 * buffer timeout
	 */
	public void timeout(Object message) {
    	GmmsMessage bufferedMsg = (GmmsMessage)message;
        try {
            if (bufferedMsg != null) {
                if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(
                        bufferedMsg.getMessageType())) {
                        bufferedMsg.setStatusCode(GmmsStatus.
                                                  FAIL_SENDOUT_DELIVERYREPORT.
                                                  getCode());
                        msm.handleInDeliveryReportRes(bufferedMsg);
                    }else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(
                        bufferedMsg.getMessageType())) {
                        bufferedMsg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
                        msm.handleOutDeliveryReportRes(bufferedMsg);
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
}
