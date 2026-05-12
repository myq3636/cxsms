package com.king.gmms.routing;

import java.util.List;

import com.king.db.DatabaseStatus;
import com.king.framework.A2PService;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.king.gmms.processor.CsmProcessorHandler;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.routing.IOSMSDispatcher;
import com.king.gmms.processor.DBBackupHandler;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.gmms.processor.SenderRentHandler;
import com.king.gmms.processor.SystemStatusChecker;

public class DeliveryRouter implements A2PService{

	private static SystemLogger log = SystemLogger.getSystemLogger(DeliveryRouter.class);
	private ModuleManager manager = null;
    private GmmsUtility gmmsUtility = null;
    private SystemStatusChecker systemStatusChecker = null;
    protected String module;
    protected ScheduledExecutorService heartbeatExecutor = null;

	public DeliveryRouter(){
		gmmsUtility = GmmsUtility.getInstance();
        module = System.getProperty("module");
		manager = ModuleManager.getInstance();
	}
    /**
     * startService
     */
	public boolean startService() {
		DatabaseStatus dbstatus = DatabaseStatus.MASTER_USED;
		String redisStatus = "M";
		
		gmmsUtility.initRedisClient(redisStatus);
		gmmsUtility.initDBManager(dbstatus);
		gmmsUtility.initCDRManager();
		DeliveryRouterHandler.getInstance();
		MessageProcessorHandler.getInstance();
		CsmProcessorHandler.getInstance();
		DBBackupHandler.getInstance();
		// V4.0 Start Redis Stream Consumer for MT messages
		MTStreamConsumer.getInstance().start();
		// V4.5 Start Redis Stream Consumer for DR messages
		InboundDRStreamConsumer.getInstance().start();
		//SenderRentHandler.getInstance();
		
		if(gmmsUtility.isStoreDRModeEnable()){
			systemStatusChecker = new SystemStatusChecker();
			systemStatusChecker.start();
		}
		
		try {
			String moduleName = System.getProperty("module");
	        if (ModuleManager.getInstance().getRouterModules().contains(moduleName)) {
	        	gmmsUtility.getCustomerManager().loadRoutingInfoToRedis(moduleName);
	        	gmmsUtility.getCustomerManager().loadSenderBlacklistToRedis(moduleName);
	        	gmmsUtility.getCustomerManager().loadRecipientBlacklistToRedis(moduleName);
	        	gmmsUtility.getCustomerManager().loadSenderWhitelistToRedis(moduleName);
	        	gmmsUtility.getCustomerManager().loadContentActionlistToRedis(moduleName, true);
	        	gmmsUtility.getCustomerManager().loadContentActionlistToRedis(moduleName, false);
	        	gmmsUtility.getCustomerManager().loadSystemRoutingReplaceToRedis(moduleName);
	        }
		} catch (Exception e) {
			log.error("load routing to redis failed.",e);
		}
		
		// V4.0 Start Redis Heartbeat
		String nodeId = System.getProperty("NodeID", "0");
		final String statusKey = "module:status:" + module + ":" + nodeId;
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    gmmsUtility.getRedisClient().setString(statusKey, "ONLINE");
                    gmmsUtility.getRedisClient().setExpire(statusKey, 30);
                } catch (Exception e) {
                    log.warn("Failed to update Redis heartbeat for " + module, e);
                }
            }
        }, 0, 10, TimeUnit.SECONDS);
		
		return true;
	}
	/**
     * stopService
     */
	public boolean stopService() {
		beforeStop();
		if(systemStatusChecker != null){
			systemStatusChecker.stop();
		}
		// V4.0 Stop Redis Stream Consumer
		MTStreamConsumer.getInstance().stop();
		// V4.5 Stop Redis Stream Consumer
		InboundDRStreamConsumer.getInstance().stop();
		return true;
	}
	/**
	 * send stop request
	 */
	public void beforeStop() {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
        }
        try {
            String nodeId = System.getProperty("NodeID", "0");
            String statusKey = "module:status:" + module + ":" + nodeId;
            gmmsUtility.getRedisClient().del(statusKey);
        } catch (Exception e) {
            log.warn("Failed to delete module status key on stop", e);
        }
	}
}
