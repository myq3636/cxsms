package com.king.gmms.routing;

import java.util.List;

import com.king.db.DatabaseStatus;
import com.king.framework.A2PService;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.gmms.processor.CsmProcessorHandler;
import com.king.gmms.messagequeue.MTStreamConsumer;
import com.king.gmms.messagequeue.InboundDRStreamConsumer;
import com.king.gmms.messagequeue.ResultStreamConsumer;
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
    protected ModuleStatusReporter statusReporter = null;

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
		gmmsUtility.initCoreShardAssignment();
		statusReporter = ModuleStatusReporter.start(gmmsUtility, "core", module, gmmsUtility.getNodeId());
		gmmsUtility.initDBManager(dbstatus);
		gmmsUtility.initCDRManager();
		DeliveryRouterHandler.getInstance();
		MessageProcessorHandler.getInstance();
		CsmProcessorHandler.getInstance();
		DBBackupHandler.getInstance();
		// Core module stream consumers
		MTStreamConsumer.getInstance().start();
		InboundDRStreamConsumer.getInstance().start();
		ResultStreamConsumer.getInstance().start();
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
		// Core module stream consumers
		MTStreamConsumer.getInstance().stop();
		InboundDRStreamConsumer.getInstance().stop();
		ResultStreamConsumer.getInstance().stop();
		return true;
	}
	/**
	 * send stop request
	 */
	public void beforeStop() {
        if (statusReporter != null) {
            statusReporter.stop();
            statusReporter = null;
        }
	}
}
