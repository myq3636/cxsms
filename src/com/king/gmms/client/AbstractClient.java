package com.king.gmms.client;

import java.net.ServerSocket;
import com.king.framework.A2PService;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.ha.ModuleStatusReporter;
import com.king.gmms.messagequeue.OutboundStreamConsumer;

public abstract class AbstractClient implements A2PService {
	private static SystemLogger log = SystemLogger.getSystemLogger(AbstractClient.class);
	protected GmmsUtility gmmsUtility;
    protected volatile boolean running;
    protected String module;
    protected ServerSocket server = null;
    protected A2PCustomerManager ctm = null;
    protected ModuleStatusReporter statusReporter = null;
    
    public AbstractClient() {
    	 gmmsUtility = GmmsUtility.getInstance();
         running = true;
         module = System.getProperty("module");
         ctm = gmmsUtility.getCustomerManager();
    }
    protected boolean initSystemManagement(){
        gmmsUtility.initRedisClient("M");
        gmmsUtility.initClientShardAssignment();
        statusReporter = ModuleStatusReporter.start(gmmsUtility, "client", module, gmmsUtility.getNodeId());
     // 初始化时启动 Client 专属的流消费者
        startOutboundConsumer();
    	return true;
    }
    
    protected void startRedisHeartbeat() {
        if (statusReporter == null) {
            statusReporter = ModuleStatusReporter.start(gmmsUtility, "client", module, gmmsUtility.getNodeId());
        }
    }

    protected void stopRedisHeartbeat() {
        if (statusReporter != null) {
            statusReporter.stop();
            statusReporter = null;
        }
    }
    
    /**
     * 新增：启动下行消息流消费者
     */
    protected void startOutboundConsumer() {
        try {
            OutboundStreamConsumer.getInstance().start();
            log.info("OutboundStreamConsumer successfully started for module: " + module);
        } catch (Exception e) {
            log.error("Failed to start OutboundStreamConsumer", e);
        }
    }

    /**
     * 新增：停止下行消息流消费者（子类实现 stop 或 shutdown 时需调用此方法）
     */
    protected void stopOutboundConsumer() {
        try {
            OutboundStreamConsumer.getInstance().stop();
            log.info("OutboundStreamConsumer safely stopped for module: " + module);
        } catch (Exception e) {
            log.error("Failed to stop OutboundStreamConsumer", e);
        }
    }

    public boolean isRunning() {
        return running;
    }
}
