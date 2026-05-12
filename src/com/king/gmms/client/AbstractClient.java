package com.king.gmms.client;

import java.net.ServerSocket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.king.framework.A2PService;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;

public abstract class AbstractClient implements A2PService {
	private static SystemLogger log = SystemLogger.getSystemLogger(AbstractClient.class);
	protected GmmsUtility gmmsUtility;
    protected volatile boolean running;
    protected String module;
    protected ServerSocket server = null;
    protected A2PCustomerManager ctm = null;
    protected ScheduledExecutorService heartbeatExecutor = null;
    
    public AbstractClient() {
    	 gmmsUtility = GmmsUtility.getInstance();
         running = true;
         module = System.getProperty("module");
         ctm = gmmsUtility.getCustomerManager();
    }
    protected boolean initSystemManagement(){
        gmmsUtility.initRedisClient("M");
    	return true;
    }
    
    protected void startRedisHeartbeat() {
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
    }

    protected void stopRedisHeartbeat() {
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

    public boolean isRunning() {
        return running;
    }
}
