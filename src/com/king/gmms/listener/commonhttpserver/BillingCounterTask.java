package com.king.gmms.listener.commonhttpserver;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;

import javax.servlet.ServletContext;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;

public class BillingCounterTask extends TimerTask{
	private static SystemLogger logger = SystemLogger.getSystemLogger(BillingCounterTask.class);
	private RedisClient redis;
	private GmmsUtility gmmsUtility;
	private BillingCounter billingCounter;
	public BillingCounterTask(GmmsUtility utility) {
		gmmsUtility = utility;
		redis = gmmsUtility.getRedisClient();
		billingCounter = BillingCounter.getInstance();
		billingCounter.setGmmsUtility(gmmsUtility);
		billingCounter.setRedis(redis);
	}

	@Override
	public void run() {

		logger.info("-------start to get the billing counter from redis--------");
    	Map<String, String> values = redis.getHashAll("balance");
		if(values != null) {
			String tString = values.get("timestamp");
			long timestamp = System.currentTimeMillis();
			if (tString != null ) {
				timestamp = Long.parseLong(tString);
			}
			for (Entry<String, String> value : values.entrySet()) {
				if (value != null) {
					logger.info(value.getKey()+":"+value.getValue());
				}				
			}			
			billingCounter.setBillingCount(values, timestamp);
		}    				
    
	}

}
