package com.king.gmms.listener.commonhttpserver;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.king.gmms.GmmsUtility;
import com.king.redis.RedisClient;

public class BillingCounter {
	
	private long timestamp;
	private Map<String, String> billingCount;
	private static BillingCounter instance = null; 
	private byte[] lock = new byte[0];
	protected GmmsUtility gmmsUtility;
	private RedisClient redis = null;
	public synchronized static BillingCounter getInstance() {
		if (instance == null) {  
            instance = new BillingCounter();  
        }  
        return instance; 
	}
	
	public GmmsUtility getGmmsUtility() {
		return gmmsUtility;
	}



	public void setGmmsUtility(GmmsUtility gmmsUtility) {
		this.gmmsUtility = gmmsUtility;
	}



	public RedisClient getRedis() {
		return redis;
	}



	public void setRedis(RedisClient redis) {
		this.redis = redis;
	}



	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, String> getBillingCount() {
		return billingCount;
	}
	public void setBillingCount(Map<String, String> billingCount, long newTimestamp) {
		if (newTimestamp == timestamp) {
			return;
		}
		synchronized(lock) {
			if (newTimestamp == timestamp) {
				return;
			}
			this.billingCount = billingCount;
			this.timestamp = newTimestamp;
		}
		
	}
	
	public Integer countMessage(String ssid) {
		Integer result = null;
		if(billingCount == null) {
			return result;
		}
		synchronized (ssid) {
			String value = billingCount.get(ssid);
			result = value==null? null:Integer.parseInt(value);
			if (value !=null) {
				result = Integer.parseInt(value);
				if (result>0) {
					synchronized (lock) {
						if (value.equals(billingCount.get(ssid))) {
							billingCount.put(ssid, ""+(result-1));
						}
					}					
				}				
			}
		}
		return result;
	}
	
	public void startTask() {
		Timer timer = new Timer();
        TimerTask task =new TimerTask(){
            public void run(){
            	Map<String, String> values = redis.getHashAll("balance");
    			if(values != null) {
    				String tString = values.get("timestamp");
    				long timestamp = System.currentTimeMillis();
    				if (tString != null ) {
    					timestamp = Long.parseLong(tString);
    				}
    				setBillingCount(values, timestamp);
    			}    				
            }
	  };
	  
	  timer.schedule(task,10,5*60*1000);
	}
}
