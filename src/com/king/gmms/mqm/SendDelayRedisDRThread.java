package com.king.gmms.mqm;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

public class SendDelayRedisDRThread extends Thread {
	private static SystemLogger log = SystemLogger.getSystemLogger(SendDelayRedisDRThread.class);
	
	protected MessageStoreManager msm = null;
	private RedisClient redis = null;
	private GmmsUtility gmmsUtility = null;
	private boolean startFlag = false;
	protected List<MQMMessageSender> messageSenders = null;
	
	private void init() {
		gmmsUtility = GmmsUtility.getInstance();
		msm = gmmsUtility.getMessageStoreManager();
		redis = gmmsUtility.getRedisClient();
		//redis = RedisClient.getInstance();
	}
	
	public void run(){
		log.debug("Redis send DR to customer thread is start!");
		while(startFlag){			
			try{
				execute();				
				Thread.sleep(100);
				
			}catch(Exception e){
				log.warn(e,e);
			}
		}
	}
	
	public void start(){
		if(startFlag){
			return;
		}
		
		init();
		
		startFlag = true;
		
		Thread thread = new Thread(A2PThreadGroup.getInstance(), this, "SendDelayRedisDRThread");	
		thread.start();
	}
	
	public void execute() {
		try {
			ArrayList<GmmsMessage> list = new ArrayList<GmmsMessage>();
			int i = 0;
			long currentTime = System.currentTimeMillis()/1000;			
			Set<String> ssidSet = redis.smembers("delayDR");
			for (String ssidKey : ssidSet) {
				int ssid = Integer.parseInt(ssidKey.split("_")[1]);
				A2PCustomerInfo customer = gmmsUtility.getCustomerManager().getCustomerBySSID(ssid);
				if(customer == null) {
					continue;
				}
				int sendDrImmediately = customer.getSendDelayDRImmediately();
				Set<String> set = null;
				if (sendDrImmediately == 0) {
					set = redis.zrange(ssidKey, 1000, currentTime);
				}else if (sendDrImmediately == 1) {
					set = redis.zrangeByIndex(ssidKey, 0, 500);
				}
								
				if(set == null || set.size() <= 0){
					continue;
				}							    		
				for(String outmsgid : set){
					if(outmsgid != null){
						String temp = redis.getString(outmsgid);
		        		if(log.isDebugEnabled()){
		        			log.debug("temp: {}",temp);
		        		}
					    if(temp != null){
					    	GmmsMessage message = SerializableHandler.convertRedisMssage2GmmsMessage(temp);
					    	if(message != null){
					    		list.add(message);
					    	}
					    }
					    redis.del(outmsgid);
					}
				}				
				messageSenders.get(i).putMsg(list);
				i++;
				i = i%messageSenders.size();
				list.clear();
				redis.zrem(ssidKey, set.toArray(new String[set.size()]));
			}						            
		} catch (Exception e) {
			log.error(e, e);
		}
	}
	
	public List<MQMMessageSender> getMessageSender() {
		return messageSenders;
	}

	public void setMessageSender(List<MQMMessageSender> senders) {
		this.messageSenders = senders;
	}

	public static void main(String[] args) {
		long t = Long.parseLong(new StringBuffer(50).append(123).toString());
		
	}
}
