package com.king.gmms.mqm;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.TimeZone;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageStoreManager;
import com.king.redis.RedisClient;
import com.king.redis.SerializableHandler;

public class RetrieveRedisDRThread extends Thread {
	private static SystemLogger log = SystemLogger.getSystemLogger(RetrieveRedisDRThread.class);
	private int sendRedisDRTime = 30 * 60;
	private static String DRTime = "DRRETRIEVETIMEKEY";
	private String table = MessageStoreManager.WDQ;
	protected MessageStoreManager msm = null;
	private RedisClient redis = null;
	private GmmsUtility gmmsUtility = null;
	private boolean startFlag = false;
	
	private void init() {
		gmmsUtility = GmmsUtility.getInstance();
		msm = gmmsUtility.getMessageStoreManager();
		redis = gmmsUtility.getRedisClient();
		//redis = RedisClient.getInstance();
		try{
			sendRedisDRTime = Integer.parseInt(
					gmmsUtility.getCommonProperty("Redis_RetrieveRedisInterval", "30") )* 60;
		}catch(Exception e){
			sendRedisDRTime = 30*60;
			log.warn(e,e);
		}
	}
	
	public void run(){
		log.debug("Redis send DR to wdq thread is start!");
		while(startFlag){			
			try{
				execute();
				
				Thread.sleep(1000);
				
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
		
		Thread thread = new Thread(A2PThreadGroup.getInstance(), this, "RetrieveRedisDRThread");	
		thread.start();
	}
	
	public void execute() {
		try {
			ArrayList<GmmsMessage> list = null;
			
			long currentTime = System.currentTimeMillis()/1000;
			//long currentTime = getGMTTime(System.currentTimeMillis())/1000;
			
			long coventTime = currentTime;
			
			try {
				String time = redis.getString(DRTime);
				if(time == null){
					coventTime = currentTime - sendRedisDRTime;
				}else{
					coventTime = Long.parseLong(time.substring(2));
					if(currentTime - sendRedisDRTime < coventTime){
						Thread.sleep((coventTime + sendRedisDRTime - currentTime)*1000L);
					}
				}
			} catch (Exception e) {
				coventTime = currentTime - sendRedisDRTime;
			}
			
			long i = coventTime;
			
			for( ; i <= currentTime - sendRedisDRTime ; i++){
				
				Set<String> set = redis.getHash(new StringBuffer(50).append("DR").append(i).toString());
				
				if(set == null || set.size() <= 0){
					continue;
				}
				
			    list = new ArrayList<GmmsMessage>();
				
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
				
				for(GmmsMessage msg : list){
					msm.insertMessageToDB(msg, table);
				}
				list.clear();
			}
			
			redis.setString(DRTime, new StringBuffer(50).append("DR").append(i).toString());

		} catch (Exception e) {
			log.error(e, e);
		}
	}
	
	public static void main(String[] args) {
		long t = Long.parseLong(new StringBuffer(50).append(123).toString());
		
	}
}
