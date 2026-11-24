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
				if (MqmActiveState.isActive()) {
					execute();
				}
				
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
	
	/**
	 * The DR lookups still without a final DR Redis_RetrieveRedisInterval after their dateIn go to WDQ: on every report
	 * master, through drl:due (DrLookup.moveDue, docs/DR匹配数据迁移设计.md 3.3)
	 */
	public void execute() {
		try {
			int moved = com.king.gmms.messagequeue.DrLookup.moveDue(sendRedisDRTime * 1000L, m -> msm.insertMessageToDB(m, table));
			if (moved > 0) {
				log.info("Redis DR lookup: {} message(s) still without a final DR after {} minutes moved to {}", moved,
						sendRedisDRTime / 60, table);
			}
		} catch (Exception e) {
			log.error(e, e);
		}
	}

	public static void main(String[] args) {
		long t = Long.parseLong(new StringBuffer(50).append(123).toString());
		
	}
}
