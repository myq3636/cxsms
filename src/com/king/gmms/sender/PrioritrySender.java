package com.king.gmms.sender;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import sun.util.logging.resources.logging;
import java_cup.internal_error;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;


public class PrioritrySender extends RunnableMsgTask{
	private static SystemLogger log = SystemLogger
			.getSystemLogger(PrioritrySender.class);
	protected ExecutorService senderThreadPool;
	private A2PCustomerInfo cst;
	private ConnectionManager connectionManager = null;
	private String moduleName;
	private boolean isRunning = true;
	private List<Integer> priorityList = new ArrayList<Integer>();
	private int allPercent=0;
	
	public PrioritrySender(ExecutorService pool, A2PCustomerInfo rcst, ConnectionManager connectionManager, String moduleName) {
		this.senderThreadPool = pool;
		this.cst = rcst;
		this.connectionManager = connectionManager;
		this.moduleName = moduleName;
	}

	@Override
	public void run() {
		try {
			Thread.sleep(60*1000);
		} catch (Exception e) {
			// TODO: handle exception
		}
		while (isRunning) {						
			try {
				Map<Integer,Integer> priorityMap = cst.getPriorityPercentMap();
				int outgoingThrottleNum = cst.getOutgoingThrottlingNum();
				priorityList.clear();
				allPercent = 0;
				if (priorityMap!=null && !priorityMap.isEmpty()) {
					for (Integer priority : priorityMap.keySet()) {
						priorityList.add(priority);
						allPercent = allPercent+priorityMap.get(priority);
					}
				}
				if (priorityList.isEmpty() || allPercent == 0) {
					Thread.sleep(50L);
					continue;
				}
				Collections.sort(priorityList);
				doSendMsg(priorityMap, outgoingThrottleNum,
						allPercent);
				Thread.sleep(5);
				
			} catch (InterruptedException e) {
			    log.error("prioritry sender tread throw InterruptedException", e);
				isRunning = false;
			}catch (Exception e) {
			   log.error("prioritry sender tread throw exception", e);
			   isRunning = false;
			}
			
		}
	}

	private void doSendMsg(Map<Integer, Integer> priorityMap,
			int outgoingThrottleNum, int allPercent) {
		int index = 0;
		for (int i = 0; i < priorityList.size(); i++) {				
			int priorityMapKey = priorityList.get(i);
			String key = "Priority_"+moduleName+"_"+cst.getSSID()+"_"+priorityMapKey;
			int percent = priorityMap.get(priorityMapKey);
			int throttleNuber = outgoingThrottleNum*percent/allPercent;
			int queryProrityNumber  = throttleNuber+index;
			for (int j = 0; j < queryProrityNumber; j++) {
				String msg = GmmsUtility.getInstance().getRedisClient().rpop(key);
				index = queryProrityNumber-j-1;
				if (msg!=null) {
					GmmsMessage message = SerializableHandler.convertRedisMssage2GmmsMessageForCorePrioritry(msg);
					log.debug(message,"get message from redis by key :{}, {}", key, message);
					if (message!=null) {
						while (cst.getOutgoingThrottlingNum() > 0 
								&& !ThrottlingControl.getInstance().isAllowedToSend(cst.getSSID())) {
							try {
								Thread.sleep(10L);
							} catch (Exception e) {								
							
							}
						}
						senderThreadPool.execute(new CustomerMessageSender(message, connectionManager, cst));
					}												
				}else {
					break;
				}					
			}				
		}
		return;
	}
    


}
