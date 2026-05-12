package com.king.gmms.sender;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForCore;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.processor.MessageProcessorThread;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.SerializableHandler;


public class CorePrioritrySender implements Runnable{
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CorePrioritrySender.class);
	protected ExecutorService senderThreadPool;
	private String moduleName;
	private List<Integer> priorityList = new ArrayList<Integer>();
	private int allPercent=0;
	
	public CorePrioritrySender(ExecutorService pool, String moduleName) {
		this.senderThreadPool = pool;
		this.moduleName = moduleName;
	}

	@Override
	public void run() {
		try {
			Thread.sleep(1*60*1000);
		} catch (Exception e) {
			// TODO: handle exception
		}
		while (true) {						
			try {
				A2PCustomerManager manager = GmmsUtility.getInstance().getCustomerManager();
				ArrayList<Integer> ssidList = manager.getCorePriority_ssid_list();
				if (ssidList == null || ssidList.isEmpty()) {
					Thread.sleep(50);
					continue;
				}
				doSendMsg(manager, ssidList);				
				Thread.sleep(5);
			} catch (Exception e) {
			   log.error("CorePriority sender tread throw exception", e);
			}
			
		}
	}

	private void doSendMsg(A2PCustomerManager manager,
			ArrayList<Integer> ssidList) {
		for (Integer ssid : ssidList) {
			A2PCustomerInfo cst = manager.getCustomerBySSID(ssid);
			Map<Integer,Integer> priorityMap = cst.getPriorityPercentMap();
			if(cst.isNeedCheckSession()){
				ConnectionManagementForCore connectionManager = ConnectionManagementForCore.getInstance();
				String queue = cst.getChlQueue();
				GmmsMessage msg = new GmmsMessage();
				msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
				Session session = connectionManager.getSession(msg, ssid, queue);
				if(session == null){
					continue;
				}
			}
			
			int outgoingThrottleNum = cst.getCoreProcessorThrottlingNum();
			priorityList.clear();
			allPercent = 0;
			if (priorityMap!=null && !priorityMap.isEmpty()) {
				for (Integer priority : priorityMap.keySet()) {
					priorityList.add(priority);
					allPercent = allPercent+priorityMap.get(priority);
				}
			}
			if (priorityList.isEmpty() || allPercent == 0) {
				continue;
			}
			Collections.sort(priorityList);
			int index=0;
			for (int i = 0; i < priorityList.size(); i++) {				
				int priorityMapKey = priorityList.get(i);
				String key = "CorePriority_processer_"+moduleName+"_"+cst.getSSID()+"_"+priorityMapKey;
				int percent = priorityMap.get(priorityMapKey);
				int throttleNuber = outgoingThrottleNum*percent/allPercent;
				int queryProrityNumber  = throttleNuber+index;
				for (int j = 0; j < queryProrityNumber; j++) {
					String msg = GmmsUtility.getInstance().getRedisClient().rpop(key);
					index = queryProrityNumber-j-1;
					if (msg!=null) {
						GmmsMessage message = SerializableHandler.convertRedisMssage2GmmsMessageForCorePrioritry(msg);
						log.debug(message,"get message from redis by key :{}", key);
						if (message != null) {
							while (cst.getCoreProcessorThrottlingNum() > 0 
									&& !ThrottlingControl.getInstance().isAllowedToHander(cst.getSSID()))  {
								try {
									Thread.sleep(10L);
								} catch (Exception e) {								
								
								}
							}
							senderThreadPool.execute(new MessageProcessorThread(message));
						}													
					}else {
						break;
					}					
				}				
			}
			
		}
	}

}
