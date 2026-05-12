package com.king.gmms.mqm;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import com.king.db.DatabaseStatus;
import com.king.framework.*;
import com.king.gmms.GmmsUtility;
import com.king.gmms.mqm.task.TaskTimer;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.ThreadPoolProfileBuilder;
import com.king.gmms.util.*;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class MsgQueueMonitor implements A2PService, QueueTimeoutInterface {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(MsgQueueMonitor.class);
	private GmmsUtility gmmsUtility = null;
	private ExpiredMessageQueueWithSafeExit messageQueueDb;
	private ExpiredMessageQueueWithSafeExit drQueueRedis;
	private Map<String, TaskExecutor> taskExecutors = new HashMap<String, TaskExecutor>();
	private List<MQMMessageSender> drSenders = null;
	private List<MQMMessageSender> retrySenders = null;
	private SystemSession systemSession = null; // system client
	private SystemSessionFactory sysFactory = null;
	private boolean isEnableSysMgt = false;
    protected boolean canHandover = false;
	private SystemListener systemListener = null;
	private ExecutorService exthreadPool = null;

	/**
	 * Creates a new instance of MsgQueueMonitor
	 */
	public MsgQueueMonitor() {
		gmmsUtility = GmmsUtility.getInstance();
		isEnableSysMgt = gmmsUtility.isSystemManageEnable();
		canHandover = gmmsUtility.isDBHandover();
		if (isEnableSysMgt||canHandover) {
			systemListener = SystemListener.getInstance();
			try {
				sysFactory = SystemSessionFactory.getInstance();
				systemSession = sysFactory.getSystemSessionForFunction();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		drSenders = new ArrayList<>();
		retrySenders = new ArrayList<>();
	}

	public boolean startService() {
		DatabaseStatus dbstatus = DatabaseStatus.MASTER_USED;
		String redisStatus = "M";
		if (isEnableSysMgt||canHandover) {
			systemListener.start();
			if (systemSession != null) {
				ModuleRegisterAck ack = sysFactory.moduleRegisterInDetail();
	        	if(ack!=null){
	        		String dbstatusStr = ack.getDbStatus();
	        		dbstatus = DatabaseStatus.get(dbstatusStr);
	        		redisStatus = ack.getRedisStatus();
	        	}else{
	        		log.warn("startService failed due to module register!");
	        	}
			}
		}
		gmmsUtility.initRedisClient(redisStatus);
		gmmsUtility.initDBManager(dbstatus);		
		gmmsUtility.initCDRManager();
		
		// V4.1 Async Transformation: No more TCP MQM connection/listener needed
		// startMQMConnection();
		// mqmListener.start();
		
		startMessageSender();
		startDelayDRMessageSender();
		startTaskTimers();
		startRedisRetrieveThread();
		startRedisSendDelayDRThread();
		log.info("Message monitor has started in asynchronous mode.");
		return true;
	}


	public boolean stopService() {
		if (isEnableSysMgt||canHandover) {
			beforeStop();
			systemListener.stop();
			if (systemSession != null) {
				systemSession.shutdown();
			}
		}
		return true;
	}

	private void startMessageSender() {
		int queueTimeout = GmmsUtility.getInstance().getCacheMsgTimeout();
		messageQueueDb = new ExpiredMessageQueueWithSafeExit(queueTimeout,
				false, "MsgQueueMonitorQueue_DB");
		messageQueueDb.setListener(this);
		messageQueueDb.start();
		for(int i=0; i<6; i++){
			MQMMessageSender sender = new MQMMessageSender(messageQueueDb);
			new Thread(A2PThreadGroup.getInstance(), sender,
					"MQM Message Sender_"+i).start();
			retrySenders.add(sender);
		}		
	}
	
	private void startDelayDRMessageSender() {
		int queueTimeout = GmmsUtility.getInstance().getCacheMsgTimeout();
		drQueueRedis = new ExpiredMessageQueueWithSafeExit(queueTimeout,
				false, "MsgQueueMonitorQueue_DR_DB");
		drQueueRedis.setListener(this);
		drQueueRedis.start();
		for(int i=0; i<6; i++){
			MQMMessageSender sender = new MQMMessageSender(drQueueRedis);
			new Thread(A2PThreadGroup.getInstance(), sender,
					"MQM dr redis Sender_"+i).start();
			drSenders.add(sender);
		}		
	}

	private void startTaskTimers() {
		TaskHolder.init();
		if (this.isEnableSysMgt) {
			boolean applyOk = false;
			do {
				try {
					applyOk = this.systemSession.applyDBSession();
					Thread.sleep(5 * 1000);
				} catch (Exception e) {
					log.warn(e, e);
				}
			} while (!applyOk);
		}
		ArrayList<String> tables = TaskHolder.getTables();
		for (String table : tables) {
			TaskExecutor taskExecutor = new TaskExecutor(table);
			new Thread(A2PThreadGroup.getInstance(), taskExecutor,
					"taskExe:" + table).start();
			ArrayList<TaskTimer> taskTimers = TaskHolder.getTasks(table);
			for (TaskTimer taskTimer : taskTimers) {
				taskTimer.setTaskExecutor(taskExecutor);
				taskTimer.setSender(retrySenders);
				taskTimer.startTimer();
			}
			taskExecutors.put(table, taskExecutor);
		}
	}
	
	private void startRedisRetrieveThread(){
		if("True".equalsIgnoreCase(gmmsUtility.getCommonProperty("RedisEnable", "True"))){
			RetrieveRedisDRThread redisRetrieve = new RetrieveRedisDRThread();
			redisRetrieve.start();
		}
	}
	
	private void startRedisSendDelayDRThread(){
		if("True".equalsIgnoreCase(gmmsUtility.getCommonProperty("RedisEnable", "True"))){
			SendDelayRedisDRThread redisRetrieve = new SendDelayRedisDRThread();
			redisRetrieve.setMessageSender(drSenders);
			redisRetrieve.start();
		}
	}

	/**
	 * buffer timeout
	 */
	public void timeout(Object message) {
		GmmsMessage msg = (GmmsMessage) message;
		try {
			if (msg != null) {
				if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
						.getMessageType())) {
					msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
							.getCode());
					gmmsUtility.getMessageStoreManager()
							.handleInDeliveryReportRes(msg);
				} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
						.equalsIgnoreCase(msg.getMessageType())) {
					msg.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
					gmmsUtility.getMessageStoreManager()
							.handleOutDeliveryReportRes(msg);
				} else {
					msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					gmmsUtility.getMessageStoreManager()
							.handleOutSubmitRes(msg);
				}
				if(log.isTraceEnabled()){
					log.trace("timeout bufferMonitor: {}", msg.toString());
				}
			}
		} catch (Exception ex) {
			log.error(msg, ex, ex);
		}
	}

	/**
	 * send stop request
	 */
	public void beforeStop() {
		if (this.isEnableSysMgt||canHandover) {
			systemSession.moduleStop();
		}
	}
}
