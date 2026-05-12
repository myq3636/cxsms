package com.king.gmms.util;

import java.util.ArrayList;
import java.util.Collection;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

/**
 * used for DeliveryRouterHandler/SenderRentHandler, others should used thread pool solution
 * @version 1.0.0
 */
public class ExpiredMessageQueueWithHandlerThreadAndSafeExit implements Queue{
	
	private static SystemLogger log = SystemLogger.getSystemLogger(ExpiredMessageQueueWithHandlerThreadAndSafeExit.class);
	private ArrayList<ExpiredMessageQueueWithSafeExit> messageQueueSet = null;
	private ArrayList<Thread> handlerThreadSet = null;
	private Class handlerClass = null;
	private QueueTimeoutInterface timeoutHandler = null;
	private int handlerNumber = 1;
	private int queueNumber = 1;
	private int limit = 20000;
	private int timeout = 90000;
	protected String queuename;
	private boolean isServer = false;
	private volatile boolean isInit = false;
	private int waitingTime = 50;
	
	public ExpiredMessageQueueWithHandlerThreadAndSafeExit(int limit, int timeout, boolean isServer,String name){
		
		this.limit = Integer.parseInt(GmmsUtility.getInstance().getCommonProperty(
				"ExpiredMessageQueueSize", "50000"));
		if (this.limit <= 0) {
			this.limit = limit;
		}

		if (timeout <= 0) {
			this.timeout = 90000;
		} else {
			this.timeout = timeout;
		}
		this.isServer = isServer;
		queuename = name;
	}
	
	public ExpiredMessageQueueWithHandlerThreadAndSafeExit(int timeout, boolean isServer, String name) {
		this(100000, timeout, isServer, name);
	}

	public ExpiredMessageQueueWithHandlerThreadAndSafeExit(boolean isServer, String name) {
		this(100000, -1, isServer, name);
	}
	
	public void init(Class handlerClass, QueueTimeoutInterface timeoutHandler,
						int queueNumber,int handlerNumber){
		if(handlerClass != null){
			this.handlerClass = handlerClass;
		}else{
			return;
		}
		if(timeoutHandler != null){
			this.timeoutHandler = timeoutHandler;
		}else{
			return;
		}
		if(queueNumber > 1 ){
			this.queueNumber = queueNumber;
		}
		if(handlerNumber > 1){
			this.handlerNumber = handlerNumber;
		}
		messageQueueSet = new ArrayList<ExpiredMessageQueueWithSafeExit>();
		handlerThreadSet = new ArrayList<Thread>();
		isInit = true;
	}
	
	public void init(QueueHandlerInterface handler, QueueTimeoutInterface timeoutHandler){
		init(handler,timeoutHandler);
	}
	
	public boolean startMessageQueue(){
		if(!isInit){
			log.warn("Before start Message Queue, please initialize");
			return false;
		}
		for(int i = 0; i < queueNumber; i++){
			ExpiredMessageQueueWithSafeExit messageQueue = new ExpiredMessageQueueWithSafeExit(limit,timeout,isServer,queuename);
			messageQueue.setListener(timeoutHandler);
			if(waitingTime > 0){
				messageQueue.setWaitingTime(waitingTime);
			}
			messageQueue.start();
			for(int j = 0; j < handlerNumber; j++){
				HandlerThread handlerThread = new HandlerThread(messageQueue,j,queuename + i);
				handlerThread.startHandlerThread();
				handlerThreadSet.add(handlerThread);
			}
			messageQueueSet.add(messageQueue);
		}
		return true;
	}
	
	
	public Object get() {
		log.warn("This is class which consume internally, which can not get any object");
		return null;
	}

	
	public boolean put(Object msg) {
		if(msg == null){
			return false;
		}
		ExpiredMessageQueueWithSafeExit queue = null;
		if(queueNumber == 1){
			queue = messageQueueSet.get(0);
		}else{
			int index = getRandomIndex(queueNumber);
			queue = messageQueueSet.get(index);
		}
		if(queue != null){
			return queue.put(msg);
		}else{
			return false;
		}
	}

	
	public boolean putAll(Collection msgCollection) {
		if(msgCollection == null){
			return false;
		}
		int index = getRandomIndex(queueNumber);
		ExpiredMessageQueue queue = messageQueueSet.get(index);
		if(queue != null){
			return queue.putAll(msgCollection);
		}else{
			return false;
		}
	}

	/**
	 * getRandomIndex
	 * @param size
	 * @return
	 */
	private int getRandomIndex(int size){
		 if(size<=1){
			return 0;
		 }
		 double d = Math.random() * (double)size + 0.5D;  
	     int index = (int)Math.round(d) - 1;
	     return index;
	}
	
	public int size() {
		return 0;
	}
	
	public void setWaitingTime(int waitingTime) {
		this.waitingTime = waitingTime;
	}
	
	
	class HandlerThread extends Thread{
		
		private ExpiredMessageQueueWithSafeExit messageQueue = null;
		private volatile boolean keepRunning = false;
		private int handlerNumber = 0;
		private String queueName = null;
		private QueueHandlerInterface handler = null;
		
		public HandlerThread(ExpiredMessageQueueWithSafeExit messageQueue, int handlerNumber, String queueName){
			this.messageQueue = messageQueue;
			this.handlerNumber = handlerNumber;
			this.queueName = queueName;
			try {
				handler = (QueueHandlerInterface)handlerClass.newInstance();
			} catch (InstantiationException e) {
				log.error("Can not initialize HandlerThread");
			} catch (IllegalAccessException e) {
				log.error("Can not initialize HandlerThread");
			}
		}
		
		public void startHandlerThread(){
			keepRunning = true;
            Thread thread = new Thread(A2PThreadGroup.getInstance(), this,
                    queueName + "_" + handlerNumber);
            thread.start();
		}
		
		public void stopHandlerThread(){
	        keepRunning = false;
	        this.interrupt();
		}
		
		public void run(){
	       if (messageQueue == null) {
	            log.warn("Queue is null in Sender. Please check!");
	        }
	        else {
	            Object object = null;
	            while (keepRunning) {
	                try {
	                	if(waitingTime > 0){
	                		object = messageQueue.get(waitingTime);
	                	}else{
	                		object = messageQueue.getNoWait();
	                	}
	                	if (object == null) {
							Thread.sleep(10);
						}else {
							handler.handle(object);
						}
	                    	                    
	                }
	                catch (Exception e) {
	                    log.error(e, e);
	                }
	            }
	        }
		}
	}


	public boolean isFull() {
		boolean result = false;
		for(ExpiredMessageQueueWithSafeExit queue : messageQueueSet){
			if(queue != null && queue.isFull()){
				result = true;
				break;
			}
		}
		return result;
	}
	
}
