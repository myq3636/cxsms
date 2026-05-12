package com.king.message.gmms;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.event.SwitchDNSEvent;

public class ExceptionMessageManager {

    private SystemLogger log = SystemLogger.getSystemLogger(ExceptionMessageManager.class);
    private static ExceptionMessageManager instance = new ExceptionMessageManager();
    private ExceptionMessageWriter asynExMsgWriter = null;
    private LinkedBlockingQueue<GmmsMessage> exMsgQueue = null;
    private ExMsgFileWriter exFileWriter = null;
    private HashMap<String,String> dcNameMap = new HashMap<String,String>();
	
	public ExceptionMessageManager(){
		asynExMsgWriter = new ExceptionMessageWriter();
		asynExMsgWriter.initialize();
		exMsgQueue = new LinkedBlockingQueue<GmmsMessage>(1000);
		exFileWriter = new ExMsgFileWriter();
		exFileWriter.start();
	}
	
	public static ExceptionMessageManager getInstance(){
    	return instance;
    }
	
    public boolean insertExceptionMessage(GmmsMessage message,String name){
    	boolean result = false;
    	
    	if(message == null){
    		log.warn("message is null when execute insertExceptionMessage() function. ");
    		return result;
    	}
    	GmmsMessage msg = new GmmsMessage(message);
    	result = exMsgQueue.offer(msg);
    	
    	if(!result){
    		if(log.isInfoEnabled()){
    			log.info(msg,"put message into exMsgQueue failed, this message will be throwed.");
    		}    		
    	}else{
    		dcNameMap.put(msg.getMsgID(), name);
    	}
    	
    	return result;
    }
    
    class ExMsgFileWriter implements Runnable {
        volatile boolean running = false;
        GmmsMessage message = null;
        public void run() {
            while (running) {
                try{
                	message = exMsgQueue.poll(500L, TimeUnit.MILLISECONDS);
                	if(message != null){
                		asynExMsgWriter.insertExceptionMessage(message,dcNameMap.get(message.getMsgID()));
                	}
                } catch (Exception e) {
                    log.error(e, e);
                }
            }
            if (log.isInfoEnabled()) {
            	log.info("ExMsgFileWriter thread stop!");
            }
        }

        public void start(){
            running = true;
            Thread monitor = new Thread(A2PThreadGroup.getInstance(), this,
                                   "ExMsgFileWriter");
            monitor.start();
            if (log.isInfoEnabled()) {
            	log.info("ExMsgFileWriter thread start!");
            }
        }

        public void stop(){
            running = false;
        }
    }

}
