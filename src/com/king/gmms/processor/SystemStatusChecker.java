package com.king.gmms.processor;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

public class SystemStatusChecker implements Runnable{

	private volatile boolean isRunning = false;
	private static SystemLogger log = SystemLogger.getSystemLogger(SystemStatusChecker.class);
	private long waitingTime = 10000L;
	private GmmsUtility gmmsUtility = null;
	private MemoryQueueManagement memoryQueueMgt = null;
	private CPUStatusChecker cpuChecker = null;
	private long lastCheckTime = System.currentTimeMillis();
	private long overloadStatusPeriod = 5*60*1000;
	
	public SystemStatusChecker(){
		gmmsUtility = GmmsUtility.getInstance();
		waitingTime = Integer.parseInt(gmmsUtility.getCommonProperty("SystemStatusCheckInterval","10"))*1000;
		overloadStatusPeriod = Integer.parseInt(gmmsUtility.getCommonProperty("RuninOverloadStatusPeriod","300"))*1000;
		memoryQueueMgt = MemoryQueueManagement.getInstance();
		cpuChecker = CPUStatusChecker.getInstance();
	}
	
	public void run() {
		while(isRunning){
			try{
				Thread.sleep(waitingTime);
				if(gmmsUtility.isRunningStoreDRMode()){
					checkAbnormalStatus();
				}else{
					checkNormalStatus();
				}
			}catch(Exception e){
				log.warn("System status checker catch exception, and exception is {}",e.getMessage());
			}
		}
	}
	
	private void checkAbnormalStatus(){
		boolean checkStatus = memoryQueueMgt.hasOverloadQueue();
		if(!checkStatus){
			checkStatus = cpuChecker.isOverload();
			if(!checkStatus){
				if(System.currentTimeMillis()-lastCheckTime <= overloadStatusPeriod){
					return;
				}else{
					gmmsUtility.resetRunningStoreDRMode();
					if(!gmmsUtility.isRunningStoreDRMode()){
						log.warn("System goes out store DR mode");
					}
				}	
			}
		}
		lastCheckTime = System.currentTimeMillis();
	}
	
	private void checkNormalStatus(){
		boolean checkStatus = memoryQueueMgt.hasOverloadQueue();
		if(checkStatus){
			checkStatus = cpuChecker.isOverload();
			if(checkStatus){
				if(System.currentTimeMillis()-lastCheckTime <= overloadStatusPeriod){
					return;
				}else{
					gmmsUtility.resetRunningStoreDRMode();
					if(gmmsUtility.isRunningStoreDRMode()){
						log.warn("System runs into store DR mode");
					}
				}
			}
		}
		lastCheckTime = System.currentTimeMillis();
	}
	
	public void start(){
        if(isRunning){
            return;
        }
        isRunning = true;
        Thread thread = new Thread(A2PThreadGroup.getInstance(), this, "SystemStatusChecker");
        thread.start();
	}
	
	public void stop(){
		isRunning = false;
		Thread.interrupted();
	}

}
