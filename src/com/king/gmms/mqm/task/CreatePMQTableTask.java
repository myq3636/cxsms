package com.king.gmms.mqm.task;

import java.util.Calendar;
import java.util.Timer;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
/**
 * CreatePMQTable Timer Task
 * @author Jianming
 * @version 1.0.1
 *
 */
public class CreatePMQTableTask extends TaskTimer {
    private static SystemLogger log = SystemLogger.getSystemLogger(CreatePMQTableTask.class);
    private int tableNumber = 0;
    public CreatePMQTableTask(){
    	timer = new Timer("CreatePMQTableTask");
    }
	@Override
	public void executeTask() {
		try {
            msm.createPMQTable(tableNumber);
        } catch (Exception e) {
            log.error(e, e);
        }
	}
	/**
	 * override 
	 * execute this task in 00:00:00 every day
	 */
	public void startTimer() {
    	Calendar cal = Calendar.getInstance(); 
    	//execute at 00:00:00:00
    	cal.set(Calendar.HOUR_OF_DAY,0);
    	cal.set(Calendar.MINUTE,0);
    	cal.set(Calendar.SECOND,0);
    	cal.set(Calendar.MILLISECOND, 0);
    	java.util.Date curdate = cal.getTime();
			log.info("start Timer, table: PMQ,taskName: CreatePMQTableTask,startTimer:{}",curdate);
        timer.scheduleAtFixedRate(this, curdate, interval);
	}
	@Override
	public void init(String tableName) {
        String configPrefix = "PMQ.CreatePMQTableTask.Number";
        TaskConfiguration config = TaskConfiguration.getInstance();
        interval = 24*3600*1000L; //run every day
        tableNumber = Integer.parseInt(config.getProperty(configPrefix, "4"));
        	log.info("init Timer: CreatePMQTableTask, tableNumber:{}",tableNumber);
	}

}
