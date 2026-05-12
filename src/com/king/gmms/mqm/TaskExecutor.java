package com.king.gmms.mqm;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.task.TaskTimer;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TaskExecutor implements Runnable {
    private static SystemLogger log = SystemLogger.getSystemLogger(TaskExecutor.class);
    private static final int TASK_MAX_NUM = 10;
    private String table;
    private LinkedBlockingQueue<TaskTimer> taskList = new LinkedBlockingQueue<TaskTimer>();

    public TaskExecutor(String table) {
        this.table = table;
    }

    /**
     * run
     */
    public void run(){
    	if(log.isInfoEnabled()){
			log.info("TaskExecutor for {} start!",table);
    	}
        TaskTimer task = null;
        while(true){
            try{
                task = taskList.poll(5000L, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.executeTask();
                }
            }catch(Exception e){
                log.error(e, e);
            }
        }
    }

    /**
     * put task
     * @param task TaskTimer
     */
    public void putTask(TaskTimer task){
        if(taskList.size() < TASK_MAX_NUM){
            if(!taskList.offer(task)) {
                log.warn("insert Task into TaskExecutor: {} failed.",table);
            }
        } else {
            log.warn("TaskExecutor: {} up to max number of Task",table);
        }
    }
}
