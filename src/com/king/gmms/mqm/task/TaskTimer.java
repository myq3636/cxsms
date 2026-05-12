package com.king.gmms.mqm.task;

import java.util.List;
import java.util.TimerTask;
import java.util.Timer;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.mqm.MQMMessageSender;
import com.king.gmms.mqm.TaskExecutor;
import com.king.message.gmms.MessageStoreManager;

public abstract class TaskTimer extends TimerTask {
	private static SystemLogger log = SystemLogger.getSystemLogger(TaskTimer.class);
	protected TaskExecutor taskExecutor = null;
	protected MessageStoreManager msm = null;
	protected List<MQMMessageSender> messageSenders = null;
	protected long interval = 10000L;
	protected Timer timer;
	protected String taskName = "";
	protected String taskTable = "";

	public TaskTimer() {
		msm = GmmsUtility.getInstance().getMessageStoreManager();
	}

	/**
	 * run to put itself into the task list of schedule
	 */
	public void run() {
		if (taskExecutor != null)
			taskExecutor.putTask(this);
	}

	public void startTimer() {
		if(log.isInfoEnabled()){
			log.info("start Timer, table: {} taskName: {}", taskTable, taskName);
		}
		timer.schedule(this, 0, interval);
	}

	public void setTaskExecutor(TaskExecutor executor) {
		this.taskExecutor = executor;
	}

	public void setSender(List<MQMMessageSender> senders) {
		this.messageSenders = senders;
	}

	public abstract void init(String tableName);

	public abstract void executeTask();
}
