/**
 * Copyright 2000-2014 King Inc. All rights reserved.
 */
package com.king.gmms.mqm.task;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.message.gmms.GmmsMessage;

import java.util.Random;
import java.util.Timer;
import java.util.ArrayList;

/**
 * 
 * @author bensonchen
 * @version 1.0.0
 */
public class SendScheduledMessageTask extends TaskTimer {
	private static SystemLogger log = SystemLogger.getSystemLogger(SendScheduledMessageTask.class);
	private String storedMessageTaskOPList = "";
	private String ropFailedMessageTaskOPList = "";

	public SendScheduledMessageTask() {
		timer = new Timer("SendStoredMessageTask");
	}

	public void init(String tableName) {
		taskTable = tableName;
		taskName = "SendScheduledMessageTask";
		String configPrefix = taskTable + "." + taskName;
		TaskConfiguration config = TaskConfiguration.getInstance();
		interval = Integer.parseInt(config.getProperty(configPrefix + ".Interval", "10")) * 1000L;
		
		storedMessageTaskOPList = config.getProperty(taskTable + ".SendStoredMessageTask" + ".OPList", "");
		ropFailedMessageTaskOPList = config.getProperty(taskTable + ".SendROPFailedMessageTask" + ".OPList", "");
		log.info("init Timer: SendScheduledMessageTask for table: {}, Interval:{}", tableName, interval);
	}

	/**
	 * get submit type message from DB
	 */
	public void executeTask() {
		try {
			ArrayList<GmmsMessage> list = msm.getScheduledMessage(storedMessageTaskOPList, ropFailedMessageTaskOPList);
			if (list == null || list.size() == 0) {
				log.trace("No scheduled Message found.");
				return;
			}
			if(log.isInfoEnabled()){
				log.info("{} Scheduled Message need to send got from DB.", list.size());
			}
			Random dom = new Random();
			messageSenders.get(dom.nextInt(messageSenders.size())).putMsg(list);
		} catch (Exception e) {
			log.error(e, e);
		}
	}
}
