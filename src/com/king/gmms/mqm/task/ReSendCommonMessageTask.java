package com.king.gmms.mqm.task;

import java.util.Random;
import java.util.Timer;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.message.gmms.GmmsMessage;

import java.util.ArrayList;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class ReSendCommonMessageTask extends TaskTimer {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ReSendCommonMessageTask.class);
	private String priorityList = "";

	public ReSendCommonMessageTask() {
		timer = new Timer("ReSendCommonMessageTask");
	}

	public void init(String tableName) {
		taskTable = tableName;
		taskName = "ReSendCommonMessageTask";
		String configPrefix = taskTable + "." + taskName;
		TaskConfiguration config = TaskConfiguration.getInstance();
		interval = Integer.parseInt(config.getProperty(configPrefix
				+ ".Interval", "60")) * 1000L;
		priorityList = config.getProperty(configPrefix + ".Priority");
		log
				.info(
						"init Timer: ReSendCommonMessageTask for table: {}, Interval:{}, Priority:{}",
						tableName, interval, priorityList);
	}

	public void executeTask() {
		try {
			ArrayList<GmmsMessage> list = msm.getResendMessage(priorityList);
			if (list == null || list.size() == 0) {
					log.debug("No Common Message need to resend.");
				return;
			}
			if(log.isInfoEnabled()){
				log.info("{} Common Message need to resend got from DB.", list
					.size());
			}
			Random dom = new Random();
			messageSenders.get(dom.nextInt(messageSenders.size())).putMsg(list);
		} catch (Exception e) {
			log.error(e, e);
		}
	}
}
