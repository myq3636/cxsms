package com.king.gmms.mqm.task;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.message.gmms.GmmsMessage;

import java.util.Random;
import java.util.Timer;
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
public class ExpiredMessageTask extends TaskTimer {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ExpiredMessageTask.class);

	public ExpiredMessageTask() {
		timer = new Timer("ExpiredMessageTask");
	}

	public void init(String tableName) {
		taskTable = tableName;
		taskName = "ExpiredMessageTask";
		String configPrefix = taskTable + "." + taskName;
		TaskConfiguration config = TaskConfiguration.getInstance();
		interval = Integer.parseInt(config.getProperty(configPrefix
				+ ".Interval", "60")) * 1000L;
		log.info("init Timer: ExpiredMessageTask for table: {}, Interval:{}",
				tableName, interval);
	}

	public void executeTask() {
		try {
			ArrayList<GmmsMessage> list = msm.getExpiredMessage(taskTable);
			if (list == null || list.size() == 0) {
        		if(log.isDebugEnabled()){
					log.debug("{} No Expired Message need to resend.",
							taskTable);
        		}
				return;
			}
			if(log.isInfoEnabled()){
				log.info("{} {} Expired Message need to resend got from DB.",
					taskTable, list.size());
			}
			Random dom = new Random();
			messageSenders.get(dom.nextInt(messageSenders.size())).putMsg(list);
		} catch (Exception e) {
			log.error(e, e);
		}
	}
}
