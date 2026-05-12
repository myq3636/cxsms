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
public class SendQueryDRTask extends TaskTimer {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SendQueryDRTask.class);

	public SendQueryDRTask() {
		timer = new Timer("SendQueryDRTask");
	}

	public void init(String tableName) {
		taskTable = tableName;
		taskName = "SendQueryDRTask";
		String configPrefix = taskTable + "." + taskName;
		TaskConfiguration config = TaskConfiguration.getInstance();
		interval = Integer.parseInt(config.getProperty(configPrefix
				+ ".Interval", "60")) * 1000L;
		log.info("init Timer: SendQueryDRTask for table: {}, Interval:{}",
				tableName, interval);
	}

	/**
	 * get resend DR from DB
	 */
	public void executeTask() {
		try {
			ArrayList<GmmsMessage> list = msm.getQueryDR();
			if (list == null || list.size() == 0) {
				log.debug("No QueryDR Message need to send or resend.");
				return;
			}
			if(log.isInfoEnabled()){
				log.info("{} QueryDR Message need to send or resend got from DB.",
					list.size());
			}
			Random dom = new Random();
			messageSenders.get(dom.nextInt(messageSenders.size())).putMsg(list);
		} catch (Exception e) {
			log.error(e, e);
		}
	}
}
