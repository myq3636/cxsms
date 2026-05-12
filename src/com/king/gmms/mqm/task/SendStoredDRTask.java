package com.king.gmms.mqm.task;

import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.message.gmms.GmmsMessage;

public class SendStoredDRTask extends TaskTimer {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(SendStoredDRTask.class);

	public SendStoredDRTask() {
		timer = new Timer("SendStoredDRTask");
	}

	@Override
	public void executeTask() {
		// TODO Auto-generated method stub

		try {
			ArrayList<GmmsMessage> list = msm.getStoredDR();
			if (list == null || list.size() == 0) {
				log.debug("No StoredDR Message need to send or resend.");
				return;
			}
			if(log.isInfoEnabled()){
				log.info("{} StoredDR Message need to send or resend got from DB.",
					list.size());
			}
			Random dom = new Random();
			messageSenders.get(dom.nextInt(messageSenders.size())).putMsg(list);
		} catch (Exception e) {
			log.error(e, e);
		}

	}

	@Override
	public void init(String tableName) {
		// TODO Auto-generated method stub
		taskTable = tableName;
		taskName = "SendStoredDRTask";
		String configPrefix = taskTable + "." + taskName;
		TaskConfiguration config = TaskConfiguration.getInstance();
		interval = Integer.parseInt(config.getProperty(configPrefix
				+ ".Interval", "60")) * 1000L;
		log.info("init Timer: SendStoredDRTask for table: {}, Interval:{}",
				tableName, interval);
	}

}
