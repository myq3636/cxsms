package com.king.gmms.mqm.task;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.message.gmms.GmmsMessage;

import java.util.Random;
import java.util.Timer;
import java.util.ArrayList;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ReSendDRTask extends TaskTimer {
    private static SystemLogger log = SystemLogger.getSystemLogger(ReSendDRTask.class);

    public ReSendDRTask() {
        timer = new Timer("ReSendDRTask");
    }

    public void init(String tableName) {
        taskTable = tableName;
        taskName = "ReSendDRTask";
        String configPrefix = taskTable + "." + taskName;
        TaskConfiguration config = TaskConfiguration.getInstance();
        interval = Integer.parseInt(config.getProperty(configPrefix + ".Interval", "60")) * 1000L;
            log.info("init Timer: ReSendDRTask for table: {}, Interval:{}",tableName, interval);
    }

    public void executeTask() {
        try {
            ArrayList<GmmsMessage> list = msm.getResendDR();
            if (list == null || list.size() == 0) {
                    log.debug("No DR Message need to resend.");
                return;
            }
            if(log.isInfoEnabled()){
				log.info("{} DR Message need to resend got from DB.",list.size());
            }
            Random dom = new Random();
			messageSenders.get(dom.nextInt(messageSenders.size())).putMsg(list);
        } catch (Exception e) {
            log.error(e, e);
        }
    }
}
