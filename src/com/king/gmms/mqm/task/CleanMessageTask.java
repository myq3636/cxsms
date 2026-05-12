package com.king.gmms.mqm.task;

import com.king.framework.SystemLogger;
import com.king.gmms.mqm.TaskConfiguration;
import com.king.message.gmms.GmmsMessage;

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
public class CleanMessageTask extends TaskTimer {
    private static SystemLogger log = SystemLogger.getSystemLogger(CleanMessageTask.class);

    public CleanMessageTask() {
        timer = new Timer("CleanMessageTask");
    }

    public void init(String tableName) {
        taskTable = tableName;
        taskName = "CleanMessageTask";
        String configPrefix = taskTable + "." + taskName;
        TaskConfiguration config = TaskConfiguration.getInstance();
        interval = Integer.parseInt(config.getProperty(configPrefix + ".Interval", "3600")) * 1000L;
	        log.info("init Timer: CleanMessageTask for table: {}, Interval:{}",tableName,interval);
    }

    public void executeTask() {
        try {
            msm.clearMessages(taskTable);
        } catch (Exception e) {
            log.error(e, e);
        }
    }
}
