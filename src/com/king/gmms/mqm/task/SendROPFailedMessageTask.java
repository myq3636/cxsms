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
public class SendROPFailedMessageTask extends TaskTimer {
    private static SystemLogger log = SystemLogger.getSystemLogger(SendROPFailedMessageTask.class);
    private String OPList = "";

    public SendROPFailedMessageTask() {
        timer = new Timer("SendROPFailedMessageTask");
    }

    public void init(String tableName) {
        taskTable = tableName;
        taskName = "SendROPFailedMessageTask";
        String configPrefix = taskTable + "." + taskName;
        TaskConfiguration config = TaskConfiguration.getInstance();
        interval = Integer.parseInt(config.getProperty(configPrefix + ".Interval", "10")) * 1000L;
        OPList = config.getProperty(configPrefix + ".OPList");
            log.info("init Timer: SendROPFailedMessageTask for table:{}, Interval:{}, OPList:{}",tableName,interval, OPList);
    }

    /**
     * get submit type message from DB
     */
    public void executeTask() {
        try {
            ArrayList<GmmsMessage> list = msm.getStoredMessage(false, OPList);
            if (list == null || list.size() == 0) {
                    log.debug("No ROPFailed Message found.");
                return;
            }
            if(log.isInfoEnabled()){
				log.info("{} ROPFailed Message need to send got from DB.",list.size());
            }
            Random dom = new Random();
			messageSenders.get(dom.nextInt(messageSenders.size())).putMsg(list);
        } catch (Exception e) {
            log.error(e, e);
        }
    }
}
