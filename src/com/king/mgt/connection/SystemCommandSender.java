package com.king.mgt.connection;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.util.UserInterfaceUtility;

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
public class SystemCommandSender {
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemCommandSender.class);
    private int timeout=0;
    private UserInterfaceUtility util;
    public SystemCommandSender() {
        util = UserInterfaceUtility.getInstance();
        timeout = Integer.parseInt(util.getProperty("SystemConnectionTimeout",
            "30")) * 1000;
    }
    public void send(SystemCommand []cmds)
    {
        SystemConnection []connections=new SystemConnection[cmds.length];
        for (int i=0; i<connections.length; i++)
        {
        	if(cmds[i]==null){
        		continue;
        	}
            log.info("Send System Command to Module: {}",cmds[i].getModule());
            connections[i]=new SystemConnection(cmds[i], timeout);
            connections[i].start();
        }
        for (int i=0; i<connections.length; i++)
        {
            try {
                if(connections[i] == null)
                    continue;
                connections[i].join(timeout + 1000);
            }
            catch (InterruptedException ex) {
                log.warn(ex,ex);
            }
        }
    }


}
