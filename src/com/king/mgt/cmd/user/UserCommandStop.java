package com.king.mgt.cmd.user;


import java.util.Arrays;

import com.king.framework.SystemLogger;

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
public class UserCommandStop
    extends UserCommand {

    public static final String keyword = "shutdown";
    private static SystemLogger log = SystemLogger.getSystemLogger(UserCommandStop.class);

    public UserCommandStop(){
        super(UserCommand.Notify_All);
    }

    /**
     * parseArgs
     *
     * @param args String[]
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean parseArgs(String[] args) {
        // shutdown -a | shutdown -ha module ... | shutdown module ...: the targets are read from the args
        // (RedisControlCommands; before, they were not kept and every module was told to stop)
        if (args.length < 2) {
            return false;
        }
        super.args = Arrays.asList(args);
        if (args[1].equals("-ha") && args.length < 3) {
            return false;
        }
        if (args[1].equals("-a")) {
            this.notifyType = Notify_All;
        } else {
            this.notifyType = Notify_Fix;
        }
        return true;
    }
    /**
     * process
     *
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean process() {
    	// published on the control channel (RedisControlCommands), not run here
    	return false;
    }
}
