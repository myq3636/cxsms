package com.king.mgt.cmd.user;


import java.util.Arrays;

import com.king.framework.SystemLogger;

public class UserCommandSwitchDNS extends UserCommand {

	public static final String keyword = "switchdns";
    private SystemLogger log = SystemLogger.getSystemLogger(UserCommandSwitchDNS.class);
    public static final int TYPE_MASTER = 1;
    public static final int TYPE_SLAVE = 2;
    public UserCommandSwitchDNS(){
        super(UserCommandSwitchDNS.Notify_Fix);
    }

    /**
     * parseArgs
     *
     * @param args String[]
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean parseArgs(String[] args) {
        // switchdns master|slave, published to every module (RedisControlCommands reads the args; before,
        // they were not kept and SWITCH_DNS went out without master / slave)
        if (args.length != 2) {
            return false;
        }
        super.args = Arrays.asList(args);
        this.notifyType = Notify_Fix;
        if ("master".equalsIgnoreCase(args[1].trim())) {
            this.type = TYPE_MASTER;
            return true;
        } else if ("slave".equalsIgnoreCase(args[1].trim())) {
            this.type = TYPE_SLAVE;
            return true;
        }
        return false;
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
