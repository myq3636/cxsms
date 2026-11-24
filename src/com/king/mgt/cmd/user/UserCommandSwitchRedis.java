package com.king.mgt.cmd.user;

import java.util.Arrays;


public class UserCommandSwitchRedis extends UserCommand {
	public static final String keyword = "switchredis";

    public UserCommandSwitchRedis(){
        super(UserCommand.Notify_Fix);
    }

	@Override
	public boolean parseArgs(String[] args) {
		super.args = Arrays.asList(args);
		if(args.length!=2){
			return false;
		}
		String opt = args[1].trim();
		if("master".equalsIgnoreCase(opt)||"slave".equalsIgnoreCase(opt)){
	        return true;
		}
        return false;
	}

	@Override
	public boolean process() {
		// published on the control channel (RedisControlCommands), not run here
		return false;
	}

}
