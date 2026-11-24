package com.king.mgt.processor;

import java.util.TimerTask;

import com.king.framework.SystemLogger;
import com.king.mgt.connection.RedisControlCommands;
import com.king.mgt.cmd.user.UserCommandSenderBlacklist;
import com.king.mgt.cmd.user.UserCommandSenderWhitelist;
import com.king.mgt.cmd.user.UserCommandWhitelist;

public class SenderListFileTask extends TimerTask {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(SenderListFileTask.class);
	
	@Override
	public void run() {		
		log.info("sender black and white list file task start");
		try {
			UserCommandSenderWhitelist whitelistCommand = new UserCommandSenderWhitelist();	
			whitelistCommand.setType(UserCommandWhitelist.TYPE_ACTIVE);
			// RELOAD_*:-a on the control channel, every module reloads the list (it went by TCP to each
			// module before, which no module receives in stream mode)
			RedisControlCommands.publish(whitelistCommand);
		} catch (Exception e) {
			log.error("Catch exception when download policy file task", e);
		}
		
		try {
			UserCommandSenderBlacklist whitelistCommand = new UserCommandSenderBlacklist();	
			whitelistCommand.setType(UserCommandWhitelist.TYPE_ACTIVE);
			// RELOAD_*:-a on the control channel, every module reloads the list (it went by TCP to each
			// module before, which no module receives in stream mode)
			RedisControlCommands.publish(whitelistCommand);
		} catch (Exception e) {
			log.error("Catch exception when download policy file task", e);
		}
		
		log.info("sender black and white list file task end");
	}

	

}
