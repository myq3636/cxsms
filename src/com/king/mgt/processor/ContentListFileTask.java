package com.king.mgt.processor;

import java.util.TimerTask;

import com.king.framework.SystemLogger;
import com.king.mgt.connection.RedisControlCommands;
import com.king.mgt.cmd.user.UserCommandContentBlacklist;
import com.king.mgt.cmd.user.UserCommandContentWhitelist;
import com.king.mgt.cmd.user.UserCommandSenderBlacklist;
import com.king.mgt.cmd.user.UserCommandSenderWhitelist;
import com.king.mgt.cmd.user.UserCommandWhitelist;

public class ContentListFileTask extends TimerTask {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(ContentListFileTask.class);
	
	@Override
	public void run() {		
		log.info("content black and white list file task start");
		try {
			UserCommandContentWhitelist whitelistCommand = new UserCommandContentWhitelist();	
			whitelistCommand.setType(UserCommandWhitelist.TYPE_ACTIVE);
			// RELOAD_*:-a on the control channel, every module reloads the list (it went by TCP to each
			// module before, which no module receives in stream mode)
			RedisControlCommands.publish(whitelistCommand);
		} catch (Exception e) {
			log.error("Catch exception when download policy file task", e);
		}
		
		try {
			UserCommandContentBlacklist whitelistCommand = new UserCommandContentBlacklist();	
			whitelistCommand.setType(UserCommandWhitelist.TYPE_ACTIVE);
			// RELOAD_*:-a on the control channel, every module reloads the list (it went by TCP to each
			// module before, which no module receives in stream mode)
			RedisControlCommands.publish(whitelistCommand);
		} catch (Exception e) {
			log.error("Catch exception when download policy file task", e);
		}
		
		log.info("content black and white list file task end");
	}

	

}
