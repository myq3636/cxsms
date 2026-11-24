package com.king.mgt.processor;

import java.util.TimerTask;

import com.king.framework.SystemLogger;
import com.king.mgt.connection.RedisControlCommands;
import com.king.mgt.cmd.user.UserCommandWhitelist;

public class WhiteListFileTask extends TimerTask {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(WhiteListFileTask.class);
	
	@Override
	public void run() {		
		log.info("white list file task start");
		try {
			UserCommandWhitelist whitelistCommand = new UserCommandWhitelist();	
			whitelistCommand.setType(UserCommandWhitelist.TYPE_ACTIVE);
			// RELOAD_*:-a on the control channel, every module reloads the list (it went by TCP to each
			// module before, which no module receives in stream mode)
			RedisControlCommands.publish(whitelistCommand);
		} catch (Exception e) {
			log.error("Catch exception when download policy file task", e);
		}
		
		log.info("white list file task end");
	}

	

}
