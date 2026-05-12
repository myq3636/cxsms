package com.king.mgt.processor;

import java.util.TimerTask;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandWhitelist;

public class WhiteListFileTask extends TimerTask {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(WhiteListFileTask.class);
	protected CommandProcessor processor = null;
	
	@Override
	public void run() {		
		log.info("white list file task start");
		try {
			UserCommandWhitelist whitelistCommand = new UserCommandWhitelist();	
			whitelistCommand.setType(UserCommandWhitelist.TYPE_ACTIVE);
			processor = new CommandProcessor(whitelistCommand);
			processor.handleCommandActive();
		} catch (Exception e) {
			log.error("Catch exception when download policy file task", e);
		}
		
		log.info("white list file task end");
	}

	

}
