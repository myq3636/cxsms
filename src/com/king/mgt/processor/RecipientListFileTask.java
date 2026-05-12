package com.king.mgt.processor;

import java.util.TimerTask;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandRecipientBlacklist;

import com.king.mgt.cmd.user.UserCommandWhitelist;

public class RecipientListFileTask extends TimerTask {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(RecipientListFileTask.class);
	protected CommandProcessor processor = null;
	
	@Override
	public void run() {		
		log.info("recipient black list file task start");
		try {
			UserCommandRecipientBlacklist whitelistCommand = new UserCommandRecipientBlacklist();	
			whitelistCommand.setType(UserCommandWhitelist.TYPE_ACTIVE);
			processor = new CommandProcessor(whitelistCommand);
			processor.handleCommandActive();
		} catch (Exception e) {
			log.error("Catch exception when download policy file task", e);
		}

		log.info("recipient black list file task end");
	}

	

}
