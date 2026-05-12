/**
 * Copyright 2000-2014 King Inc. All rights reserved.
 */
package com.king.mgt.cmd.user;

import com.king.mgt.cmd.system.SystemCommand;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class UserCommandGwVersion extends UserCommand {
	
	public static final String keyword = "version";

	/**
	 * @param notifyType
	 */
	public UserCommandGwVersion() {
		super(UserCommand.Notify_None);
	}

	/** 
	 * @param args
	 * @return
	 * @see com.king.mgt.cmd.user.UserCommand#parseArgs(java.lang.String[])
	 */
	@Override
	public boolean parseArgs(String[] args) {
		if (args != null && args.length == 1) {
			return true;
		}
		return false;
	}

	/** 
	 * @return
	 * @see com.king.mgt.cmd.user.UserCommand#process()
	 */
	@Override
	public boolean process() {
		processor.handleCommandVersion();
        return true;
	}

	/** 
	 * @return
	 * @see com.king.mgt.cmd.user.UserCommand#buildSystemCommand()
	 */
	@Override
	public SystemCommand buildSystemCommand() {
		return null;
	}

}
