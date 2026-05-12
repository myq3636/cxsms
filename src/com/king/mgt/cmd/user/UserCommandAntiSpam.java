package com.king.mgt.cmd.user;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemCommandAntiSpam;
import com.king.mgt.connection.FTP4Client;

public class UserCommandAntiSpam extends UserCommand {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(UserCommandAntiSpam.class);
	public static final int TYPE_RELOAD = 4;
	public static final int TYPE_GENERATE = 5;
	public static final int TYPE_ACTIVE = 6;

	public static final String keyword = "antiSpam";

	public UserCommandAntiSpam() {
		super(UserCommand.Notify_All);
	}

	@Override
	public SystemCommand buildSystemCommand() {
		return new SystemCommandAntiSpam(this);
	}

	@Override
	public boolean parseArgs(String[] args) {
		// TODO Auto-generated method stub
		if (args.length == 2 && args[1].equals("-r")) {
			this.type = TYPE_RELOAD;
			return true;
		} else if (args.length == 2 && args[1].equals("-g")) {
			this.type = TYPE_GENERATE;			
			return true;
		} else if (args.length == 2 && args[1].equals("-a")) {
			this.type = TYPE_ACTIVE;
			return true;
		}
		return false;
	}

	@Override
	public boolean process() {
		// TODO Auto-generated method stub
		boolean rtnval = false;
		switch (type) {
		case TYPE_GENERATE: {
			FTP4Client ftp = new FTP4Client();
			return ftp.downloadConfigFile(keyword);
		}
		case TYPE_RELOAD: {
			FTP4Client ftp = new FTP4Client();
			ftp.downloadConfigFile(keyword);
		}
		case TYPE_ACTIVE: {
			return processor.handleCommandActive();
		}
		default: {
			log.error("Unknow command type: {}", type);
		}
		}
		return rtnval;
	}

}
