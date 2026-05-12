/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.mgt.cmd.user;


import com.king.framework.SystemLogger;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemCommandContentTpl;
import com.king.mgt.connection.FTP4Client;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class UserCommandContentTpl extends UserCommand {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(UserCommandContentTpl.class);
	public static final int TYPE_LIST = 1;
    public static final int TYPE_ADD = 2;
    public static final int TYPE_DEL = 3;
	public static final int TYPE_RELOAD = 4;
	public static final int TYPE_GENERATE = 5;
    public static final int TYPE_ACTIVE = 6;
    public static final String keyword = "contentTemplate";
	
	public UserCommandContentTpl(){
    	super(UserCommand.Notify_All);
    }
	
	@Override
	public SystemCommand buildSystemCommand() {
		return new SystemCommandContentTpl(this);
	}

	/** 
	 * @param args
	 * @return
	 * @see com.king.mgt.cmd.user.UserCommand#parseArgs(java.lang.String[])
	 */
	@Override
	public boolean parseArgs(String[] args) {
		if (args.length == 2 && args[1].equals("-r")) {
            this.type = TYPE_RELOAD;
            return true;
		 }else if (args.length == 2 && args[1].equals("-g")) {
        	this.type = TYPE_GENERATE;        	
            return true;
        }else if (args.length == 2 && args[1].equals("-a")) {
        	this.type = TYPE_ACTIVE;
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
