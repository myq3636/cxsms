/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.mgt.cmd.system;


import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandContentTpl;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class SystemCommandContentTpl extends SystemCommand {

	private static SystemLogger log=SystemLogger.getSystemLogger(SystemCommandContentTpl.class);
	
    public static short ID=0x0005;
    public static byte ARG_LIST=00;
    public static byte ARG_ADD=01;
    public static byte ARG_DEL=02;
    public static byte ARG_RELOAD=03;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    
    public SystemCommandContentTpl(UserCommandContentTpl cmd) {
		super(cmd, ID);
	}
	/** 
	 * @return
	 * @see com.king.mgt.cmd.system.SystemCommand#genBody()
	 */
	@Override
	public boolean genBody() {
		int type = cmd.getType();
		switch (type) {
			case UserCommandContentTpl.TYPE_RELOAD: {
				body.appendInt(1);
				body.appendByte(ARG_RELOAD);
				break;
			}
			 case UserCommandContentTpl.TYPE_GENERATE: {
				body.appendInt(1);
				body.appendByte(ARG_GENERATE);
				break;
			}
			case UserCommandContentTpl.TYPE_ACTIVE: {
				body.appendInt(1);
				body.appendByte(ARG_ACTIVE);
				break;
			}
			default: {
				log.error("Command type {} is not supported.",cmd.getType());
				return false;
			}
		}
		return true;
	}

}
