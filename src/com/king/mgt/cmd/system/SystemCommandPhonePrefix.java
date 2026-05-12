package com.king.mgt.cmd.system;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandPhonePrefix;

public class SystemCommandPhonePrefix extends SystemCommand {
    private static SystemLogger log=SystemLogger.getSystemLogger(SystemCommandPhonePrefix.class);
    public static short ID=0x000A;
    public static byte ARG_RELOAD=03;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    
    public SystemCommandPhonePrefix(UserCommandPhonePrefix cmd) {
		super(cmd, ID);
	}

	@Override
	public boolean genBody() {
		int type = cmd.getType();
		switch (type) {
			case UserCommandPhonePrefix.TYPE_RELOAD: {
				body.appendInt(1);
				body.appendByte(ARG_RELOAD);
				break;
			}
			case UserCommandPhonePrefix.TYPE_GENERATE: {
				body.appendInt(1);
				body.appendByte(ARG_GENERATE);
				break;
			}
			case UserCommandPhonePrefix.TYPE_ACTIVE: {
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
