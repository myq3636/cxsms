package com.king.mgt.cmd.system;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandCustomer;

public class SystemCommandCustomer extends SystemCommand {
    private static SystemLogger log=SystemLogger.getSystemLogger(SystemCommandCustomer.class);
    public static short ID=0x0008;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    
    public SystemCommandCustomer(UserCommandCustomer cmd) {
		super(cmd, ID);
	}

	@Override
	public boolean genBody() {
		int type = cmd.getType();
		switch (type) {
			case UserCommandCustomer.TYPE_GENERATE: {
				body.appendInt(1);
				body.appendByte(ARG_GENERATE);
				break;
			}
			case UserCommandCustomer.TYPE_ACTIVE: {
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
