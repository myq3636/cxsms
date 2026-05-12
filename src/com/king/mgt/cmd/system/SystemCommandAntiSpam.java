package com.king.mgt.cmd.system;



import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandAntiSpam;

public class SystemCommandAntiSpam extends SystemCommand {

	private static SystemLogger log=SystemLogger.getSystemLogger(SystemCommandAntiSpam.class);
    public static short ID=0x0004;
    public static byte ARG_LIST=00;
    public static byte ARG_ADD=01;
    public static byte ARG_DEL=02;
    public static byte ARG_RELOAD=03;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;

    
    
    public SystemCommandAntiSpam(UserCommandAntiSpam cmd) {
		super(cmd, ID);
	}

	@Override
	public boolean genBody() {
		int type = cmd.getType();
		switch (type) {
			case UserCommandAntiSpam.TYPE_RELOAD: {
				body.appendInt(1);
				body.appendByte(ARG_RELOAD);
				break;
			}
			case UserCommandAntiSpam.TYPE_GENERATE: {
				body.appendInt(1);
				body.appendByte(ARG_GENERATE);
				break;
			}
			case UserCommandAntiSpam.TYPE_ACTIVE: {
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
