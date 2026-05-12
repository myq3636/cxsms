package com.king.mgt.cmd.system;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandBlacklist;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2008</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class SystemCommandBlacklist extends SystemCommand{
    private static SystemLogger log=SystemLogger.getSystemLogger(SystemCommandBlacklist.class);
    public static short ID=0x0001;
    public static byte ARG_LIST=00;
    public static byte ARG_ADD=01;
    public static byte ARG_DEL=02;
    public static byte ARG_RELOAD=03;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    public SystemCommandBlacklist(UserCommandBlacklist cmd) {
        super(cmd, ID);
    }

    public boolean genBody() {
        int type=cmd.getType();
        switch(type)
        {
        case UserCommandBlacklist.TYPE_RELOAD:
        {
            body.appendInt(1);
            body.appendByte(ARG_RELOAD);
            break;
        }
        case UserCommandBlacklist.TYPE_GENERATE: {
			body.appendInt(1);
			body.appendByte(ARG_GENERATE);
			break;
		}
		case UserCommandBlacklist.TYPE_ACTIVE: {
			body.appendInt(1);
			body.appendByte(ARG_ACTIVE);
			break;
		}
        default:
        {
            log.error("Command type {} is not supported.",cmd.getType());
            return false;
        }

        }
        return true;
    }

}
