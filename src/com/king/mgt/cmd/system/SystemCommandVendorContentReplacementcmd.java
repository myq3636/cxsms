package com.king.mgt.cmd.system;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandVendorContentReplacement;

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
public class SystemCommandVendorContentReplacementcmd extends SystemCommand{
    private static SystemLogger log=SystemLogger.getSystemLogger(SystemCommandVendorContentReplacementcmd.class);
    public static short ID=0x013;
    public static byte ARG_LIST=00;
    public static byte ARG_ADD=01;
    public static byte ARG_DEL=02;
    public static byte ARG_RELOAD=03;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    public SystemCommandVendorContentReplacementcmd(UserCommandVendorContentReplacement cmd) {
        super(cmd, ID);
    }

    public boolean genBody() {
        int type=cmd.getType();
        switch(type)
        {
        case UserCommandVendorContentReplacement.TYPE_RELOAD:
        {
            body.appendInt(1);
            body.appendByte(ARG_RELOAD);
            break;
        }
        case UserCommandVendorContentReplacement.TYPE_GENERATE: {
			body.appendInt(1);
			body.appendByte(ARG_GENERATE);
			break;
		}
		case UserCommandVendorContentReplacement.TYPE_ACTIVE: {
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
