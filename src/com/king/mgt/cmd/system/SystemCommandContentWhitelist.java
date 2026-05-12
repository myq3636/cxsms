package com.king.mgt.cmd.system;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.user.UserCommandContentWhitelist;
import com.king.mgt.cmd.user.UserCommandRecipientAddressRule;
import com.king.mgt.cmd.user.UserCommandWhitelist;

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
public class SystemCommandContentWhitelist extends SystemCommand{
    private static SystemLogger log=SystemLogger.getSystemLogger(SystemCommandContentWhitelist.class);
    public static short ID=0x010;
    public static byte ARG_LIST=00;
    public static byte ARG_ADD=01;
    public static byte ARG_DEL=02;
    public static byte ARG_RELOAD=03;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    public SystemCommandContentWhitelist(UserCommandContentWhitelist cmd) {
        super(cmd, ID);
    }

    public boolean genBody() {
        int type=cmd.getType();
        switch(type)
        {
        case UserCommandRecipientAddressRule.TYPE_RELOAD:
        {
            body.appendInt(1);
            body.appendByte(ARG_RELOAD);
            break;
        }
        case UserCommandRecipientAddressRule.TYPE_GENERATE: {
			body.appendInt(1);
			body.appendByte(ARG_GENERATE);
			break;
		}
		case UserCommandRecipientAddressRule.TYPE_ACTIVE: {
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
