package com.king.mgt.cmd.user;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.system.*;
import com.king.mgt.connection.FTP4Client;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class UserCommandPhonePrefix
    extends UserCommand {
    private static SystemLogger log = SystemLogger.getSystemLogger(UserCommandPhonePrefix.class);

    public static final int TYPE_RELOAD = 4;
    public static final int TYPE_GENERATE = 5;
    public static final int TYPE_ACTIVE = 6;
    public static final String keyword = "senderReplacement";
    public UserCommandPhonePrefix() {
        super(UserCommand.Notify_None);
    }

    /**
     * buildSystemCommand
     *
     * @return SystemCommand
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public SystemCommand buildSystemCommand() {
        return new SystemCommandPhonePrefix(this);
    }

    /**
     * parseArgs
     *
     * @param args String[]
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean parseArgs(String[] args) {
    	if (args.length == 2 && args[1].equals("-r")) {
        	this.type = TYPE_RELOAD;
            return true;
        }
    	else if (args.length == 2 && args[1].equals("-g")) {
        	this.type = TYPE_GENERATE;
            return true;
        }else if (args.length == 2 && args[1].equals("-a")) {
        	this.type = TYPE_ACTIVE;
            return true;
        }
        return false;

    }

    /**
     * process
     *
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
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
            case TYPE_ACTIVE:
	        {
	            this.notifyType = Notify_All;
	        	return processor.handleCommandActive();
	        }
            default: {
                log.error("Unknow command type: {}" , type);
            }
        }
        return rtnval;

    }
}
