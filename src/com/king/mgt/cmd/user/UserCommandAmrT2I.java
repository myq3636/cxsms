package com.king.mgt.cmd.user;

import com.king.framework.SystemLogger;
import com.king.mgt.cmd.system.SystemCommand;
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
public class UserCommandAmrT2I
    extends UserCommand {
    private static SystemLogger log = SystemLogger.getSystemLogger(UserCommandAmrT2I.class);
    public static final int TYPE_RELOAD = 4;
    public static final String keyword = "amrt2i";
    public UserCommandAmrT2I() {
        super(UserCommand.Notify_None);
    }

    /**
     * buildSystemCommand
     *
     * @return SystemCommand
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public SystemCommand buildSystemCommand() {
        return null;
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
            case TYPE_RELOAD: {
                FTP4Client ftp = new FTP4Client();
                return ftp.downloadConfigFile(keyword);                           
            }

            default: {
                log.error("Unknow command type: {}", type);
            }
        }
        return rtnval;

    }
}
