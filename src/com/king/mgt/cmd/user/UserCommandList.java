package com.king.mgt.cmd.user;

import com.king.mgt.cmd.system.SystemCommand;

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
public class UserCommandList
    extends UserCommand {


    public static final String keyword = "list";
    public UserCommandList(){
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
        if (args != null && args.length == 1) {
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
        processor.handleCommandList();
        return true;
    }
}
