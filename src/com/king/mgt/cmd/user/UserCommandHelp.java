package com.king.mgt.cmd.user;

import com.king.mgt.cmd.system.SystemCommand;


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
public class UserCommandHelp extends UserCommand {
    public static final String keyword="help";
    public UserCommandHelp() {
        super(UserCommand.Notify_None);
    }

    /**
     * parseUserInput
     *
     * @param inputLine String
     * @return int
     * @todo Implement.mgt.Command.UserCommand method
     */
    public boolean parseArgs(String []args) {
        if(args != null && args.length == 1){
            return true;
        }
        return false;
    }

    /**
     * toSystemCommand
     *
     * @return byte[]
     * @todo Implement.mgt.Command.UserCommand method
     */

    public boolean process() {
        processor.handleCommandHelp();
        return true;
    }

    public SystemCommand buildSystemCommand() {
        return null;
    }

}
