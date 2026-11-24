package com.king.mgt.cmd.user;


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
public class UserCommandQuit
    extends UserCommand {

    public static final String keyword = "quit";

    public UserCommandQuit(){
        super(UserCommand.Notify_None);
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
        processor.handleCommandQuit();
        return true;
    }
}
