package com.king.mgt.cmd.system;

import com.king.mgt.cmd.user.*;

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
public class SystemCommandStop
    extends SystemCommand {

    public static short ID=0x0002;

    public SystemCommandStop(UserCommand cmd){
        super(cmd,ID);
    }
    /**
     * genBody
     *
     * @return boolean
     * @todo Implement.mgt.cmd.system.SystemCommand method
     */
    public boolean genBody() {
        return true;
    }
}
