package com.king.mgt.cmd.system;

import java.util.List;

import com.king.mgt.cmd.user.UserCommand;

public class SystemCommandSwitchDB extends SystemCommand {
	public static short ID=0x0006;
	private List argList = null;
    public SystemCommandSwitchDB(UserCommand cmd){
        super(cmd,ID);
        argList = cmd.getArgs();
    }
    /**
     * genBody
     *
     * @return boolean
     * @todo Implement.mgt.cmd.system.SystemCommand method
     */
    public boolean genBody() {
    	if(argList!=null && !argList.isEmpty()){
        	String opt = (String)argList.get(1);
        	body.appendString(opt);
        }
        return true;
    }
}
