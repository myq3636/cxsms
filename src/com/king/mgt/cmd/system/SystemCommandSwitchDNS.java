package com.king.mgt.cmd.system;

import com.king.framework.SystemLogger;
import com.king.message.gmms.ExceptionMessageManager;
import com.king.mgt.cmd.user.UserCommand;
import com.king.mgt.cmd.user.UserCommandSwitchDNS;

public class SystemCommandSwitchDNS extends SystemCommand {
	private SystemLogger log = SystemLogger.getSystemLogger(SystemCommandSwitchDNS.class);
	public static short ID=0x0009;
    public static byte ARG_MASTER=01;
    public static byte ARG_SLAVE=02;
    public static byte ARG_UNKNOW=00;
    public SystemCommandSwitchDNS(UserCommand cmd){
        super(cmd,ID);
    }
    /**
     * genBody
     *
     * @return boolean
     * @todo Implement.mgt.cmd.system.SystemCommand method
     */
    public boolean genBody() {
    	int type=cmd.getType();
        switch(type)
        {
	        case UserCommandSwitchDNS.TYPE_MASTER:
	        {
	            body.appendByte(ARG_MASTER);
	            break;
	        }
	        case UserCommandSwitchDNS.TYPE_SLAVE:
	        {
	            body.appendByte(ARG_SLAVE);
	            break;
	        }
	        default:
	        {
	        	body.appendByte(ARG_UNKNOW);
	        	log.error("Command type "+cmd.getType()+" is not supported.");
	            return false;
	        }
        }
        return true;
    }

}
