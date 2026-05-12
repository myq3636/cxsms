package com.king.mgt.cmd.user;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.util.SystemConstants;
import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemCommandSwitchDNS;
import com.king.mgt.util.UserInterfaceUtility;

public class UserCommandSwitchDNS extends UserCommand {

	public static final String keyword = "switchdns";
    private SystemLogger log = SystemLogger.getSystemLogger(UserCommandSwitchDNS.class);
    private Map<String,Integer> modulesName = new HashMap<String,Integer>();
    public static final int TYPE_MASTER = 1;
    public static final int TYPE_SLAVE = 2;
    /**
     * buildSystemCommand
     *
     * @return SystemCommand
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public UserCommandSwitchDNS(){
        super(UserCommandSwitchDNS.Notify_Fix);
    }

    public SystemCommand buildSystemCommand() {
        return new SystemCommandSwitchDNS(this);
    }

    /**
     * parseArgs
     *
     * @param args String[]
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean parseArgs(String[] args) {
    	UserInterfaceUtility util = UserInterfaceUtility.getInstance();
    	String switchDNSModules =util.getProperty("SwitchDNSModules");
    	ArrayList<String> routerList =  (ArrayList<String>)ModuleManager.getInstance().getRouterModules();
    	ArrayList<String> serverList =  (ArrayList<String>)ModuleManager.getInstance().getServerModules();
    	ArrayList<String> clientList =  (ArrayList<String>)ModuleManager.getInstance().getClientModules();
    	if(!"".equals(switchDNSModules)){
            int i=0;
        	String[] moduleTypes = switchDNSModules.split(",");
            for(String type:moduleTypes){
            	//load CoreEngine
            	if(type.equalsIgnoreCase(SystemConstants.ROUTER_MODULE_TYPE)){
            		for(String str:routerList){
               		   modulesName.put(str.trim(), i++);
               	 	}
            	}
            	//load MultiSmppServer
            	else if(type.endsWith(SystemConstants.SERVER_MODULE_TYPE)){
            		for(String str:serverList){
                		if(str.startsWith(type)){
                			 modulesName.put(str.trim(), i++);
                		}
               	 	}
            	}
            	//load MultiSmppClient
            	else if(type.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
            		for(String str:clientList){
                		if(str.startsWith(type)){
                			 modulesName.put(str.trim(), i++);
                		}
               	 	}
            	}
            }
        }else if("".equals(switchDNSModules)){
    		log.info("No module to switch DNS!");
    		return false;
    	}
        if (args.length == 2) {
            this.notifyType = Notify_Fix;
            if("master".equalsIgnoreCase(args[1].trim())){
            	this.type=TYPE_MASTER;
            	return true;
            }else if("slave".equalsIgnoreCase(args[1].trim())){
            	this.type=TYPE_SLAVE;
            	return true;
            }else{
                return false;   	
            }
        }
        else {
            return false;
        }
    }
    /**
     * process
     *
     * @return boolean
     * @todo Implement.mgt.cmd.user.UserCommand method
     */
    public boolean process() {
        this.processor.handleCommandSwitchDNS(modulesName);
        return true;
    }

}
