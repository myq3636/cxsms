package com.king.mgt.cmd.user;




import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.mgt.cmd.system.SystemCommand;
import com.king.mgt.cmd.system.SystemCommandRoutingInfo;
import com.king.mgt.connection.FTP4Client;

public class UserCommandRoutingInfo extends UserCommand {
	
	
	private static Logger log = LogManager.getLogger(UserCommandRoutingInfo.class);
    public static final int TYPE_LIST = 1;
    public static final int TYPE_ADD = 2;
    public static final int TYPE_DEL = 3;
    public static final int TYPE_RELOAD = 4;
    public static final int TYPE_GENERATE = 5;
    public static final int TYPE_ACTIVE = 6;
    public static final String keyword = "routingInfo";
    
//    private static final String NOTIFY_MODULE = "DeliveryRouter";
    
    public UserCommandRoutingInfo(){
    	
    	super(UserCommand.Notify_All);//just notify DeliveryRouter
//    	notifyModules = new Module[1];
    	
//    	Module del = ModuleManager.getInstance().getModuleByName(NOTIFY_MODULE);
//    	
//    	if(del == null){
//    		log.info("can not get"+ NOTIFY_MODULE+" module information from configurtion!");
//    		del = new Module(NOTIFY_MODULE,"127.0.0.1",8007);
//    	}
//    	
//    	notifyModules[0] = del;
    	
    }
	
	

	@Override
	public SystemCommand buildSystemCommand() {
		// TODO Auto-generated method stub
		return new SystemCommandRoutingInfo(this);
	}

	@Override
	public boolean parseArgs(String[] args) {
		// TODO Auto-generated method stub
        if (args.length == 2 && args[1].equals("-r")) {
            this.type = TYPE_RELOAD;
            return true;
        }else if (args.length == 2 && args[1].equals("-g")) {
        	this.type = TYPE_GENERATE;
        	super.notifyType = UserCommand.Notify_None;
            return true;
        }else if (args.length == 2 && args[1].equals("-a")) {
        	this.type = TYPE_ACTIVE;
            return true;
        }
        return false;
	}

	@Override
	public boolean process() {
		// TODO Auto-generated method stub
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
		case TYPE_ACTIVE: {
			return processor.handleCommandActive();
		}
		default: {
			log.error("Unknow command type: {}", type);
		}
		}
        return rtnval;
	}

}
