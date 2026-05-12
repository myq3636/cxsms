package com.king.framework.lifecycle.event;

import java.util.ArrayList;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.cmd.SystemCommand;
import com.king.gmms.routing.ADSServerMonitor;
import com.king.mgt.cmd.system.SystemCommandSwitchDNS;

public class SwitchDNSEvent extends Event {
	 private SystemLogger log = SystemLogger.getSystemLogger(SwitchDNSEvent.class);
	 public SwitchDNSEvent(){
	        super(Event.TYPE_SWITCHDNS);
	 }
	    /**
	     * event subtype commonly is the arguments following command.
	     *
	     * @return boolean
	     */
	    public boolean parseArgs(SystemCommand cmd) {
	    	ArrayList<String> args = (ArrayList<String>) cmd.getArgs();
	    	if(args.size()==1){
	    		String arg = args.get(0);
	    		if(arg.equalsIgnoreCase("Master")){
	    			this.setEventSubType(SUBTYPE_SWITCHDNS_MASTER);
	    		}else if(arg.equalsIgnoreCase("Slave")){
	    			this.setEventSubType(SUBTYPE_SWITCHDNS_SLAVE);
	    		}
	    	}
	        return false;
	    }
}
