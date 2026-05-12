package com.king.framework.lifecycle.event;

import java.util.List;

import com.king.framework.lifecycle.cmd.SystemCommand;

public class SwitchDBEvent extends Event {
	public SwitchDBEvent(){
        super(Event.TYPE_SWITCHDB);
    }
	@Override
	public boolean parseArgs(SystemCommand cmd) {
		List args = cmd.getArgs();
		if(args==null || args.isEmpty()){
			return false;
		}
		String opt = (String)args.get(0);
		if("master".equalsIgnoreCase(opt)){
	        super.setEventSubType(SUBTYPE_SWITCH_MASTER);
		}else if("slave".equalsIgnoreCase(opt)){
	        super.setEventSubType(SUBTYPE_SWITCH_SLAVE);
		}
		return true;
	}

}
