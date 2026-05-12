package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;

public class SystemCommandSwitchDB extends SystemCommand {
	public static char ID = 0x0006;
	public SystemCommandSwitchDB(){
		super();
        isSysLevel = true;
	}
	@Override
	public Event getEvent() {
		EventFactory factory=EventFactory.getInstance();
		Event e = factory.newEvent(Event.TYPE_SWITCHDB);
		e.parseArgs(this);
        return e;
	}
	
	@Override
	public SystemResponse makeResponse(int result, List args) {
		SystemResponseSwitchDB response=new SystemResponseSwitchDB(this,result, args);
        return response;
	}
}
