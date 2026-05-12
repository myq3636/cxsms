package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;

public class SystemCommandSwitchRedis extends SystemCommand {
	public static char ID = 0x0007;
	public SystemCommandSwitchRedis(){
		super();
        isSysLevel = true;
	}
	@Override
	public Event getEvent() {
		EventFactory factory=EventFactory.getInstance();
		Event e = factory.newEvent(Event.TYPE_SWITCHREDIS);
		e.parseArgs(this);
        return e;
	}

	@Override
	public SystemResponse makeResponse(int result, List args) {
		SystemResponseSwitchRedis response=new SystemResponseSwitchRedis(this,result, args);
        return response;
	}
}
