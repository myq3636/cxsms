package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.protocol.tcp.ByteBuffer;

public class SystemCommandRoutingInfo extends SystemCommand {
    
    public static char ID = 0x0003;
    public static byte ARG_LIST = 00;
    public static byte ARG_ADD = 01;
    public static byte ARG_DEL = 02;
    public static byte ARG_RELOAD = 03;
    
	public SystemCommandRoutingInfo() {
		// TODO Auto-generated constructor stub
		super();
	}

	@Override
	public Event getEvent() {
		// TODO Auto-generated method stub
        EventFactory factory=EventFactory.getInstance();
        return factory.newEvent(Event.TYPE_ROUTINFO_RELOAD);
	}

	@Override
	public SystemResponse makeResponse(int result, List args) {
		// TODO Auto-generated method stub
        SystemResponseRoutingInfo response=new SystemResponseRoutingInfo(this,result, args);
        return response;
	}

	@Override
	public boolean parseArgs(ByteBuffer buf) {
		// TODO Auto-generated method stub
		return true;
	}

}
