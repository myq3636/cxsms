package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.protocol.tcp.ByteBuffer;

public class SystemCommandCustomer extends SystemCommand {
    
    public static char ID = 0x0008;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    
	public SystemCommandCustomer() {
		super();
	}

	@Override
	public Event getEvent() {
        EventFactory factory=EventFactory.getInstance();
        return factory.newEvent(Event.TYPE_CUSTOMER_RELOAD);
	}

	@Override
	public SystemResponse makeResponse(int result, List args) {
		SystemResponseCustomer response=new SystemResponseCustomer(this,result, args);
        return response;
	}

	@Override
	public boolean parseArgs(ByteBuffer buf) {
		return true;
	}

}
