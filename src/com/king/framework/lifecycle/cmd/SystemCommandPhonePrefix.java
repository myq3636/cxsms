package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.protocol.tcp.ByteBuffer;

public class SystemCommandPhonePrefix extends SystemCommand {
    
    public static char ID = 0x000A;
    public static byte ARG_GENERATE=04;
    public static byte ARG_ACTIVE=05;
    private static SystemLogger log = SystemLogger
	.getSystemLogger(SystemCommandPhonePrefix.class);
	public SystemCommandPhonePrefix() {
		super();
	}

	@Override
	public Event getEvent() {
        EventFactory factory=EventFactory.getInstance();
        return factory.newEvent(Event.TYPE_PHONEPREFIX_RELOAD);
	}

	@Override
	public SystemResponse makeResponse(int result, List args) {
		SystemResponsePhonePrefix response=new SystemResponsePhonePrefix(this,result, args);
        return response;
	}

	@Override
	public boolean parseArgs(ByteBuffer buf) {
		return true;
	}

}
