package com.king.framework.lifecycle.cmd;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;




import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.protocol.tcp.ByteBuffer;

public class SystemCommandAntiSpam extends SystemCommand {	
    public static char ID = 0x0004;
    public static byte ARG_LIST = 00;
    public static byte ARG_ADD = 01;
    public static byte ARG_DEL = 02;
    public static byte ARG_RELOAD = 03;
    
    public SystemCommandAntiSpam() {
		super();
	}
	@Override
	public Event getEvent() {
		EventFactory factory=EventFactory.getInstance();
        return factory.newEvent(Event.TYPE_ANTISPAM_RELOAD);
	}

	@Override
	public SystemResponse makeResponse(int result, List args) {
		 SystemResponseAntiSpam response=new SystemResponseAntiSpam(this,result, args);
	     return response;
	}

	@Override
	public boolean parseArgs(ByteBuffer buf) {
		return true;
	}

}
