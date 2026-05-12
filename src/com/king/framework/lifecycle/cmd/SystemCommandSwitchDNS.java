package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.protocol.tcp.ByteBuffer;

public class SystemCommandSwitchDNS extends SystemCommand {
	public static char ID = 0x0009;//----

    public SystemCommandSwitchDNS() {
        super();
        isSysLevel = true;
    }

    /**
     * getEvent
     *
     * @return Event
     *   method
     */
    public Event getEvent() {
        EventFactory factory=EventFactory.getInstance();
        Event event = factory.newEvent(Event.TYPE_SWITCHDNS);
        event.parseArgs(this);
    	return event;
    }

    /**
     * makeResponse
     *
     * @param result int
     * @param args ArrayList
     * @return SystemResponse
     *   method
     */
    public SystemResponseSwitchDNS makeResponse(int result, List args) {
    	SystemResponseSwitchDNS response=new SystemResponseSwitchDNS(this,result, args);
        return response;
    }

    /**
     * parseArgs
     *
     * @param buf ByteBuffer
     * @return boolean
     *   method
     */
    public boolean parseArgs(ByteBuffer buf) {
        return true;
    }
}
