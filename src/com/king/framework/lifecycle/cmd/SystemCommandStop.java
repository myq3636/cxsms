package com.king.framework.lifecycle.cmd;

import java.util.*;

import com.king.framework.lifecycle.event.*;
import com.king.gmms.protocol.tcp.*;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class SystemCommandStop
    extends SystemCommand {

    public static char ID = 0x0002;

    public SystemCommandStop() {
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
        return factory.newEvent(Event.TYPE_SHUTDOWN);
    }

    /**
     * makeResponse
     *
     * @param result int
     * @param args List
     * @return SystemResponse
     *   method
     */
    public SystemResponse makeResponse(int result, List args) {
        SystemResponseStop response=new SystemResponseStop(this,result, args);
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
