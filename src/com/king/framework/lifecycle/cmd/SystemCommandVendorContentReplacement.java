package com.king.framework.lifecycle.cmd;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.protocol.tcp.ByteBuffer;

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
public class SystemCommandVendorContentReplacement extends SystemCommand{    
    public static char ID = 0x013;
    public static byte ARG_LIST = 00;
    public static byte ARG_ADD = 01;
    public static byte ARG_DEL = 02;
    public static byte ARG_RELOAD = 03;

    public SystemCommandVendorContentReplacement() {
        super();
    }

    public SystemResponse makeResponse(int result, List args) {
    	SystemResponseVendorContentReplacement response=new SystemResponseVendorContentReplacement(this,result, args);
        return response;
    }


    public Event getEvent() {
        EventFactory factory=EventFactory.getInstance();
        return factory.newEvent(Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD);
    }
}
