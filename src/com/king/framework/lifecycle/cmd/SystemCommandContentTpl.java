
package com.king.framework.lifecycle.cmd;

import java.util.List;

import com.king.framework.lifecycle.event.Event;
import com.king.framework.lifecycle.event.EventFactory;
import com.king.gmms.protocol.tcp.ByteBuffer;


public class SystemCommandContentTpl extends SystemCommand {
	
	public static char ID = 0x0005;
	public static byte ARG_LIST = 00;
	public static byte ARG_ADD = 01;
	public static byte ARG_DEL = 02;
	public static byte ARG_RELOAD = 03;
	
	public SystemCommandContentTpl() {
		super();
	}
	

	/** 
	 * @param buf
	 * @return
	 * @see com.king.framework.lifecycle.cmd.SystemCommand#parseArgs(com.king.gmms.protocol.tcp.ByteBuffer)
	 */
	@Override
	public boolean parseArgs(ByteBuffer buf) {
		return true;
	}

	/** 
	 * @param result
	 * @param args
	 * @return
	 * @see com.king.framework.lifecycle.cmd.SystemCommand#makeResponse(int, java.util.List)
	 */
	@Override
	public SystemResponse makeResponse(int result, List args) {
		SystemResponseContentTpl response=new SystemResponseContentTpl(this,result, args);
	    return response;
	}

	/** 
	 * @return
	 * @see com.king.framework.lifecycle.cmd.SystemCommand#getEvent()
	 */
	@Override
	public Event getEvent() {
		EventFactory factory=EventFactory.getInstance();
        return factory.newEvent(Event.TYPE_CONTENT_TEMPLATE_RELOAD);
	}

}
