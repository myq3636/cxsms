/**
 */
package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.cmd.SystemCommand;


public class ReloadContentTplEvent extends Event {


	public ReloadContentTplEvent() {
		super(Event.TYPE_CONTENT_TEMPLATE_RELOAD);
	}

	/** 
	 * @param cmd
	 * @return
	 * @see com.king.framework.lifecycle.event.Event#parseArgs(com.king.framework.lifecycle.cmd.SystemCommand)
	 */
	@Override
	public boolean parseArgs(SystemCommand cmd) {
		return true;
	}

}
