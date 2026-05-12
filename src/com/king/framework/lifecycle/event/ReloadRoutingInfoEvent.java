package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.cmd.SystemCommand;

public class ReloadRoutingInfoEvent extends Event {

	public ReloadRoutingInfoEvent() {
		super(Event.TYPE_ROUTINFO_RELOAD);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean parseArgs(SystemCommand cmd) {
		// TODO Auto-generated method stub
		return true;
	}

}
