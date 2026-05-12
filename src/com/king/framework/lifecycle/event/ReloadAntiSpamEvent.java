package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.cmd.SystemCommand;

public class ReloadAntiSpamEvent extends Event {
	public ReloadAntiSpamEvent() {
		super(Event.TYPE_ANTISPAM_RELOAD);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean parseArgs(SystemCommand cmd) {
		// TODO Auto-generated method stub
		return true;
	}

}
