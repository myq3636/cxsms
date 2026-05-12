package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.cmd.SystemCommand;

public class ReloadGmmsConfigEvent extends Event {

	public ReloadGmmsConfigEvent() {
		super(Event.TYPE_GMMS_CONFIG_RELOAD);
	}

	@Override
	public boolean parseArgs(SystemCommand cmd) {
		return true;
	}
}
