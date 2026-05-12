package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.cmd.SystemCommand;

public class ReloadPhonePrefixEvent extends Event {

	public ReloadPhonePrefixEvent() {
		super(Event.TYPE_PHONEPREFIX_RELOAD);
	}

	@Override
	public boolean parseArgs(SystemCommand cmd) {
		return true;
	}

}
