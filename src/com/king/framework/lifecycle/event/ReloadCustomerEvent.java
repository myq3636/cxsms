package com.king.framework.lifecycle.event;

import com.king.framework.lifecycle.cmd.SystemCommand;

public class ReloadCustomerEvent extends Event {

	public ReloadCustomerEvent() {
		super(Event.TYPE_CUSTOMER_RELOAD);
	}

	@Override
	public boolean parseArgs(SystemCommand cmd) {
		return true;
	}

}
