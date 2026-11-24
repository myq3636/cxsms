package com.king.framework.lifecycle.event;

import java.util.ArrayList;

import com.king.framework.SystemLogger;
import com.king.gmms.routing.ADSServerMonitor;

public class SwitchDNSEvent extends Event {
	 private SystemLogger log = SystemLogger.getSystemLogger(SwitchDNSEvent.class);
	 public SwitchDNSEvent(){
	        super(Event.TYPE_SWITCHDNS);
	 }
}
