/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.throttle;

import com.king.framework.SystemLogger;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.pdu.ReportInMsgCount;
import com.king.gmms.util.AbstractTimer;

/**
 * Protocol module report module incoming msg cout to SYS module
 * @author bensonchen
 * @version 1.0.0
 */
public class ReportInMsgCountTimer extends AbstractTimer {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(ReportInMsgCountTimer.class);
	
	private SystemSession systemSession =  null;

	/**
	 * @param wakeupTime
	 */
	public ReportInMsgCountTimer(SystemSession systemSession, long wakeupTime) {
		super(wakeupTime);
		this.systemSession = systemSession;
	}

	/** 
	 * protocol module report module incoming msg count to SYS
	 * default: per minutes 
	 * @see com.king.gmms.util.AbstractTimer#excute()
	 */
	@Override
	public void excute() {
		try {
			int inMsgCount = ThrottlingControl.getInstance().getModuleIncomingMsgCount();
			// reset module count
			ThrottlingControl.getInstance().resetModuleIncomingMsgCount();
			
			ReportInMsgCount message = new ReportInMsgCount();
			message.setModuleIncomingMsgCount(inMsgCount);
			
			boolean isSucessSend = systemSession.send(message);
			if (!isSucessSend) {
				if (log.isInfoEnabled()) {
					log.warn("reportModuleInMsgCount failed, inMsgCount=" + inMsgCount);
				}
			}
		} catch (Exception e) {
			log.error("reportModuleInMsgCount exception:", e);
		}
		
	}
}
