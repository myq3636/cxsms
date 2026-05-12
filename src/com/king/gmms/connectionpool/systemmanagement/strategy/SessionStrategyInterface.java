package com.king.gmms.connectionpool.systemmanagement.strategy;

import com.king.gmms.connectionpool.session.Session;
import com.king.message.gmms.GmmsMessage;

public interface SessionStrategyInterface {
	
	public Session getSession(GmmsMessage msg);

}
