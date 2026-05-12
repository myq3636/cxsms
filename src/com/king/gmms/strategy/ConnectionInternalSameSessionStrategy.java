package com.king.gmms.strategy;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.ha.TransactionURI;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class ConnectionInternalSameSessionStrategy extends ConnectionStrategy {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ConnectionInternalSameSessionStrategy.class);
	ConnectionManager connMan = null;

	public ConnectionInternalSameSessionStrategy(ConnectionManager connMan) {
		this.connMan = connMan;
	}

	public Session execute(GmmsMessage msg) {
		TransactionURI transaction = msg.getInnerTransaction();
		if (transaction == null) {
			if(log.isInfoEnabled()){
				log.info(msg, "transaction is null");
			}
			return null;
		} else {
			if(log.isDebugEnabled()){
				log.debug("transaction:{}" , transaction);
			}
		}
		if (connMan == null) {
			if(log.isInfoEnabled()){
				log.info(msg, "Connection manager is null when get session");
			}
			return null;
		} 

		return connMan.getSession(transaction);
	}

}
