package com.king.gmms.strategy;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.ha.TransactionURI;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ConnectionSameSessionStrategy extends ConnectionStrategy {
    private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionSameSessionStrategy.class);
    ConnectionManager connMan = null;

    public ConnectionSameSessionStrategy(ConnectionManager connMan) {
        this.connMan = connMan;
    }

    public Session execute(GmmsMessage msg){
        TransactionURI transaction = msg.getTransaction();
        if(transaction==null){
        		log.debug("transaction is null");
        	return null;
        }else{
        	if(log.isDebugEnabled()){
				log.debug("transaction:{}",transaction);
        	}
        }
        
        if(connMan==null){
            	log.debug("connMan is null");
        	return null;
        }
        
        return connMan.getSession(transaction);
    }

}
