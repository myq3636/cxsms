package com.king.gmms.strategy;

import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.*;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>Title: </p>
 *
 * <p>Description: find random connection to send message </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ConnectionRandomStrategy extends ConnectionStrategy{
    ConnectionManager connMan ;

    public ConnectionRandomStrategy(ConnectionManager connMan) {
        if (connMan != null) {
            connMan.setStrategy(new IndexRandomStrategy());
            this.connMan = connMan;
        }
    }

    public Session execute(GmmsMessage msg){
        return connMan.getSession();
    }
}
