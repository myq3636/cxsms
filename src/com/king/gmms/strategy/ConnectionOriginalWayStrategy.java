package com.king.gmms.strategy;

import java.util.ArrayList;

import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.MessageMode;
import com.king.message.gmms.GmmsMessage;
/**
 * <p>Title: </p>
 *
 * <p>Description: send DR by the original connection </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ConnectionOriginalWayStrategy extends ConnectionStrategy{
    ConnectionManager connMan = null;

    public ConnectionOriginalWayStrategy(ConnectionManager connMan) {
        this.connMan = connMan;
    }


    public Session execute(GmmsMessage msg){
        String connectionID = msg.getConnectionID();
        ArrayList list = new ArrayList();
        list.add(connectionID);
        boolean isDR = false;
        if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.
            getMessageType())){
            isDR = true;
        }
        return connMan.getSession(list,isDR);
    }
}
