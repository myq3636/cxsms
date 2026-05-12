package com.king.gmms.strategy;

import java.util.ArrayList;
import java.util.Iterator;

import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.MessageMode;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>Title: </p>
 *
 * <p>Description: return DR by the same IP address of incoming connection</p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */

public class ConnectionSameIPStrategy extends ConnectionStrategy{
    ConnectionManager connMan ;
    A2PCustomerManager customerMan;

    public ConnectionSameIPStrategy(ConnectionManager connMan) {
        this.connMan = connMan;
        customerMan = GmmsUtility.getInstance().getCustomerManager();
    }

    public Session execute(GmmsMessage msg){

        String connectionID = msg.getConnectionID();

//        if (msg.getMessageMode() == MessageMode.PROXY &&
//            GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equalsIgnoreCase(msg.
//            getMessageType())) {
//            connectionID = msg.getTransaction().getConnectionName();
//        }


        if (connectionID == null) {
            return null;
        }
        else {
            String ip = customerMan.getIpByConnection(connectionID);
            Iterator it = connectionMap.values().iterator();
            ArrayList list = new ArrayList();
            while(it.hasNext()){
                ConnectionInfo connInfo = (ConnectionInfo)it.next();
                if(ip.equalsIgnoreCase(connInfo.getURL())){
                    list.add(connInfo.getConnectionName());
                }
            }
            boolean isDR = false;
            if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.
                getMessageType())) {
                isDR = true;
            }

            return connMan.getSession(list,isDR);
        }
    }
}
