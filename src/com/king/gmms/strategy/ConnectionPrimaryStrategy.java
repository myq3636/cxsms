package com.king.gmms.strategy;

import java.util.ArrayList;
import java.util.Iterator;

import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.*;
import com.king.message.gmms.GmmsMessage;

/**
 * <p>Title: </p>
 *
 * <p>Description: use primary connection to send message </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */

public class ConnectionPrimaryStrategy extends ConnectionStrategy{
    ConnectionManager connMan = null;
    String primary = null;
    private ArrayList connectionList = new ArrayList();
    private boolean isInit = false;

    public ConnectionPrimaryStrategy(ConnectionManager connMan, String primary) {
        this.connMan = connMan;
        this.primary = primary;
    }

    public void init(){
        connectionList.add(primary);
        Iterator connections = connectionMap.keySet().iterator();
        while (connections.hasNext()) {
            String connID = (String) connections.next();
            if(!connID.equalsIgnoreCase(primary)){
                connectionList.add(connID);
            }
        }

        isInit = true;
    }


    public Session execute(GmmsMessage msg){

        if (!isInit){
            init();
        }
        return connMan.getSession(connectionList,false);

    }
}
