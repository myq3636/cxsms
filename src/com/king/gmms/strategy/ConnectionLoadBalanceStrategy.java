package com.king.gmms.strategy;

import java.util.Iterator;
import java.util.ArrayList;

import com.king.gmms.connectionpool.connection.*;
import com.king.gmms.connectionpool.session.*;
import com.king.message.gmms.GmmsMessage;

public class ConnectionLoadBalanceStrategy extends ConnectionStrategy{
    ConnectionManager connMan = null;


    public ConnectionLoadBalanceStrategy(ConnectionManager connMan) {
        if(connMan != null){
            connMan.setStrategy(new IndexBalanceStrategy());
            this.connMan = connMan;
        }
    }

    public Session execute(GmmsMessage msg) {
        return connMan.getSession();
    }

}
