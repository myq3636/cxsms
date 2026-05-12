package com.king.gmms.strategy;

import java.util.*;

import com.king.gmms.connectionpool.session.*;
import com.king.message.gmms.*;

/**
 * <p>Title: ConnectionStrategy</p>
 *
 * <p>Description: to get a connection by GmmsMessage </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company:King </p>
 *
 * @author not attributable
 * @version 1.0
 */
public abstract class ConnectionStrategy implements Strategy{
    protected Map connectionMap;

    public abstract Session execute(GmmsMessage msg);

    public void setConnectionMap(Map connectionMap) {
        this.connectionMap = connectionMap;
    }

}
