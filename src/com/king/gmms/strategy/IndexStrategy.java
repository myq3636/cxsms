package com.king.gmms.strategy;

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
public abstract class IndexStrategy implements Strategy{

    public Object execute(GmmsMessage msg){
        return null;
    }
    public abstract int getNextIndex(int size);
}
