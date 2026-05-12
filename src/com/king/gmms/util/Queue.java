package com.king.gmms.util;

import java.util.Collection;

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
public interface Queue {

    public Object get();;

    public boolean put(Object msg);

    public boolean putAll(Collection msgCollection);

    public int size();
    
    public boolean isFull();
}
