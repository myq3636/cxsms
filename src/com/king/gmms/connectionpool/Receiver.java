package com.king.gmms.connectionpool;

import java.nio.ByteBuffer;
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
public interface Receiver {

    public void parse(ByteBuffer buffer);

    public boolean receive(Object obj);

    public void connectionUnavailable() ;
}
