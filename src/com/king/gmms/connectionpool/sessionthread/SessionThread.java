package com.king.gmms.connectionpool.sessionthread;

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
public interface SessionThread extends Runnable{
    public void start();
    public void stopThread();
    public void interrupt();
}
