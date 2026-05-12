package com.king.gmms.messagequeue;

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
public interface OperatorMessageQueue {
	
    public boolean putMsg(GmmsMessage msg);
    
//    public ExpiredMessageQueue getMessageQueue();
    
//    public boolean startMessageQueue();
    
    public void stopMessageQueue();
    
}
