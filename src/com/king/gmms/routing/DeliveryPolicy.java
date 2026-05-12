package com.king.gmms.routing;

import com.king.message.gmms.GmmsMessage;

/**
 * <p>Title: </p>
 * <p/>
 * <p>Description: </p>
 * <p/>
 * <p>Copyright: Copyright (c) 2006</p>
 * <p/>
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public abstract class DeliveryPolicy {
    public DeliveryPolicy() {
    }

    /**
     * abstract method, judge if the message is permitted to send
     *
     * @param msg GmmsMessage
     * @return boolean
     */
    abstract public boolean isPermit(GmmsMessage msg);
}
