package com.king.gmms.customerconnectionfactory;

import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.messagequeue.ShortConnectionCustomerMessageQueue;

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
public abstract class ShortConnectionFactory extends AbstractShortConnectionFactory {

    public ShortConnectionFactory() {
        super();
    }

    /**
     * createOperatorMessageQueue
     *
     * @param ssid int
     * @return OperatorMessageQueue
     * @todo Implement this
     *   com.king.gmms.customerconnectionfactory.AbstractConnectionFactory
     *   method
     */
    protected OperatorMessageQueue createOperatorMessageQueue(int ssid) {
        A2PCustomerInfo info = cim.getCustomerBySSID(ssid);
        OperatorMessageQueue operatorMessageQueue = new ShortConnectionCustomerMessageQueue(info,isServer);
        ssid2messageQueues.put(ssid, operatorMessageQueue);
        return operatorMessageQueue;
    }

    /**
     * startOperatorMessageQueue
     *
     * @param queue OperatorMessageQueue
     * @param ssid int
     * @todo Implement this
     *   com.king.gmms.customerconnectionfactory.AbstractConnectionFactory
     *   method
     */
    protected abstract void startOperatorMessageQueue(OperatorMessageQueue queue,
                                             int ssid);
}
