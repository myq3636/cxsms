package com.king.gmms.connectionpool.sessionfactory;

import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.messagequeue.OperatorMessageQueue;

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
public abstract class SessionFactory {

    protected A2PCustomerInfo custInfo;
    protected OperatorMessageQueue operatorMessageQueue = null;

    public SessionFactory(A2PCustomerInfo info) {
        custInfo = info;
    }

    public abstract Session getSession();

	public OperatorMessageQueue getOperatorMessageQueue() {
		return operatorMessageQueue;
	}

	public void setOperatorMessageQueue(OperatorMessageQueue operatorMessageQueue) {
		this.operatorMessageQueue = operatorMessageQueue;
	}

}
