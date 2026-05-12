package com.king.gmms.connectionpool.sessionfactory;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.*;
import com.king.gmms.domain.A2PCustomerInfo;

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
public class ClickatellSessionFactory
    extends SessionFactory {


    public ClickatellSessionFactory(A2PCustomerInfo info) {
        super(info);
    }

    public Session getSession() {
        Session session = null;

        session = new ClickatellSession(this.custInfo);

        return session;

    }

}
