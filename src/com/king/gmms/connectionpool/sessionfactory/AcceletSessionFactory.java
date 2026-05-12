package com.king.gmms.connectionpool.sessionfactory;

import com.king.framework.*;
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
public class AcceletSessionFactory
    extends SessionFactory {


    public AcceletSessionFactory(A2PCustomerInfo info) {
        super(info);
    }

    public Session getSession() {
        Session session = null;

        session = new AcceletSession(this.custInfo);

        return session;

    }
}
