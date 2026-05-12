package com.king.gmms.connectionpool.sessionfactory;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.session.SpringSession;
import com.king.gmms.domain.A2PCustomerInfo;

public class SpringSessionFactory extends SessionFactory {
	
    
	public SpringSessionFactory(A2PCustomerInfo info) {
		super(info);


	}

	@Override
	public Session getSession() {
        Session session = new SpringSession(this.custInfo);
        return session;

	}

}
