package com.king.gmms.connectionpool.sessionfactory;

import com.king.gmms.connectionpool.session.CommonHttpSession;
import com.king.gmms.connectionpool.session.CommonRESTHttpSession;
import com.king.gmms.connectionpool.session.SecCommonHttpSession;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.session.WebServiceSession;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpConstants;

public class CommonHttpSessionFactory extends SessionFactory {
	
	public CommonHttpSessionFactory(A2PCustomerInfo info) {
		super(info);
	}

	@Override
	public Session getSession() {
		Session session = null;
		String httpMethod = custInfo.getHttpMethod();
		if(HttpConstants.HTTP_METHOD_WEBSERVICE.equalsIgnoreCase(httpMethod)){
			session = new WebServiceSession(this.custInfo);
		}else if(HttpConstants.HTTP_SEC_SESSION.equalsIgnoreCase(custInfo.getSMSOptionChoiceSecurityHttpSession())){
			session = new SecCommonHttpSession(this.custInfo);
		}//add by kevin for REST,already defiend post/get for REST	
		else if(HttpConstants.HTTP_METHOD_REST_XML.equalsIgnoreCase(httpMethod)||HttpConstants.HTTP_METHOD_REST_JSON.equalsIgnoreCase(httpMethod)||HttpConstants.HTTP_METHOD_REST_URLENCODE.equalsIgnoreCase(httpMethod))
		{
			session=new CommonRESTHttpSession(this.custInfo);
		}else{
			session = new CommonHttpSession(this.custInfo);
		}
        return session;
	}

}
