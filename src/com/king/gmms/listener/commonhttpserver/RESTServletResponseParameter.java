package com.king.gmms.listener.commonhttpserver;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.king.gmms.protocol.commonhttp.HttpStatus;

public class RESTServletResponseParameter extends ServletResponseParameter{
	

		
	//REST need
	String contentType;

	public RESTServletResponseParameter(String interfaceName, HttpStatus hs,
									HttpServletRequest request, HttpServletResponse response,
									HttpServlet servlet,String contentType) {
		super(interfaceName,hs,request,response,servlet);
		
		this.contentType=contentType;
	}

	

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	
	
	

}
