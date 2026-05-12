package com.king.gmms.listener.commonhttpserver;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.message.gmms.GmmsMessage;

public class ServletResponseParameter {
	
	String interfaceName  = null;
	HttpStatus hs = null;
	HttpServletRequest request = null;
	HttpServletResponse response = null;
	HttpServlet servlet = null;
	
	// add by kevin for v3.9.1 multi-recipient

	Map<String, GmmsMessage> recipientMsgMap = null;

	CountDownLatch cdl;
	

	public ServletResponseParameter(String interfaceName, HttpStatus hs,
									HttpServletRequest request, HttpServletResponse response,
									HttpServlet servlet) {
		this.interfaceName = interfaceName;
		this.hs = hs;
		this.request = request;
		this.response = response;
		this.servlet = servlet;
	}

	public String getInterfaceName() {
		return interfaceName;
	}

	public void setInterfaceName(String interfaceName) {
		this.interfaceName = interfaceName;
	}

	public HttpStatus getHttpStatus() {
		return hs;
	}

	public void setHttpStatus(HttpStatus hs) {
		this.hs = hs;
	}

	public HttpServletRequest getRequest() {
		return request;
	}

	public void setRequest(HttpServletRequest request) {
		this.request = request;
	}

	public HttpServletResponse getResponse() {
		return response;
	}

	public void setResponse(HttpServletResponse response) {
		this.response = response;
	}

	public HttpServlet getServlet() {
		return servlet;
	}

	public void setServlet(HttpServlet servlet) {
		this.servlet = servlet;
	}
	
	
	public Map<String, GmmsMessage> getRecipientMsgMap() {
		return recipientMsgMap;
	}

	public void setRecipientMsgMap(Map<String, GmmsMessage> recipientMsgMap) {
		this.recipientMsgMap = recipientMsgMap;
	}

	public CountDownLatch getCdl() {
		return cdl;
	}

	public void setCdl(CountDownLatch cdl) {
		this.cdl = cdl;
	}
	
}