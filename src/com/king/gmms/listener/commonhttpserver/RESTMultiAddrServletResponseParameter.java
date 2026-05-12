package com.king.gmms.listener.commonhttpserver;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.message.gmms.GmmsMessage;

public class RESTMultiAddrServletResponseParameter extends
		RESTServletResponseParameter {



	public RESTMultiAddrServletResponseParameter(String interfaceName,
			HttpStatus hs, HttpServletRequest request,
			HttpServletResponse response, HttpServlet servlet,
			String contentType,Map<String, GmmsMessage> recipientMsgMap) {
		super(interfaceName, hs, request, response, servlet, contentType);
		this.contentType = contentType;
		this.recipientMsgMap = recipientMsgMap;

	}



}
