package com.king.gmms.protocol.commonhttp.xml;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.xml.XmlMessageConverter;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;
import com.king.rest.util.StringUtility;

public class XmlHttpHandler extends XmlMessageConverter {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(XmlHttpHandler.class);

	public XmlHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * make request
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		String postData = null;
		String interfaceName = hi.getInterfaceName().toLowerCase();
		String pkgpath = null;
		String xsdFileName = null;
		List<HttpParam> params = null;
		try {
			boolean outClientPull = Boolean.getBoolean(cst.getOutClientPull());
			message.setOutClientPull(outClientPull);
		} catch (Exception e) {
			log.warn(message, e.getMessage());
		}
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			xsdFileName = interfaceName + "_submit.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".submit";
			params = hi.getMtSubmitRequest().getParamList();
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			xsdFileName = interfaceName + "_dr.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".dr";
			params = hi.getMoDRRequest().getParamList();
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			xsdFileName = interfaceName + "_dr.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".dr";
			params = hi.getMtDRRequest().getParamList();
		}
		if (xsdFileName != null && pkgpath != null) {
			super.init(xsdFileName);
			Object instance = super.gmms2xml(pkgpath, message, params, cst);
			postData = buildXml(instance);
		}
		if(log.isDebugEnabled()){
    		log.debug(message, "makeRequest postData = {}" , postData);
		}
		return postData;
	}

	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		String response = null;
		String interfaceName = hi.getInterfaceName().toLowerCase();
		String pkgpath = null;
		String xsdFileName = null;
		List<HttpParam> params = null;
		if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)
				|| msg.getMessageType()
						.equals(GmmsMessage.MSG_TYPE_SUBMIT_RESP)) {
			xsdFileName = interfaceName + "_response.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".response";
			params = hi.getMoSubmitResponse().getParamList();
		} else if (msg.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)
				|| msg.getMessageType().equals(
						GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP)) {
			xsdFileName = interfaceName + "_drresponse.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".drresponse";
			params = hi.getMtDRResponse().getParamList();
		} else if (msg.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)
				|| msg.getMessageType().equals(
						GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP)) {
			xsdFileName = interfaceName + "_drresponse.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".drresponse";
			params = hi.getMoDRResponse().getParamList();
		}
		if (xsdFileName != null && pkgpath != null) {
			super.init(xsdFileName);
			response = super.gmms2xml(pkgpath, msg, params, cst);
		}
		if(log.isDebugEnabled()){
    		log.debug(msg, "makeResponse response = {}" , response);
		}
		return response;
	}

	/**
	 * parse request
	 */
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		String protocol = hi.getInterfaceName();
		A2PCustomerInfo cst = null;
		if (protocol != null && protocol.trim().length() > 0) {
			ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
					.getSsidByProtocol(protocol);

			if (alSsid == null || alSsid.size() < 1) {
				log.warn(message, "getSsid by interfaceName {} failed" , protocol);
				return hi.mapGmmsStatus2HttpStatus(
						GmmsStatus.AUTHENTICATION_ERROR, message
								.getMessageType());
			}
			int rssid = alSsid.get(0);

			cst = gmmsUtility.getCustomerManager().getCustomerBySSID(rssid);
			try {
				if (!checkIncomingThrottlingControl(cst.getSSID(), message)) {
					return hi.mapGmmsStatus2HttpStatus(GmmsStatus.Throttled, message.getMessageType());
				}
			} catch (Exception e) {
				log.warn("Error occur when processing throttling control in XmlHttpHandler.parseRequest", e);
			}
		}
		// throttling control process
		String pkgpath = null;
		String xsdFileName = null;
		List<HttpParam> params = null;
		String interfaceName = hi.getInterfaceName().toLowerCase();
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			xsdFileName = interfaceName + "_submit.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".submit";
			params = hi.getMoSubmitRequest().getParamList();
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			xsdFileName = interfaceName + "_dr.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".dr";
			params = hi.getMoDRRequest().getParamList();
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			xsdFileName = interfaceName + "_dr.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".dr";
			params = hi.getMtDRRequest().getParamList();
		}
		String xmlcontent = request.getQueryString();
		if (xsdFileName != null && pkgpath != null) {
			super.init(xsdFileName);
			message = super.xml2gmms(pkgpath, xmlcontent, params, cst, message
					.getMessageType());
		} else {
			message.setOSsID(cst.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(cst
					.getSSID());
			message.setMsgID(commonMsgID);
			if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
				return subFail;
			} else if (message.getMessageType().equals(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)
					|| message.getMessageType().equals(
							GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
				return drFail;
			}
		}
		message.setOSsID(cst.getSSID());
		String commonMsgID = MessageIdGenerator.generateCommonMsgID(cst
				.getSSID());
		message.setMsgID(commonMsgID);
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			return subSuccess;
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)
				|| message.getMessageType().equals(
						GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			return drSuccess;
		} else {
			return subFail;
		}
	}

	/**
	 * parse response
	 */
	public void parseResponse(GmmsMessage message, String resp) {
		String interfaceName = hi.getInterfaceName().toLowerCase();
		String pkgpath = null;
		String xsdFileName = null;
		List<HttpParam> params = null;
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			xsdFileName = interfaceName + "_response.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".response";
			params = hi.getMtSubmitResponse().getParamList();
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			xsdFileName = interfaceName + "_drresponse.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".drresponse";
			params = hi.getMoDRResponse().getParamList();
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			xsdFileName = interfaceName + "_drresponse.xsd";
			pkgpath = "com.king.gmms.protocol.commonhttp.xml."
					+ interfaceName + ".drresponse";
			params = hi.getMtDRResponse().getParamList();
		} else {
			log.error(message, "Unknow message type:"
					+ message.getMessageType());
			return;
		}
		if (xsdFileName != null && pkgpath != null) {
			super.init(xsdFileName);
			message = super.xml2gmms(pkgpath, resp, params, null, message
					.getMessageType());
		}
	}
	
	protected boolean checkIncomingThrottlingControl(int ssid, GmmsMessage msg) {
		boolean ret = false;
		
		if(!StringUtility.stringIsNotEmpty(GmmsUtility.getInstance().getRedisClient().getString("thcon"))){    		
        	return ret;
    	}
		
		if (ThrottlingControl.getInstance().isAllowedToReceive(ssid)) {
			ret = true;
		} else {
			if (log.isInfoEnabled()) {
				log.info(msg, "refuced by incoming throttling control");
			}
		}
		return ret;
	}
	
	protected boolean checkIncomingThrottlingControl(int ssid, String args) {
		boolean ret = false;
		
		if (ThrottlingControl.getInstance().isAllowedToReceive(ssid)) {
			ret = true;
		} else {
			if (log.isInfoEnabled()) {
				log.info(args + " refuced by incoming throttling control");
			}
		}
		return ret;
	}
}
