package com.king.gmms.domain.http;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportQueryRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportRESTJsonHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageRESTJsonHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpCharset;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.SecCommonDeliveryHttpHandler;
import com.king.gmms.protocol.commonhttp.SecCommonMessageHttpHandler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class HttpInterface {
	private SystemLogger log = SystemLogger
			.getSystemLogger(HttpInterfaceManager.class);
	private String interfaceName;
	private String username;
	private String password;

	private HttpPdu moSubmitRequest = new HttpPdu();
	private HttpPdu moSubmitResponse = new HttpPdu();
	private HttpPdu moDRRequest = new HttpPdu();
	private HttpPdu moDRResponse = new HttpPdu();
	private HttpPdu mtSubmitRequest = new HttpPdu();
	private HttpPdu mtSubmitResponse = new HttpPdu();
	private HttpPdu mtDRRequest = new HttpPdu();
	private HttpPdu mtDRResponse = new HttpPdu();
	private Map charsetMap = new HashMap<HttpCharset, String>();// <httpCharset,gmmsCharset>
	private Map<String, Object> handlerClassMap = new ConcurrentHashMap<String, Object>();
	private Map<HttpStatus, GmmsStatus> statusSubmitMap = new HashMap<HttpStatus, GmmsStatus>();
	private Map<HttpStatus, GmmsStatus> statusDRMap = new HashMap<HttpStatus, GmmsStatus>();
	private Map<HttpStatus, GmmsStatus> statusDRRespMap = new HashMap<HttpStatus, GmmsStatus>();
	private final Class[] MAKEREQUEST_CLASSES = { GmmsMessage.class,
			String.class, A2PCustomerInfo.class };
	private final Class[] WSREQUEST_CLASSES = { GmmsMessage.class,
			A2PCustomerInfo.class };
	private final Class[] MAKERESPONSE_CLASSES = { HttpStatus.class,
			GmmsMessage.class, A2PCustomerInfo.class };
	private final Class[] PARSEREQUEST_CLASSES = { GmmsMessage.class,
			HttpServletRequest.class };
	private final Class[] PARSERESPONSE_CLASSES = { GmmsMessage.class,
			String.class };
	private final Class[] PARSELISTRESPONSE_CLASSES = { GmmsMessage.class,
			A2PCustomerInfo.class, String.class };
	private final Class[] PARSERELISTQUEST_CLASSES = { java.util.List.class,
			HttpServletRequest.class };
	
	private final Class[] MULTI_MAKERESPONSE_CLASSES = { HttpStatus.class,
			Set.class, A2PCustomerInfo.class };
	

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public HttpPdu getMoSubmitRequest() {
		return moSubmitRequest;
	}

	public void setMoSubmitRequest(HttpPdu moSubmitRequest) {
		this.moSubmitRequest = moSubmitRequest;
	}

	public HttpPdu getMoSubmitResponse() {
		return moSubmitResponse;
	}

	public void setMoSubmitResponse(HttpPdu moSubmitResponse) {
		this.moSubmitResponse = moSubmitResponse;
	}

	public HttpPdu getMoDRRequest() {
		return moDRRequest;
	}

	public void setMoDRRequest(HttpPdu moDRRequest) {
		this.moDRRequest = moDRRequest;
	}

	public HttpPdu getMoDRResponse() {
		return moDRResponse;
	}

	public void setMoDRResponse(HttpPdu moDRResponse) {
		this.moDRResponse = moDRResponse;
	}

	public HttpPdu getMtSubmitRequest() {
		return mtSubmitRequest;
	}

	public void setMtSubmitRequest(HttpPdu mtSubmitRequest) {
		this.mtSubmitRequest = mtSubmitRequest;
	}

	public HttpPdu getMtSubmitResponse() {
		return mtSubmitResponse;
	}

	public void setMtSubmitResponse(HttpPdu mtSubmitResponse) {
		this.mtSubmitResponse = mtSubmitResponse;
	}

	public HttpPdu getMtDRRequest() {
		return mtDRRequest;
	}

	public void setMtDRRequest(HttpPdu mtDRRequest) {
		this.mtDRRequest = mtDRRequest;
	}

	public HttpPdu getMtDRResponse() {
		return mtDRResponse;
	}

	public void setMtDRResponse(HttpPdu mtDRResponse) {
		this.mtDRResponse = mtDRResponse;
	}

	public Map getCharsetMap() {
		return charsetMap;
	}

	public void setCharsetMap(Map charsetMap) {
		this.charsetMap = charsetMap;
	}

	public Map getHandlerClassMap() {
		return handlerClassMap;
	}

	public void setHandlerClassMap(Map handlerClassMap) {
		this.handlerClassMap = handlerClassMap;
	}

	public Map<HttpStatus, GmmsStatus> getStatusDRRespMap() {
		return statusDRRespMap;
	}

	public void setStatusDRRespMap(Map<HttpStatus, GmmsStatus> statusDRRespMap) {
		this.statusDRRespMap = statusDRRespMap;
	}

	/**
	 * invoke handler method
	 * 
	 * @param className
	 * @param methodName
	 * @param args
	 * @return
	 */
	public Object invokeHandler(String className, String methodName,
			Object[] args) {
		try {
			Object handler = handlerClassMap.get(className);
			Method method = null;
			Class clazz = Class.forName(className);
			if(HttpConstants.HANDLER_METHOD_MAKEREQUEST.equals(methodName)){
				method = clazz.getMethod(methodName,MAKEREQUEST_CLASSES);
			}else if(HttpConstants.WS_HANDLER_METHOD_NEWREQUEST.equals(methodName)
					||HttpConstants.WS_HANDLER_METHOD_DRREQUEST.equals(methodName)){
				method = clazz.getMethod(methodName,WSREQUEST_CLASSES);
			}else if(HttpConstants.WS_HANDLER_METHOD_PARSEMOLISTREQUEST.equals(methodName)
					||HttpConstants.WS_HANDLER_METHOD_PARSMTELISTREQUEST.equals(methodName)){
				method = clazz.getMethod(methodName,WSREQUEST_CLASSES);
			}else if(HttpConstants.HANDLER_METHOD_MAKERESPONSE.equals(methodName)){
				
//				method = clazz.getMethod(methodName,MAKERESPONSE_CLASSES);
				if (args[1] instanceof Set) {
					method = clazz.getMethod(methodName, MULTI_MAKERESPONSE_CLASSES);
				} else {
					method = clazz.getMethod(methodName, MAKERESPONSE_CLASSES);
				}
				
			}else if(HttpConstants.HANDLER_METHOD_PARSEREQUEST.equals(methodName)){
				method = clazz.getMethod(methodName,PARSEREQUEST_CLASSES);
			}else if(HttpConstants.HANDLER_METHOD_PARSERESPONSE.equals(methodName)){
				method = clazz.getMethod(methodName,PARSERESPONSE_CLASSES);
			}else if(HttpConstants.HANDLER_METHOD_PARSELISTRESPONSE.equals(methodName)){
				method = clazz.getMethod(methodName,PARSELISTRESPONSE_CLASSES);
			}else if(HttpConstants.HANDLER_METHOD_PARSELISTREQUEST.equals(methodName)){
				method = clazz.getMethod(methodName,PARSERELISTQUEST_CLASSES);
			}else{
				log.error("No such method:"+className+"."+methodName);
				return null;
			}
			Object rt = method.invoke(handler, args);
			return rt;
		} catch (Exception e) {
			log.fatal("Invoke exception with className:{},methodName:{}",
					className, methodName, e);
			handleException(e);
		}
		return null;
	}

	/**
	 * getCommonMessageHandler
	 * 
	 * @return
	 */
	public CommonMessageHttpHandler getCommonMessageHandler() {
		CommonMessageHttpHandler commonMsgHandler = (CommonMessageHttpHandler) this
				.getHandlerClassMap()
				.get(HttpConstants.COMMON_MSG_HANDLERCLASS);
		return commonMsgHandler;
	}

	/**
	 * getCommonDeliveryReportHandler
	 * 
	 * @return
	 */
	public CommonDeliveryReportHttpHandler getCommonDeliveryReportHandler() {
		CommonDeliveryReportHttpHandler commonDRHandler = (CommonDeliveryReportHttpHandler) this
				.getHandlerClassMap().get(HttpConstants.COMMON_DR_HANDLERCLASS);
		return commonDRHandler;
	}

	/**
	 * handle InvocationTargetException
	 * 
	 * @param e
	 */
	private void handleException(Exception e) {
		String msg = null;
		if (e instanceof InvocationTargetException) {
			Throwable targetEx = ((InvocationTargetException) e)
					.getTargetException();
			if (targetEx != null) {
				msg = targetEx.getMessage();
			}
		} else {
			msg = e.getMessage();
		}
		log.fatal(msg, e);
		e.printStackTrace();
	}

	/**
	 * get http status by http status code
	 * 
	 * @param pdu
	 * @param code
	 * @return
	 */
	public HttpStatus getHttpSubmitStatusByCode(String code) {// <HttpStatus,GmmsStatus>
		Map m = this.getStatusSubmitMap();
		Set keys = m.keySet();
		Iterator iter = keys.iterator();
		while (iter.hasNext()) {
			HttpStatus status = (HttpStatus) iter.next();
			if (status.getCode().equalsIgnoreCase(code)) {
				return status;
			}
		}
		log.error("getHttpStatusByCode return null when code is {}", code);
		return HttpStatus.STATUSMAPERROR;
	}

	public HttpStatus getHttpDRStatusByCode(String code) {// <HttpStatus,GmmsStatus>
		Map m = this.getStatusDRMap();
		Set keys = m.keySet();
		Iterator iter = keys.iterator();
		while (iter.hasNext()) {
			HttpStatus status = (HttpStatus) iter.next();
			if (status.getCode().equalsIgnoreCase(code)) {
				return status;
			}
		}
		log.error("getHttpStatusByCode return null when code is {}", code);
		return HttpStatus.STATUSMAPERROR;
	}

	/**
	 * map gmms status to http status
	 * 
	 * @param status
	 * @return
	 */
	public GmmsStatus mapHttpSubStatus2GmmsStatus(HttpStatus status) {// <HttpStatus,GmmsStatus>
		GmmsStatus gs = null;
		gs = (GmmsStatus) this.getStatusSubmitMap().get(status);
		if (gs == null) {
			log
					.error(
							"mapHttpSubStatus2GmmsStatus return null when HttpStatus is {}",
							status.toString());
			gs = GmmsStatus.UNKNOWN_ERROR;
		}
		return gs;
	}

	public GmmsStatus mapHttpDRStatus2GmmsStatus(HttpStatus status) {// <HttpStatus,GmmsStatus>
		GmmsStatus gs = null;
		gs = (GmmsStatus) this.getStatusDRMap().get(status);
		if (gs == null) {
			log
					.error(
							"mapHttpDRStatus2GmmsStatus return null when HttpStatus is {}",
							status.toString());
			gs = GmmsStatus.UNKNOWN;
		}
		return gs;
	}

	public GmmsStatus mapHttpDRRespStatus2GmmsStatus(HttpStatus status) {// <HttpStatus,GmmsStatus>
		GmmsStatus gs = null;
		gs = (GmmsStatus) this.getStatusDRRespMap().get(status);
		if (gs == null) {
			log
					.error(
							"mapHttpDRRespStatus2GmmsStatus return null when HttpStatus is {}",
							status.toString());
			gs = GmmsStatus.UNKNOWN;
		}
		return gs;
	}

	/**
	 * 
	 * @param status
	 * @param msgType
	 * @return
	 */
	public GmmsStatus mapHttpStatus2GmmsStatus(HttpStatus status, String msgType) {
		if (GmmsMessage.MSG_TYPE_SUBMIT.equals(msgType)) {
			return this.mapHttpSubStatus2GmmsStatus(status);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equals(msgType)
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equals(msgType)) {
			return this.mapHttpDRStatus2GmmsStatus(status);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equals(msgType)
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP
						.equals(msgType)) {
			return this.mapHttpDRRespStatus2GmmsStatus(status);
		}
		return GmmsStatus.UNKNOWN_ERROR;
	}

	/**
	 * map http status to gmms status
	 * 
	 * @param status
	 * @return
	 */
	public HttpStatus mapGmmsStatus2HttpSubStatus(GmmsStatus status) {// <HttpStatus,GmmsStatus>
		Map m = this.getStatusSubmitMap();
		if (m == null) {
			return HttpStatus.STATUSMAPERROR;
		}
		Set keys = m.keySet();
		Iterator iter = keys.iterator();
		HttpStatus hStatus = null;
		while (iter.hasNext()) {
			hStatus = (HttpStatus) iter.next();
			GmmsStatus gStatus = (GmmsStatus) m.get(hStatus);
			if (gStatus.equals(status)) {
				return hStatus;
			}
		}
		log
				.error(
						"mapGmmsStatus2HttpSubStatus return null when GmmsStatus is {}",
						status.toString());
		/*
		 * if get Httpstatus is null, the first will get Httpstatus from mapping
		 * gmmsStatus.UNKNOWN. and still get null, it will return any one
		 * HttpStatus from map
		 */
		if (m.containsValue(GmmsStatus.UNKNOWN_ERROR)) {
			return this.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN_ERROR);
		}
		return hStatus;
	}

	/**
	 * 
	 * @param status
	 * @return
	 */
	public HttpStatus mapGmmsStatus2HttpDRStatus(GmmsStatus status) {// <HttpStatus,GmmsStatus>
		Map m = this.getStatusDRMap();

		if (m == null) {
			return HttpStatus.STATUSMAPERROR;
		}
		Set keys = m.keySet();
		Iterator iter = keys.iterator();
		HttpStatus hStatus = null;
		while (iter.hasNext()) {
			hStatus = (HttpStatus) iter.next();
			GmmsStatus gStatus = (GmmsStatus) m.get(hStatus);
			if (gStatus.equals(status)) {
				return hStatus;
			}
		}
		log.error(
				"mapGmmsStatus2HttpDRStatus return null when GmmsStatus is {}",
				status.toString());
		/*
		 * if get Httpstatus is null, the first will get Httpstatus from mapping
		 * gmmsStatus.UNKNOWN. and still get null, it will return any one
		 * HttpStatus from map
		 */
		if (m.containsValue(GmmsStatus.UNKNOWN)) {
			return this.mapGmmsStatus2HttpDRStatus(GmmsStatus.UNKNOWN);
		}
		return hStatus;

	}

	/**
	 * 
	 * @param status
	 * @return
	 */
	public HttpStatus mapGmmsStatus2HttpDRRespStatus(GmmsStatus status) {// <HttpStatus,GmmsStatus>
		Map m = this.getStatusDRRespMap();
		if (m == null) {
			return HttpStatus.STATUSMAPERROR;
		}
		Set keys = m.keySet();
		Iterator iter = keys.iterator();
		HttpStatus hStatus = null;
		while (iter.hasNext()) {
			hStatus = (HttpStatus) iter.next();
			GmmsStatus gStatus = (GmmsStatus) m.get(hStatus);
			if (gStatus.equals(status)) {
				return hStatus;
			}
		}
		log
				.error(
						"mapGmmsStatus2HttpDRRespStatus return null when GmmsStatus is {}",
						status.toString());
		/*
		 * if get Httpstatus is null, the first will get Httpstatus from mapping
		 * gmmsStatus.UNKNOWN. and still get null, it will return any one
		 * HttpStatus from map
		 */
		if (m.containsValue(GmmsStatus.UNKNOWN)) {
			return this.mapGmmsStatus2HttpDRRespStatus(GmmsStatus.UNKNOWN);
		}
		return hStatus;
	}

	/**
	 * 
	 * @param status
	 * @param msgType
	 * @return
	 */
	public HttpStatus mapGmmsStatus2HttpStatus(GmmsStatus status, String msgType) {
		if (GmmsMessage.MSG_TYPE_SUBMIT.equals(msgType)) {
			return this.mapGmmsStatus2HttpSubStatus(status);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equals(msgType)) {
			return this.mapGmmsStatus2HttpDRStatus(status);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equals(msgType)) {
			return this.mapGmmsStatus2HttpDRStatus(status);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP.equals(msgType)
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP
						.equals(msgType)) {
			return this.mapGmmsStatus2HttpDRRespStatus(status);
		}
		return HttpStatus.STATUSMAPERROR;
	}

	/**
	 * is a Success new message Status
	 */
	public boolean isSuccessMessageStatus(HttpStatus status) {
		GmmsStatus gs = (GmmsStatus) this.getStatusSubmitMap().get(status);
		return GmmsStatus.SUCCESS.equals(gs);
	}

	/**
	 * is a Success DR Status
	 */
	public boolean isSuccessDRStatus(HttpStatus status) {
		GmmsStatus gs = (GmmsStatus) this.getStatusDRMap().get(status);
		return (gs == GmmsStatus.DELIVERED);
	}

	/**
	 * is a Success DRResp Status
	 */
	public boolean isSuccessDRRespStatus(HttpStatus status) {
		GmmsStatus gs = (GmmsStatus) this.getStatusDRRespMap().get(status);
		return (gs == GmmsStatus.DELIVERED || gs == GmmsStatus.ACCEPT);
	}

	/**
	 * map GmmsCharset to HttpCharset
	 * 
	 * @param contentType
	 * @return
	 */
	public String mapHttpCharset2GmmsCharset(String mtype) {
		if (null == mtype) {
			log.error("mtype is null, use ascii as default.");
			return GmmsMessage.AIC_CS_ASCII;
		}
		Map dataCodingMap = this.getCharsetMap();// <mtype
													// value="charset">gmmsCharset</mtype>
		if (dataCodingMap == null) {
			log.error("getCharsetMap is null, use ascii as default.");
			return GmmsMessage.AIC_CS_ASCII;
		}
		HttpCharset httpCharset = new HttpCharset(mtype, mtype);
		String gmmsCharset = (String) dataCodingMap.get(httpCharset);
		if (gmmsCharset == null) {
			log
					.error("mapHttpCharset2GmmsCharset is null, use ascii as default.");
			return GmmsMessage.AIC_CS_ASCII;
		}
		return gmmsCharset;
	}

	/**
	 * map HttpCharset to GmmsCharset
	 * 
	 * @param contentType
	 * @return
	 */
	public HttpCharset mapGmmsCharset2HttpCharset(String contentType) {// <mtype
																		// value="charset">gmmsCharset</mtype>
		Map dataCodingMap = this.getCharsetMap();
		if (dataCodingMap == null || contentType == null) {
			return new HttpCharset(GmmsMessage.AIC_CS_ASCII,
					GmmsMessage.AIC_CS_ASCII);
		}
		Set keys = dataCodingMap.keySet();
		Iterator iter = keys.iterator();
		while (iter.hasNext()) {
			HttpCharset httpCharset = (HttpCharset) iter.next();
			String gmmsCharset = (String) dataCodingMap.get(httpCharset);
			if (gmmsCharset.equals(contentType)) {
				return httpCharset;
			}
		}
		if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(contentType)) {
			return new HttpCharset(GmmsMessage.AIC_MSG_TYPE_BINARY,
					GmmsMessage.AIC_MSG_TYPE_BINARY);
		} else {
			return new HttpCharset(GmmsMessage.AIC_CS_ASCII,
					GmmsMessage.AIC_CS_ASCII);
		}
	}

	/**
	 * map HttpCharset to GmmsCharset
	 * 
	 * @param contentType
	 * @return
	 */
	public HttpCharset getHttpCharsetByMessageType(String mtype) {// <mtype
																	// value="charset">gmmsCharset</mtype>
		Map dataCodingMap = this.getCharsetMap();
		if (dataCodingMap == null) {
			return new HttpCharset(GmmsMessage.AIC_CS_ASCII,
					GmmsMessage.AIC_CS_ASCII);
		}
		Iterator iter = dataCodingMap.keySet().iterator();
		while (iter.hasNext()) {
			HttpCharset httpCharset = (HttpCharset) iter.next();
			if (httpCharset.getMessageType().equalsIgnoreCase(mtype)) {
				return httpCharset;
			}
		}
		return new HttpCharset(GmmsMessage.AIC_CS_ASCII,
				GmmsMessage.AIC_CS_ASCII);
	}

	/**
	 * interface Charset & message==> GmmsMessage content type & content
	 * 
	 * @param mtype
	 * @param message
	 * @return
	 * @throws UnsupportedCharsetException
	 */
	public String convert2GmmsMessageContent(String mtype, String message)
			throws UnsupportedEncodingException {
		if (null == mtype || "".equals(mtype) || null == message) {
			return message;
		}
		String gmmsCharset = mapHttpCharset2GmmsCharset(mtype);
		return new String(message.getBytes(mtype), gmmsCharset);
	}

	/**
	 * interface Charset & message==> GmmsMessage content type & content
	 * 
	 * @param mtype
	 * @param message
	 * @return
	 * @throws UnsupportedCharsetException
	 */
	public String convert2HttpContent(String contentType, String message)
			throws UnsupportedEncodingException {
		if (null == contentType || "".equals(contentType) || null == message
				|| GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(contentType)) {
			return message;
		}
		HttpCharset httpCharset = this.mapGmmsCharset2HttpCharset(contentType);
		String charset = httpCharset.getCharset();
		return new String(message.getBytes(contentType), charset); // TODO:deal
																	// with
																	// "UTF-16BE"
	}
	
	/**
	 * getCommonMessageHandler
	 * 
	 * @return
	 */
	public CommonMessageRESTXmlHttpHandler getCommonMessageRESTXmlHandler() {
		CommonMessageRESTXmlHttpHandler commonMsgHandler = (CommonMessageRESTXmlHttpHandler) this
				.getHandlerClassMap()
				.get(HttpConstants.COMMON_MSG_REST_XML_HANDLERCLASS);
		return commonMsgHandler;
	}
	
	
	public CommonMessageRESTJsonHttpHandler getCommonMessageRESTJsonHandler() {
		CommonMessageRESTJsonHttpHandler commonMsgHandler = (CommonMessageRESTJsonHttpHandler) this
				.getHandlerClassMap()
				.get(HttpConstants.COMMON_MSG_REST_JSON_HANDLERCLASS);
		return commonMsgHandler;
	}

	/**
	 * getCommonDeliveryReportHandler
	 * 
	 * @return
	 */
	public CommonDeliveryReportRESTXmlHttpHandler getCommonDeliveryReportRESTXmlHandler() {
		CommonDeliveryReportRESTXmlHttpHandler commonDRHandler = (CommonDeliveryReportRESTXmlHttpHandler) this
				.getHandlerClassMap().get(HttpConstants.COMMON_DR_REST_XML_HANDLERCLASS);
		return commonDRHandler;
	}
	
	public CommonDeliveryReportRESTJsonHttpHandler getCommonDeliveryReportRESTJsonHandler() {
		CommonDeliveryReportRESTJsonHttpHandler commonDRHandler = (CommonDeliveryReportRESTJsonHttpHandler) this
				.getHandlerClassMap().get(HttpConstants.COMMON_DR_REST_JSON_HANDLERCLASS);
		return commonDRHandler;
	}
	
	
	public CommonDeliveryReportQueryRESTXmlHttpHandler getCommonDeliveryReportQueryRESTXmlHandler() {
		CommonDeliveryReportQueryRESTXmlHttpHandler commonDRHandler = (CommonDeliveryReportQueryRESTXmlHttpHandler) this
				.getHandlerClassMap().get(HttpConstants.COMMON_DR_QUERY_REST_XML_HANDLERCLASS);
		return commonDRHandler;
	}
	
	public SecCommonMessageHttpHandler getSecCommonMessageHttpHandler() {
		SecCommonMessageHttpHandler commonMsgHandler = (SecCommonMessageHttpHandler) this
				.getHandlerClassMap()
				.get(HttpConstants.SEC_COMMON_MSG_HANDLERCLASS);
		return commonMsgHandler;
	}
	
	public SecCommonDeliveryHttpHandler getSecCommonDeliveryHttpHandler() {
		SecCommonDeliveryHttpHandler commonDeliveryReportHandler = (SecCommonDeliveryHttpHandler) this
				.getHandlerClassMap()
				.get(HttpConstants.SEC_COMMON_DELIVERY_REPORT_HANDLERCLASS);
		return commonDeliveryReportHandler;
	}

	public String toString() {
		return "\n" + "username = " + username + "\n" + "password = "
				+ password + "\n" + "moSubmitRequest=" + this.moSubmitRequest
				+ "\n" + "moSubmitResponse=" + this.moSubmitResponse + "\n"
				+ "moDRRequest=" + this.moDRRequest + "\n" + "moDRResponse="
				+ this.moDRResponse + "\n" + "mtSubmitRequest="
				+ this.mtSubmitRequest + "\n" + "mtSubmitResponse="
				+ this.mtSubmitResponse + "\n" + "mtDRRequest="
				+ this.mtDRRequest + "\n" + "mtDRResponse=" + this.mtDRResponse;
	}

	public Map<HttpStatus, GmmsStatus> getStatusSubmitMap() {
		return statusSubmitMap;
	}

	public void setStatusSubmitMap(Map<HttpStatus, GmmsStatus> statusSubmitMap) {
		this.statusSubmitMap = statusSubmitMap;
	}

	public Map<HttpStatus, GmmsStatus> getStatusDRMap() {
		return statusDRMap;
	}

	public void setStatusDRMap(Map<HttpStatus, GmmsStatus> statusDRMap) {
		this.statusDRMap = statusDRMap;
	}

	public String getInterfaceName() {
		return interfaceName;
	}

	public void setInterfaceName(String interfaceName) {
		this.interfaceName = interfaceName;
	}
}
