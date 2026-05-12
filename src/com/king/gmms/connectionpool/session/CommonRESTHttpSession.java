package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.json.JSONObject;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportQueryRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportRESTJsonHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageRESTJsonHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.protocol.commonhttp.SecCommonDeliveryHttpHandler;
import com.king.gmms.protocol.commonhttp.SecCommonMessageHttpHandler;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class CommonRESTHttpSession extends HttpSession {

	protected HttpInterface hi;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonHttpSession.class);
	protected A2PCustomerInfo cst = null;

	public CommonRESTHttpSession(A2PCustomerInfo info) {

		super(info);

		init(info);

	}

	private void init(A2PCustomerInfo info) {
		if (null == info) {
			log.error("A2PCustomerInfo is null!");
			return;
		}
		cst = info;
		String interfaceName = info.getProtocol().trim();
		HttpInterfaceManager him = gmmsUtility.getHttpInterfaceManager();
		if (null == him) {
			log.error("Get null HttpInterfaceManager instance!");
			return;
		}
		him.initHttpInterfaceMap(interfaceName);
		try {
			hi = him.getHttpInterfaceMap().get(interfaceName);
		} catch (Exception e) {
			log.error("Can not find the Interface, and name is "
					+ interfaceName, e);
		}
	}

	public boolean submit(GmmsMessage msg) throws IOException {

		if (log.isDebugEnabled()) {
			log.debug("Http Client Submit msg:" + msg);
		}

		boolean bret = false;
		URL url = null;
		String contentType = null;
		String httpMethod = cst.getHttpMethod();

		String post = "";

		String get = "";

		String resp = null;

		if (HttpConstants.HTTP_METHOD_REST_XML.equalsIgnoreCase(httpMethod)) {
			contentType = HttpConstants.MEDIA_TYPE_XML;
		} else if (HttpConstants.HTTP_METHOD_REST_JSON
				.equalsIgnoreCase(httpMethod)) {
			contentType = HttpConstants.MEDIA_TYPE_JSON;
		} else if (HttpConstants.HTTP_METHOD_REST_URLENCODE
				.equalsIgnoreCase(httpMethod)) {
			contentType = HttpConstants.MEDIA_TYPE_URLENCODE;
		}

		if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {

			HttpPdu submitReq = hi.getMtSubmitRequest();
			if (submitReq.hasHandlerClass()) {
				String className = submitReq.getHandlerClass();
				Object[] args = { msg, this.charset, cst };
				post = (String) hi.invokeHandler(className,
						HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
				log.debug(msg, "Used handlerclass:{},invoke method:{}",
						className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
			} else {

				if (HttpConstants.HTTP_METHOD_REST_XML
						.equalsIgnoreCase(httpMethod)) {
					CommonMessageRESTXmlHttpHandler commonMessageHttpHandler = hi
							.getCommonMessageRESTXmlHandler();
					post = commonMessageHttpHandler.makeRequest(msg,
							this.charset, cst);

				} else if (HttpConstants.HTTP_METHOD_REST_JSON
						.equalsIgnoreCase(httpMethod)) {
					CommonMessageRESTJsonHttpHandler commonMessageHttpHandler = hi
							.getCommonMessageRESTJsonHandler();
					post = commonMessageHttpHandler.makeRequest(msg,
							this.charset, cst);

				} else if (HttpConstants.HTTP_METHOD_REST_URLENCODE
						.equalsIgnoreCase(httpMethod)) {
					SecCommonMessageHttpHandler commonMessageHttpHandler = hi
							.getSecCommonMessageHttpHandler();
					post = commonMessageHttpHandler.makeRequest(msg,
							this.charset, cst);

				}
			}

			if (post == null || "".equals(post)) {
				log.error(msg, "Can't send empty message!");
				return false;
			}

			url = new URL(serverAddress);
			if (log.isDebugEnabled()) {
				log.debug("http post mothed send url: {}", url.toString());
			}

			if (log.isDebugEnabled()) {
				log.debug("contentType={}", contentType);
			}

			try {
				resp = super.doPost(url, post, null, contentType,
						cst.isSMSOptionIsSupportHttps());
				if (log.isDebugEnabled()) {
					log.debug(msg, "resp = {}" + resp);
				}
				dealResp(msg, resp);
				bret = true;
			} catch (IOException e) {
				handleHttpStatusCode(msg);
				dealHttpErrorResp(msg, resp);
				log.warn(e, e);

				return true;

			} catch (Exception e) {

				log.warn(e, e);

				return false;
			}

		} else if (msg.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {

			HttpPdu drReq = hi.getMoDRRequest();
			if (drReq.hasHandlerClass()) {
				String className = drReq.getHandlerClass();
				Object[] args = { msg, this.charset, cst };
				post = (String) hi.invokeHandler(className,
						HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
				log.debug(msg, "Used handlerclass:{},invoke method:{}",
						className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
			} else {

				if (HttpConstants.HTTP_METHOD_REST_XML
						.equalsIgnoreCase(httpMethod)) {
					CommonDeliveryReportRESTXmlHttpHandler commonDeliveryReportHttpHandler = hi
							.getCommonDeliveryReportRESTXmlHandler();
					post = commonDeliveryReportHttpHandler.makeRequest(msg,
							this.charset, cst);

				} else if (HttpConstants.HTTP_METHOD_REST_JSON
						.equalsIgnoreCase(httpMethod)) {
					CommonDeliveryReportRESTJsonHttpHandler commonDeliveryReportHttpHandler = hi
							.getCommonDeliveryReportRESTJsonHandler();
					post = commonDeliveryReportHttpHandler.makeRequest(msg,
							this.charset, cst);

				} else if (HttpConstants.HTTP_METHOD_REST_URLENCODE
						.equalsIgnoreCase(httpMethod)) {
					SecCommonDeliveryHttpHandler commonDeliveryReportHttpHandler = hi
							.getSecCommonDeliveryHttpHandler();
					post = commonDeliveryReportHttpHandler.makeRequest(msg,
							this.charset, cst);

				}
			}

			if (post == null || "".equals(post)) {
				log.error(msg, "Can't send empty message!");
				return false;
			}
			url = new URL(serverDRAddress);
			if (log.isDebugEnabled()) {
				log.debug("http post mothed send url: {}", url.toString());
			}
			try {
				resp = super.doPost(url, post, null, contentType,
						cst.isSMSOptionIsSupportHttps());
				if (log.isDebugEnabled()) {
					log.debug(msg, "resp = {}" + resp);
				}
				dealResp(msg, resp);
				bret = true;
			} catch (IOException e) {
				handleHttpStatusCode(msg);
				dealHttpErrorResp(msg, resp);
				return true;

			} catch (Exception e) {
				return false;
			}

		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(msg.getMessageType())) {

			HttpPdu drReq = hi.getMtDRRequest();
			if (drReq.hasHandlerClass()) {
				String className = drReq.getHandlerClass();
				Object[] args = { msg, this.charset, cst };
				get = (String) hi.invokeHandler(className,
						HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
				log.debug(msg, "Used handlerclass:{},invoke method:{}",
						className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
			} else {

				CommonDeliveryReportQueryRESTXmlHttpHandler commonDeliveryReportQueryHttpHandler = hi
						.getCommonDeliveryReportQueryRESTXmlHandler();
				get = commonDeliveryReportQueryHttpHandler.makeRequest(msg,
						this.charset, cst);
			}

			if (get == null || "".equals(get)) {
				log.error(msg, "Can't send empty message!");
				return false;
			}
			url = new URL(serverDRAddress + "?" + get);
			if (log.isDebugEnabled()) {
				log.debug("http get mothed send url: {}", url.toString());
			}

			try {
				resp = super.doGet(url, cst.isSMSOptionIsSupportHttps());
				if (log.isDebugEnabled()) {
					log.debug(msg, "resp = {}" + resp);
				}
				dealResp(msg, resp);
				bret = true;
			} catch (IOException e) {
				handleHttpStatusCode(msg);
				dealHttpErrorResp(msg, resp);
				return true;

			} catch (Exception e) {
				return false;
			}
		}

		return bret;

	}

	public boolean queryBatchDR(GmmsMessage msg) throws IOException {
		boolean bret = false;
		if (msg == null) {
			return false;
		}
		String post = this.appendData(msg).toString();
		if ("".equals(post)) {
			log.error(msg, "Can't send empty message!");
			return false;
		}
		String resp = null;
		try {
			URL url = null;
			String httpMethod = cst.getHttpMethod();
			if (httpMethod.equalsIgnoreCase("get")) {
				url = new URL(serverDRAddress + "?" + post);
				log.debug("http get mothed send url: {}", url.toString());
				resp = super.doGet(url, cst.isSMSOptionIsSupportHttps());
			} else {
				url = new URL(serverDRAddress);
				log.debug("http post mothed send url: {}", url.toString());
				resp = super.doPost(url, post, null,
						cst.isSMSOptionIsSupportHttps());
			}
			if (log.isInfoEnabled()) {
				log.info(msg, "received queryDR resp = {}", resp);
			}
			this.dealQueryBatchDRResp(msg, resp);
			bret = true;
		} catch (IOException e) {
			handleHttpStatusCode(msg);
			return false;
		} catch (Exception e) {
			return false;
		}
		return bret;
	}

	public void dealQueryBatchDRResp(GmmsMessage msg, String resp) {
		if (resp == null || "".equals(resp)) {
			return;
		}
		List<GmmsMessage> msgs = null;
		if (msg.getMessageType().equalsIgnoreCase(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			HttpPdu drResp = hi.getMtDRResponse();
			if (drResp.hasHandlerClass()) {
				String className = drResp.getHandlerClass();
				Object[] args = { msg, resp };
				Object c = hi.invokeHandler(className,
						HttpConstants.HANDLER_METHOD_PARSELISTRESPONSE, args);
				if (c == null) {
					return;
				}
				if (c.getClass().getName().equals("java.util.ArrayList")) {
					msgs = (List<GmmsMessage>) c;
				}
				if (log.isDebugEnabled()) {
					log.debug(msg, "Used handlerclass:{},invoke method:{}",
							className,
							HttpConstants.HANDLER_METHOD_PARSERESPONSE);
				}
			}

		} else {
			log.error(msg, "Unsupport message type:{}", msg.getMessageType());
		}
		if (msgs == null) {
			return;
		} else {
			for (GmmsMessage message : msgs) {
				message.setRSsID(cst.getSSID());
				message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				if (!putGmmsMessage2RouterQueue(message)) {
					if (log.isInfoEnabled()) {
						log.info(message,
								"Send response to core engine failed!");
					}
				}
			}
		}

	}

	public void dealResp(GmmsMessage msg, String resp) {

		if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_SUBMIT)) {
			if (resp == null || "".equals(resp)) {
				List<HttpParam> parameters = hi.getMtSubmitResponse()
						.getParamList();
				if (parameters == null || parameters.size() == 0) {
					handleHttpStatusCode(msg);
				} else {
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}
			} else {
				dealSubmitResp(msg, resp);
			}
			if (msg.getOutMsgID() == null
					|| "".equalsIgnoreCase(msg.getOutMsgID())) {
				String client_ref_id = MessageIdGenerator
						.generateCommonStringID();
				msg.setOutMsgID(client_ref_id);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		} else if (msg.getMessageType().equalsIgnoreCase(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {

			if (resp == null || "".equals(resp)) {
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
						.getCode());
			} else {
				dealDRResp(msg, resp);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
		} else if (msg.getMessageType().equalsIgnoreCase(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			if (resp == null || "".equals(resp)) {
				msg.setStatusCode(GmmsStatus.ENROUTE.getCode());
			} else {
				dealDRQueryResp(msg, resp);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
		} else {
			log.error(msg, "Unsupport message type:{}", msg.getMessageType());
		}

		if (!putGmmsMessage2RouterQueue(msg)) {
			log.info(msg, "Send response to core engine failed!");
		}
	}
	
	public void dealHttpErrorResp(GmmsMessage msg, String resp) {
		if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_SUBMIT)) {			
			if (msg.getOutMsgID() == null || "".equalsIgnoreCase(msg.getOutMsgID())) {
				String client_ref_id = MessageIdGenerator.generateCommonStringID();
				msg.setOutMsgID(client_ref_id);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		} else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {			
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
		}else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {			
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
		} 
		else {
			log.error(msg, "Unsupport message type:{}" , msg.getMessageType());
		}

		if (!putGmmsMessage2RouterQueue(msg)) {
			log.info(msg, "Send response to core engine failed!");
		}
	}

	/**
	 * deal with submit response
	 * 
	 * @param msg
	 * @param resp
	 */
	private void dealSubmitResp(GmmsMessage msg, String resp) {
		String respStr = resp.trim();

		String jobid = "";
		String msg_status_code = null;
		String reason_phrase = "";
		HttpPdu submitResp = hi.getMtSubmitResponse();
		if (submitResp.hasHandlerClass()) {
			String className = submitResp.getHandlerClass();

			Object[] args = { msg, resp };
			hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE, args);

			if (log.isDebugEnabled()) {
				log.debug(msg, "Used handlerclass:{},invoke method:{}",
						className, HttpConstants.HANDLER_METHOD_PARSERESPONSE);
			}

		} else {

			try {
				Document doc = DocumentHelper.parseText(respStr);

				Element rootElement = doc.getRootElement();

				List<HttpParam> parameters = hi.getMtSubmitResponse()
						.getParamList();
				for (HttpParam param : parameters) {
					String pval = param.getParam();
					String oval = param.getOppsiteParam();
					Element hpElt = rootElement.element(oval);
					String vaule = hpElt.getText();
					if ("outMsgID".equalsIgnoreCase(pval)) {
						jobid = vaule;
					} else if ("StatusCode".equalsIgnoreCase(pval)) {
						msg_status_code = vaule;
					} else if ("StatusText".equalsIgnoreCase(pval)) {
						reason_phrase = vaule;
					}

				}
				msg.setOutMsgID(jobid);
				if (log.isInfoEnabled()) {
					log.info(msg, "received submit resp {}", resp);
				}
				HttpStatus status = new HttpStatus(msg_status_code,
						reason_phrase);
				GmmsStatus gstatus = hi.mapHttpSubStatus2GmmsStatus(status);
				msg.setStatus(gstatus);

			} catch (DocumentException e) {
				e.printStackTrace();
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return;
			}
		}
	}

	/**
	 * deal with DR response
	 * 
	 * @param msg
	 * @param resp
	 */
	private void dealDRResp(GmmsMessage msg, String resp) {

		String respStr = resp.trim();
		String jobid = "";
		int msg_status_code = -1;
		HttpPdu drResp = hi.getMoDRResponse();

		if (drResp.hasHandlerClass()) {
			String className = drResp.getHandlerClass();
			Object[] args = { msg, resp };
			hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE, args);

			if (log.isDebugEnabled()) {
				log.debug(msg, "Used handlerclass:{},invoke method:{}",
						className, HttpConstants.HANDLER_METHOD_PARSERESPONSE);
			}

		} else {

			try {
				String reason_phrase = "";
				Document doc = DocumentHelper.parseText(respStr);
				Element rootElement = doc.getRootElement();
				// Element notificationRepElt = rootElement
				// .element("notificationResponse");
				Element notificationRepElt = rootElement;
				List<HttpParam> parameters = hi.getMoDRResponse()
						.getParamList();
				for (HttpParam param : parameters) {
					String pval = param.getParam();
					String oval = param.getOppsiteParam();
					Element hpElt = notificationRepElt.element(oval);
					String vaule = hpElt.getTextTrim();
					if ("inMsgID".equalsIgnoreCase(pval)) {
						jobid = vaule;
					} else if ("StatusCode".equalsIgnoreCase(pval)) {
						msg_status_code = Integer.parseInt(vaule);
					} else if ("StatusText".equalsIgnoreCase(pval)) {
						reason_phrase = vaule;
					}

				}

				if (log.isInfoEnabled()) {
					log.info(msg,
							"Receive DR response,outmsgId {}, statuscode {}",
							jobid, msg_status_code);
				}

				HttpStatus hs = new HttpStatus(String.valueOf(msg_status_code),
						reason_phrase);

				GmmsStatus gsStatus = hi.mapHttpDRRespStatus2GmmsStatus(hs);
				// success delivery ,don't update message status ,un success delivery ,update message status
				if (gsStatus.getCode() != gsStatus.DELIVERED.getCode()) {
					msg.setStatusCode(gsStatus
							.getCode());
				}

			} catch (DocumentException e) {
				e.printStackTrace();
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return;
			}
		}
	}

	private void dealDRQueryResp(GmmsMessage msg, String resp) {
		HttpPdu drResp = hi.getMtDRResponse();
		if (drResp.hasHandlerClass()) {
			String className = drResp.getHandlerClass();
			Object[] args = { msg, resp };
			Object c = hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE, args);

			if (log.isDebugEnabled()) {
				log.debug(msg, "Used handlerclass:{},invoke method:{}",
						className, HttpConstants.HANDLER_METHOD_PARSERESPONSE);
			}
			msg.setRSsID(cst.getSSID());
		} else {
			log.error(msg,
					"there is no handler class for processing message! {}");
		}
	}

	@Override
	public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {

		return null;
	}

	protected StringBuffer appendData(GmmsMessage message) {
		StringBuffer postData = new StringBuffer();
		String postStr = "";
		try {
			if (message.getMessageType().equals(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
				HttpPdu drReq = hi.getMtDRRequest();
				if (drReq.hasHandlerClass()) {
					String className = drReq.getHandlerClass();
					Object[] args = { message, this.charset, cst };
					postStr = (String) hi.invokeHandler(className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
					log.debug(message, "Used handlerclass:{},invoke method:{}",
							className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
				} else {
					CommonDeliveryReportQueryRESTXmlHttpHandler commonDeliveryReportQueryHttpHandler = hi
							.getCommonDeliveryReportQueryRESTXmlHandler();
					postStr = commonDeliveryReportQueryHttpHandler.makeRequest(
							message, this.charset, cst);
				}

				if (postStr != null && !"".equals(postStr.trim())) {
					postData.append("&").append(postStr);
				} else {
					message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				}
			} else {
				log.error(message,
						"Unsupport message type:" + message.getMessageType());
			}
		} catch (UnsupportedEncodingException e) {
			log.error(message, "urlEncode Error!", e);
		} catch (Exception e) {
			log.error(message, "", e);
		}
		String post = postData.toString();
		if (post.startsWith("&")) {
			postData.deleteCharAt(0);
		}
		if (log.isInfoEnabled()) {
			log.info(message, "post msg {}", postData.toString());
		}
		return postData;
	}

	/**
	 * handle http status code
	 * 
	 * @param msg
	 */
	private void handleHttpStatusCode(GmmsMessage msg) {
		if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			handleHttpStatusCode4Submit(msg);
		} else if (msg.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			handleHttpStatusCode4DR(msg);
		}
	}

	/**
	 * handle http status code for submit
	 * 
	 * @param msg
	 */
	private void handleHttpStatusCode4Submit(GmmsMessage msg) {
		int httpstatus = 0;
		try {
			httpstatus = this.connection.getResponseCode();

			if (log.isDebugEnabled()) {
				log.debug(msg, "httpstatus = {}", httpstatus);
			}
		} catch (IOException e) {
			log.warn(e, e);
		}
		String strStatus = String.valueOf(httpstatus);
		if (strStatus.startsWith("4")) {
			switch (httpstatus) {
			case 401:
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				break;
			case 403:
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				break;
			case 408:
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				break;
			default:
				msg.setStatus(GmmsStatus.UNDELIVERED);
				break;
			}
		} else if (strStatus.startsWith("5")) {
			switch (httpstatus) {
			case 503:
				msg.setStatus(GmmsStatus.Throttled);
				break;
			case 511:
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				break;
			case 598:
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				break;
			case 599:
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				break;
			default:
				msg.setStatus(GmmsStatus.UNDELIVERED);
				break;
			}
		} else if (strStatus.startsWith("2")) {
			if (httpstatus >= 200 && httpstatus < 300) {
				msg.setStatus(GmmsStatus.SUCCESS);
			} else {
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			}
		} else {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
	}

	/**
	 * handle http status code for DR
	 * 
	 * @param msg
	 */
	private void handleHttpStatusCode4DR(GmmsMessage msg) {
		int httpstatus = 0;
		try {
			httpstatus = this.connection.getResponseCode();
			if (log.isDebugEnabled()) {
				log.debug(msg, "httpstatus = {}", httpstatus);
			}
		} catch (IOException e) {
			log.warn(e, e);
		}
		String strStatus = String.valueOf(httpstatus);
		if (strStatus.startsWith("4")) {
			switch (httpstatus) {
			case 401:
				msg.setStatus(GmmsStatus.REJECTED);
				break;
			case 403:
				msg.setStatus(GmmsStatus.REJECTED);
				break;
			case 408:
				msg.setStatus(GmmsStatus.UNDELIVERABLE);
				break;
			default:
				msg.setStatus(GmmsStatus.UNKNOWN);
				break;
			}
		} else if (strStatus.startsWith("5")) {
			switch (httpstatus) {
			case 504:
				msg.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				break;
			case 511:
				msg.setStatus(GmmsStatus.REJECTED);
				break;
			case 598:
				msg.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				break;
			case 599:
				msg.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				break;
			default:
				msg.setStatus(GmmsStatus.UNKNOWN);
				break;
			}
		} else {
			msg.setStatus(GmmsStatus.UNKNOWN);
		}
	}

	// public boolean submit(GmmsMessage msg) throws IOException {
	// boolean bret = false;
	// URL url = null;
	// String contentType = null;
	// String httpMethod = cst.getHttpMethod();
	//
	// String post = "";
	//
	// String get = "";
	//
	// if (HttpConstants.HTTP_METHOD_REST_XML.equalsIgnoreCase(httpMethod)) {
	// contentType = HttpConstants.MEDIA_TYPE_XML;
	// post = this.appendXMLData(msg).toString();
	//
	// } else if (HttpConstants.HTTP_METHOD_REST_JSON
	// .equalsIgnoreCase(httpMethod)) {
	// contentType = HttpConstants.MEDIA_TYPE_JSON;
	// post = this.appendJSONData(msg);
	//
	// } else if (HttpConstants.HTTP_METHOD_REST_URLENCODE
	// .equalsIgnoreCase(httpMethod)) {
	// contentType = HttpConstants.MEDIA_TYPE_URLENCODE;
	// post = this.appendData(msg).toString();
	//
	// }
	//
	// if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(msg
	// .getMessageType())) {
	// contentType = HttpConstants.MEDIA_TYPE_URLENCODE;
	// get = this.appendData(msg).toString();
	//
	// }
	//
	// if ("".equals(post)) {
	// log.error(msg, "Can't send empty message!");
	// return false;
	// }
	// String resp = null;
	// try {
	//
	// if (msg.getMessageType().equalsIgnoreCase(
	// GmmsMessage.MSG_TYPE_SUBMIT)
	// || msg.getMessageType().equalsIgnoreCase(
	// GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
	// url = new URL(serverAddress);
	// if (log.isDebugEnabled()) {
	// log.debug("http post mothed send url: {}", url.toString());
	// }
	// resp = super.doPost(url, post, null, contentType);
	// } else if (msg.getMessageType().equalsIgnoreCase(
	// GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
	// url = new URL(serverAddress + "?" + get);
	// if (log.isDebugEnabled()) {
	// log.debug("http get mothed send url: {}", url.toString());
	// }
	// resp = super.doGet(url);
	// }
	//
	// if (log.isDebugEnabled()) {
	// log.debug(msg, "resp = {}" + resp);
	// }
	//
	// dealResp(msg, resp);
	//
	// bret = true;
	// } catch (IOException e) {
	// handleHttpStatusCode(msg);
	// return false;
	// } catch (Exception e) {
	// return false;
	// }
	// return bret;
	// }

	// protected StringBuffer appendData(GmmsMessage message) {
	// StringBuffer postData = new StringBuffer();
	// String postStr = "";
	// try {
	//
	// String userName = hi.getUsername();
	// String password = hi.getPassword();
	// String mobile = message.getRecipientAddress();
	// if ((userName != null) && (password != null)) {
	//
	// postData.append(userName).append("=")
	// .append(urlEncode(super.systemId));
	// String pwd = HttpUtils.md5Encrypt(super.systemId + ":"
	// + super.password + ":" + mobile);
	// postData.append("&").append(password).append("=")
	// .append(urlEncode(pwd));
	//
	// } else if (password == null && userName != null) {
	// postData.append(userName).append("=")
	// .append(urlEncode(super.systemId));
	// } else if (password != null && userName == null) {
	// String encryptMethod = cst.getPasswdEncryptMethod();
	// if (encryptMethod != null) {
	// String encryptPass = HttpUtils.encrypt(super.password,
	// encryptMethod);
	// postData.append(password).append("=")
	// .append(urlEncode(encryptPass));
	// } else {
	// postData.append(password).append("=")
	// .append(urlEncode(super.password));
	// }
	// }
	// if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
	// HttpPdu submitReq = hi.getMtSubmitRequest();
	// if (submitReq.hasHandlerClass()) {
	// String className = submitReq.getHandlerClass();
	// Object[] args = { message, this.charset, cst };
	// postStr = (String) hi.invokeHandler(className,
	// HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
	// log.debug(message, "Used handlerclass:{},invoke method:{}",
	// className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
	//
	// } else {
	// CommonMessageHttpHandler commonMessageHttpHandler = hi
	// .getCommonMessageHandler();
	// postStr = commonMessageHttpHandler.makeRequest(message,
	// this.charset, cst);
	// }
	// // deal with null poststr
	// if (postStr != null && !"".equals(postStr.trim())) {
	// postData.append("&").append(postStr);
	// } else {
	// message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
	// }
	// } else if (message.getMessageType().equals(
	// GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
	// HttpPdu drReq = hi.getMoDRRequest();
	// if (hi.getMtSubmitRequest().hasHandlerClass()) {
	// String className = drReq.getHandlerClass();
	// Object[] args = { message, this.charset, cst };
	// postStr = (String) hi.invokeHandler(className,
	// HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
	// log.debug(message, "Used handlerclass:{},invoke method:{}",
	// className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
	// } else {
	// CommonDeliveryReportHttpHandler commonDeliveryReportHttpHandler = hi
	// .getCommonDeliveryReportHandler();
	// postStr = commonDeliveryReportHttpHandler.makeRequest(
	// message, this.charset, cst);
	// }
	// // deal with null poststr
	// if (postStr != null && !"".equals(postStr.trim())) {
	// postData.append("&").append(postStr);
	// } else {
	// message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
	// }
	// } else if (message.getMessageType().equals(
	// GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
	// HttpPdu drReq = hi.getMtDRRequest();
	// if (drReq.hasHandlerClass()) {
	// String className = drReq.getHandlerClass();
	// Object[] args = { message, this.charset, cst };
	// postStr = (String) hi.invokeHandler(className,
	// HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
	// log.debug(message, "Used handlerclass:{},invoke method:{}",
	// className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
	// } else {
	// CommonDeliveryReportQueryRESTXmlHttpHandler
	// commonDeliveryReportQueryHttpHandler = hi
	// .getCommonDeliveryReportQueryRESTXmlHandler();
	// postStr = commonDeliveryReportQueryHttpHandler.makeRequest(
	// message, this.charset, cst);
	// }
	//
	// if (postStr != null && !"".equals(postStr.trim())) {
	// postData.append("&").append(postStr);
	// } else {
	// message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
	// }
	// } else {
	// log.error(message,
	// "Unsupport message type:" + message.getMessageType());
	// }
	// } catch (UnsupportedEncodingException e) {
	// log.error(message, "urlEncode Error!", e);
	// } catch (Exception e) {
	// log.error(message, "", e);
	// }
	// String post = postData.toString();
	// if (post.startsWith("&")) {
	// postData.deleteCharAt(0);
	// }
	// if (log.isInfoEnabled()) {
	// log.info(message, "post msg {}", postData.toString());
	// }
	// return postData;
	// }

	// protected String appendXMLData(GmmsMessage message) {
	// if (log.isDebugEnabled()) {
	// log.debug(message, "msg = {}", message);
	// }
	// String postStr = "";
	// Document doc = DocumentHelper.createDocument();
	// try {
	//
	// if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
	//
	// Element rootElmt = doc.addElement("outboundSMSRequest");
	//
	// String userName = hi.getUsername();
	// String password = hi.getPassword();
	// String mobile = message.getRecipientAddress();
	// if ((userName != null) && (password != null)) {
	//
	// Element userNameElmt = rootElmt.addElement("userName");
	// userNameElmt.setText(urlEncode(super.systemId));
	// String pwd = HttpUtils.md5Encrypt(super.systemId + ":"
	// + super.password + ":" + mobile);
	// Element passwordElmt = rootElmt.addElement("password");
	// passwordElmt.setText(urlEncode(pwd));
	//
	// } else if (password == null && userName != null) {
	//
	// Element userNameElmt = rootElmt.addElement("userName");
	// userNameElmt.setText(urlEncode(super.systemId));
	//
	// } else if (password != null && userName == null) {
	// String encryptMethod = cst.getPasswdEncryptMethod();
	// if (encryptMethod != null) {
	// String encryptPass = HttpUtils.encrypt(super.password,
	// encryptMethod);
	//
	// Element passwordElmt = rootElmt.addElement("password");
	// passwordElmt.setText(urlEncode(encryptPass));
	//
	// } else {
	// Element passwordElmt = rootElmt.addElement("password");
	// passwordElmt.setText(urlEncode(super.password));
	// }
	// }
	//
	// HttpPdu submitReq = hi.getMtSubmitRequest();
	// if (submitReq.hasHandlerClass()) {
	// String className = submitReq.getHandlerClass();
	// Object[] args = { message, this.charset, cst };
	// postStr = (String) hi.invokeHandler(className,
	// HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
	// log.debug(message, "Used handlerclass:{},invoke method:{}",
	// className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
	//
	// } else {
	// CommonMessageRESTXmlHttpHandler commonMessageHttpHandler = hi
	// .getCommonMessageRESTXmlHandler();
	// postStr = commonMessageHttpHandler.makeRequest(message,
	// this.charset, cst);
	// }
	// // deal with null poststr
	// if (postStr != null && !"".equals(postStr.trim())) {
	// Document docc = DocumentHelper.parseText(postStr);
	// Element subRootElt = docc.getRootElement();
	// rootElmt.add(subRootElt);
	// } else {
	// message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
	// }
	// } else if (message.getMessageType().equals(
	// GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
	//
	// Element rootElmt = doc.addElement("deliveryReportNotification");
	//
	// String userName = hi.getUsername();
	// String password = hi.getPassword();
	// String mobile = message.getRecipientAddress();
	// if ((userName != null) && (password != null)) {
	//
	// Element userNameElmt = rootElmt.addElement("userName");
	// userNameElmt.setText(urlEncode(super.systemId));
	// String pwd = HttpUtils.md5Encrypt(super.systemId + ":"
	// + super.password + ":" + mobile);
	// Element passwordElmt = rootElmt.addElement("password");
	// passwordElmt.setText(urlEncode(pwd));
	//
	// } else if (password == null && userName != null) {
	// Element userNameElmt = rootElmt.addElement("userName");
	// userNameElmt.setText(urlEncode(super.systemId));
	// } else if (password != null && userName == null) {
	// String encryptMethod = cst.getPasswdEncryptMethod();
	// if (encryptMethod != null) {
	// String encryptPass = HttpUtils.encrypt(super.password,
	// encryptMethod);
	// Element passwordElmt = rootElmt.addElement("password");
	// passwordElmt.setText(urlEncode(encryptPass));
	//
	// } else {
	// Element passwordElmt = rootElmt.addElement("password");
	// passwordElmt.setText(urlEncode(super.password));
	// }
	// }
	//
	// HttpPdu drReq = hi.getMoDRRequest();
	// if (hi.getMtSubmitRequest().hasHandlerClass()) {
	// String className = drReq.getHandlerClass();
	// Object[] args = { message, this.charset, cst };
	// postStr = (String) hi.invokeHandler(className,
	// HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
	// log.debug(message, "Used handlerclass:{},invoke method:{}",
	// className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
	// } else {
	// CommonDeliveryReportRESTXmlHttpHandler commonDeliveryReportHttpHandler =
	// hi
	// .getCommonDeliveryReportRESTXmlHandler();
	// postStr = commonDeliveryReportHttpHandler.makeRequest(
	// message, this.charset, cst);
	// }
	// // deal with null poststr
	// if (postStr != null && !"".equals(postStr.trim())) {
	// Document docc = DocumentHelper.parseText(postStr);
	// Element subRootElt = docc.getRootElement();
	// rootElmt.add(subRootElt);
	// } else {
	// message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
	// }
	// } else {
	// log.error(message,
	// "Unsupport message type:" + message.getMessageType());
	// }
	//
	// } catch (UnsupportedEncodingException e) {
	// log.error(message, "urlEncode Error!", e);
	// } catch (Exception e) {
	// log.error(message, "", e);
	// }
	//
	// if (log.isInfoEnabled()) {
	// log.info(message, "post msg {}", doc.asXML());
	// }
	// return doc.asXML();
	// }
	//
	// protected String appendJSONData(GmmsMessage message) {
	// if (log.isDebugEnabled()) {
	// log.debug(message, "msg = {}", message);
	// }
	//
	// String postStr = "";
	// JSONObject jsonObj = new JSONObject();
	// try {
	//
	// if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
	//
	// Map<String, Object> outboundSMSRequest = new HashMap<String, Object>();
	//
	// // postData.append("\"outboundSMSReponse\":{");
	//
	// String userName = hi.getUsername();
	// String password = hi.getPassword();
	// String mobile = message.getRecipientAddress();
	// if ((userName != null) && (password != null)) {
	//
	// String pwd = HttpUtils.md5Encrypt(super.systemId + ":"
	// + super.password + ":" + mobile);
	//
	// outboundSMSRequest.put(userName, urlEncode(super.systemId));
	// outboundSMSRequest.put(password, urlEncode(pwd));
	//
	// } else if (password == null && userName != null) {
	//
	// outboundSMSRequest.put(userName, urlEncode(super.systemId));
	//
	// } else if (password != null && userName == null) {
	// String encryptMethod = cst.getPasswdEncryptMethod();
	// if (encryptMethod != null) {
	// String encryptPass = HttpUtils.encrypt(super.password,
	// encryptMethod);
	//
	// outboundSMSRequest
	// .put(password, urlEncode(encryptPass));
	//
	// } else {
	//
	// outboundSMSRequest.put(password,
	// urlEncode(super.password));
	//
	// }
	// }
	//
	// HttpPdu submitReq = hi.getMtSubmitRequest();
	// if (submitReq.hasHandlerClass()) {
	// String className = submitReq.getHandlerClass();
	// Object[] args = { message, this.charset, cst };
	// postStr = (String) hi.invokeHandler(className,
	// HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
	// log.debug(message, "Used handlerclass:{},invoke method:{}",
	// className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
	//
	// } else {
	// CommonMessageRESTJsonHttpHandler commonMessageHttpHandler = hi
	// .getCommonMessageRESTJsonHandler();
	// postStr = commonMessageHttpHandler.makeRequest(message,
	// this.charset, cst);
	// }
	// if (postStr != null && !"".equals(postStr.trim())) {
	//
	// JSONObject postObj = new JSONObject(postStr);
	//
	// outboundSMSRequest.put("SMSMessage", postObj);
	//
	// jsonObj.put("outboundSMSRequest", outboundSMSRequest);
	// } else {
	// message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
	// }
	// } else if (message.getMessageType().equals(
	// GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
	//
	// Map<String, Object> deliveryReportNotification = new HashMap<String,
	// Object>();
	//
	// String userName = hi.getUsername();
	// String password = hi.getPassword();
	// String mobile = message.getRecipientAddress();
	// if ((userName != null) && (password != null)) {
	//
	// String pwd = HttpUtils.md5Encrypt(super.systemId + ":"
	// + super.password + ":" + mobile);
	//
	// deliveryReportNotification.put(userName,
	// urlEncode(super.systemId));
	// deliveryReportNotification.put(password, urlEncode(pwd));
	//
	// } else if (password == null && userName != null) {
	//
	// deliveryReportNotification.put(userName,
	// urlEncode(super.systemId));
	//
	// } else if (password != null && userName == null) {
	// String encryptMethod = cst.getPasswdEncryptMethod();
	// if (encryptMethod != null) {
	// String encryptPass = HttpUtils.encrypt(super.password,
	// encryptMethod);
	//
	// deliveryReportNotification.put(password,
	// urlEncode(encryptPass));
	//
	// } else {
	// deliveryReportNotification.put(password,
	// urlEncode(super.password));
	//
	// }
	// }
	//
	// HttpPdu drReq = hi.getMoDRRequest();
	// if (hi.getMtSubmitRequest().hasHandlerClass()) {
	// String className = drReq.getHandlerClass();
	// Object[] args = { message, this.charset, cst };
	// postStr = (String) hi.invokeHandler(className,
	// HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
	// log.debug(message, "Used handlerclass:{},invoke method:{}",
	// className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
	// } else {
	// CommonDeliveryReportRESTJsonHttpHandler commonDeliveryReportHttpHandler =
	// hi
	// .getCommonDeliveryReportRESTJsonHandler();
	// postStr = commonDeliveryReportHttpHandler.makeRequest(
	// message, this.charset, cst);
	// }
	// if (postStr != null && !"".equals(postStr.trim())) {
	// JSONObject postObj = new JSONObject(postStr);
	// deliveryReportNotification.put("deliveryInfo", postObj);
	//
	// jsonObj.put("deliveryReportNotification",
	// deliveryReportNotification);
	// } else {
	// message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
	// }
	// } else {
	// log.error(message,
	// "Unsupport message type:" + message.getMessageType());
	// }
	//
	// } catch (UnsupportedEncodingException e) {
	// log.error(message, "urlEncode Error!", e);
	// } catch (Exception e) {
	// log.error(message, "", e);
	// }
	//
	// if (log.isInfoEnabled()) {
	// log.info(message, "post msg {}", jsonObj.toString());
	// }
	// return jsonObj.toString();
	// }

}
