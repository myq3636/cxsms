package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;

public class TWMobileHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(TWMobileHttpHandler.class);

	public TWMobileHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * make request
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		String postData = null;
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			postData = hi.getCommonMessageHandler().makeRequest(message,
					urlEncoding, cst);
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			postData = hi.getCommonDeliveryReportHandler().makeRequest(message,
					urlEncoding, cst);
		}
		if(log.isDebugEnabled()){
    		log.debug(message, "makeRequest postData = {}" , postData);
		}
		return postData;
	}

	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		String response = null;
		if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)
				|| msg.getMessageType()
						.equals(GmmsMessage.MSG_TYPE_SUBMIT_RESP)) {
			response = this.makeSubmitResponse(hs, msg, cst);
		} else if (msg.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)
				|| msg.getMessageType().equals(
						GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP)) {
			response = this.makeDRResponse(hs, msg, cst);
		}
		if(log.isDebugEnabled()){
    		log.debug(msg, "makeResponse response = {}" , response);
		}
		return response;
	}

	/**
	 * do response for submit request
	 */
	private String makeSubmitResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		String respContent = null;
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		respContent = this.generateSubmitResponse(hs, msg, submitResp, cst);
		return respContent;
	}

	/**
	 * generate response
	 * 
	 * @param hs
	 * @param msg
	 * @param submitResp
	 * @param response
	 * @return
	 */
	private String generateSubmitResponse(HttpStatus hs, GmmsMessage msg,
			HttpPdu submitResp, A2PCustomerInfo cst) {
		StringBuffer content = new StringBuffer(60);
		List<HttpParam> respList = submitResp.getParamList();
		String paramDelimiter = submitResp.getParameterDelimiter();
		String respDelimiter = submitResp.getResponseDelimiter();
		String respValue = "";
		if (paramDelimiter == null || "".equals(paramDelimiter)) {
			for (HttpParam hp : respList) {
				String param = hp.getParam();
				if ("StatusText".equalsIgnoreCase(param)) {
					content.append(hs.getText()).append(respDelimiter);
				} else if ("StatusCode".equalsIgnoreCase(param)) {
					content.append(hs.getCode()).append(respDelimiter);
				} else {
					Object value = msg.getProperty(param);
					respValue = HttpUtils.getParameter(hp, msg, cst);
					respValue = respValue != null ? respValue : hp
							.getDefaultValue();

					if(log.isDebugEnabled()){
						log.debug("hp.getParam()={};value={};respValue={}", param,
							value, respValue);
					}
					content.append(respValue).append(respDelimiter);
				}
				respValue = "";
			}
		} else {
			for (HttpParam hp : respList) {
				String param = hp.getParam();
				String oparam = hp.getOppsiteParam();
				if ("StatusText".equalsIgnoreCase(param)) {
					content.append(oparam).append(paramDelimiter).append(
							hs.getText()).append(respDelimiter);
				} else if ("StatusCode".equalsIgnoreCase(param)) {
					content.append(oparam).append(paramDelimiter).append(
							hs.getCode()).append(respDelimiter);
				} else {
					Object value = msg.getProperty(param);
					respValue = HttpUtils.getParameter(hp, msg, cst);
					respValue = respValue != null ? respValue : hp
							.getDefaultValue();

					log.debug("hp.getParam()={};value={};respValue={}", param,
							value, respValue);

					content.append(oparam).append(paramDelimiter).append(
							respValue).append(respDelimiter);
				}
				respValue = "";
			}
		}
		String respContent = content.toString();
		if (respContent.endsWith(respDelimiter)) {
			respContent = respContent.substring(0, respContent.length()
					- respDelimiter.length());
		}
		if(log.isDebugEnabled()){
    		log.debug(msg, "generateSubmitResponse respContent = {}" , respContent);
		}
		return respContent;
	}

	/**
	 * generate DR response
	 */
	private String makeDRResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		StringBuffer content = new StringBuffer();
		if (hs == null) {
			hs = drFail;
		}

		List<HttpParam> respList = hi.getMtDRResponse().getParamList();
		String respValue = "";
		String paramDelimiter = hi.getMtDRResponse().getParameterDelimiter();
		String respDelimiter = hi.getMtDRResponse().getResponseDelimiter();
		if (paramDelimiter == null || "".equals(paramDelimiter)) {
			for (HttpParam hp : respList) {
				String param = hp.getParam();
				if ("StatusCode".equalsIgnoreCase(param)) {
					content.append(hs.getCode()).append(respDelimiter);
				} else if ("StatusText".equalsIgnoreCase(param)) {
					content.append(hs.getText()).append(respDelimiter);
				} else {
					respValue = HttpUtils.getParameter(hp, message, cst);
					content.append(respValue + respDelimiter);
				}
			}
		} else {
			for (HttpParam hp : respList) {
				String param = hp.getParam();
				if ("StatusCode".equalsIgnoreCase(param)) {
					content.append(hp.getOppsiteParam()).append(paramDelimiter)
							.append(hs.getCode()).append(respDelimiter);
				} else if ("StatusText".equalsIgnoreCase(param)) {
					content.append(hp.getOppsiteParam()).append(paramDelimiter)
							.append(hs.getText()).append(respDelimiter);
				} else {
					respValue = HttpUtils.getParameter(hp, message, cst);
					content.append(hp.getOppsiteParam() + paramDelimiter
							+ respValue + respDelimiter);
				}

			}
		}
		String respContent = content.toString();
		if (respContent.endsWith(respDelimiter)) {
			respContent = respContent.substring(0, respContent.length()
					- respDelimiter.length());
		}
		return respContent;
	}

	/**
	 * parse request
	 */
	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		HttpStatus hs = null;
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			hs = this.parseSubmitRequest(message, request);
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			hs = this.parseDRRequest(message, request);
		}
		return hs;
	}

	/**
	 * parse request
	 */
	private HttpStatus parseSubmitRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		String hiUsername = hi.getUsername();
		int rssid = -1;
		String username = null;
		A2PCustomerInfo csts = null;
		if (hiUsername == null) {
			String protocol = hi.getInterfaceName();
			if (protocol != null && protocol.trim().length() > 0) {
				ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
						.getSsidByProtocol(protocol);
				int size = alSsid.size();
				if (size != 1) {
					log.warn(message, "getSsid by interfaceName {}  failed with ssid size:{}" , protocol,size);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
				}
				rssid = alSsid.get(0);
				csts = gmmsUtility.getCustomerManager()
						.getCustomerBySSID(rssid);
			}
		} else {
			username = request.getParameter(hi.getUsername());

			if(log.isDebugEnabled()){
				log.debug("username={}", username);
			}
			if (username == null || username.trim().length() < 1) {
				// response(HttpStatus.MISSUSERNAME, msg, response);
				message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
			}

			String password = request.getParameter(hi.getPassword());

			log.debug("password={}", password);

			if (password == null || password.trim().length() < 1) {
				message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
				HttpStatus hs = hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
				makeResponse(hs, message, null);
				return hs;
			}

			csts = gmmsUtility.getCustomerManager().getCustomerBySpID(username);

			A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) csts;
			if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {
				log.debug("customerInfo == {} by serverid = {}", sInfo,
						username);

				message.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
			}
		}

		// throttling control process
		try {
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), message)) {
				return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.Throttled);
			}
		} catch (Exception e) {
			log
					.warn(
							"Error occur when processing throttling control in ExtendibleDelimiterHttpHandler.parseSubmitRequest",
							e);
		}

		String mtype = null;
		String messageContent = null;
		String udh = null;
		for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
			String requestValue = request.getParameter(hp.getOppsiteParam());
			String value = requestValue != null ? requestValue : hp
					.getDefaultValue();

			log
					.debug(
							"param name={};OppsiteParam={}; requestvalue = {}; value={}",
							hp.getParam(), hp.getOppsiteParam(), requestValue,
							value);

			if ("deliveryReport".equalsIgnoreCase(hp.getParam())) {
				if ("1".equalsIgnoreCase(value)) {
					message.setProperty(hp.getParam(), true);
				} else {
					message.setProperty(hp.getParam(), false);
				}
			} else if ("expiryDate".equalsIgnoreCase(hp.getParam())) {
				Date expireDate = parseHttpExpiryDate(csts, hp, value);
				if (expireDate != null) {
					expireDate = gmmsUtility.getGMTTime(expireDate);
				}
				message.setProperty(hp.getParam(), expireDate);
			} else if ("textContent".equalsIgnoreCase(hp.getParam())) {// textContent
				messageContent = value;
			} else if ("udh".equalsIgnoreCase(hp.getParam())) {// udh
				udh = value;
			} else if ("contentType".equalsIgnoreCase(hp.getParam())) {// contentType
				mtype = value;
			} else {
				message.setProperty(hp.getParam(), value);
			}
		}

		String gmmsCharset = hi.mapHttpCharset2GmmsCharset(mtype);
		message.setContentType(gmmsCharset);

		if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
			message.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
		}

		try {
			if (messageContent == null || "".equalsIgnoreCase(messageContent)) {
				message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
			} else {
				super.parseHttpContent(message, mtype, messageContent, udh);
			}
			if (message.getRecipientAddress() == null) {
				message.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
			}

			message.setOSsID(csts.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(csts
					.getSSID());
			message.setMsgID(commonMsgID);
			return subSuccess;

		} catch (UnsupportedEncodingException e) {
			log.error(message, " MessageType=" + message.getMessageType(),e);
			return subFail;
		} catch (Exception e) {
			log.error("common exception!", e);
			return subFail;
		}

	}

	/**
	 * parse DR request
	 */
	private HttpStatus parseDRRequest(GmmsMessage message,
			HttpServletRequest request) {
		try {
			boolean isNeedAuth = true;
			String hiUser = hi.getUsername();
			String username = null;
			if (hiUser == null) {
				isNeedAuth = false;
			} else {
				username = request.getParameter(hiUser);
				if (username == null) {
					isNeedAuth = false;
				}
			}
			A2PCustomerInfo csts = null;
			int rssid = 0;
			if (!isNeedAuth) {
				String protocol = hi.getInterfaceName();
				if (protocol != null && protocol.trim().length() > 0) {
					ArrayList<Integer> alSsid = gmmsUtility
							.getCustomerManager().getSsidByProtocol(protocol);
					int size = alSsid.size();
					if (size != 1) {
						log.warn(message, "getSsid by interfaceName {}  failed with ssid size:{}", protocol, size);
						return drFail;
					}
					rssid = alSsid.get(0);
					csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
							rssid);
					message.setRSsID(rssid);
				}
			} else {
				if (null != username && username.trim().length() > 0) {
					csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
							username);
				}

				// DR username and password authentication
				if (username == null || username.trim().length() < 1) {
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				}

				String password = request.getParameter(hi.getPassword());

				if (password == null || password.trim().length() < 1) {
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				}

				A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) csts;
				if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {

					if(log.isDebugEnabled()){
						log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);
					}
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				} else {
					message.setRSsID(sInfo.getSSID());
				}
			}

			// throttling control process
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), message)) {
				return drFail;
			}

			String statusCode = null;
			String statusText = null;
			for (HttpParam hp : hi.getMtDRRequest().getParamList()) {
				String param = hp.getParam();
				String value = request.getParameter(hp.getOppsiteParam()) != null ? request
						.getParameter(hp.getOppsiteParam())
						: hp.getDefaultValue();

				log
						.debug(
								"param name={};OppsiteParam={}; requestvalue = {}; value={}",
								hp.getOppsiteParam(), hp.getOppsiteParam(),
								request.getParameter(hp.getOppsiteParam()),
								value);

				if ("statusText".equalsIgnoreCase(param)) {
					statusText = value;
				} else if ("statusCode".equalsIgnoreCase(param)) {
					statusCode = value;
				} else if ("expiryDate".equalsIgnoreCase(hp.getParam())) {
					Date expireDate = parseHttpExpiryDate(csts, hp, value);
					if (expireDate != null) {
						expireDate = gmmsUtility.getGMTTime(expireDate);
					}
					message.setProperty(hp.getParam(), expireDate);
				} else if ("timeStamp".equalsIgnoreCase(hp.getParam())) {
					Date expireDate = parseHttpExpiryDate(csts, hp, value);
					if (expireDate != null) {
						expireDate = gmmsUtility.getGMTTime(expireDate);
					}
					message.setProperty(hp.getParam(), expireDate);
				} else {
					message.setProperty(hp.getParam(), value);
				}
			}
			GmmsStatus gs = genStatus4DR(statusCode, statusText);
			message.setStatus(gs);

			// use msgId to swap between modules
			message.setMsgID(message.getOutMsgID());

			return drSuccess;
		} catch (Exception e) {
			log.error("process dr error!");
			return drFail;
		}
	}

	/**
	 * parse response
	 */
	public void parseResponse(GmmsMessage message, String resp) {
		List<HttpParam> parameters = null;
		String inMsgId = null;
		String outMsgId = null;
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			HttpPdu submitResp = hi.getMtSubmitResponse();
			parameters = submitResp.getParamList();
			String paramDelimiter = submitResp.getParameterDelimiter();
			paramDelimiter = HttpUtils.escapeSpecialChars(paramDelimiter);
			String respDelimiter = submitResp.getResponseDelimiter();
			respDelimiter = HttpUtils.escapeSpecialChars(respDelimiter);
			String[] lines = resp.split(respDelimiter);
			Map<String, String> respMap = genResponseMap(message, lines,
					paramDelimiter, true);
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String oval = param.getOppsiteParam();

				if(log.isDebugEnabled()){
					log.debug("pval={},oval={}", pval, oval);
				}
				if ("outMsgID".equalsIgnoreCase(pval)) {
					outMsgId = respMap.get(oval);
				} else if ("StatusCode".equalsIgnoreCase(pval)) {
					// ignore StatusCode
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					// ignore StatusText
				} else {
					String value = respMap.get(oval);
					message.setProperty(pval, value);
				}
			}
			if (outMsgId == null) {
				message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			} else if (outMsgId.startsWith("-")) {//
				GmmsStatus gs = dealFailedMsgId(outMsgId);
				message.setStatus(gs);
				message.setOutMsgID(outMsgId);
			} else {
				message.setOutMsgID(outMsgId);
			}
			if(log.isInfoEnabled()){
				log.info(message, "Receive submit response and the outMsgId is {}",outMsgId);
			}
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			HttpPdu drResp = hi.getMoDRResponse();
			parameters = drResp.getParamList();
			String paramDelimiter = drResp.getParameterDelimiter();
			String respDelimiter = drResp.getResponseDelimiter();
			String[] lines = resp.split(respDelimiter);
			Map<String, String> respMap = genResponseMap(message, lines,
					paramDelimiter, false);
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String oval = param.getOppsiteParam();
				if ("inMsgID".equalsIgnoreCase(pval)) {
					inMsgId = respMap.get(oval);
				} else if ("StatusCode".equalsIgnoreCase(pval)) {
					// ignore StatusCode
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					// ignore StatusText
				} else {
					String value = respMap.get(oval);
					message.setProperty(pval, value);
				}
			}
			if(log.isInfoEnabled()){
				log.info(message, "Receive DR response and the inMsgId is {}",inMsgId);
			}

		} else {
			log.error(message, "Unknow message type:"
					+ message.getMessageType());
			return;
		}
	}

	/**
	 * deal with failed msgId
	 * 
	 * @param outMsgId
	 * @return
	 */
	private GmmsStatus dealFailedMsgId(String outMsgId) {
		int outmsgId = 0;
		try {
			outmsgId = Integer.parseInt(outMsgId);
		} catch (Exception e) {
			log.error("outMsgId isn't integer format", e);
		}

		switch (outmsgId) {
		case -1:// CGI ERROR
			return GmmsStatus.SERVICE_ERROR;
		case -2:
		case -60004:
		case -60007:
		case -60008:
			return GmmsStatus.AUTHENTICATION_ERROR;
		case -3:
		case -6:
			return GmmsStatus.POLICY_DENIED;
		case -4:
		case -5:
		case -7:
		case -8:
		case -9:
		case -10:
		case -20:
			return GmmsStatus.INVALID_MSG_FIELD;
		case -21:
		case -60014:
			return GmmsStatus.POLICY_DENIED;
		case -11:
		case -12:
		case -19:
			return GmmsStatus.COMMUNICATION_ERROR;
		case -1000:
			return GmmsStatus.SPAMED;
		default:
			return GmmsStatus.UNKNOWN_ERROR;
		}
	}

	/**
	 * generate response map and deal with status
	 * 
	 * @param message
	 * @param lines
	 * @return
	 */
	private Map<String, String> genResponseMap(GmmsMessage message,
			String[] lines, String paramDelimiter, boolean flagIsSubmit) {
		Map<String, String> map = new HashMap<String, String>();
		HttpStatus hs = null;
		List<HttpParam> parameters = null;
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			parameters = hi.getMtSubmitResponse().getParamList();
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			parameters = hi.getMoDRResponse().getParamList();
		}

		if (paramDelimiter == null || "".equals(paramDelimiter)) {// paramDelimiter
																	// is null,
																	// response
																	// like
																	// Spring
																	// format
			if (parameters != null && parameters.size() >= lines.length) {
				int row = 0;
				for (HttpParam param : parameters) {
					String oval = param.getOppsiteParam();
					String line = "";
					while ("".equals(line.trim()) && row < lines.length) {
						line = lines[row++];
					}
					map.put(oval, line);
				}
			}
		} else {
			for (String line : lines) {
				if ("".equals(line.trim())) {
					continue;
				}
				String[] resp = line.split(paramDelimiter);
				if (resp.length != 2) {
					log.error("No '{}' to split line:{}", paramDelimiter, line);
					continue;
				}
				map.put(resp[0].trim(), resp[1].trim());
			}
		}

		// parse HttpStatus
		String hsCodeKey = "";
		String hsTextKey = "";
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("StatusCode".equalsIgnoreCase(pval)) {
				hsCodeKey = param.getOppsiteParam();
			} else if ("StatusText".equalsIgnoreCase(pval)) {
				hsTextKey = param.getOppsiteParam();
			}
		}
		String hsCode = map.get(hsCodeKey);
		String hsText = map.get(hsTextKey);
		if (null != hsCode || null != hsText) {
			hs = new HttpStatus(hsCode, hsText);
		}

		if (hs == null) {
			log.error(message, "Parse status null error!");
			if (flagIsSubmit) {
				message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			} else {
				message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
			}
		} else {
			GmmsStatus gs = null;
			if (flagIsSubmit) {
				gs = hi.mapHttpSubStatus2GmmsStatus(hs);
			} else {
				gs = hi.mapHttpDRStatus2GmmsStatus(hs);
			}
			message.setStatus(gs);
			if(log.isInfoEnabled()){
				log.info(message, "Get HttpStatus for Submit response:{}", hs);
			}
		}
		return map;
	}

	/**
	 * deal with Common Http status
	 * 
	 * @param status
	 * @return
	 * @throws Exception
	 */
	private GmmsStatus genStatus4DR(String statusCode, String statusText)
			throws Exception {
		if(log.isDebugEnabled()){
			log.debug("DR statuscode={},statusText={}", statusCode, statusText);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		try {
			HttpStatus hs = new HttpStatus(statusCode, statusText);
			gs = hi.mapHttpDRStatus2GmmsStatus(hs);
		} catch (NumberFormatException e) {
			log.error("Parse status code error!", e);
			throw e;
		} catch (Exception e) {
			log.error("Process status error!", e);
			throw e;
		}
		return gs;
	}
}