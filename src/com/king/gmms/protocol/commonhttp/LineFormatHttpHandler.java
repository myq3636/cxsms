package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

public class LineFormatHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(LineFormatHttpHandler.class);
	private static final String LINE_WRAP_CHAR = "\r\n";

	public LineFormatHttpHandler(HttpInterface hie) {
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

	/**
	 * make response
	 */
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
			log.debug(msg, "makeResponse response = {}" + response);
		}

		return response;
	}

	/**
	 * do response for submit request
	 */
	public String makeSubmitResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}

		return this.generateSubmitResponse(hs, msg, submitResp, cst);

	}

	/**
	 * generate submit response
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
		String respValue = "";
		for (HttpParam hp : respList) {
			String param = hp.getParam();
			Object value = msg.getProperty(hp.getParam());
			if ("StatusCode".equalsIgnoreCase(param)) {
				respValue = "" + hs.getCode();
			} else if ("StatusText".equalsIgnoreCase(param)) {
				respValue = hs.getText();
			} else {
				respValue = HttpUtils.getParameter(hp, msg, cst);
			}
			respValue = respValue != null ? respValue : hp.getDefaultValue();

			if(log.isDebugEnabled()){
				log.debug("hp.getParam()={};value={};respValue={}", hp.getParam(),
					value, respValue);
			}
			
			content.append(respValue).append(LINE_WRAP_CHAR);
			respValue = "";
		}
		String respContent = content.toString();
		if (respContent.endsWith(LINE_WRAP_CHAR)) {
			respContent = respContent.substring(0, respContent.length() - 1);
		}
		return respContent;
	}

	/**
	 * make DR response
	 */
	private String makeDRResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		StringBuffer content = new StringBuffer();
		if (hs == null) {
			hs = drFail;
		}

		List<HttpParam> respList = hi.getMtDRResponse().getParamList();
		for (HttpParam hp : respList) {
			String respValue = "";
			String param = hp.getParam();
			if ("StatusText".equalsIgnoreCase(param)) {
				respValue = hs.getText();
			} else if ("StatusCode".equalsIgnoreCase(param)) {
				respValue = "" + hs.getCode();
			} else {
				respValue = HttpUtils.getParameter(hp, message, cst);
			}

			if(log.isDebugEnabled()){
				log.debug("hp.getParam()={};respValue={}", hp.getParam(),
							respValue);
			}
			content.append(respValue + LINE_WRAP_CHAR);
		}
		String respContent = content.toString();
		if (respContent.endsWith(LINE_WRAP_CHAR)) {
			// log.debug("response:begin "+respContent);
			respContent = respContent.substring(0, respContent.length()
					- LINE_WRAP_CHAR.length());
			// log.debug("response:end "+respContent);
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
	 * parse submit request
	 */
	private HttpStatus parseSubmitRequest(GmmsMessage message,
			HttpServletRequest request) throws ServletException, IOException {
		try {
			String hiUsername = hi.getUsername();
			int rssid = -1;
			String username = null;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;

			if (hiUsername == null) {
				String protocol = hi.getInterfaceName();
				if (protocol != null && protocol.trim().length() > 0) {
					ArrayList<Integer> alSsid = gmmsUtility
							.getCustomerManager().getSsidByProtocol(protocol);
					if (alSsid == null || alSsid.size() < 1) {
						log.warn(message, "getSsid by interfaceName {} failed" , protocol);
						return hi
								.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
					}
					rssid = alSsid.get(0);
					csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
							rssid);
					sInfo = (A2PSingleConnectionInfo) csts;
				}
			} else {
				username = request.getParameter(hi.getUsername());

				if(log.isDebugEnabled()){
					log.debug("username={}", username);
				}
				if (username == null || username.trim().length() < 1) {
					message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
				}
				String password = request.getParameter(hi.getPassword());

				log.debug("password={}", password);

				if (password == null || password.trim().length() < 1) {
					message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
				}

				csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
						username);
				sInfo = (A2PSingleConnectionInfo) csts;
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
								"Error occur when processing throttling control in LineFormatHttpHandler.parseSubmitRequest",
								e);
			}

			String mtype = null;
			String messageContent = null;
			String udh = null;
			for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
				String requestValue = request
						.getParameter(hp.getOppsiteParam());
				String value = requestValue != null ? requestValue : hp
						.getDefaultValue();

				log
						.debug(
								"param name={};OppsiteParam={}; requestvalue = {}; value={}",
								hp.getParam(), hp.getOppsiteParam(),
								requestValue, value);

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

			message.setOSsID(sInfo.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo
					.getSSID());
			message.setMsgID(commonMsgID);

			return subSuccess;

		} catch (UnsupportedEncodingException e) {
			log.error(message, " MessageType={}" , message.getMessageType());
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
			String hiUsername = hi.getUsername();
			int rssid = -1;
			String username = null;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;

			if (hiUsername == null) {
				String protocol = hi.getInterfaceName();
				if (protocol != null && protocol.trim().length() > 0) {
					ArrayList<Integer> alSsid = gmmsUtility
							.getCustomerManager().getSsidByProtocol(protocol);
					if ((alSsid == null || alSsid.size() < 1)) {
						log.warn(message, "getSsid by interfaceName {} failed" , protocol);
						return drFail;
					}
					rssid = alSsid.get(0);
					message.setRSsID(rssid);
					csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
							rssid);
					sInfo = (A2PSingleConnectionInfo) csts;
				}
			} else {
				username = request.getParameter(hi.getUsername());

				if(log.isDebugEnabled()){
					log.debug("username={}", username);
				}
				if (username == null || username.trim().length() < 1) {
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				}
				String password = request.getParameter(hi.getPassword());

					log.debug("password={}", password);

				if (password == null || password.trim().length() < 1) {
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				}

				csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
						username);
				sInfo = (A2PSingleConnectionInfo) csts;
				if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {

					log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);

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
								hp.getParam(), hp.getOppsiteParam(), request
										.getParameter(hp.getOppsiteParam()),
								value);

				if ("statusText".equalsIgnoreCase(param)) {
					statusText = value;
				} else if ("statusCode".equalsIgnoreCase(param)) {
					statusCode = value;
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
		if(log.isDebugEnabled()){
    		log.debug(message, "parseResponse para resp = {}" , resp);
		}
		String[] lines = resp.split(LINE_WRAP_CHAR);
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			parameters = hi.getMtSubmitResponse().getParamList();
			int i = 0;
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				if ("StatusText".equalsIgnoreCase(pval)) {
					String status = lines[i++].trim();

					HttpStatus hs = new HttpStatus(null, status);
					GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(hs);
					message.setStatus(gs);
				} else {
					String value = lines[i++].trim();
					HttpUtils.setParameter(param, message, null, value);
				}
			}
		} else if (message.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			parameters = hi.getMoDRResponse().getParamList();
			int i = 0;
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String value = lines[i++].trim();
				if(log.isInfoEnabled()){
					log.info(message, "Received DR with param:{},value:{}", pval , value);
				}
			}
		} else {
			log.error(message, "Unknow message type:{}",message.getMessageType());
			return;
		}

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
		if (statusCode == null) {
			return gs;
		}
		try {
			// int code = Integer.parseInt(statusCode);
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
