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

public class YilixiongfengHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(YilixiongfengHttpHandler.class);
	private static final String LINE_WRAP_CHAR = "\r\n";
	private static final String COLON_CHAR = ":";

	public YilixiongfengHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * make request
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		String sender = message.getSenderAddress();
		String recipient = message.getRecipientAddress();
		String[] address = new String[] { sender, recipient };
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[0], urlEncoding));// sender_id
			} else if ("recipientAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[1], urlEncoding));// recipient
			} else if ("textContent".equalsIgnoreCase(pval)) {
				postData
						.append("&")
						.append(param.getOppsiteParam())
						.append("=")
						.append(
								urlEncode(message.getTextContent(), urlEncoding));// message
			} else if ("outTransID".equalsIgnoreCase(pval)) {
				String client_ref_id = MessageIdGenerator
						.generateCommonStringID();
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(client_ref_id);// client_ref_id
			} else if ("deliveryReport".equalsIgnoreCase(pval)) {
				String value = parseHttpDeliveryReport(param, message);
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(value);
			} else if ("expiryDate".equalsIgnoreCase(pval)) {
				String expiryDate = parseGmmsExpiredDate(param, message);
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(expiryDate, urlEncoding));
			} else if ("contentType".equalsIgnoreCase(pval)) {
				HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(message
						.getContentType());
				String mtype = httpCharset.getMessageType();
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(mtype, urlEncoding));
			} else {
				String mval = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(mval, urlEncoding));
			}
		}
		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
		}
		return postData.toString();
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
    		log.debug(msg, "makeResponse response = {}" , response);
		}
		return response;
	}

	/**
	 * do response for submit request
	 */
	public String makeSubmitResponse(HttpStatus hs, GmmsMessage msg,
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
			content.append(respValue).append(COLON_CHAR);
			respValue = "";
		}
		String respContent = content.toString();
		if (respContent.endsWith(COLON_CHAR)) {
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
				if ((hs != null && hs == hi
						.mapGmmsStatus2HttpDRRespStatus(GmmsStatus.DELIVERED))
						|| message.getStatus() == GmmsStatus.DELIVERED) {
					respValue = "ok";
				} else {
					respValue = message.getStatus().getText();
				}
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
			respContent = respContent.substring(0, respContent.length() - 1);
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
			String hiUser = hi.getUsername();
			A2PCustomerInfo csts = null;

			if (hiUser != null) {
				String username = request.getParameter(hiUser);

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
				A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) csts;
				String md5AuthKey = HttpUtils.md5Encrypt(sInfo.getAuthKey());
				if (sInfo == null || !(password.equals(md5AuthKey))) {

					log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);

					message.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
				}
			} else {
				String protocol = hi.getInterfaceName();
				if (protocol != null && protocol.trim().length() > 0) {
					ArrayList<Integer> alSsid = gmmsUtility
							.getCustomerManager().getSsidByProtocol(protocol);
					if (alSsid == null || alSsid.size() < 1) {
						log.warn(message, "getSsid by interfaceName {} failed" ,protocol);
						return hi
								.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
					}
					int rssid = alSsid.get(0);
					message.setRSsID(rssid);
					csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
							rssid);
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

			message.setOSsID(csts.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(csts
					.getSSID());
			message.setMsgID(commonMsgID);

			return subSuccess;

		} catch (UnsupportedEncodingException e) {
			log.error(message, " MessageType={}" + message.getMessageType(),e);
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

			if (isNeedAuth) {
				if(log.isDebugEnabled()){
					log.debug("username={}", username);
				}
				if (null != username && username.trim().length() > 0) {
					csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
							username);
				}
				String password = request.getParameter(hi.getPassword());

				if(log.isDebugEnabled()){
					log.debug("password={}", password);
				}
				if (password == null || password.trim().length() < 1) {
					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				}
				A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) csts;
				if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {

					log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);

					message.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				} else {
					message.setRSsID(sInfo.getSSID());
				}
			} else {
				String protocol = hi.getInterfaceName();
				if (protocol != null && protocol.trim().length() > 0) {
					ArrayList<Integer> alSsid = gmmsUtility
							.getCustomerManager().getSsidByProtocol(protocol);
					if (alSsid == null || alSsid.size() < 1) {
						log.warn(message, "getSsid by interfaceName "
								+ protocol + " failed");
						return drFail;
					}
					int rssid = alSsid.get(0);
					message.setRSsID(rssid);
					csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
							rssid);
				}
			}

			// throttling control process
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), message)) {
				return drFail;
			}

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
				if ("messageType".equalsIgnoreCase(param)) {
					if (!"4".equals(value)) {
						log.debug("Expect DR but Message type is {}", value);
						break;
					}
				} else if ("statusText".equalsIgnoreCase(param)) {
					statusText = value;
				} else {
					message.setProperty(hp.getParam(), value);
				}
			}
			GmmsStatus gs = GmmsStatus.UNDELIVERED;
			HttpStatus hs = null;
			try {
				hs = new HttpStatus(null, statusText);
				if (hs != null) {
					gs = hi.mapHttpDRStatus2GmmsStatus(hs);
				} else {
					hs = drFail;
					gs = GmmsStatus.UNDELIVERED;
				}

			} catch (NumberFormatException e) {
				log.error("Parse status code error!", e);
				throw e;
			} catch (Exception e) {
				log.error("Process status error!", e);
				throw e;
			}
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
		String[] lines = resp.split(COLON_CHAR);
		if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			int rspfields = lines.length;
			String statusTxt = lines[0].trim();
			if ("ok".equalsIgnoreCase(statusTxt) && rspfields >= 3) {
				message.setStatus(GmmsStatus.SUCCESS);
				message.setOutMsgID(lines[1].trim());
				message.setRecipientAddress(lines[2].trim());
			} else if ("error".equalsIgnoreCase(statusTxt) && rspfields >= 2) {
				String statusCode = lines[1].trim();
				HttpStatus hs = new HttpStatus(statusCode, null);
				if(log.isInfoEnabled()){
					log.info(message, "return response with status:{}",hs.getText());
				}
				GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(hs);
				message.setStatus(gs);
				message.setOutMsgID(message.getMsgID());
			} else if ("fail".equalsIgnoreCase(statusTxt) && rspfields >= 2) {
				String statusCode = lines[1].trim();
				HttpStatus hs = new HttpStatus(statusCode, null);
				GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(hs);
				message.setStatus(gs);
				message.setOutMsgID(message.getMsgID());
				if(log.isInfoEnabled()){
					log.info(message, "return response:{}", resp);
				}
			} else {
				message.setStatus(GmmsStatus.UNKNOWN_ERROR);
				message.setOutMsgID(message.getMsgID());
				if(log.isInfoEnabled()){
					log.info(message, "Invalid format response:{}" , resp);
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
					log.info(message, "Received DR with param:{},value:{}" ,pval ,value);
				}
			}
		} else {
			log.error(message, "Unknow message type:"
					+ message.getMessageType());
			return;
		}

	}
}
