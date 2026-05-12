package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

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

public class CommonMessageRESTXmlHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonMessageRESTXmlHttpHandler.class);

	public CommonMessageRESTXmlHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * Add by kevin for REST
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msg, submitResp, cst);

	}

	

	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		try {
			A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) cst;
			String charset = "utf8";
			charset = sInfo.getChlCharset();
			if (charset == null || "".equals(charset)) {
				charset = "utf8";
			}

			Document doc = DocumentHelper.createDocument();

			// add charset
			doc.setXMLEncoding(charset);

			List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();

			Element rootElmt = doc.addElement("SMSRequest");
			String mobile = message.getRecipientAddress();

			String outUserNameElt = hi.getUsername();
			String outPasswordElt = hi.getPassword();
			String inUserNameElt = null;
			String inPasswordElt = null;
			String inUserNameValue = null;
			String inPasswordValue = null;

			if ((outUserNameElt != null) && (outPasswordElt != null)) {

				Element userNameElmt = rootElmt.addElement(outUserNameElt);
				userNameElmt
						.setText(urlEncode(sInfo.getChlAcctName(), charset));
				String pwd = HttpUtils.md5Encrypt(sInfo.getChlAcctName() + ":"
						+ sInfo.getChlPassword() + ":" + mobile);
				Element passwordElmt = rootElmt.addElement(outPasswordElt);
				passwordElmt.setText(urlEncode(pwd, charset));

			} else if (outPasswordElt == null && outUserNameElt != null) {

				Element userNameElmt = rootElmt.addElement(outUserNameElt);
				userNameElmt
						.setText(urlEncode(sInfo.getChlAcctName(), charset));

			} else if (outPasswordElt != null && outUserNameElt == null) {
				String encryptMethod = cst.getPasswdEncryptMethod();
				if (encryptMethod != null) {
					String encryptPass = HttpUtils.encrypt(
							sInfo.getChlPassword(), encryptMethod);

					Element passwordElmt = rootElmt.addElement(outPasswordElt);
					passwordElmt.setText(urlEncode(encryptPass, charset));

				} else {
					Element passwordElmt = rootElmt.addElement(outPasswordElt);
					passwordElmt.setText(urlEncode(sInfo.getChlPassword(),
							charset));
				}
			} else {
				for (HttpParam param : parameters) {
					String pval = param.getParam();
					if ("username".equalsIgnoreCase(pval)) {
						inUserNameElt = param.getOppsiteParam();
						inUserNameValue = HttpUtils.getParameter(param,
								message, cst);
					}
					if ("password".equalsIgnoreCase(pval)) {
						inPasswordElt = param.getOppsiteParam();
						inPasswordValue = HttpUtils.getParameter(param,
								message, cst);
					}
				}
				if (inUserNameElt != null && inPasswordElt != null) {
					Element userNameElmt = rootElmt.addElement(inUserNameElt);

					userNameElmt.setText(urlEncode(inUserNameValue, charset));

					String pwd = HttpUtils.md5Encrypt(inUserNameValue + ":"
							+ inPasswordValue + ":" + mobile);

					Element passwordElmt = rootElmt.addElement(inPasswordElt);
					passwordElmt.setText(urlEncode(pwd, charset));
				} else if (inPasswordElt == null && inUserNameElt != null) {
					Element userNameElmt = rootElmt.addElement(inUserNameElt);

					userNameElmt.setText(urlEncode(inUserNameValue, charset));
				} else if (inPasswordElt != null && inUserNameElt == null) {
					String encryptMethod = cst.getPasswdEncryptMethod();
					if (encryptMethod != null) {

						String encryptPass = HttpUtils.encrypt(inPasswordValue,
								encryptMethod);

						Element passwordElmt = rootElmt
								.addElement(inPasswordElt);
						passwordElmt.setText(urlEncode(encryptPass, charset));

					} else {
						Element passwordElmt = rootElmt
								.addElement(inPasswordElt);

						passwordElmt
								.setText(urlEncode(inPasswordValue, charset));
					}
				}
			}

			Element subRootElmt = rootElmt.addElement("SMSMessage");

			String sender = message.getSenderAddress();
			String recipient = message.getRecipientAddress();
			String[] address = new String[] { sender, recipient };
			boolean isUrlEncodingTwice = cst.isUrlEncodingTwice();
			String[] content = this.parseGmmsContent(message, urlEncoding,
					isUrlEncodingTwice);// content & udh

			for (HttpParam param : parameters) {
				String pval = param.getParam();
				if ("senderAddress".equalsIgnoreCase(pval)) {

					Element senderElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					senderElmt.setText(urlEncode(address[0], urlEncoding));

				} else if ("recipientAddress".equalsIgnoreCase(pval)) {

					Element recipientElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					recipientElmt.setText(urlEncode(address[1], urlEncoding));

				} else if ("udh".equalsIgnoreCase(pval)) {// textContent
					String udh = content[1];
					if (udh != null && !"".equals(udh.trim())) {

						Element udhElmt = subRootElmt.addElement(param
								.getOppsiteParam());
						udhElmt.setText(udh);

					}
				} else if ("textContent".equalsIgnoreCase(pval)) {

					Element textContentElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					textContentElmt.setText(content[0]);

				} else if ("outTransID".equalsIgnoreCase(pval)) {

					String client_ref_id = MessageIdGenerator
							.generateCommonStringID();

					Element outTransIDElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					outTransIDElmt.setText(client_ref_id);

				} else if ("deliveryReport".equalsIgnoreCase(pval)) {
					String value = parseHttpDeliveryReport(param, message);

					Element deliveryReportElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					deliveryReportElmt.setText(value);

				} else if ("expiryDate".equalsIgnoreCase(pval)) {
					String expiryDate = parseGmmsExpiredDate(param, message);

					Element expiryDateElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					expiryDateElmt.setText(urlEncode(expiryDate, urlEncoding));

				} else if ("contentType".equalsIgnoreCase(pval)) {
					HttpCharset httpCharset = hi
							.mapGmmsCharset2HttpCharset(message
									.getContentType());
					String mtype = httpCharset.getMessageType();

					Element contentTypeElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					contentTypeElmt.setText(urlEncode(mtype, urlEncoding));

				} else if ("outMsgID".equalsIgnoreCase(pval)) {

					String outMsgid = message.getOutMsgID();
					if (outMsgid == null) {
						outMsgid = MessageIdGenerator.generateCommonStringID();
						message.setOutMsgID(outMsgid);
					}

					Element outMsgIdElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					outMsgIdElmt.setText(outMsgid);

				} else if ("username".equalsIgnoreCase(pval)
						|| "password".equalsIgnoreCase(pval)) {
					// do nothing
				}else if("timestamp".equalsIgnoreCase(pval)){
					String timestamp=parseTimestamp(param, message);
					
					Element timestampElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					
					timestampElmt.setText(urlEncode(timestamp, urlEncoding));
					
				} else {
					String mval = HttpUtils.getParameter(param, message, cst);

					Element elseElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					elseElmt.setText(urlEncode(mval, urlEncoding));

				}
			}

			if (log.isDebugEnabled()) {
				log.debug("makeRequest:" + doc.asXML());
			}

			return doc.asXML();
		} catch (Exception e) {
			log.warn(message, e.getMessage());
		}
		return null;
	}

	private String generateResponse(HttpStatus hs, GmmsMessage msg,
			HttpPdu submitResp, A2PCustomerInfo cst) {
		
		Document doc = DocumentHelper.createDocument();

		try {
			Element rootElmt = doc.addElement("SMSResponse");
			List<HttpParam> respList = submitResp.getParamList();
			String respValue = null;
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
				respValue = respValue != null ? respValue : hp
						.getDefaultValue();
				if (log.isDebugEnabled()) {
					log.debug("hp.getParam()={};value={};respValue={}",
							hp.getParam(), value, respValue);
				}
				Element hpElmt = rootElmt.addElement(hp.getOppsiteParam());
				hpElmt.setText(respValue);
				respValue = null;
			}

			return doc.asXML();
		} catch (Exception e) {
			log.warn(msg, e.getMessage());
		}
		return null;
	}

	// kevin add for REST

	public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest request)
			throws ServletException, IOException {

		try {
			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;

			String mtype = null;
			String messageContent = null;
			String udh = null;

			String requestBody = this.getRequestBody(request);
			if (requestBody == null || "".equals(requestBody.trim())) {
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			}

			String xmlContent = requestBody;
			Document doc = DocumentHelper.parseText(xmlContent);
			Element rootElement = doc.getRootElement();

			String outUsernameParName = hi.getUsername();

			String outPasswordParName = hi.getPassword();// ������Χpassword

			Element SMSRequestElt = rootElement.element("SMSMessage");

			String recipientAddress = null;
			String inUsernameElt = null;
			String inPasswordElt = null;
			for (HttpParam hpa : hi.getMoSubmitRequest().getParamList()) {
				if ("recipientAddress".equalsIgnoreCase(hpa.getParam())) {
					recipientAddress = SMSRequestElt.element(
							hpa.getOppsiteParam()).getTextTrim();
					msg.setRecipientAddress(recipientAddress);
				}
				if ("username".equalsIgnoreCase(hpa.getParam())) {
					inUsernameElt = hpa.getOppsiteParam();
				}
				if ("password".equalsIgnoreCase(hpa.getParam())) {
					inPasswordElt = hpa.getOppsiteParam();
				}
			}

			if (log.isDebugEnabled()) {
				log.debug("recipientAddress={}", recipientAddress);
			}

			// add
			if (recipientAddress == null) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			}
			if ((outUsernameParName == null || outUsernameParName.trim()
					.length() < 1)
					&& (inUsernameElt == null || inUsernameElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
			}
			if ((outPasswordParName == null || outPasswordParName.trim()
					.length() < 1)
					&& (inPasswordElt == null || inPasswordElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
			}

			String username = null;
			String password = null;
			if ((outUsernameParName != null && outUsernameParName.trim()
					.length() > 1)
					&& (outPasswordParName != null && outPasswordParName.trim()
							.length() > 1)) {
				Element usernameElt = rootElement.element(outUsernameParName);
				if (usernameElt != null) {
					username = usernameElt.getTextTrim();
				}
				Element passwordElt = rootElement.element(outPasswordParName);
				if (passwordElt != null) {
					password = passwordElt
							.getTextTrim();
				}
			} else if ((inUsernameElt != null && inUsernameElt.trim().length() > 1)
					&& (inPasswordElt != null && inPasswordElt.trim().length() > 1)) {
				Element usernameElt= rootElement.element(inUsernameElt);
				if(usernameElt!=null)
				{
					username =usernameElt.getTextTrim();
				}
				Element passwordElt=rootElement.element(inPasswordElt);
				if(passwordElt!=null)
				{
					password =passwordElt.getTextTrim();
				}
				
			}

			if (username == null || username.trim().length() < 1
					|| password == null || password.trim().length() < 1) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			}
			// add
			csts = gmmsUtility.getCustomerManager().getCustomerBySpID(username);
			sInfo = (A2PSingleConnectionInfo) csts;
			if (sInfo == null) {

				log.debug("customerInfo == {} by serverid = {}", sInfo,
						username);

				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
			} else {
				String md5 = username + ":" + sInfo.getAuthKey() + ":"
						+ recipientAddress;
				String md5Pwd = HttpUtils.md5Encrypt(md5);

				if (log.isDebugEnabled()) {
					log.debug("md5 src String == {} and encry md5 string {}",
							md5, md5Pwd);
				}

				if (!password.equalsIgnoreCase(md5Pwd)) {
					msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
				}
			}
			try {
				if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.SERVER_ERROR);
				}
			} catch (Exception e) {
				log.warn(
						"Error occur when processing throttling control in CommonMessageHttpHandler.parseRequest",
						e);
			}

			for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
				String param = hp.getParam();
				if ("username".equalsIgnoreCase(param)
						|| "password".equalsIgnoreCase(param)||"recipientAddress".equalsIgnoreCase(param)) {
					continue;
				}
				String requestPar = hp.getOppsiteParam();
				Element hpElt = SMSRequestElt.element(requestPar);

				if (log.isDebugEnabled()) {
					log.debug("test:hpELt:" + requestPar);
				}

				if (hpElt == null) {
					continue;
				}

				String requestValue = hpElt.getTextTrim();

				String value = requestValue != null ? requestValue : hp
						.getDefaultValue();

				if (log.isDebugEnabled()) {
					log.debug(
							"param name={};OppsiteParam={}; requestvalue = {}; value={}",
							hp.getParam(), hp.getOppsiteParam(), requestValue,
							value);
				}

				if ("deliveryReport".equalsIgnoreCase(param)) {
					if ("1".equalsIgnoreCase(value)) {
						msg.setProperty(param, true);
					} else {
						msg.setProperty(param, false);
					}
				} else if ("expiryDate".equalsIgnoreCase(param)) {
					Date expireDate = parseHttpExpiryDate(csts, hp, value);
					if (expireDate != null) {
						expireDate = gmmsUtility.getGMTTime(expireDate);
						msg.setProperty(param, expireDate);
					}
				} else if ("textContent".equalsIgnoreCase(param)) {// textContent
					messageContent = value;
				} else if ("udh".equalsIgnoreCase(param)) {// udh
					udh = value;
				} else if ("contentType".equalsIgnoreCase(param)) {// contentType
					mtype = value;
				} else if ("scheduleDeliveryTime".equalsIgnoreCase(param)) {
					try {
						Date scheduleDate = parseHttpScheduleDeliveryTime(csts, hp, value);
						if (scheduleDate != null) {
							scheduleDate = gmmsUtility.getGMTTime(scheduleDate);
							msg.setProperty(param, scheduleDate);
						}
					} catch (Exception e) {
						log.warn(msg, "Invalid scheduleDeliveryTime {}", value);
						msg.setStatus(GmmsStatus.INVALID_SCHEDULED_TIME);
						return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_SCHEDULED_TIME);
					}
					
				} else if ("username".equalsIgnoreCase(param)
						|| "password".equalsIgnoreCase(param)) {
					// do nothing
				} else {
					HttpUtils.setParameter(hp, msg, csts, requestValue);
				}
			}
			String gmmsCharset = hi.mapHttpCharset2GmmsCharset(mtype);
			msg.setContentType(gmmsCharset);

			if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
				msg.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
			}

			if (messageContent == null || "".equalsIgnoreCase(messageContent)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			} else {
				this.parseHttpContent(msg, mtype, messageContent, udh);
			}
			if (msg.getRecipientAddress() == null) {
				msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
			}

			msg.setOSsID(sInfo.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo
					.getSSID());
			msg.setMsgID(commonMsgID);

			return subSuccess;

		} catch (Exception e) {
			log.error("common exception!", e);
			return subFail;
		}

	}

	public String getRequestBody(HttpServletRequest request) {
		StringBuffer buffer = new StringBuffer();
		BufferedReader reader;
		try {
			reader = request.getReader();
			String str = reader.readLine();
			while (str != null) {
				buffer.append(str);
				str = reader.readLine();
			}
			return buffer.toString();
		} catch (IOException e) {

			log.warn(e.getMessage());
		}
		return null;

	}

	@Override
	public void parseResponse(GmmsMessage msg, String resp) {

		String respStr = resp.trim();

		if (log.isDebugEnabled()) {
			log.debug("CommonMessageRESTXmlHttpHandler parseResponse:"
					+ respStr);
		}

		String jobid = "";
		String msg_status_code = null;
		String reason_phrase = "";
		try {
			Document doc = DocumentHelper.parseText(respStr);

			Element rootElement = doc.getRootElement();

			List<HttpParam> parameters = hi.getMtSubmitResponse()
					.getParamList();
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String oval = param.getOppsiteParam();
				if (log.isDebugEnabled()) {
					log.debug("pva:" + pval + "oval:" + oval);
				}
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
			if (jobid != null && !jobid.trim().equals("")) {
				msg.setOutMsgID(jobid);
			}

			if (log.isInfoEnabled()) {
				log.info(msg, "received submit resp {}", resp);
			}
			HttpStatus status = new HttpStatus(msg_status_code, reason_phrase);
			GmmsStatus gstatus = hi.mapHttpSubStatus2GmmsStatus(status);
			msg.setStatus(gstatus);

			if (log.isDebugEnabled()) {
				log.debug("CommonMessageRESTXMLHandler msg:" + msg);
			}

		} catch (DocumentException e) {
			log.warn(msg, e.getMessage());
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}
	}
	
	
	public String makeResponse(HttpStatus hs, Set<GmmsMessage> msgSet,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msgSet, submitResp, cst);

	}
	private String generateResponse(HttpStatus hs, Set<GmmsMessage> msgSet,
			HttpPdu submitResp, A2PCustomerInfo cst) {

		Document doc = DocumentHelper.createDocument();

		try {
			Element rootElmt = doc.addElement("SMSResponseList");
			List<HttpParam> respList = submitResp.getParamList();
			String respValue = null;

			for (GmmsMessage msg : msgSet) {
				
				if (!(msg.getStatus().equals(GmmsStatus.UNASSIGNED))) {
					hs = hi.mapGmmsStatus2HttpSubStatus(msg.getStatus());
				}else
				{
					hs=hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SUCCESS);
				}
				
				Element subRootElmt = rootElmt.addElement("SMSResponse");
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
					respValue = respValue != null ? respValue : hp
							.getDefaultValue();
					if (log.isDebugEnabled()) {
						log.debug("hp.getParam()={};value={};respValue={}",
								hp.getParam(), value, respValue);
					}
					Element hpElmt = subRootElmt.addElement(hp
							.getOppsiteParam());
					hpElmt.setText(respValue);
					
					respValue = null;
				}
			}

			return doc.asXML();
		} catch (Exception e) {
			for (GmmsMessage msg : msgSet) {
				log.warn(msg, e.getMessage());
			}
		}
		return null;
	}

}
