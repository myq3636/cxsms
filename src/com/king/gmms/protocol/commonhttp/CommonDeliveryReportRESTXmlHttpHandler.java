package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.json.JSONObject;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class CommonDeliveryReportRESTXmlHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonDeliveryReportRESTXmlHttpHandler.class);

	public CommonDeliveryReportRESTXmlHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * Add by kevin for REST
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu drResp = hi.getMtDRResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msg, drResp, cst);

	}

	/**
	 * generate DR request data
	 */
	

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

			doc.setXMLEncoding(charset);

			Element rootElmt = doc.addElement("deliveryReportNotification");
			String mobile = message.getRecipientAddress();
			List<HttpParam> parameters = hi.getMoDRRequest().getParamList();

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
			Element subRootElmt = rootElmt.addElement("deliveryReport");

			for (HttpParam param : parameters) {
				String pval = param.getParam();
				if ("statusText".equalsIgnoreCase(pval)) {
					if (message.getStatusCode() == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
							.getCode()) {
						message.setStatusCode(HttpUtils
								.getCodeFromDRStatus(message.getStatusText()));
					}
					HttpStatus hs = hi.mapGmmsStatus2HttpDRStatus(message
							.getStatus());
					String del_status = hs.getText();

					Element statusTextElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					statusTextElmt.setText(urlEncode(del_status, urlEncoding));

				} else if ("statusCode".equalsIgnoreCase(pval)) {

					Element statusCodeElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					statusCodeElmt.setText(String.valueOf(message
							.getStatusCode()));

				} else if ("dateIn".equalsIgnoreCase(pval)) {
					String dateIn = parseHttpDateIn(param);

					Element dateInElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					dateInElmt.setText(urlEncode(dateIn, urlEncoding));

				} else if ("username".equalsIgnoreCase(pval)
						|| "password".equalsIgnoreCase(pval)) {
					// do nothing
				} else {
					String mval = HttpUtils.getParameter(param, message, cst);

					Element elseElmt = subRootElmt.addElement(param
							.getOppsiteParam());
					elseElmt.setText(urlEncode(mval, urlEncoding));
				}
			}

			return doc.asXML();
		} catch (Exception e) {
			log.warn(message, e.getMessage());
		}
		return null;
	}

	private String generateResponse(HttpStatus hs, GmmsMessage msg,
			HttpPdu submitResp, A2PCustomerInfo cst) {
		try {
			List<HttpParam> respList = submitResp.getParamList();
			Document doc = DocumentHelper.createDocument();
			Element rootElmt = doc.addElement("notificationResponse");
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

			String statusCode = null;
			String statusText = null;

			String requestBody = this.getRequestBody(request);
			if (requestBody == null || "".equals(requestBody.trim())) {
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			}

			String xmlContent = requestBody;
			Document doc = DocumentHelper.parseText(xmlContent);
			Element rootElement = doc.getRootElement();

			Element deliveryInfoElt = rootElement.element("deliveryReport");
			String recipientAddress = null;
			String inUsernameElt = null;
			String inPasswordElt = null;

			for (HttpParam hpa : hi.getMtDRRequest().getParamList()) {
				if ("recipientAddress".equalsIgnoreCase(hpa.getParam())) {
					recipientAddress = deliveryInfoElt.element(
							hpa.getOppsiteParam()).getTextTrim();
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

			String outUsernameParName = hi.getUsername();

			String outPasswordParName = hi.getPassword();

			// add
			if (recipientAddress == null) {
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}
			if ((outUsernameParName == null || outUsernameParName.trim()
					.length() < 1)
					&& (inUsernameElt == null || inUsernameElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}
			if ((outPasswordParName == null || outPasswordParName.trim()
					.length() < 1)
					&& (inPasswordElt == null || inPasswordElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}

			String username = null;
			String password = null;
			if ((outUsernameParName != null && outUsernameParName.trim()
					.length() > 1)
					&& (outPasswordParName != null && outPasswordParName.trim()
							.length() > 1)) {
				
				Element usernameElt= rootElement.element(outUsernameParName);
				if(usernameElt!=null)
				{
					username=usernameElt.getTextTrim();
				}
				Element passwordElt= rootElement.element(outPasswordParName);
				if(passwordElt!=null)
				{
					password=passwordElt.getTextTrim();
				}

			} else if ((inUsernameElt != null && inUsernameElt.trim().length() > 1)
					&& (inPasswordElt != null && inPasswordElt.trim().length() > 1)) {
				
				Element usernameElt=rootElement.element(inUsernameElt);
				if(usernameElt!=null)
				{
					username=usernameElt.getTextTrim();
				}
				Element passwordElt=rootElement.element(inPasswordElt);
				if(passwordElt!=null)
				{
					password = passwordElt.getTextTrim();
				}
				
			}

			if (username == null || username.trim().length() < 1
					|| password == null || password.trim().length() < 1) {
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}
			// add

			csts = gmmsUtility.getCustomerManager().getCustomerBySpID(username);
			sInfo = (A2PSingleConnectionInfo) csts;
			if (sInfo == null) {

				log.debug("customerInfo == {} by serverid = {}", sInfo,
						username);

				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			} else if (sInfo != null) {
				String md5 = username + ":" + sInfo.getAuthKey() + ":"
						+ recipientAddress;
				String md5Pwd = HttpUtils.md5Encrypt(md5);

				log.debug("md5 src String == {} and encry md5 string {}", md5,
						md5Pwd);

				if (!password.equalsIgnoreCase(md5Pwd)) {
					msg.setStatus(GmmsStatus.UNKNOWN);
					return drFail;
				} else {
					msg.setRSsID(sInfo.getSSID());
				}
			}
			// throttling control process
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
				return drFail;
			}

			for (HttpParam hp : hi.getMtDRRequest().getParamList()) {

				String param = hp.getParam();

				if ("username".equalsIgnoreCase(param)
						|| "password".equalsIgnoreCase(param)) {
					continue;
				}

				String requestPar = hp.getOppsiteParam();
				Element hpElt = deliveryInfoElt.element(requestPar);

				if (log.isDebugEnabled()) {
					log.debug("test:hpELt:" + requestPar);
				}
				
				if (hpElt == null) {
					continue;
				}

				String requestValue = hpElt.getTextTrim();
				String value = requestValue != null ? requestValue : hp
						.getDefaultValue();

				log.debug(
						"param name={};OppsiteParam={}; requestvalue = {}; value={}",
						hp.getParam(), hp.getOppsiteParam(), requestValue,
						value);

				if ("statusText".equalsIgnoreCase(param)) {
					statusText = value;
				} else if ("statusCode".equalsIgnoreCase(param)) {
					statusCode = value;
				} else if ("username".equalsIgnoreCase(param)
						|| "password".equalsIgnoreCase(param)) {
					// do nothing
				} else {
					msg.setProperty(hp.getParam(), value);
				}
			}
			GmmsStatus gs = processStatus(statusCode, statusText);
			msg.setStatus(gs);
			if (log.isDebugEnabled()) {
				log.debug("outmsgid:{}", msg.getOutMsgID());
			}
			if (msg.getOutMsgID() == null) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return drFail;
			}
			// use msgId to swap between modules
			msg.setMsgID(msg.getOutMsgID());

			// log.debug("CommonDeliveryReportRESTXmlHttpHandler:"+msg.toString());

			return drSuccess;

		} catch (Exception e) {
			log.error("common exception!", e);
			return drFail;
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

	/**
	 * deal with Common Http status
	 * 
	 * @param status
	 * @return
	 * @throws Exception
	 */
	private GmmsStatus processStatus(String statusCode, String statusText)
			throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("DR statuscode={},statusText={}", statusCode, statusText);
		}
		GmmsStatus gs = GmmsStatus.UNKNOWN;
		try {
			// HttpStatus hs = CommonHttpStatus.getDRStatusByCode(code);
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

	@Override
	public void parseResponse(GmmsMessage message, String resp) {
		
		try {
			String respStr = resp.trim();
			String jobid = "";
			int msg_status_code = -1;
			String reason_phrase = "";
			Document doc = DocumentHelper.parseText(respStr);
			Element rootElement = doc.getRootElement();
//			Element notificationRepElt = rootElement
//					.element("notificationResponse");
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
				log.info(message,
						"Receive DR response,outmsgId {}, statuscode {}",
						jobid, msg_status_code);
			}
			
			HttpStatus hs=new HttpStatus(String.valueOf(msg_status_code),reason_phrase);
			
			GmmsStatus gsStatus = hi.mapHttpDRRespStatus2GmmsStatus(hs);
			// success delivery ,don't update message status ,un success delivery ,update message status
			if (gsStatus.getCode() != gsStatus.DELIVERED.getCode()) {
				message.setStatusCode(gsStatus
						.getCode());
			}

		} catch (DocumentException e) {
			e.printStackTrace();
			message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}

	}

}
