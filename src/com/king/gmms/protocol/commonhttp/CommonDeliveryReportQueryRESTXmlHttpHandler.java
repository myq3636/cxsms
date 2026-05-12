package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

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

public class CommonDeliveryReportQueryRESTXmlHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonDeliveryReportQueryRESTXmlHttpHandler.class);

	public CommonDeliveryReportQueryRESTXmlHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * Add by kevin for REST
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu drQueryResp = hi.getMoDRResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msg, drQueryResp, cst);
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
			List<HttpParam> parameters = hi.getMtDRRequest().getParamList();

			String mobile = message.getRecipientAddress();

			StringBuffer postData = new StringBuffer();
			String outUserNameElt = hi.getUsername();
			String outPasswordElt = hi.getPassword();
			String inUserNameElt = null;
			String inPasswordElt = null;
			String inUserNameValue = null;
			String inPasswordValue = null;

			for (HttpParam param : parameters) {
				String pval = param.getParam();
				if ("username".equalsIgnoreCase(pval)) {
					inUserNameElt = param.getOppsiteParam();
					inUserNameValue = HttpUtils.getParameter(param, message,
							cst);
				}
				if ("password".equalsIgnoreCase(pval)) {
					inPasswordElt = param.getOppsiteParam();
					inPasswordValue = HttpUtils.getParameter(param, message,
							cst);
				}
			}

			if ((outUserNameElt != null) && (outPasswordElt != null)) {
				String pwd = HttpUtils.md5Encrypt(sInfo.getChlAcctName() + ":"
						+ sInfo.getChlPassword() + ":" + mobile);
				postData.append(outUserNameElt).append("=")
						.append(urlEncode(sInfo.getChlAcctName(), charset));
				postData.append("&").append(outPasswordElt).append("=")
						.append(urlEncode(pwd, charset));
			} else if (outPasswordElt == null && outUserNameElt != null) {

				postData.append(outUserNameElt).append("=")
						.append(urlEncode(sInfo.getChlAcctName(), charset));

			} else if (outPasswordElt != null && outUserNameElt == null) {
				String encryptMethod = cst.getPasswdEncryptMethod();
				if (encryptMethod != null) {
					String encryptPass = HttpUtils.encrypt(
							sInfo.getChlPassword(), encryptMethod);
					postData.append(outPasswordElt).append("=")
							.append(urlEncode(encryptPass, charset));
				} else {
					postData.append(outPasswordElt).append("=")
							.append(urlEncode(sInfo.getChlPassword(), charset));
				}
			} else {
				if (inUserNameElt != null && inPasswordElt != null) {
					String pwd = HttpUtils.md5Encrypt(inUserNameValue + ":"
							+ inPasswordValue + ":" + mobile);
					postData.append(inUserNameElt).append("=")
							.append(urlEncode(inUserNameValue, charset));

					postData.append("&").append(inPasswordElt).append("=")
							.append(urlEncode(pwd, charset));

				} else if (inPasswordElt == null && inUserNameElt != null) {

					postData.append(inUserNameElt).append("=")
							.append(urlEncode(inUserNameValue, charset));

				} else if (inPasswordElt != null && inUserNameElt == null) {
					String encryptMethod = cst.getPasswdEncryptMethod();
					if (encryptMethod != null) {
						String encryptPass = HttpUtils.encrypt(inPasswordValue,
								encryptMethod);
						postData.append(inPasswordElt).append("=")
								.append(urlEncode(encryptPass, charset));

					} else {
						postData.append(inPasswordElt).append("=")
								.append(urlEncode(inPasswordElt, charset));
					}
				}
			}

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
					postData.append("&").append(param.getOppsiteParam())
							.append("=")
							.append(urlEncode(del_status, urlEncoding));
				} else if ("statusCode".equalsIgnoreCase(pval)) {
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(message.getStatusCode());
				} else if ("dateIn".equalsIgnoreCase(pval)) {
					String dateIn = parseHttpDateIn(param);
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(urlEncode(dateIn, urlEncoding));
				} else if ("username".equalsIgnoreCase(pval)) {
					// do nothing
				} else if ("password".equalsIgnoreCase(pval)) {
					// do nothing
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
		} catch (Exception e) {
			log.warn(message, e.getMessage());
		}
		return null;
	}

	private String generateResponse(HttpStatus hs, GmmsMessage msg,
			HttpPdu drQueryResp, A2PCustomerInfo cst) {

		try {
			hs = hi.mapGmmsStatus2HttpDRStatus(msg.getStatus());

			List<HttpParam> respList = drQueryResp.getParamList();
			Document doc = DocumentHelper.createDocument();
			Element rootElmt = doc.addElement("deliveryReport");

			String respValue = null;
			for (HttpParam hp : respList) {
				String param = hp.getParam();
				Object value = msg.getProperty(hp.getParam());
				if ("StatusCode".equalsIgnoreCase(param)) {

					respValue = "" + hs.getCode();

				} else if ("StatusText".equalsIgnoreCase(param)) {

					String del_status = hs.getText();
					respValue = del_status;

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

			log.debug("hi.getUsername():" + hi.getUsername());
			String outUsernameElt = hi.getUsername();

			String outPasswordElt = hi.getPassword();

			String recipientAddress = null;
			String inUsernameElt = null;
			String inPasswordElt = null;
			for (HttpParam hpa : hi.getMoDRRequest().getParamList()) {

				if ("recipientAddress".equalsIgnoreCase(hpa.getParam())) {

					recipientAddress = request.getParameter(hpa
							.getOppsiteParam());
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
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}
			if ((outUsernameElt == null || outUsernameElt.trim().length() < 1)
					&& (inUsernameElt == null || inUsernameElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}
			if ((outPasswordElt == null || outPasswordElt.trim().length() < 1)
					&& (inPasswordElt == null || inPasswordElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}

			String username = null;
			String password = null;

			if ((outUsernameElt != null && outUsernameElt.trim().length() > 1)
					&& (outPasswordElt != null && outPasswordElt.trim()
							.length() > 1)) {
				username = request.getParameter(outUsernameElt);
				password = request.getParameter(outPasswordElt);
			} else if ((inUsernameElt != null && inUsernameElt.trim().length() > 1)
					&& (inPasswordElt != null && inPasswordElt.trim().length() > 1)) {
				username = request.getParameter(inUsernameElt);
				password = request.getParameter(inPasswordElt);
			}

			if (log.isDebugEnabled()) {
				log.debug("username={}", username);
			}
			if (log.isDebugEnabled()) {
				log.debug("password={}", password);
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
			}

			if (sInfo.getInClientPull() != 2) {
				log.debug("customerInfo == {} by serverid = {}", sInfo,
						username);
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}

			String md5 = username + ":" + sInfo.getAuthKey() + ":"
					+ recipientAddress;
			String md5Pwd = HttpUtils.md5Encrypt(md5);

			log.debug("md5 src String == {} and encry md5 string {}", md5,
					md5Pwd);

			if (!password.equalsIgnoreCase(md5Pwd)) {
				msg.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			} else {
				msg.setOSsID(sInfo.getSSID());
			}

			// throttling control process
			if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
				return drFail;
			}

			// String messageID = null;
			for (HttpParam hp : hi.getMoDRRequest().getParamList()) {

				if ("username".equalsIgnoreCase(hp.getParam())) {
					// do nothing
				} else if ("password".equalsIgnoreCase(hp.getParam())) {
					// do nothing
				} else {
					String value = request.getParameter(hp.getOppsiteParam()) != null ? request
							.getParameter(hp.getOppsiteParam()) : hp
							.getDefaultValue();

					log.debug(
							"param name={};OppsiteParam={}; requestvalue = {}; value={}",
							hp.getParam(), hp.getOppsiteParam(),
							request.getParameter(hp.getOppsiteParam()), value);

					msg.setProperty(hp.getParam(), value);
				}

			}
			
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo
					.getSSID());
			msg.setMsgID(commonMsgID);
			
			// inMsgID null
			// fixed

			if (msg.getInMsgID() == null
					|| "".equalsIgnoreCase(msg.getInMsgID().trim())) {
				return drFail;
			}

			return drSuccess;

		} catch (Exception e) {
			log.error("common exception!", e);
			return drFail;
		}

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

	}

}
