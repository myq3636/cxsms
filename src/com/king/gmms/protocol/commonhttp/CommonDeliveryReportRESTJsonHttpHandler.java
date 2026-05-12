package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.dom4j.Element;
import org.json.JSONException;
import org.json.JSONObject;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class CommonDeliveryReportRESTJsonHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonDeliveryReportRESTJsonHttpHandler.class);

	public CommonDeliveryReportRESTJsonHttpHandler(HttpInterface hie) {
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

	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		try {
			A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) cst;
			String charset = "utf8";
			charset = sInfo.getChlCharset();
			if (charset == null || "".equals(charset)) {
				charset = "utf8";
			}

			Map<String, Object> deliveryReportNotification = new HashMap<String, Object>();

			List<HttpParam> parameters = hi.getMoDRRequest().getParamList();

			String mobile = message.getRecipientAddress();

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
				deliveryReportNotification.put(outUserNameElt,
						urlEncode(sInfo.getChlAcctName(), charset));
				deliveryReportNotification.put(outPasswordElt,
						urlEncode(pwd, charset));
			} else if (outPasswordElt == null && outUserNameElt != null) {
				deliveryReportNotification.put(outUserNameElt,
						urlEncode(sInfo.getChlAcctName(), charset));
			} else if (outPasswordElt != null && outUserNameElt == null) {
				String encryptMethod = cst.getPasswdEncryptMethod();
				if (encryptMethod != null) {
					String encryptPass = HttpUtils.encrypt(
							sInfo.getChlPassword(), encryptMethod);
					deliveryReportNotification.put(outPasswordElt,
							urlEncode(encryptPass, charset));
				} else {
					deliveryReportNotification.put(outPasswordElt,
							urlEncode(sInfo.getChlPassword(), charset));
				}
			} else {
				if (inUserNameElt != null && inPasswordElt != null) {
					deliveryReportNotification.put(inUserNameElt,
							urlEncode(inUserNameValue, charset));

					String pwd = HttpUtils.md5Encrypt(inUserNameValue + ":"
							+ inPasswordValue + ":" + mobile);

					deliveryReportNotification.put(inPasswordElt,
							urlEncode(pwd, charset));

				} else if (inPasswordElt == null && inUserNameElt != null) {

					deliveryReportNotification.put(inUserNameElt,
							urlEncode(inUserNameValue, charset));

				} else if (inPasswordElt != null && inUserNameElt == null) {
					String encryptMethod = cst.getPasswdEncryptMethod();
					if (encryptMethod != null) {
						String encryptPass = HttpUtils.encrypt(inPasswordValue,
								encryptMethod);
						deliveryReportNotification.put(inPasswordElt,
								urlEncode(encryptPass, charset));

					} else {
						deliveryReportNotification.put(inPasswordElt,
								urlEncode(inPasswordValue, charset));
					}
				}
			}

			JSONObject subRoot = new JSONObject();

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

					subRoot.put(param.getOppsiteParam(),
							urlEncode(del_status, urlEncoding));

				} else if ("statusCode".equalsIgnoreCase(pval)) {

					subRoot.put(param.getOppsiteParam(),
							message.getStatusCode());
				} else if ("dateIn".equalsIgnoreCase(pval)) {
					String dateIn = parseHttpDateIn(param);

					subRoot.put(param.getOppsiteParam(),
							urlEncode(dateIn, urlEncoding));
				} else if ("username".equalsIgnoreCase(pval)) {
					// do nothing
				} else if ("password".equalsIgnoreCase(pval)) {
					// do nothing
				} else {
					String mval = HttpUtils.getParameter(param, message, cst);

					subRoot.put(param.getOppsiteParam(),
							urlEncode(mval, urlEncoding));
				}
			}

			deliveryReportNotification.put("deliveryReport", subRoot);

			JSONObject rootObj = new JSONObject();
			rootObj.put("deliveryReportNotification",
					deliveryReportNotification);

			return rootObj.toString();
		} catch (JSONException e) {

			e.printStackTrace();
		}
		return null;

	}

	private String generateResponse(HttpStatus hs, GmmsMessage msg,
			HttpPdu submitResp, A2PCustomerInfo cst) {
		try {
			List<HttpParam> respList = submitResp.getParamList();
			JSONObject jsonObj = new JSONObject();
			Map<String, String> notificationReponse = new HashMap<String, String>();
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
				respValue = respValue != null ? respValue : hp
						.getDefaultValue();
				if (log.isDebugEnabled()) {
					log.debug("hp.getParam()={};value={};respValue={}",
							hp.getParam(), value, respValue);
				}
				notificationReponse.put(hp.getOppsiteParam(), respValue);
				respValue = "";
			}

			jsonObj.put("notificationResponse", notificationReponse);
			return jsonObj.toString();
		} catch (JSONException e) {
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

			String jsonContent = requestBody;
			JSONObject jsonObject = new JSONObject(jsonContent);
			JSONObject deliveryReportNotificationObj = jsonObject
					.getJSONObject("deliveryReportNotification");
			JSONObject deliveryInfoObj = deliveryReportNotificationObj
					.getJSONObject("deliveryReport");

			String outUsernameElt = null;
			// outUsernameElt =
			// deliveryReportNotificationObj.getString(hi.getUsername());
			outUsernameElt = hi.getUsername();

			String recipientAddress = null;
			String inUsernameElt = null;
			String inPasswordElt = null;
			for (HttpParam hpa : hi.getMtDRRequest().getParamList()) {
				if ("recipientAddress".equalsIgnoreCase(hpa.getParam())) {
					recipientAddress = deliveryInfoObj.getString(hpa
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

			String outPasswordElt = hi.getPassword();

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
				username = deliveryReportNotificationObj
						.getString(outUsernameElt);
				password = deliveryReportNotificationObj
						.getString(outPasswordElt);
			} else if ((inUsernameElt != null && inUsernameElt.trim().length() > 1)
					&& (inPasswordElt != null && inPasswordElt.trim().length() > 1)) {
				username = deliveryReportNotificationObj
						.getString(inUsernameElt);
				password = deliveryReportNotificationObj
						.getString(inPasswordElt);
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
				String requestValue = deliveryInfoObj.getString(hp
						.getOppsiteParam());

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
			log.debug("outmsgid:{}", msg.getOutMsgID());
			if (msg.getOutMsgID() == null) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return drFail;
			}
			// use msgId to swap between modules
			msg.setMsgID(msg.getOutMsgID());

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

			e.printStackTrace();
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
			JSONObject jsonObject = new JSONObject(respStr);
			JSONObject deliveryReportNotificationObj = jsonObject
					.getJSONObject("notificationResponse");
			List<HttpParam> parameters = hi.getMoDRResponse()
					.getParamList();
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String oval = param.getOppsiteParam();
				String value = deliveryReportNotificationObj.getString(oval);
				if ("inMsgID".equalsIgnoreCase(pval)) {
					jobid = value;
				} else if ("StatusCode".equalsIgnoreCase(pval)) {
					msg_status_code = Integer.parseInt(value);
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					reason_phrase = value;
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

		} catch (JSONException e) {
			e.printStackTrace();
			message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}

	}

}
