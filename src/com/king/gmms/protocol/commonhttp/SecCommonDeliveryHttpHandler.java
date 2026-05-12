package com.king.gmms.protocol.commonhttp;

import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;

public class SecCommonDeliveryHttpHandler extends
		CommonDeliveryReportHttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(SecCommonDeliveryHttpHandler.class);

	public SecCommonDeliveryHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * parse DR request
	 */
	// public HttpStatus parseRequest(GmmsMessage message,
	// HttpServletRequest request) {
	// try {
	// String hiUsername = hi.getUsername();
	// int rssid = -1;
	// String username = null;
	//
	// A2PCustomerInfo csts = null;
	// A2PSingleConnectionInfo sInfo = null;
	// if (hiUsername == null) {
	// String protocol = hi.getInterfaceName();
	// if (protocol != null && protocol.trim().length() > 0) {
	// ArrayList<Integer> alSsid = gmmsUtility
	// .getCustomerManager().getSsidByProtocol(protocol);
	// if (alSsid == null || alSsid.size() < 1) {
	// log.warn(message, "getSsid by interfaceName {} failed" , protocol);
	// return drFail;
	// }
	// rssid = alSsid.get(0);
	// message.setRSsID(rssid);
	// csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
	// rssid);
	// sInfo = (A2PSingleConnectionInfo) csts;
	// }
	// } else {
	// username = request.getParameter(hi.getUsername());
	//
	// if(log.isDebugEnabled()){
	// log.debug("username={}", username);
	// }
	// String recipientAddress = null;
	// for (HttpParam hpa : hi.getMoSubmitRequest().getParamList()) {
	// if ("recipientAddress".equalsIgnoreCase(hpa.getParam())) {
	// recipientAddress = request.getParameter(hpa
	// .getOppsiteParam());
	// }
	// }
	//
	// log.debug("recipientAddress={}", recipientAddress);
	//
	// if (username == null || username.trim().length() < 1
	// || recipientAddress == null) {
	// message.setStatus(GmmsStatus.UNKNOWN);
	// return drFail;
	// }
	// String password = request.getParameter(hi.getPassword());
	//
	// log.debug("password={}", password);
	//
	// if (password == null || password.trim().length() < 1) {
	// message.setStatus(GmmsStatus.UNKNOWN);
	// return drFail;
	// }
	//
	// csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
	// username);
	// sInfo = (A2PSingleConnectionInfo) csts;
	// if (sInfo == null) {
	//
	// log.debug("customerInfo == {} by serverid = {}", sInfo,
	// username);
	//
	// message.setStatus(GmmsStatus.UNKNOWN);
	// return drFail;
	// } else if (sInfo != null) {
	// String md5 = username + ":" + sInfo.getAuthKey() + ":"
	// + recipientAddress;
	// String md5Pwd = HttpUtils.md5Encrypt(md5);
	//
	// log.debug("md5 src String == {} and encry md5 string {}",
	// md5, md5Pwd);
	//
	// if (!password.equalsIgnoreCase(md5Pwd)) {
	// message.setStatus(GmmsStatus.UNKNOWN);
	// return drFail;
	// } else {
	// message.setRSsID(sInfo.getSSID());
	// }
	// }
	// }
	//
	// // throttling control process
	// if (!super.checkIncomingThrottlingControl(csts.getSSID(), message)) {
	// return drFail;
	// }
	//
	// String statusCode = null;
	// String statusText = null;
	// for (HttpParam hp : hi.getMtDRRequest().getParamList()) {
	// String param = hp.getParam();
	// String value = request.getParameter(hp.getOppsiteParam()) != null ?
	// request
	// .getParameter(hp.getOppsiteParam())
	// : hp.getDefaultValue();
	//
	// log
	// .debug(
	// "param name={};OppsiteParam={}; requestvalue = {}; value={}",
	// hp.getParam(), hp.getOppsiteParam(), request
	// .getParameter(hp.getOppsiteParam()),
	// value);
	//
	// if ("statusText".equalsIgnoreCase(param)) {
	// statusText = value;
	// } else if ("statusCode".equalsIgnoreCase(param)) {
	// statusCode = value;
	// } else {
	// message.setProperty(hp.getParam(), value);
	// }
	// }
	// GmmsStatus gs = processStatus(statusCode, statusText);
	// message.setStatus(gs);
	// log.debug("outmsgid:{}", message.getOutMsgID());
	// if (message.getOutMsgID() == null) {
	// message
	// .setOutMsgID(MessageIdGenerator
	// .generateCommonStringID());
	// }
	// // use msgId to swap between modules
	// message.setMsgID(message.getOutMsgID());
	//
	// return drSuccess;
	// } catch (Exception e) {
	// log.error(e, e);
	// return drFail;
	// }
	// }

	public HttpStatus parseRequest(GmmsMessage message,
			HttpServletRequest request) {
		try {

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;

			String outUsernameElt = hi.getUsername();
			String outPasswordElt = hi.getPassword();

			String recipientAddress = null;
			String inUsernameElt = null;
			String inPasswordElt = null;

			for (HttpParam hpa : hi.getMtDRRequest().getParamList()) {
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

			log.debug("recipientAddress={}", recipientAddress);

			if (recipientAddress == null) {
				message.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}
			if ((outUsernameElt == null || outUsernameElt.trim().length() < 1)
					&& (inUsernameElt == null || inUsernameElt.trim().length() < 1)) {
				message.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}
			if ((outPasswordElt == null || outPasswordElt.trim().length() < 1)
					&& (inPasswordElt == null || inPasswordElt.trim().length() < 1)) {
				message.setStatus(GmmsStatus.UNKNOWN);
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
				message.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			}

			csts = gmmsUtility.getCustomerManager().getCustomerBySpID(username);
			sInfo = (A2PSingleConnectionInfo) csts;
			if (sInfo == null) {

				log.debug("customerInfo == {} by serverid = {}", sInfo,
						username);

				message.setStatus(GmmsStatus.UNKNOWN);
				return drFail;
			} else if (sInfo != null) {
				String md5 = username + ":" + sInfo.getAuthKey() + ":"
						+ recipientAddress;
				String md5Pwd = HttpUtils.md5Encrypt(md5);

				log.debug("md5 src String == {} and encry md5 string {}", md5,
						md5Pwd);

				if (!password.equalsIgnoreCase(md5Pwd)) {
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

				if ("username".equalsIgnoreCase(param)
						|| "password".equalsIgnoreCase(param)) {
					continue;
				}

				String value = request.getParameter(hp.getOppsiteParam()) != null ? request
						.getParameter(hp.getOppsiteParam()) : hp
						.getDefaultValue();

				log.debug(
						"param name={};OppsiteParam={}; requestvalue = {}; value={}",
						hp.getParam(), hp.getOppsiteParam(),
						request.getParameter(hp.getOppsiteParam()), value);

				if ("statusText".equalsIgnoreCase(param)) {
					statusText = value;
				} else if ("statusCode".equalsIgnoreCase(param)) {
					statusCode = value;
				} else if ("username".equalsIgnoreCase(param)) {
					// do nothing
				} else if ("password".equalsIgnoreCase(param)) {
					// do nothing
				} else {
					message.setProperty(hp.getParam(), value);
				}
			}
			GmmsStatus gs = processStatus(statusCode, statusText);
			message.setStatus(gs);
			log.debug("outmsgid:{}", message.getOutMsgID());
			if (message.getOutMsgID() == null) {
				message.setOutMsgID(MessageIdGenerator.generateCommonStringID());
			}
			// use msgId to swap between modules
			message.setMsgID(message.getOutMsgID());

			return drSuccess;
		} catch (Exception e) {
			log.error(e, e);
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
	
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {

		try {
			A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) cst;
			String charset = "utf8";
			charset = sInfo.getChlCharset();
			if (charset == null || "".equals(charset)) {
				charset = "utf8";
			}

			List<HttpParam> parameters = hi.getMoDRRequest().getParamList();

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
	
	
	/**
	 * parse DR response
	 */
	public void parseResponse(GmmsMessage message, String resp) {
		String respStr = resp.trim();
		String jobid = "";
		int msg_status_code = -1;
		String reason_phrase = "";
		String[] arr_r = respStr.split("&");
		HashMap<String, String> respmap = new HashMap<String, String>();
		for (String str : arr_r) {
			String[] arr_v = str.split("=");
			if (arr_v.length != 2) {
				log.error("Invlid response format!");
				message.setStatus(GmmsStatus.UNKNOWN);
				return;
			}
			respmap.put(arr_v[0], arr_v[1]);
		}
		List<HttpParam> parameters = hi.getMoDRResponse().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			String oval = param.getOppsiteParam();
			if ("inMsgID".equalsIgnoreCase(pval)) {
				jobid = respmap.get(oval);
			} else if ("StatusCode".equalsIgnoreCase(pval)) {
				try {
					msg_status_code = Integer.parseInt(respmap.get(oval));
				} catch (Exception e) {
					log.error("Invlid response format:{}", oval);
					message.setStatus(GmmsStatus.UNKNOWN);
					return;
				}

			} else if ("StatusText".equalsIgnoreCase(pval)) {
				reason_phrase = respmap.get(oval);
			}
		}
		
		HttpStatus hs=new HttpStatus(String.valueOf(msg_status_code),reason_phrase);
		GmmsStatus gsStatus = hi.mapHttpDRRespStatus2GmmsStatus(hs);
		// success delivery ,don't update message status ,un success delivery ,update message status
		if (gsStatus.getCode() != gsStatus.DELIVERED.getCode()) {
			message.setStatusCode(gsStatus
					.getCode());
		}
		
		if(log.isInfoEnabled()){
			log.info(message, "Receive DR response,JobID: {} , statuscode : {} " ,
				jobid,msg_status_code);
		}
	}
}
