package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.Set;

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

public class SecCommonMessageHttpHandler extends CommonMessageHttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(SecCommonMessageHttpHandler.class);

	public SecCommonMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	// public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest
	// request)
	// throws ServletException, IOException {
	// try {
	// String hiUsername = hi.getUsername();
	// int rssid = -1;
	// String username = null;
	//
	// A2PCustomerInfo csts = null;
	// A2PSingleConnectionInfo sInfo = null;
	//
	// if (hiUsername == null) {
	// String protocol = hi.getInterfaceName();
	// if (protocol != null && protocol.trim().length() > 0) {
	// ArrayList<Integer> alSsid = gmmsUtility
	// .getCustomerManager().getSsidByProtocol(protocol);
	//
	// if (alSsid == null || alSsid.size() < 1) {
	// log.warn(msg, "getSsid by interfaceName {}  failed" , protocol);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
	// }
	// rssid = alSsid.get(0);
	//
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
	// msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// }
	// String password = request.getParameter(hi.getPassword());
	//
	// log.debug("password={}", password);
	//
	// if (password == null || password.trim().length() < 1) {
	// msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// }
	//
	// csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
	// username);
	// sInfo = (A2PSingleConnectionInfo) csts;
	//
	// if (sInfo == null) {
	//
	// log.debug("customerInfo == {} by serverid = {}", sInfo,
	// username);
	//
	// msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
	// } else {
	// String md5 = username + ":" + sInfo.getAuthKey() + ":"
	// + recipientAddress;
	// String md5Pwd = HttpUtils.md5Encrypt(md5);
	//
	// log.debug("md5 src String == {} and encry md5 string {}",
	// md5, md5Pwd);
	//
	// if (!password.equalsIgnoreCase(md5Pwd)) {
	// msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
	// }
	// }
	// }
	//
	// // throttling control process
	// try {
	// if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
	// return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SERVER_ERROR);
	// }
	// } catch (Exception e) {
	// log
	// .warn(
	// "Error occur when processing throttling control in CommonMessageHttpHandler.parseRequest",
	// e);
	// }
	//
	// String mtype = null;
	// String messageContent = null;
	// String udh = null;
	// for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
	// String requestValue = request
	// .getParameter(hp.getOppsiteParam());
	// String value = requestValue != null ? requestValue : hp
	// .getDefaultValue();
	// String param = hp.getParam();
	//
	// log
	// .debug(
	// "param name={};OppsiteParam={}; requestvalue = {}; value={}",
	// hp.getParam(), hp.getOppsiteParam(),
	// requestValue, value);
	//
	// if ("deliveryReport".equalsIgnoreCase(param)) {
	// if ("1".equalsIgnoreCase(value)) {
	// msg.setProperty(param, true);
	// } else {
	// msg.setProperty(param, false);
	// }
	// } else if ("expiryDate".equalsIgnoreCase(param)) {
	// Date expireDate = parseHttpExpiryDate(csts, hp, value);
	// if (expireDate != null) {
	// expireDate = gmmsUtility.getGMTTime(expireDate);
	// }
	// msg.setProperty(param, expireDate);
	// } else if ("textContent".equalsIgnoreCase(param)) {// textContent
	// messageContent = value;
	// } else if ("udh".equalsIgnoreCase(param)) {// udh
	// udh = value;
	// } else if ("contentType".equalsIgnoreCase(param)) {// contentType
	// mtype = value;
	// } else {
	// HttpUtils.setParameter(hp, msg, csts, requestValue);
	// }
	// }
	//
	// String gmmsCharset = hi.mapHttpCharset2GmmsCharset(mtype);
	// msg.setContentType(gmmsCharset);
	//
	// if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
	// msg.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
	// }
	//
	// if (messageContent == null || "".equalsIgnoreCase(messageContent)) {
	// msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// } else {
	// this.parseHttpContent(msg, mtype, messageContent, udh);
	// }
	// if (msg.getRecipientAddress() == null) {
	// msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
	// }
	//
	// msg.setOSsID(sInfo.getSSID());
	// String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo
	// .getSSID());
	// msg.setMsgID(commonMsgID);
	//
	// return subSuccess;
	// } catch (UnsupportedEncodingException e) {
	// log.error(msg, " MessageType=" + msg.getMessageType(),e);
	// return subFail;
	// } catch (Exception e) {
	// log.error("common exception!", e);
	// return subFail;
	// }
	//
	// }

	public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest request)
			throws ServletException, IOException {
		try {

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;

			String outUsernameElt = hi.getUsername();
			String outPasswordElt = hi.getPassword();

			String recipientAddress = null;
			String inUsernameElt = null;
			String inPasswordElt = null;

			for (HttpParam hpa : hi.getMoSubmitRequest().getParamList()) {
				if ("recipientAddress".equalsIgnoreCase(hpa.getParam())) {
					recipientAddress = request.getParameter(hpa
							.getOppsiteParam());
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

			if (recipientAddress == null) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
			}
			if ((outUsernameElt == null || outUsernameElt.trim().length() < 1)
					&& (inUsernameElt == null || inUsernameElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
			}
			if ((outPasswordElt == null || outPasswordElt.trim().length() < 1)
					&& (inPasswordElt == null || inPasswordElt.trim().length() < 1)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
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
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			}

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

				log.debug("md5 src String == {} and encry md5 string {}", md5,
						md5Pwd);

				if (!password.equalsIgnoreCase(md5Pwd)) {
					msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
				}
			}

			// throttling control process
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

			String mtype = null;
			String messageContent = null;
			String udh = null;
			for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {

				String param = hp.getParam();

				if ("username".equalsIgnoreCase(param)
						|| "password".equalsIgnoreCase(param)||"recipientAddress".equalsIgnoreCase(param)) {
					continue;
				}

				String requestValue = request
						.getParameter(hp.getOppsiteParam());
				String value = requestValue != null ? requestValue : hp
						.getDefaultValue();

				log.debug(
						"param name={};OppsiteParam={}; requestvalue = {}; value={}",
						hp.getParam(), hp.getOppsiteParam(), requestValue,
						value);

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
				} else if ("username".equalsIgnoreCase(param)) {
					// do nothing
				} else if ("password".equalsIgnoreCase(param)) {
					// do nothing
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
		} catch (UnsupportedEncodingException e) {
			log.error(msg, " MessageType=" + msg.getMessageType(), e);
			return subFail;
		} catch (Exception e) {
			log.error("common exception!", e);
			return subFail;
		}

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

			List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
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
				postData.append("&").append(outUserNameElt).append("=")
						.append(urlEncode(sInfo.getChlAcctName(), charset));
				postData.append("&").append(outPasswordElt).append("=")
						.append(urlEncode(pwd, charset));
			} else if (outPasswordElt == null && outUserNameElt != null) {

				postData.append("&").append(outUserNameElt).append("=")
						.append(urlEncode(sInfo.getChlAcctName(), charset));

			} else if (outPasswordElt != null && outUserNameElt == null) {
				String encryptMethod = cst.getPasswdEncryptMethod();
				if (encryptMethod != null) {
					String encryptPass = HttpUtils.encrypt(
							sInfo.getChlPassword(), encryptMethod);
					postData.append("&").append(outPasswordElt).append("=")
							.append(urlEncode(encryptPass, charset));
				} else {
					postData.append("&").append(outPasswordElt).append("=")
							.append(urlEncode(sInfo.getChlPassword(), charset));
				}
			} else {
				if (inUserNameElt != null && inPasswordElt != null) {
					String pwd = HttpUtils.md5Encrypt(inUserNameValue + ":"
							+ inPasswordValue + ":" + mobile);
					postData.append("&").append(inUserNameElt).append("=")
							.append(urlEncode(inUserNameValue, charset));

					postData.append("&").append(inPasswordElt).append("=")
							.append(urlEncode(pwd, charset));

				} else if (inPasswordElt == null && inUserNameElt != null) {

					postData.append("&").append(inUserNameElt).append("=")
							.append(urlEncode(inUserNameValue, charset));

				} else if (inPasswordElt != null && inUserNameElt == null) {
					String encryptMethod = cst.getPasswdEncryptMethod();
					if (encryptMethod != null) {
						String encryptPass = HttpUtils.encrypt(inPasswordValue,
								encryptMethod);
						postData.append("&").append(inPasswordElt).append("=")
								.append(urlEncode(encryptPass, charset));

					} else {
						postData.append("&").append(inPasswordElt).append("=")
								.append(urlEncode(inPasswordElt, charset));
					}
				}
			}

			String sender = message.getSenderAddress();
			String recipient = message.getRecipientAddress();
			String[] address = new String[] { sender, recipient };
			boolean isUrlEncodingTwice = cst.isUrlEncodingTwice();
			String[] content = this.parseGmmsContent(message, urlEncoding,
					isUrlEncodingTwice);// content & udh

			for (HttpParam param : parameters) {
				String pval = param.getParam();
				if ("senderAddress".equalsIgnoreCase(pval)) {
					postData.append("&").append(param.getOppsiteParam())
							.append("=")
							.append(urlEncode(address[0], urlEncoding));// sender_id
				} else if ("recipientAddress".equalsIgnoreCase(pval)) {
					postData.append("&").append(param.getOppsiteParam())
							.append("=")
							.append(urlEncode(address[1], urlEncoding));// recipient
				} else if ("udh".equalsIgnoreCase(pval)) {// textContent
					String udh = content[1];
					if (udh != null && !"".equals(udh.trim())) {
						postData.append("&").append(param.getOppsiteParam())
								.append("=").append(udh);// udh
					}
				} else if ("textContent".equalsIgnoreCase(pval)) {
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(content[0]);// message
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
							.append("=")
							.append(urlEncode(expiryDate, urlEncoding));
				} else if ("contentType".equalsIgnoreCase(pval)) {
					HttpCharset httpCharset = hi
							.mapGmmsCharset2HttpCharset(message
									.getContentType());
					String mtype = httpCharset.getMessageType();
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(urlEncode(mtype, urlEncoding));
				} else if ("outMsgID".equalsIgnoreCase(pval)) {

					String outMsgid = message.getOutMsgID();
					if (outMsgid == null) {
						outMsgid = MessageIdGenerator.generateCommonStringID();
						message.setOutMsgID(outMsgid);
					}
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(outMsgid);// client_ref_id
				} else if ("username".equalsIgnoreCase(pval)) {
					// do nothing
				} else if ("password".equalsIgnoreCase(pval)) {
					// do nothing
				}else if("timestamp".equalsIgnoreCase(pval)){
					String timestamp=parseTimestamp(param, message);
					postData.append("&").append(param.getOppsiteParam())
					.append("=").append(urlEncode(timestamp, urlEncoding));
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
	
	
	//
	
	public String makeResponse(HttpStatus hs, Set<GmmsMessage> msgSet,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msgSet, submitResp, cst);

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
	private String generateResponse(HttpStatus hs, Set<GmmsMessage> msgSet,
			HttpPdu submitResp, A2PCustomerInfo cst) {

		StringBuffer content = new StringBuffer("");
		List<HttpParam> respList = submitResp.getParamList();
		String respValue = "";
		
		for (GmmsMessage msg : msgSet) {
			
			if (!(msg.getStatus().equals(GmmsStatus.UNASSIGNED))) {
				hs = hi.mapGmmsStatus2HttpSubStatus(msg.getStatus());
			}else
			{
				hs=hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SUCCESS);
			}
			
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
				content.append(hp.getOppsiteParam() + "=" + respValue + "&");
				respValue = "";
			}
			
			if(content.toString().endsWith("&"))
			{
				content.deleteCharAt(content.toString().length()-1);
			}
			content.append("\r\n");
			
		}
		return content.toString();
	}
}
