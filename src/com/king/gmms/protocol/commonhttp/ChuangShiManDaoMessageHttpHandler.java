package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
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

public class ChuangShiManDaoMessageHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(ChuangShiManDaoMessageHttpHandler.class);

	public ChuangShiManDaoMessageHttpHandler(HttpInterface hie) {
		super(hie);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		String sender = message.getSenderAddress();
		String recipient = message.getRecipientAddress();
		String[] address = new String[] { sender, recipient };
		String sn = "";
		String password = "";
		String passwordName = "";

		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=");
				if (!"".equals(cst.getSMSOptionHttpCustomParameter())) {
					Integer extRange = null;
					if (this.isNumber(cst.getSMSOptionHttpCustomParameter())) {
						extRange = Integer.valueOf(cst
								.getSMSOptionHttpCustomParameter());
						if (address[0].length() <= extRange
								&& this.isNumber(address[0]) && extRange <= 9) {
							postData.append(urlEncode(address[0], urlEncoding));// sender_id
						} else {
							postData.append("");
						}
					} else {
						postData.append("");
					}
				} else
					postData.append("");
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
			} else if ("timeStamp".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append("");
			} else if ("contentType".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append("");
			} else if ("outMsgID".equalsIgnoreCase(pval)) {
				String outMsgid = message.getOutMsgID();
				if (outMsgid == null) {
					outMsgid = MessageIdGenerator.generateCommonStringID();
					message.setOutMsgID(outMsgid);
				}
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(outMsgid);// client_ref_id
			} else {
				String mval = HttpUtils.getParameter(param, message, cst);
				if ("ChlPassword".equalsIgnoreCase(pval)) {
					password = mval;
					passwordName = param.getOppsiteParam();
				} else if ("ChlAcctName".equalsIgnoreCase(pval)) {
					sn = mval;
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(urlEncode(mval, urlEncoding));
				} else {
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(urlEncode(mval, urlEncoding));
				}
			}
		}
		// construct pwd parameter for request:
		String encryptMethod = cst.getPasswdEncryptMethod();
		String encryptPass = password;
		if (encryptMethod != null) {
			encryptPass = HttpUtils.encrypt(sn + password, encryptMethod);
		}
		postData.append("&").append(passwordName).append("=").append(
				urlEncode(encryptPass.toUpperCase(), urlEncoding));

		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
		}

		return postData.toString();
	}

	public boolean isNumber(String str) {
		java.util.regex.Pattern pattern = java.util.regex.Pattern
				.compile("-[0-9]+|[0-9]+");
		java.util.regex.Matcher match = pattern.matcher(str);
		if (match.matches() == false) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public String makeResponse(HttpStatus hs, GmmsMessage message,
			A2PCustomerInfo cst) throws IOException, ServletException {
		if (hs == null) {
			hs = subFail;
		}
		return hs.getCode();
	}

	@Override
	public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest request)
			throws ServletException, IOException {
		try {
			int opSsid = -1;
			String username = null;
			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;

			String hiUsername = hi.getUsername();

			if (hiUsername == null) {
				String protocol = hi.getInterfaceName();
				if (protocol != null && protocol.trim().length() > 0) {
					ArrayList<Integer> alSsid = gmmsUtility
							.getCustomerManager().getSsidByProtocol(protocol);

					if (alSsid == null || alSsid.size() < 1) {
						log.warn(msg, "getSsid by interfaceName {} failed" ,protocol);
						return hi
								.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
					}
					opSsid = alSsid.get(0);
					csts = gmmsUtility.getCustomerManager().getCustomerBySSID(
							opSsid);
					sInfo = (A2PSingleConnectionInfo) csts;
				}
			} else {
				username = request.getParameter(hi.getUsername());
        		if(log.isDebugEnabled()){
        			log.debug("username={}", username);
        		}
				if (username == null || username.trim().length() < 1) {
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}
				String password = request.getParameter(hi.getPassword());

				log.debug("password={}", password);

				if (password == null || password.trim().length() < 1) {
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}

				csts = gmmsUtility.getCustomerManager().getCustomerBySpID(
						username);
				sInfo = (A2PSingleConnectionInfo) csts;
				if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {

					log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);

					msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					return hi
							.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
				}
			}

			if (sInfo == null) {
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
			}

			// throttling control process
			try {
				if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SERVER_ERROR);
				}
			} catch (Exception e) {
				log
						.warn(
								"Error occur when processing throttling control in ChuangShiManDaoMessageHttpHandler.parseRequest",
								e);
			}

			String mtype = "";
			if (sInfo.getSupportedCharsets() == null) {
				log.warn(msg,
						"operator of message does not configure charset item.");
				mtype = "gb2312";
			} else {
				mtype = (String) sInfo.getSupportedCharsets().get(0);
			}

			String args = request.getParameter("args");

			String[] parmeters = null;
			if (args != null && !("").equals(args.trim())) {
				parmeters = args.split(",");
			} else {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			}

			if (parmeters.length < 5) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi
						.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			}

			String moID = parmeters[0];
			String teFuNumber = parmeters[1];
			String mobile = parmeters[2];
			String content = "";
			String time = parmeters[parmeters.length - 1];
			String ext = "";

			if (parmeters.length > 5) {
				for (int i = 3; i < parmeters.length - 1; i++) {
					if ("".equals(parmeters[i])) {
						content += ",";
					} else {
						content += parmeters[i];
					}
				}
			} else {// length = 5
				content = parmeters[3];
			}

			int specialServiceNumLength = sInfo
					.getSMSOptionHttpSpecialServiceNum().length();

			if (teFuNumber.length() < 0
					|| teFuNumber.length() < specialServiceNumLength) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);
				// return
				// hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
			} else {
				ext = teFuNumber.substring(specialServiceNumLength);
				if (ext.length() > 0) {
					msg.setRecipientAddress(ext);
				} else {
					msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
					// return
					// hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
				}
			}

			if (mobile.length() > 0) {
				msg.setSenderAddress(mobile);
			} else {
				msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
				// return
				// hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SENDER_ADDR_ERROR);
			}
			String gmmsCharset = hi.mapHttpCharset2GmmsCharset(mtype);
			msg.setContentType(gmmsCharset);

			if ("".equals(content)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				// return
				// hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			} else {
				this.parseHttpContent(msg, mtype, content, null);
			}

			if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
				msg.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
			}
			msg.setOSsID(sInfo.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo
					.getSSID());
			msg.setMsgID(commonMsgID);

			return subSuccess;
		} catch (UnsupportedEncodingException e) {
			log.error(msg, " MessageType={}" , msg.getMessageType());
			return subFail;
		} catch (Exception e) {
			log.error("common exception!", e);
			return subFail;
		}
	}

	@Override
	public void parseResponse(GmmsMessage msg, String resp) {
		String msg_status_code = resp.trim();
		if(log.isDebugEnabled()){
    		log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
		}
		if (this.isNumber(msg_status_code)) {
			if (Double.parseDouble(msg_status_code) > 0) {
				msg_status_code = "0";
			}
		} else {
			msg_status_code = "9000";
		}

		HttpStatus status = new HttpStatus(msg_status_code, null);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		if(log.isDebugEnabled()){
    		log.debug(msg, "HttpStatus={} and GmmsStatus={}" , status, gs);
		}
		msg.setStatus(gs);
	}
}
