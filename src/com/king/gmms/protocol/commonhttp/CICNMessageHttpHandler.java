package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class CICNMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CICNMessageHttpHandler.class);
	private static final String PHONE_PREFIX = "886"; // Tai Wan phone prefix
	private static final String GLOBALSMS = "Y"; // GlobalSms

	public CICNMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		String recipient = message.getRecipientAddress();
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		boolean isUrlEncodingTwice = cst.isUrlEncodingTwice();
		String[] content = this.parseGmmsContent(message, urlEncoding,
				isUrlEncodingTwice);
		String SourceMsgID = null;
		String SourceProdID = null;
		String MemberID = null;
		String Password = null;
		String passName = null;
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				if (!PHONE_PREFIX.equalsIgnoreCase(recipient.substring(0, 3))) {
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(GLOBALSMS);// GlobalSms
				}
			} else if ("textContent".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(content[0]);// message
			} else if ("outMsgID".equalsIgnoreCase(pval)) {
				SourceProdID = cst.getSMSOptionHttpCustomParameter();
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(SourceProdID);
			} else if ("msgID".equalsIgnoreCase(pval)) {
				// SourceMsgID = message.getMsgID();
				SourceMsgID = MessageIdGenerator.generateCommonStringID();
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(SourceMsgID);
			} else if ("expiryDate".equalsIgnoreCase(pval)) {
				String expiryDate = null;
				Date tempExpireDate = message.getExpiryDate();
				if (tempExpireDate == null) {
					expiryDate = "86400";
				} else {
					long expiryTime = (message.getExpiryDate().getTime() - message
							.getTimeStamp().getTime()) / 1000;
					if (expiryTime < 7200) {
						expiryDate = "7201";
					} else if (expiryTime > 86400) {
						expiryDate = "86400";
					} else {
						expiryDate = Long.toString(expiryTime);
					}
				}
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(expiryDate);
			} else if ("contentType".equalsIgnoreCase(pval)) {
				HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(message
						.getContentType());
				String mtype = httpCharset.getMessageType();
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(mtype);
			} else if ("chlAcctNamer".equalsIgnoreCase(pval)) {
				MemberID = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(MemberID);
			} else if ("chlPasswordr".equalsIgnoreCase(pval)) {
				Password = HttpUtils.getParameter(param, message, cst);
				passName = param.getOppsiteParam();
			} else {
				String mval = HttpUtils.getParameter(param, message, cst);
				if (mval != null) {
					postData.append("&").append(param.getOppsiteParam())
							.append("=").append(mval);
				}
			}

		}
		String md5Src = null;
		String md5Pass = null;
		if (MemberID != null) {
			md5Src = new StringBuffer(MemberID).append(":").append(Password)
					.append(":").append(SourceProdID).append(":").append(
							SourceMsgID).toString();
    		if(log.isDebugEnabled()){
    			log.debug("pssword md5 src: {}", md5Src);
    		}
			md5Pass = HttpUtils.md5Encrypt(md5Src);
		}

		postData.append("&").append(passName).append("=").append(md5Pass);
		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
		}
		return postData.toString();
	}

	/**
	 * do response for submit request
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		return null;
	}

	/**
	 * parse submit request and give response
	 */
	public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest request)
			throws ServletException, IOException {
		return null;

	}

	/**
	 * parse submit response
	 */
	public void parseResponse(GmmsMessage msg, String resp) {
		String respStr = resp.trim();
		String msg_status_code = "";
		String reason_phrase = "";
		String[] arr_r = respStr.split("&");
		HashMap<String, String> respmap = new HashMap<String, String>();
		for (String str : arr_r) {
			String[] arr_v = str.split("=");
			if (arr_v.length != 2) {
				log.error("Invlid response format!");
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return;
			}
			respmap.put(arr_v[0], arr_v[1]);
			// log.debug("<key,value>="+arr_v[0]+","+arr_v[1]);
		}
		List<HttpParam> parameters = hi.getMtSubmitResponse().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			String oval = param.getOppsiteParam();
    		if(log.isDebugEnabled()){
    			log.debug("pval={},oval={}", pval, oval);
    		}
			if ("StatusCode".equalsIgnoreCase(pval)) {
				msg_status_code = respmap.get(oval);
			} else {
				msg.setProperty(pval, respmap.get(oval));
			}
		}
		// log.debug("jobid="+jobid+",msg_status_code="+msg_status_code+",reason_phrase="+reason_phrase);
		if(log.isDebugEnabled()){
			log.debug(msg, "outMsgId={}" , msg.getOutMsgID());
		}
		HttpStatus status = new HttpStatus(msg_status_code, null);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		msg.setStatus(gs);
	}
}
