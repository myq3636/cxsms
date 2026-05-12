package com.king.gmms.protocol.commonhttp;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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
import com.king.rest.util.StringUtility;

public class TwoSixMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(TwoSixMessageHttpHandler.class);

	public TwoSixMessageHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message, String urlEncoding,
			A2PCustomerInfo cst) throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		String sender = message.getSenderAddress();
		String recipient = message.getRecipientAddress();
		String[] address = new String[] { sender, recipient };
		boolean isUrlEncodingTwice = cst.isUrlEncodingTwice();		
		String content = message.getTextContent();
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		HttpCharset httpCharset = hi
				.mapGmmsCharset2HttpCharset(message
						.getContentType());
		String mtype = httpCharset.getMessageType();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[0], mtype));// sender_id
			} else if ("recipientAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[1], mtype));// recipient
			}  else if ("textContent".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(content, mtype));// message
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
						.append("=").append(urlEncode(expiryDate, mtype));
			} else if ("contentType".equalsIgnoreCase(pval)) {				
				
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(mtype, mtype));
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
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(mval, mtype));
			}
		}
		if (postData.length() > 0) {
			postData.deleteCharAt(0);// delete &
		}
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        String timestamp = format.format(new Date());
		String chlAccount = cst.getChlAcctNamer();
		String chlPwd = cst.getChlPasswordr();		
		String userId = ((A2PSingleConnectionInfo)cst).getChlAcctName();		
		String sign = HttpUtils.encrypt(chlAccount+chlPwd+timestamp, "MD5");
		postData.append("&account=").append(urlEncode(chlAccount, mtype));
		postData.append("&ts=").append(urlEncode(timestamp, mtype));
		postData.append("&pswd=").append(urlEncode(sign, mtype));
		postData.append("&needstatus=true");
		return postData.toString();
	}

	/**
	 * do response for submit request
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msg, submitResp, cst);

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
	private String generateResponse(HttpStatus hs, GmmsMessage msg,
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
			content.append(hp.getOppsiteParam() + "=" + respValue + "&");
			respValue = "";
		}
		String respContent = content.toString();
		if (respContent.endsWith("&")) {
			respContent = respContent.substring(0, respContent.length() - 1);
		}
		return respContent;
	}


	@Override
	public HttpStatus parseRequest(GmmsMessage msg,
			HttpServletRequest request) throws ServletException, IOException {
		try {
			int rssid = -1;

			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;

			String protocol = hi.getInterfaceName();
			if (protocol != null && protocol.trim().length() > 0) {
				ArrayList<Integer> alSsid = gmmsUtility
						.getCustomerManager().getSsidByProtocol(protocol);

				if (alSsid == null || alSsid.size() < 1) {
					log.warn(msg, "getSsid by interfaceName {} failed" , protocol);
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
				}
				rssid = alSsid.get(0);
				csts = gmmsUtility.getCustomerManager().getCustomerBySSID(rssid);
				sInfo = (A2PSingleConnectionInfo) csts;
			}

			// throttling control process
			try {
				if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SERVER_ERROR);
				}
			} catch (Exception e) {
				log.warn("Error occur when processing throttling control " +
						"in EGGMessageHttpHandler.parseRequest",e);
			}

			String mtype = null;
			String messageContent = null;
			String udh = null;
			for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
				String requestValue = request
						.getParameter(hp.getOppsiteParam());
				String value = requestValue != null ? requestValue : hp
						.getDefaultValue();
				String param = hp.getParam();
				if(log.isDebugEnabled()){
					log.debug("param name={};OppsiteParam={}; requestvalue = {}; value={}",
							hp.getParam(), hp.getOppsiteParam(),
							requestValue, value);
				}
				
				if ("textContent".equalsIgnoreCase(param)) {//textContent
					messageContent = value;
				} else {
					HttpUtils.setParameter(hp, msg, csts, requestValue);
				}
			}

			String gmmsCharset = GmmsMessage.AIC_CS_ASCII;
			msg.setContentType(gmmsCharset);

			if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
				msg.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
			}

			if (messageContent == null || "".equalsIgnoreCase(messageContent)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
			} else {
				this.parseHttpContent(msg, mtype, messageContent, udh);
			}

			msg.setOSsID(sInfo.getSSID());
			String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo.getSSID());
			msg.setMsgID(commonMsgID);

			//set configure recipient address for Egg MO message:
			String recipientAddr = msg.getRecipientAddress();
			if(recipientAddr == null || "".equalsIgnoreCase(recipientAddr)){
				recipientAddr =	sInfo.getSMSOptionHttpCustomParameter();		
				if(recipientAddr == null || "".equalsIgnoreCase(recipientAddr)){
					msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
					return subFail;
				}else{
					msg.setRecipientAddress(recipientAddr);
				}
			}
			
			return subSuccess;
		} catch (UnsupportedEncodingException e) {
			log.error(msg, " MessageType()={}" , msg.getMessageType());
			return subFail;
		} catch (Exception e) {
			log.error("common exception!", e);
			return subFail;
		}
	}

	/**
	 * parse submit response
	 */
	public void parseResponse(GmmsMessage msg, String resp) {
		String respStr = resp.trim();
		String msg_status_code = respStr;
		String outMsgID = "";
        if (respStr!= null && respStr.contains("\n")) {
        	String code_string = respStr.split("\\n")[0];
        	if (StringUtility.stringIsNotEmpty(code_string)) {
				msg_status_code = code_string.split(",")[1].trim();
			}
        	outMsgID= respStr.split("\\n")[1].trim();
		}else {
			if (StringUtility.stringIsNotEmpty(respStr)) {
				msg_status_code = respStr.split(",")[1].trim();
			}
		}               
		HttpStatus status = new HttpStatus(msg_status_code,msg_status_code);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		if (StringUtility.stringIsNotEmpty(outMsgID)) {
			msg.setOutMsgID(outMsgID);
		}		
		if(gs != null){
			msg.setStatus(gs);
		}else{
			log.error(msg,"mapHttpStatus2GmmsStatus return null when HttpStatus is {}", status.toString());
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}		
		
	}
	
	public static void main(String[] args) {
		String mString  = "abcde,0\n adfdaaf";
		if(mString.contains("\n")) {
			System.out.println(mString.split("\\n")[0]);
		}else {
			System.out.println(mString.split(",")[1]);
		}
	}
}
