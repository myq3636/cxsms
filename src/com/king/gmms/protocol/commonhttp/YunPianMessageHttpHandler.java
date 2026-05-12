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

import com.alibaba.fastjson.JSON;
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

public class YunPianMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(YunPianMessageHttpHandler.class);

	public YunPianMessageHttpHandler(HttpInterface hie) {
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
		String content = message.getTextContent();
		if(!content.startsWith("【")) {
			int lastPre = content.lastIndexOf("【");
			int lastEnd = content.lastIndexOf("】");
			content = content.substring(lastPre, lastEnd+1)+content.substring(0, lastPre);
		}
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		HttpCharset httpCharset = hi
				.mapGmmsCharset2HttpCharset(message
						.getContentType());
		String mtype = "UTF-8";
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(address[0]);// sender_id
			} else if ("recipientAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(address[1]);// recipient
			}  else if ("textContent".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(content);// message
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
		String chlPwd = cst.getChlPasswordr();
		postData.append("&apikey=").append(chlPwd);
		
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
		Map jsonObject = JSON.parseObject(respStr, Map.class);
		String respCode = String.valueOf(jsonObject.get("code"));
		String outMsgId = String.valueOf(jsonObject.get("sid"));				       
		HttpStatus status = new HttpStatus(respCode,respCode);
		GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
		if (StringUtility.stringIsNotEmpty(outMsgId)) {
			msg.setOutMsgID(outMsgId);
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
		String content="我是验证请求通过!【壹公里社区服务】";
		String json = "{\r\n" + 
				"    \"code\": 0,\r\n" + 
				"    \"msg\": \"发送成功\",\r\n" + 
				"    \"count\": 1,\r\n" + 
				"    \"fee\": 0.05,\r\n" + 
				"    \"unit\": \"RMB\",\r\n" + 
				"    \"mobile\": \"13200000000\",\r\n" + 
				"    \"sid\": 3310228982\r\n" + 
				"}";
		Map jsonObject = JSON.parseObject(json, Map.class);        
        System.out.println("0".equalsIgnoreCase(String.valueOf(jsonObject.get("code"))));
		if(content.startsWith("【")) {
			System.out.println("get content from content");
		}else {
			int lastPre = content.lastIndexOf("【");
			int lastEnd = content.lastIndexOf("】");
			content = content.substring(lastPre, lastEnd+1)+content.substring(0, lastPre);
			System.out.println(content);
		}
	}
}
