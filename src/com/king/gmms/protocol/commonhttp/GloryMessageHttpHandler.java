package com.king.gmms.protocol.commonhttp;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;


import com.king.framework.SystemLogger;
import com.king.gmms.Constant;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;
import com.king.rest.util.StringUtility;


public class GloryMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger.getSystemLogger(GloryMessageHttpHandler.class);
	public GloryMessageHttpHandler(HttpInterface hie){		
		super(hie);	
	}
	/**
	 * generate submit request data
	 */
	public String makeRequest(GmmsMessage message,String urlEncoding, A2PCustomerInfo cst)throws UnsupportedEncodingException {
		StringBuffer postData = new StringBuffer();
		String sender = message.getSenderAddress();
		String recipient = message.getRecipientAddress();
		String[] address = new String[]{sender,recipient};
        String[] content = this.parseGmmsContent(message);//content & udh
    	List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for(HttpParam param:parameters){
			String pval = param.getParam();
			if("recipientAddress".equalsIgnoreCase(pval)){
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(address[1],urlEncoding));//recipient
			}else if("textContent".equalsIgnoreCase(pval)){
				String textContent = message.getTextContent();				
			    if (message.hasContentTemplateParamter()) {
			    	int signatureIndex = textContent.indexOf(Constant.CONTENT_SIGNATURE_KEYWORD);
					int templateIndex = textContent.indexOf(Constant.CONTENT_TEMPLATE_KEYWORD);
					String template = "";
					String signature = "";
					if (templateIndex>-1) {
						if (signatureIndex>0) {
							template = textContent.substring(templateIndex+Constant.CONTENT_TEMPLATE_KEYWORD.length(), signatureIndex);
							signature = textContent.substring(signatureIndex+Constant.CONTENT_SIGNATURE_KEYWORD.length(), textContent.length());						
						}else {
							template = textContent.substring(templateIndex+Constant.CONTENT_TEMPLATE_KEYWORD.length(), textContent.length());
						}
				    	
					}else {
						log.error(message, "content didn't has template keywords.");
						return "";
					}
					
					String templateId = template.trim();
					String paramters = "";
					int templateIdIndex = template.indexOf("=");
			    	if (templateIdIndex>-1) {
						templateId = template.substring(0, templateIdIndex).trim();
						paramters = template.substring(templateIdIndex+1).trim();
					}
			    	String dcTemplateId = customerManager.getContentTpl().getTemplateIdMaps(templateId);
			    	
					if (StringUtility.stringIsNotEmpty(dcTemplateId)) {						 
					    postData.append("&tempid=").append(dcTemplateId.trim());						
					}else {
						log.error(message, "can't found the template Id from templateId.conf.");
						return "";
					}
					
					String signaid = customerManager.getContentTpl().getSignatureMaps(signature);
					if (StringUtility.stringIsNotEmpty(signaid)) {
						postData.append("&signaid=").append(signaid.trim());
					}
					if (StringUtility.stringIsNotEmpty(paramters)) {
						postData.append("&params=").append(urlEncode(paramters,urlEncoding));
					}
				}else {
					postData.append("&").append(param.getOppsiteParam()).append("=").append(content[0]);//message
				}
															
			}else{
				String mval = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(mval,urlEncoding));
			}
		}
		if(postData.length()>0){
			postData.deleteCharAt(0);//delete &
		}
		String chlAccount = cst.getChlAcctNamer();
		String chlPwd = cst.getChlPasswordr();
		long timestamp = System.currentTimeMillis();
		String sign = HttpUtils.encryptUP(chlAccount+chlPwd+timestamp, "MD5");
		
		String signvalue = cst.getSMSOptionHttpCustomParameter();
		String signaid = null;
		String params = null;
		if (StringUtility.stringIsNotEmpty(signvalue)) {
			if (signvalue.indexOf("=")>0) {
				signaid= signvalue.split("=")[0];
				params= signvalue.substring(signaid.length()+1);
				
			}else {
				params= signvalue;
			}
		}
		
		postData.append("&appkey=").append(chlAccount)
		        .append("&secretkey=").append(chlPwd)
		        .append("&timestamp=").append(timestamp)
		        .append("&sign=").append(sign);				
		return postData.toString();
	}
	/**
	 * do response for submit request
	 */
	public String makeResponse(HttpStatus hs,GmmsMessage msg, A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		
		return this.generateResponse(hs, msg, submitResp, cst);
	
	}
	/**
	 * generate response
	 * @param hs
	 * @param msg
	 * @param submitResp
	 * @param response
	 * @return
	 */
	private String generateResponse(HttpStatus hs, GmmsMessage msg,HttpPdu submitResp, A2PCustomerInfo cst){
		StringBuffer content = new StringBuffer(60);
		List<HttpParam> respList = submitResp.getParamList();
		String respValue = "";
		for (HttpParam hp : respList) {
			String param = hp.getParam();
			Object value = msg.getProperty(hp.getParam());
			// response won't have "deliveryReport"
//			if ("deliveryReport".equalsIgnoreCase(param)) {
//				if ((Boolean) value) {
//					respValue = "1";
//				} else {
//					respValue = "0";
//				}
//			}
			if ("StatusCode".equalsIgnoreCase(param)) {
				respValue = "" + hs.getCode();
			} else if ("StatusText".equalsIgnoreCase(param)) {
				respValue = hs.getText();
			} else {
				respValue = HttpUtils.getParameter(hp, msg, cst);
			}
			respValue = respValue != null ? respValue : hp.getDefaultValue();
			
			if(log.isDebugEnabled()){
				log.debug("hp.getParam()={};value={};respValue={}",param,value, respValue);
			}
			content.append(hp.getOppsiteParam() + "=" + respValue + "&");
			respValue = "";
		}
		String respContent = content.toString();
		if(respContent.endsWith("&")){
			respContent = respContent.substring(0, respContent.length()-1);
		}
		return respContent;
	}
	/**
	 * parse submit request and give response
	 */
	public HttpStatus parseRequest(GmmsMessage msg,	HttpServletRequest request)throws ServletException, IOException {
		try {
			// authentication
			String hiUsername = hi.getUsername();
			int rssid=-1;
			String username =null;
			
			A2PCustomerInfo csts = null;
			A2PSingleConnectionInfo sInfo = null;
			
			if(hiUsername==null){
				String protocol = hi.getInterfaceName();
				if(protocol != null && protocol.trim().length() > 0){
					ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager().getSsidByProtocol(protocol);					
					if(alSsid == null || alSsid.size()< 1){
						log.warn(msg,"getSsid by interfaceName {} failed", protocol);
						return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
					}
					rssid = alSsid.get(0);
					csts =gmmsUtility.getCustomerManager().getCustomerBySSID(rssid);
					sInfo = (A2PSingleConnectionInfo) csts;
				}
			}else{
                  username = request.getParameter(hi.getUsername());
				
                  if(log.isDebugEnabled()){
      				log.debug("username={}", username);
                  }
				if (username == null || username.trim().length()<1) {
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}
				String password = request.getParameter(hi.getPassword());
				
					log.debug("password={}", password);
				
				if (password == null || password.trim().length()<1) {
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}

				 csts = gmmsUtility.getCustomerManager().getCustomerBySpID(username);
				 sInfo = (A2PSingleConnectionInfo) csts;
				if (sInfo == null || !(password.equals(sInfo.getAuthKey()))) {
					
						log.debug("customerInfo == {} by serverid = {}",sInfo, username);
					
					msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
				}
			}
				
			
			
			// throttling control process
			try {
				if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
					return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.Throttled);
				}
			} catch (Exception e) {
	            log.warn("Error occur when processing throttling control in UrlMessageHttpHandler.parseRequest",e);
	        }
			
			String mtype = null;
			String messageContent = null;
			String udh = null;
			for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
				String requestValue = request.getParameter(hp.getOppsiteParam());
				String value = requestValue != null ? requestValue : hp.getDefaultValue();
				String param = hp.getParam();
				
					log.debug("param name={};OppsiteParam={}; requestvalue = {}; value={}",hp.getParam(),hp.getOppsiteParam(),requestValue, value);
				
				if ("deliveryReport".equalsIgnoreCase(param)) {
					if ("1".equalsIgnoreCase(value)) {
						msg.setProperty(param, true);
					} else {
						msg.setProperty(param, false);
					}
				} else if ("expiryDate".equalsIgnoreCase(param)) {
					Date expireDate = parseHttpExpiryDate(csts,hp, value);
					if (expireDate != null) {
						expireDate = gmmsUtility.getGMTTime(expireDate);
					}
					msg.setProperty(param, expireDate);
				} else if ("textContent".equalsIgnoreCase(param)) {//textContent
					messageContent=value;
				} else if ("udh".equalsIgnoreCase(param)) {//udh
					udh=value;
				} else if ("contentType".equalsIgnoreCase(param)) {//contentType
					mtype = value;
				} else {
					HttpUtils.setParameter(hp, msg, csts,requestValue);
				}
			}
			
			String gmmsCharset = hi.mapHttpCharset2GmmsCharset(mtype);
			msg.setContentType(gmmsCharset);
			
			if (MessageBase.AIC_MSG_TYPE_BINARY.equals(gmmsCharset)) {
				msg.setGmmsMsgType(MessageBase.AIC_MSG_TYPE_BINARY);
			}
		
			if (messageContent == null || "".equalsIgnoreCase(messageContent)) {
				msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);
				return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FIELD);
			} else {
				super.parseHttpContent(msg, mtype, messageContent, udh);
			}
			
			if (msg.getRecipientAddress() == null) {
				msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
				return hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
			}

			msg.setOSsID(sInfo.getSSID());
			String  commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo.getSSID());
			msg.setMsgID(commonMsgID);
			
			return subSuccess;
			
		} catch (UnsupportedEncodingException e) {
			log.error(msg, " MessageType=" + msg.getMessageType(),e);
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
			String msg_status_code = "";
			String outMsgID = "";
            if (respStr!= null && respStr.contains("code")) {
            	msg_status_code = respStr.split("\"code\":\"")[1].split("\"")[0];
			}
            
            if (respStr!= null && respStr.contains("messageid")) {
            	outMsgID = respStr.split("\"messageid\":\"")[1].split("\"")[0];
			}
			HttpStatus status = new HttpStatus(msg_status_code,msg_status_code);
			GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
			msg.setOutMsgID(outMsgID);
			if(gs != null){
				msg.setStatus(gs);
			}else{
				log.error(msg,"mapHttpStatus2GmmsStatus return null when HttpStatus is {}", status.toString());
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			}
		}
}
