package com.king.gmms.connectionpool.session;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * 
 * @author King Ma
 * @version 1.2.2
 * 
 */
public class SecCommonHttpSession extends CommonHttpSession {	
	private static SystemLogger log = SystemLogger.getSystemLogger(SecCommonHttpSession.class);	
	public SecCommonHttpSession(A2PCustomerInfo info) {
		super(info);		
	}

	@Override
	protected StringBuffer appendData(GmmsMessage message) {
		if(log.isDebugEnabled()){
			log.debug(message, "msg = {}", message);
		}
		StringBuffer postData = new StringBuffer();
		String postStr = "";
		try {

			String userName = hi.getUsername();
			String password = hi.getPassword();
			String mobile = message.getRecipientAddress();
			if((userName != null)&& (password != null) ){
				postData.append(userName).append("=").append(urlEncode(super.systemId));
				String pwd = HttpUtils.md5Encrypt(super.systemId+":"+super.password+":"+mobile);
			    postData.append("&").append(password).append("=").append(urlEncode(pwd));				
			} else if (password == null && userName != null) {
				postData.append(userName).append("=").append(urlEncode(super.systemId));
			} else if (password != null && userName == null) {
				String encryptMethod = cst.getPasswdEncryptMethod();
				if(encryptMethod!=null){
					String encryptPass = HttpUtils.encrypt(super.password,encryptMethod);
					postData.append(password).append("=").append(urlEncode(encryptPass));
				}else{
					postData.append(password).append("=").append(urlEncode(super.password));
				}				
			}	
			if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
				HttpPdu submitReq = hi.getMtSubmitRequest();
				if (submitReq.hasHandlerClass()) {
					String className = submitReq.getHandlerClass();
					Object[] args = { message, this.charset, cst };
					postStr = (String) hi.invokeHandler(className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
					log.debug(message, "Used handlerclass:{},invoke method:{}",
							className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
				} else {
					CommonMessageHttpHandler commonMessageHttpHandler = hi
							.getCommonMessageHandler();
					postStr = commonMessageHttpHandler.makeRequest(message,
							this.charset, cst);
				}
				// deal with null poststr
				if (postStr != null && !"".equals(postStr.trim())) {
					postData.append("&").append(postStr);
				} else {
					message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
				}
			} else if (message.getMessageType().equals(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				HttpPdu drReq = hi.getMoDRRequest();
				if (hi.getMtSubmitRequest().hasHandlerClass()) {
					String className = drReq.getHandlerClass();
					Object[] args = { message, this.charset, cst };
					postStr = (String) hi.invokeHandler(className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
					log.debug(message, "Used handlerclass:{},invoke method:{}",
							className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
				} else {
					CommonDeliveryReportHttpHandler commonDeliveryReportHttpHandler = hi
							.getCommonDeliveryReportHandler();
					postStr = commonDeliveryReportHttpHandler.makeRequest(
							message, this.charset, cst);
				}
				// deal with null poststr
				if (postStr != null && !"".equals(postStr.trim())) {
					postData.append("&").append(postStr);
				} else {
					message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				}
			} else if (message.getMessageType().equals(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
				HttpPdu drReq = hi.getMtDRRequest();
				if (drReq.hasHandlerClass()) {
					String className = drReq.getHandlerClass();
					Object[] args = { message, this.charset, cst };
					postStr = (String) hi.invokeHandler(className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
					log.debug(message, "Used handlerclass:{},invoke method:{}",
							className, HttpConstants.HANDLER_METHOD_MAKEREQUEST);
				} else {
					CommonDeliveryReportHttpHandler commonDeliveryReportHttpHandler = hi
							.getCommonDeliveryReportHandler();
					postStr = commonDeliveryReportHttpHandler.makeRequest(
							message, this.charset, cst);
				}

				if (postStr != null && !"".equals(postStr.trim())) {
					postData.append("&").append(postStr);
				} else {
					message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
				}
			} else {
				log.error(message, "Unsupport message type:"
						+ message.getMessageType());
			}
		} catch (UnsupportedEncodingException e) {
			log.error(message, "urlEncode Error!", e);
		} catch (Exception e) {
			log.error(message, "", e);
		}

		String post = postData.toString();
		if (post.startsWith("&")) {
			postData.deleteCharAt(0);
		}

			log.debug("postData = {}", postData.toString());

		return postData;
	}
	
}
