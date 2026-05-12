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
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;

public class EGGMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(EGGMessageHttpHandler.class);

	public EGGMessageHttpHandler(HttpInterface hie) {
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
		String[] content = this.parseGmmsContent(message, urlEncoding,
				isUrlEncodingTwice);// content & udh
		List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
		for (HttpParam param : parameters) {
			String pval = param.getParam();
			if ("senderAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[0], urlEncoding));// sender_id
			} else if ("recipientAddress".equalsIgnoreCase(pval)) {
				postData.append("&").append(param.getOppsiteParam())
						.append("=").append(urlEncode(address[1], urlEncoding));// recipient
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
						.append("=").append(urlEncode(expiryDate, urlEncoding));
			} else if ("contentType".equalsIgnoreCase(pval)) {
				HttpCharset httpCharset = hi.mapGmmsCharset2HttpCharset(message
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
	public void parseResponse(GmmsMessage message, String resp) {
		HttpPdu submitResp = hi.getMtSubmitResponse();
		String outMsgId = null;
		List<HttpParam> parameters =submitResp.getParamList();
		String paramDelimiter = submitResp.getParameterDelimiter();
		paramDelimiter = HttpUtils.escapeSpecialChars(paramDelimiter);
		String respDelimiter = submitResp.getResponseDelimiter();		
		respDelimiter = HttpUtils.escapeSpecialChars(respDelimiter);
		String[] lines = resp.split(respDelimiter);			
		Map<String,String> respMap = genResponseMap(message,lines,paramDelimiter,true);
		for(HttpParam param:parameters){
			String pval = param.getParam();
			String oval = param.getOppsiteParam();
			if(log.isDebugEnabled()){
				log.debug("pval={},oval={}",pval,oval);
			}
			if("outMsgID".equalsIgnoreCase(pval)){
				outMsgId = respMap.get(oval);
			}else if("StatusCode".equalsIgnoreCase(pval)){
				//ignore StatusCode
			}else if("StatusText".equalsIgnoreCase(pval)){
				//ignore StatusText
			}else{
				String value = respMap.get(oval);
				message.setProperty(pval, value);
			}
		}
		
		if (outMsgId != null && outMsgId.trim().length() > 0) {
			message.setOutMsgID(outMsgId);
		}
		
		if (log.isInfoEnabled()) {
			log.info(message,"Receive submit response and the outMsgId is {} ",message.getOutMsgID());
		}		
		
	}
	/**
	 * generate response map and deal with status
	 * @param message
	 * @param lines
	 * @return
	 */
    private Map<String, String> genResponseMap(GmmsMessage message,String[] lines,String paramDelimiter,boolean flagIsSubmit){
    	Map<String,String> map = new HashMap<String,String>();
        HttpStatus hs = null;
        List<HttpParam> parameters = hi.getMtSubmitResponse().getParamList();
        String hsCodeKey = null;
		String hsTextKey = null;
		for(HttpParam param:parameters){
			String pval = param.getParam();
			if("StatusCode".equalsIgnoreCase(pval)){
				hsCodeKey = param.getOppsiteParam();
			} else if ("StatusText".equalsIgnoreCase(pval)) {
				hsTextKey = param.getOppsiteParam();
			}
		}   		
        if(paramDelimiter==null || "".equals(paramDelimiter)){//paramDelimiter is null, response like Spring format
        	if(parameters!=null && parameters.size()>=lines.length){
        		int row = 0;
        		for(HttpParam param:parameters){
    				String oval = param.getOppsiteParam();
    				String line = "";
    				while("".equals(line.trim()) && row <lines.length){
    					line = lines[row++];
    				}
    				map.put(oval, line);
    			}
        	}else if(lines.length==1&&(hsCodeKey!=null||hsTextKey!=null)){
        		String[] resp = lines[0].split(paramDelimiter);
	            if (resp.length ==1) {
	            	if(hsTextKey!=null){
	            		map.put(hsTextKey, resp[0].trim());
	            	}else{
	            		map.put(hsCodeKey, resp[0].trim());
	            	}	            	
	            }else if(resp.length>1){
	            	if(hsTextKey!=null && hsCodeKey!=null){
	            		map.put(hsCodeKey, resp[0].trim());
	            		map.put(hsTextKey, resp[1].trim());
	            	}else if(hsCodeKey!=null && hsTextKey==null){
	            		map.put(hsCodeKey, resp[1].trim());
	            	}else if(hsCodeKey==null && hsTextKey!=null){
	            		map.put(hsTextKey, resp[1].trim());
	            	}
	            }
        	}        	
		}else{
			if(lines.length==1&&(hsCodeKey!=null||hsTextKey!=null)){
				String[] resp = lines[0].split(paramDelimiter);
	            if (resp.length ==1) {
	            	if(hsTextKey!=null){
	            		map.put(hsTextKey, resp[0].trim());
	            	}else{
	            		map.put(hsCodeKey, resp[0].trim());
	            	}	            	
	            }else if(resp.length>1){
	            	if(hsTextKey!=null && hsCodeKey!=null){
	            		map.put(hsCodeKey, resp[0].trim());
	            		map.put(hsTextKey, resp[1].trim());
	            	}else if(hsCodeKey!=null && hsTextKey==null){
	            		map.put(hsCodeKey, resp[1].trim());
	            	}else if(hsCodeKey==null && hsTextKey!=null){
	            		map.put(hsTextKey, resp[1].trim());
	            	}
	            }
			}else{
				for(String line:lines){
					if("".equals(line.trim())){
						continue;
					}
					String[] resp = line.split(paramDelimiter);
		            if (resp.length != 2) {
		            	log.error("No '{}' to split line:{}",paramDelimiter,line);
		                continue;
		            }
		            map.put(resp[0].trim(), resp[1].trim());
		    	}
			}
			
		}
    	
		// parse HttpStatus
		
		String hsCode = map.get(hsCodeKey);
		String hsText = map.get(hsTextKey);
		log.debug("hsCode:{},hsText:{}",hsCode,hsText);
		if (null != hsCode || null != hsText) {
    		hs = new HttpStatus(hsCode, hsText);
    	}
		
		if(hs == null){
    		log.error(message,"Parse status null error!");
    		if(flagIsSubmit){
    			message.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
    		}else{
    			message.setStatus(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT);
    		}
    	}else{
    		GmmsStatus gs = null;
    		if(flagIsSubmit){
    			gs = hi.mapHttpSubStatus2GmmsStatus(hs);
    		}else{
    			gs = hi.mapHttpDRStatus2GmmsStatus(hs);
    		}    
            message.setStatus(gs);
            if(log.isInfoEnabled()){
				log.info(message,"Get HttpStatus for Submit response:{}",hs.toString());
            }
    	}
    	return map;
    }
}
