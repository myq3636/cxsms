package com.king.gmms.protocol.commonhttp;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;









import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.Constant;
import com.king.gmms.connectionpool.session.HttpSession;
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


public class SanZhuMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger.getSystemLogger(SanZhuMessageHttpHandler.class);
	public SanZhuMessageHttpHandler(HttpInterface hie){		
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
    	List<HttpParam> parameters = hi.getMtSubmitRequest().getParamList();
    	String content = message.getTextContent();
    	content = content.replaceAll("\r\n", ""+(char)6);
    	content = content.replaceAll("\n", ""+(char)6);
    	/*if(content.contains("&")){
    		content = content.replaceAll("&", "%26");
    	}*/
		
		if(postData.length()>0){
			postData.deleteCharAt(0);//delete &
		}
		String chlAccount = cst.getChlAcctNamer();
		String chlPwd = cst.getChlPasswordr();
		String drURL = ((A2PSingleConnectionInfo)cst).getSMSOptionHttpCustomParameter();		
		
		
		postData.append("username=").append(chlAccount)
		        .append("&password=").append(chlPwd)			
		        .append("&CharsetURL=UTF-8");	
		if(StringUtility.stringIsNotEmpty(drURL)){
			postData.append("&response=").append(drURL);
		}
		
		for(HttpParam param:parameters){
			String pval = param.getParam();
			if("recipientAddress".equalsIgnoreCase(pval)){
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(address[1],urlEncoding));//recipient
			}else if("textContent".equalsIgnoreCase(pval)){			
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(content,"utf8"));//message
			}else{
				String mval = HttpUtils.getParameter(param, message, cst);
				postData.append("&").append(param.getOppsiteParam()).append("=").append(mval);
			}
		}
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
		
		return null;
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
			try {
				String respStr = resp.trim();				
				String[] respList = respStr.split("\r\n");
				if(respList.length == 1){
					respList = respStr.split("\n");
				}
				if(respList.length == 1){
					respList = respStr.split(""+(char)10);
				}				
				if(respList!=null){
					for(String parameter : respList){
						String[] paramters = parameter.split("=");
						if(paramters.length == 2){
							if("msgid".equalsIgnoreCase(paramters[0])){
								msg.setOutMsgID(paramters[1]);
							}
							if("statuscode".equalsIgnoreCase(paramters[0])){
								HttpStatus status = new HttpStatus(paramters[1],paramters[1]);
								GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
								if(gs != null){
									msg.setStatus(gs);
								}else{
									log.error(msg,"mapHttpStatus2GmmsStatus return null when HttpStatus is {}", status.toString());
									msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
								}
							}
							
						}
					}
				}
			} catch (Exception e) {
				log.error(msg,"mapHttpStatus2GmmsStatus return null when HttpStatus is {}", e);
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			}
			
		}
	
	public static void main(String[] args) {
		String content = "abddre\r\naete\ntete中a&&&aee ";
    	try {
    		URL url = new URL("https://smsapi.mitake.com.tw:443/api/mtk/SmSend?CharsetURL=UTF-8");
    		HostnameVerifier hv = new HostnameVerifier() {
				public boolean verify(String urlHostName, SSLSession session) {
					// TODO Auto-generated method stub
					log.info("Warning: URL Host: " + urlHostName + " vs "+ session.getPeerHost());
					return true;
					//return urlHostName.equals(session.getPeerHost());
				}
		     };
		    
		     HttpSession.trustAllHttpsCertificates();
		     HttpsURLConnection.setDefaultHostnameVerifier(hv);
		     HttpURLConnection  connection = (HttpsURLConnection) url.openConnection();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	//content = content.replaceAll("\n", ""+(char)6);
    	
		System.out.println(content);
		
	}
}
