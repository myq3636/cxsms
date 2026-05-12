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





import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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


public class VocomMessageHttpHandler extends HttpHandler {
	private static SystemLogger log = SystemLogger.getSystemLogger(VocomMessageHttpHandler.class);
	public VocomMessageHttpHandler(HttpInterface hie){		
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
    	String content = urlEncode(message.getTextContent(), urlEncoding);
		for(HttpParam param:parameters){
			String pval = param.getParam();
			if("recipientAddress".equalsIgnoreCase(pval)){
				postData.append("&").append(param.getOppsiteParam()).append("=").append(urlEncode(address[1],urlEncoding));//recipient
			}else if("textContent".equalsIgnoreCase(pval)){			
				postData.append("&").append(param.getOppsiteParam()).append("=").append(content);//message
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
		String send = ((A2PSingleConnectionInfo)cst).getSMSOptionHttpCustomParameter();
		String userId = ((A2PSingleConnectionInfo)cst).getChlAcctName();		
        String md5Src = new StringBuffer(chlPwd).append(userId).append(message.getTextContent())
				.append(recipient).toString();
		if(log.isDebugEnabled()){
			log.debug("pssword md5 src: {}", md5Src);
		}
		String md5Pass = HttpUtils.md5Encrypt(md5Src);
		
		postData.append("&action=").append(send)
		        .append("&account=").append(chlAccount)
		        .append("&password=").append(md5Pass)
		        .append("&extno=").append(userId)			
		        .append("&rt=json");				
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
				JSONObject json = JSONObject.parseObject(respStr);
				String msg_status_code = json.getString("status");				
				HttpStatus status = new HttpStatus(msg_status_code,msg_status_code);
				GmmsStatus gs = hi.mapHttpSubStatus2GmmsStatus(status);
				if(GmmsStatus.SUCCESS.equals(gs)){
					JSONArray jsonArr = json.getJSONArray("list");
					if (jsonArr!=null && !jsonArr.isEmpty()) {
						JSONObject jsonMsg = jsonArr.getJSONObject(0);
						String outMsgId = (String)jsonMsg.get("mid");
						if (StringUtility.stringIsNotEmpty(outMsgId)) {
							msg.setOutMsgID(outMsgId);
						}
						String code = jsonMsg.getString("result");
						status = new HttpStatus(code,code);
						gs = hi.mapHttpSubStatus2GmmsStatus(status);
					}
				}
							
				if(gs != null){
					msg.setStatus(gs);
				}else{
					log.error(msg,"mapHttpStatus2GmmsStatus return null when HttpStatus is {}", status.toString());
					msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
				}
			} catch (Exception e) {
				log.error(msg,"mapHttpStatus2GmmsStatus return null when HttpStatus is {}", e);
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			}
			
		}
	
	public static void main(String[] args) {
		String resp="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><MtMessageRes><resDetail><phoneNumber>18500375454</phoneNumber><stat>r:000</stat><statDes></statDes></resDetail><smsId>909241720201</smsId><subStat>r:000</subStat>";
		if (resp.contains("<stat>r:000<")) {
			System.out.println("OK");
		}
		
	}
}
