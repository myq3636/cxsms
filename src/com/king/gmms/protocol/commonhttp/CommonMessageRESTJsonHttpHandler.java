package com.king.gmms.protocol.commonhttp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class CommonMessageRESTJsonHttpHandler extends HttpHandler {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonMessageRESTXmlHttpHandler.class);

	public CommonMessageRESTJsonHttpHandler(HttpInterface hie) {
		super(hie);
	}

	/**
	 * Add by kevin for REST
	 */
	public String makeResponse(HttpStatus hs, GmmsMessage msg,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msg, submitResp, cst);

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

			JxMessage jxMsg = new JxMessage();
			List<JxSubMessage> jxSubMsgs = new ArrayList<JxSubMessage>();
			JxSubMessage jxSubMsg = new JxSubMessage();
			jxSubMsgs.add(jxSubMsg);
			jxMsg.setMSG(jxSubMsgs);
			JxToken token = new JxToken();
			String productToken = cst.getChlPasswordr();
       	    token.setPRODUCTTOKEN(productToken);
	       	jxSubMsg.setDCS(GmmsMessage.AIC_CS_UCS2.equals(message.getContentType())?"8":"0");
	       	jxSubMsg.setFROM(message.getSenderAddress());
	       	jxSubMsg.setBODY(message.getTextContent());
	       	jxSubMsg.setTO(message.getRecipientAddress());
	       	 if (message.getOutMsgID() == null) {
	       		 String commonMsgID = MessageIdGenerator.generateCommonStringID();
					message.setOutMsgID(commonMsgID);
				  }
	       	jxSubMsg.setREFERENCE(message.getOutMsgID());
	       	jxMsg.setAUTHENTICATION(token);       	    
			String msgString = JSONObject.toJSONString(jxMsg);
			log.debug("send msg with json {}", msgString);			
			return msgString;

		} catch (JSONException e) {
			log.warn(message, e.getMessage());
		}
		return null;

	}

	private String generateResponse(HttpStatus hs, GmmsMessage msg,
			HttpPdu submitResp, A2PCustomerInfo cst) {
		try {
			List<HttpParam> respList = submitResp.getParamList();
			JSONObject jsonObj = new JSONObject();
			Map<String, String> outboundSMSReponse = new HashMap<String, String>();

			String respValue = null;
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
				outboundSMSReponse.put(hp.getOppsiteParam(), respValue);
				respValue = null;
			}

			jsonObj.put("SMSResponse", outboundSMSReponse);
			return jsonObj.toString();
		} catch (JSONException e) {
			log.warn(msg, e.getMessage());
		}
		return null;
	}

	// kevin add for REST

	// public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest
	// request)
	// throws ServletException, IOException {
	//
	// try {
	// A2PCustomerInfo csts = null;
	// A2PSingleConnectionInfo sInfo = null;
	// String mtype = null;
	// String messageContent = null;
	// String udh = null;
	// String requestBody = this.getRequestBody(request);
	// if (requestBody == null || "".equals(requestBody.trim())) {
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// }
	//
	// String jsonContent = requestBody;
	//
	// JSONObject jsonObject = new JSONObject(jsonContent);
	//
	// JSONObject outboundSMSRequestObj = jsonObject
	// .getJSONObject("outboundSMSRequest");
	//
	// JSONObject smsRequestObj = outboundSMSRequestObj
	// .getJSONObject("SMSMessage");
	//
	// String recipientAddress = null;
	// String inUsername=null;
	// String inPassword=null;
	// for (HttpParam hpa : hi.getMoSubmitRequest().getParamList()) {
	// if ("recipientAddress".equalsIgnoreCase(hpa.getParam())) {
	// recipientAddress = smsRequestObj.getString(hpa
	// .getOppsiteParam());
	// }
	// if("username".equalsIgnoreCase(hpa.getParam()))
	// {
	// inUsername=smsRequestObj.getString(hpa
	// .getOppsiteParam());
	// }
	// if("password".equalsIgnoreCase(hpa.getParam()))
	// {
	// inPassword=smsRequestObj.getString(hpa
	// .getOppsiteParam());
	// }
	// }
	// log.debug("recipientAddress={}", recipientAddress);
	//
	// String outUsername = null;
	// outUsername = outboundSMSRequestObj.getString(hi.getUsername());
	// if (log.isDebugEnabled()) {
	// log.debug("username={}", outUsername);
	// }
	//
	// String outPassword = outboundSMSRequestObj.getString(hi.getPassword());
	// //add
	// if(recipientAddress == null)
	// {
	// msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// }
	// if((outUsername == null || outUsername.trim().length() <
	// 1)&&(inUsername==null||inUsername.trim().length()<1))
	// {
	// msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// }
	// if((outPassword == null || outPassword.trim().length() < 1)&&(inPassword
	// == null || inPassword.trim().length() < 1))
	// {
	// msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.INVALID_MSG_FORMAT);
	// }
	//
	// String username=null;
	// if(inUsername!=null&&inUsername.trim().length()>1)
	// {
	// username=inUsername;
	// }else
	// {
	// username=outUsername;
	// }
	//
	// String password=null;
	// if(inPassword!=null&&inPassword.trim().length()>1)
	// {
	// password=inPassword;
	// }else
	// {
	// password=outPassword;
	// }
	// //add
	//
	// csts = gmmsUtility.getCustomerManager().getCustomerBySpID(username);
	// sInfo = (A2PSingleConnectionInfo) csts;
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
	// log.debug("md5 src String == {} and encry md5 string {}", md5,
	// md5Pwd);
	//
	// if (!password.equalsIgnoreCase(md5Pwd)) {
	// msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.AUTHENTICATION_ERROR);
	// }
	// }
	//
	// // throttling control process
	// try {
	// if (!super.checkIncomingThrottlingControl(csts.getSSID(), msg)) {
	// return hi
	// .mapGmmsStatus2HttpSubStatus(GmmsStatus.SERVER_ERROR);
	// }
	// } catch (Exception e) {
	// log.warn(
	// "Error occur when processing throttling control in CommonMessageHttpHandler.parseRequest",
	// e);
	// }
	//
	// for (HttpParam hp : hi.getMoSubmitRequest().getParamList()) {
	// String requestPar = hp.getOppsiteParam();
	// String requestValue = smsRequestObj.getString(hp
	// .getOppsiteParam());
	//
	// String value = requestValue != null ? requestValue : hp
	// .getDefaultValue();
	// String param = hp.getParam();
	//
	// log.debug(
	// "param name={};OppsiteParam={}; requestvalue = {}; value={}",
	// hp.getParam(), hp.getOppsiteParam(), requestValue,
	// value);
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
	//
	// return subSuccess;
	//
	// } catch (Exception e) {
	// log.error("common exception!", e);
	// return subFail;
	// }
	//
	// }

	public HttpStatus parseRequest(GmmsMessage msg, HttpServletRequest request)
			throws ServletException, IOException {

		return null;

	}

	public String getRequestBody(HttpServletRequest request) {
		StringBuffer buffer = new StringBuffer();
		BufferedReader reader;
		try {
			reader = request.getReader();
			String str = reader.readLine();
			while (str != null) {
				buffer.append(str);
				str = reader.readLine();
			}
			return buffer.toString();
		} catch (IOException e) {

			log.warn(e.getMessage());
		}
		return null;

	}

	@Override
	public void parseResponse(GmmsMessage msg, String resp) {

		String respStr = resp.trim();

		if (log.isDebugEnabled()) {
			log.debug("CommonMessageRESTJsonHttpHandler parseResponse:"
					+ respStr);
		}

		String jobid = "";
		String msg_status_code = null;
		String reason_phrase = "";
		try {
			JSONObject jsonObject = JSONObject.parseObject(respStr);			

			List<HttpParam> parameters = hi.getMtSubmitResponse()
					.getParamList();
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String oval = param.getOppsiteParam();

				if (log.isDebugEnabled()) {
					log.debug("pva:" + pval + "oval:" + oval);
				}

				if ("outMsgID".equalsIgnoreCase(pval)) {
					jobid = jsonObject.getString(oval);
				} else if ("StatusCode".equalsIgnoreCase(pval)) {
					msg_status_code = jsonObject.getString(oval);
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					reason_phrase = jsonObject.getString(oval);
				}

			}

			if (jobid != null && !jobid.trim().equals("")) {
				msg.setOutMsgID(jobid);
			}
			if (log.isInfoEnabled()) {
				log.info(msg, "received submit resp {}", resp);
			}
			HttpStatus status = new HttpStatus(msg_status_code, reason_phrase);
			GmmsStatus gstatus = hi.mapHttpSubStatus2GmmsStatus(status);
			msg.setStatus(gstatus);

			if (log.isDebugEnabled()) {
				log.debug("CommonMessageRESTJsonHttpHandler msg:" + msg);
			}
		} catch (Exception e) {
			log.warn(msg, e.getMessage());
			msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
			return;
		}

	}
	

	
	public String makeResponse(HttpStatus hs, Set<GmmsMessage> msgSet,
			A2PCustomerInfo cst) throws IOException, ServletException {
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (hs == null) {
			hs = subFail;
		}
		return this.generateResponse(hs, msgSet, submitResp, cst);

	}
	
	
	private String generateResponse(HttpStatus hs, Set<GmmsMessage> msgSet,
			HttpPdu submitResp, A2PCustomerInfo cst) {		
		return null;
	}
	
	public static void main(String[] args) {
		try {
            SSLContext context = SSLContext.getDefault();
            String[] protocols = context.getSupportedSSLParameters().getProtocols();
            System.out.println("Supported SSL/TLS protocols:");
            for (String protocol : protocols) {
                System.out.println(protocol);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
