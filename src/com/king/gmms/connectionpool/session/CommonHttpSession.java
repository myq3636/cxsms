package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Base64;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportHttpHandler;
import com.king.gmms.protocol.commonhttp.CommonMessageHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpCharset;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

/**
 * 
 * @author Jianming Yang
 * @version 1.2.2
 * 
 */
public class CommonHttpSession extends HttpSession {
	protected HttpInterface hi;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonHttpSession.class);
	protected A2PCustomerInfo cst = null;

	private static final String SUCCESS = "SUCCESS"; // mapping interface handlerClass response if interface has not parameter.
	public CommonHttpSession(A2PCustomerInfo info) {
		super(info);
		init(info);
	}

	private void init(A2PCustomerInfo info) {
		if (null == info) {
			log.error("A2PCustomerInfo is null!");
			return;
		}
		cst = info;
		String interfaceName = info.getProtocol().trim();
		HttpInterfaceManager him = gmmsUtility.getHttpInterfaceManager();
		if (null == him) {
			log.error("Get null HttpInterfaceManager instance!");
			return;
		}
		him.initHttpInterfaceMap(interfaceName);
		try {
			hi = him.getHttpInterfaceMap().get(interfaceName);
		} catch (Exception e) {
			log.error("Can not find the Interface, and name is "
					+ interfaceName, e);
		}
	}

	public boolean submit(GmmsMessage msg) throws IOException {
		boolean bret = false;
		URL url = null;
		String contentType = null;
		String httpMethod = cst.getHttpMethod();
		Map<String, String> header = null;
		if("JatisDr".equalsIgnoreCase(cst.getHttpQueryMessageFlag())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
				httpMethod = "post";
				contentType = "text/xml";
				serverAddress = serverDRAddress;
			}else{
				A2PSingleConnectionInfo sInfo = (A2PSingleConnectionInfo) cst;
				serverAddress = sInfo.getChlURL()[0];
			}
		}else if("SIOO".equalsIgnoreCase(cst.getProtocol())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				HttpCharset httpCharset = hi
						.mapGmmsCharset2HttpCharset(msg
								.getContentType());
				String mtype = httpCharset.getMessageType();
				httpMethod = "post";
				contentType = "application/x-www-form-urlencoded;charset="+mtype;
				
			}
		}else if("JiaHua".equalsIgnoreCase(cst.getProtocol())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				HttpCharset httpCharset = hi
						.mapGmmsCharset2HttpCharset(msg
								.getContentType());
				String mtype = httpCharset.getMessageType();
				httpMethod = "post";
				contentType = "application/json;charset="+mtype;				
			}else{
				serverAddress = serverDRAddress;
			}
		}else if("Mingliang".equalsIgnoreCase(cst.getProtocol())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				HttpCharset httpCharset = hi
						.mapGmmsCharset2HttpCharset(msg
								.getContentType());
				String mtype = httpCharset.getMessageType();
				httpMethod = "post";
				contentType = "application/json;charset="+mtype;				
			}else{
				serverAddress = serverDRAddress;
			}
		}else if("STIV".equalsIgnoreCase(cst.getProtocol())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				HttpCharset httpCharset = hi
						.mapGmmsCharset2HttpCharset(msg
								.getContentType());
				String mtype = httpCharset.getMessageType();
				httpMethod = "post";
				contentType = "application/json;charset="+mtype;				
			}else{
				serverAddress = serverDRAddress;
			}
		}else if("Yulore".equalsIgnoreCase(cst.getProtocol())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/json";
			}else {
				serverAddress = serverDRAddress;
			}
		}else if("CommonJson".equalsIgnoreCase(cst.getProtocol())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/json";
			}else {
				serverAddress = serverDRAddress;
			}
		}else if("CommonCainiao".equalsIgnoreCase(cst.getProtocol())){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
				httpMethod = "post";
				contentType = "application/json";
				serverAddress = serverDRAddress;
			}else {
				serverAddress = serverDRAddress;
			}
		}else if("Common".equalsIgnoreCase(cst.getProtocol())){			
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "text/xml";
			}else {
				serverAddress = serverDRAddress;
			}
		}else if("Equii".equalsIgnoreCase(cst.getProtocol())){			
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/x-www-form-urlencoded;charset=utf-8";
			}else {
				serverAddress = serverDRAddress;
			}
		}else if("South".equalsIgnoreCase(cst.getProtocol())){
			if ("SouthDR".equalsIgnoreCase(cst.getHttpQueryMessageFlag())
					||"MtDr".equalsIgnoreCase(cst.getHttpQueryMessageFlag())) {
				if (msg.getMessageType().equalsIgnoreCase(
						GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP)) {
					msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
					msg.setStatusCode(0);
					if (!putGmmsMessage2RouterQueue(msg)) {
						if (log.isInfoEnabled()) {
							log.info(msg, "Send response to core engine failed!");
						}
					}
					return true;
				}
			}
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/json";
				header = new HashMap<String, String>();
				String userName = cst.getChlAcctNamer();
				header.put("SCT-SMS-ACCOUNT", userName);
				Date d = new Date(System.currentTimeMillis());
		        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
				String dateTime = sdf.format(d);
				String timestamp = dateTime+":00:00";
				String key = cst.getChlPasswordr();
				header.put("SCT-SMS-TIMESTAMP", timestamp);	
				String md5Src = new StringBuffer(userName).append(timestamp)
						.append(key).toString();	    		
				String md5Pass = HttpUtils.md5Encrypt(md5Src);
				header.put("SCT-SMS-SIGN", md5Pass);				
			}else {
				serverAddress = serverDRAddress;
			}
		}else if("sanzhu".equalsIgnoreCase(cst.getProtocol())){			
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/x-www-form-urlencoded;charset=utf-8";
				serverAddress = serverAddress+"?CharsetURL=UTF-8";
			}else {
				serverAddress = serverDRAddress;
			}
		}else if(cst.getProtocol().contains("Twilio")){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/x-www-form-urlencoded;charset=utf-8";
				
				header = new HashMap<String, String>();
				String userName = cst.getChlAcctNamer();
				//header.put("ACCOUNT_SID", userName);				
				String key = cst.getChlPasswordr();
				String plainCredentials = userName + ":" + key;
		        String base64Credentials = new String(Base64.encodeBase64(plainCredentials.getBytes()));		        
		        // Create authorization header
		        String authorizationHeader = "Basic " + base64Credentials;
				//header.put("AUTH_TOKEN", key);	
		        header.put("Authorization", authorizationHeader);
			}else {
				serverAddress = serverDRAddress;
			}
		}else if(cst.getProtocol().contains("Juphoon")){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/json";
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");				 				
				Date now = new Date(); 
				sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
				String dateT = sdf.format(now);
				
				header = new HashMap<String, String>();
				String userName = cst.getChlAcctNamer();
				//header.put("ACCOUNT_SID", userName);				
				String key = cst.getChlPasswordr();
				String plainCredentials = userName + ":" + key;
				String nocker = msg.getMsgID();
		        String base64Credentials = new String(Base64.encodeBase64(HttpUtils.getSHA256(nocker+dateT+key).getBytes()));		        
		        // Authorization：
		        //Username=abcdjXIk;PasswordDigest=M2UwNWNkYWU5NjViNTQxMzJjZjQzYTYyNGNmNDIxZjE1ZmY2NTZjMjYxMjU5MzA3ZDU4NzQzNTZmNzBiNTNlNQ==;
		        //Nonce=123456;Created=2023-08-27T17:03:26Z
		        String authorizationHeader = "Username=" + userName+";PasswordDigest="+base64Credentials+";Nonce="+nocker+";Created="+dateT;					
		        header.put("Authorization", authorizationHeader);
		        header.put("RequestId", msg.getMsgID());
			}else {
				serverAddress = serverDRAddress;
			}
		}else if(cst.getProtocol().contains("Mitto")){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/json";
				header = new HashMap<String, String>();
				String userName = cst.getChlAcctNamer();
				header.put("X-Mitto-API-Key", userName);				
			}else {
				serverAddress = serverDRAddress;
			}
		}else if(cst.getProtocol().contains("ALiYun")){
			if (msg.getMessageType().equalsIgnoreCase(
					GmmsMessage.MSG_TYPE_SUBMIT)) {
				httpMethod = "post";
				contentType = "application/x-www-form-urlencoded;charset=utf-8";							
			}else {
				serverAddress = serverDRAddress;
			}
		}
		
		String post = this.appendData(msg).toString();
		if ("".equals(post)) {
			log.error(msg, "Can't send empty message!");
			return false;
		}
		String resp = null;
		try {
			if ("YunPian".equalsIgnoreCase(cst.getProtocol())) {
				resp = super.post(serverAddress, post);
			}else {
				if (httpMethod.equalsIgnoreCase("get")) {
					url = new URL(serverAddress + "?" + post);
					if (log.isDebugEnabled()) {
						log.debug("http get mothed send url: {}", url.toString());
					}
					resp = super.doGet(url,cst.isSMSOptionIsSupportHttps());
				} else {					
					url = new URL(serverAddress);
					if (log.isDebugEnabled()) {
						log.debug("http post mothed send url: {}, {}", url.toString(), post);
					}
					resp = super.doPost(url, post, header,contentType,cst.isSMSOptionIsSupportHttps());
				}
			}
			
			if (log.isDebugEnabled()) {
				log.debug(msg, "resp {}",resp);
			}

			dealResp(msg, resp);
			
			bret = true;
		} catch (IOException e) {
			log.error(msg,"send msg to vendor throw exception:{}", resp, e);
			handleHttpStatusCode(msg);
			dealHttpErrorResp(msg, resp);
			return true;
		} catch (Exception e) {
			return false;
		}
		return bret;
	}

	public boolean queryMessage(GmmsMessage msg) throws IOException {
		boolean bret = false;
		if (msg == null) {
			return false;
		}		
		String post = this.appendData(msg).toString();
		
		String resp = null;
		try {
			URL url = null;
			String httpMethod = cst.getHttpQueryDRMethod();
			if (httpMethod.equalsIgnoreCase("get")) {
				if ("".equals(post)) {
					url = new URL(serverDRAddress);
				}else {
					url = new URL(serverDRAddress + "?" + post);
				}
				
				log.debug("http get mothed send url: {}", url.toString());
				Map<String, String>header = new HashMap<String, String>();
				String userName = cst.getChlAcctNamer();
				header.put("SCT-SMS-ACCOUNT", userName);
				Date d = new Date(System.currentTimeMillis());
		        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
		        sdf.setTimeZone(TimeZone.getTimeZone("GMT+8"));
				String dateTime = sdf.format(d);
				String timestamp = dateTime+":00:00";
				String key = cst.getChlPasswordr();
				header.put("SCT-SMS-TIMESTAMP", timestamp);	
				String md5Src = new StringBuffer(userName).append(timestamp)
						.append(key).toString();
	    			    		
				String md5Pass = HttpUtils.md5Encrypt(md5Src);
				header.put("SCT-SMS-SIGN", md5Pass);				
				resp = super.doQueryMsgWithHeader(url, header, cst.isSMSOptionIsSupportHttps());
			} else {
				url = new URL(serverDRAddress);
				log.debug("http post mothed send url: {}", url.toString());
				resp = super.doPost(url, post, null,cst.isSMSOptionIsSupportHttps());
			}
			if(log.isInfoEnabled()){
				log.info(msg, "received queryDR resp = {}" , resp);
			}
			this.dealQueryBatchDRResp(msg, resp);
			bret = true;
		} catch (IOException e) {
			handleHttpStatusCode(msg);
			return false;
		} catch (Exception e) {
			return false;
		}
		return bret;
	}

	@SuppressWarnings("unchecked")
	public void dealQueryBatchDRResp(GmmsMessage msg, String resp) {
		if (resp == null || "".equals(resp)) {
			return;
		}
		List<GmmsMessage> msgs = null;
		if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			HttpPdu drResp = hi.getMtDRResponse();
			if (drResp.hasHandlerClass()) {
				String className = drResp.getHandlerClass();
				Object[] args = { msg, this.cst, resp };
				Object c = hi.invokeHandler(className,
						HttpConstants.HANDLER_METHOD_PARSELISTRESPONSE, args);
				if (c == null) {
					return ;
				}
				if (c.getClass().getName().equals("java.util.ArrayList")) {
					msgs = (List<GmmsMessage>) c;
				}
				if(log.isDebugEnabled()){
	        		log.debug(msg, "Used handlerclass:{},invoke method:{}",className,
						HttpConstants.HANDLER_METHOD_PARSELISTRESPONSE);
				}
			}

		} else {
			log.error(msg, "Unsupport message type:{}" , msg.getMessageType());
		}
		if (msgs == null) {
			return;
		} else {
			for (GmmsMessage message : msgs) {
				message.setRSsID(cst.getSSID());
				message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
				if (!putGmmsMessage2RouterQueue(message)) {
					if(log.isInfoEnabled()){
						log.info(message, "Send response to core engine failed!");
					}
				}
			}
		}

	}

	public void dealResp(GmmsMessage msg, String resp) {
		if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_SUBMIT)) {
			if(resp == null || "".equals(resp)){
				List<HttpParam> parameters = hi.getMtSubmitResponse().getParamList();
				if (parameters == null || parameters.size() == 0) {
					handleHttpStatusCode(msg);
				} else {
					msg.setStatus(GmmsStatus.INVALID_MSG_FORMAT);
				}
			}else{
				dealSubmitResp(msg, resp);
			}
			if (msg.getOutMsgID() == null || "".equalsIgnoreCase(msg.getOutMsgID())) {
				String client_ref_id = MessageIdGenerator.generateCommonStringID();
				msg.setOutMsgID(client_ref_id);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		} else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			if(resp == null || "".equals(resp)){
				msg.setStatusCode(GmmsStatus.DELIVERED.getCode());
			}else{
				dealDRResp(msg, resp);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
		}else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {
			if(resp == null || "".equals(resp)){
				msg.setStatusCode(GmmsStatus.ENROUTE.getCode());
			}else{
				dealDRQueryResp(msg, resp);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
		} 
		else {
			log.error(msg, "Unsupport message type:{}" , msg.getMessageType());
		}

		if (!putGmmsMessage2RouterQueue(msg)) {
			log.info(msg, "Send response to core engine failed!");
		}
	}
	
	public void dealHttpErrorResp(GmmsMessage msg, String resp) {
		if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_SUBMIT)) {			
			if (msg.getOutMsgID() == null || "".equalsIgnoreCase(msg.getOutMsgID())) {
				String client_ref_id = MessageIdGenerator.generateCommonStringID();
				msg.setOutMsgID(client_ref_id);
			}
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
		} else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {			
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
		}else if (msg.getMessageType().equalsIgnoreCase(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY)) {			
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
		} 
		else {
			log.error(msg, "Unsupport message type:{}" , msg.getMessageType());
		}

		if (!putGmmsMessage2RouterQueue(msg)) {
			log.info(msg, "Send response to core engine failed!");
		}
	}

	/**
	 * deal with submit response
	 * 
	 * @param msg
	 * @param resp
	 */
	private void dealSubmitResp(GmmsMessage msg, String resp) {
		String respStr = resp.trim();
		String jobid = "";
		String msg_status_code = null;
		String reason_phrase = "";
		HttpPdu submitResp = hi.getMtSubmitResponse();
		if (submitResp.hasHandlerClass()) {
			String className = submitResp.getHandlerClass();
			Object[] args = { msg, resp };
			hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE, args);

			if(log.isDebugEnabled()){
        		log.debug(msg, "Used handlerclass:{},invoke method:{}", className
					,HttpConstants.HANDLER_METHOD_PARSERESPONSE);
			}

		} else {
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
			}
			List<HttpParam> parameters = hi.getMtSubmitResponse()
					.getParamList();
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String oval = param.getOppsiteParam();

				if ("outMsgID".equalsIgnoreCase(pval)) {
					jobid = respmap.get(oval);
				} else if ("StatusCode".equalsIgnoreCase(pval)) {
					msg_status_code = respmap.get(oval);
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					reason_phrase = respmap.get(oval);
				}
			}
			msg.setOutMsgID(jobid);
			if(log.isInfoEnabled()){
				log.info(msg, "received submit resp {}" ,resp);
			}
			HttpStatus status = new HttpStatus(msg_status_code, reason_phrase);
			GmmsStatus gstatus = hi.mapHttpSubStatus2GmmsStatus(status);
			msg.setStatus(gstatus);
		}
	}

	/**
	 * deal with DR response
	 * 
	 * @param msg
	 * @param resp
	 */
	private void dealDRResp(GmmsMessage msg, String resp) {
		String respStr = resp.trim();
		String jobid = "";
		int msg_status_code = -1;
		HttpPdu drResp = hi.getMoDRResponse();
		if (drResp.hasHandlerClass()) {
			String className = drResp.getHandlerClass();
			Object[] args = { msg, resp };
			hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE, args);

			if(log.isDebugEnabled()){
        		log.debug(msg, "Used handlerclass:{},invoke method:{}",className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE);
			}

		} else {
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
			}
			List<HttpParam> parameters = hi.getMoDRResponse().getParamList();
			for (HttpParam param : parameters) {
				String pval = param.getParam();
				String oval = param.getOppsiteParam();
				if ("inMsgID".equalsIgnoreCase(pval)) {
					jobid = respmap.get(oval);
				} else if ("StatusCode".equalsIgnoreCase(pval)) {
					msg_status_code = Integer.parseInt(respmap.get(oval));
				} else if ("StatusText".equalsIgnoreCase(pval)) {
					reason_phrase = respmap.get(oval);
				}
			}
			
			if(log.isInfoEnabled()){
				log.info(msg, "Receive DR response,outmsgId {}, statuscode {}" ,jobid, msg_status_code);
			}
		}
	}
	
	private void dealDRQueryResp(GmmsMessage msg, String resp){
		HttpPdu drResp = hi.getMtDRResponse();
		if (drResp.hasHandlerClass()) {
			String className = drResp.getHandlerClass();
			Object[] args = { msg, resp };
			Object c = hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE, args);
			
			if(log.isDebugEnabled()){
        		log.debug(msg, "Used handlerclass:{},invoke method:{}",className,
					HttpConstants.HANDLER_METHOD_PARSERESPONSE);
			}
			msg.setRSsID(cst.getSSID());
		}else{
			log.error(msg, "there is no handler class for processing message! {}");
		}
	}

	@Override
	public ByteBuffer submitAndRec(GmmsMessage msg) throws IOException {
		// TODO Auto-generated method stub
		return null;
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
			if ((userName != null) && (password != null)) {
				postData.append(userName).append("=").append(
						urlEncode(super.systemId));
				String encryptMethod = cst.getPasswdEncryptMethod();
				if (encryptMethod != null) {
					String encryptPass = HttpUtils.encrypt(super.password,
							encryptMethod);
					postData.append("&").append(password).append("=").append(
							urlEncode(encryptPass));
				} else {
					postData.append("&").append(password).append("=").append(
							urlEncode(super.password));
				}
			} else if (password == null && userName != null) {
				postData.append(userName).append("=").append(
						urlEncode(super.systemId));
			} else if (password != null && userName == null) {
				String encryptMethod = cst.getPasswdEncryptMethod();
				if (encryptMethod != null) {
					String encryptPass = HttpUtils.encrypt(super.password,
							encryptMethod);
					postData.append(password).append("=").append(
							urlEncode(encryptPass));
				} else {
					postData.append(password).append("=").append(
							urlEncode(super.password));
				}
			}
			if (message.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
				HttpPdu submitReq = hi.getMtSubmitRequest();
				if (submitReq.hasHandlerClass()) {
					String className = submitReq.getHandlerClass();
					Object[] args = { message, this.charset, cst };
					postStr = (String) hi.invokeHandler(className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST, args);
					log.debug(message, "Used handlerclass:{},invoke method:{}",className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST);

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
					log.debug(message, "Used handlerclass:{},invoke method:{}",className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST);
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
					log.debug(message, "Used handlerclass:{},invoke method:{}",className,
							HttpConstants.HANDLER_METHOD_MAKEREQUEST);
				} else {
					CommonDeliveryReportHttpHandler commonDeliveryReportHttpHandler = hi
							.getCommonDeliveryReportHandler();
					
//					postStr = commonDeliveryReportHttpHandler.makeRequest(
//							message, this.charset, cst);
					
					postStr = commonDeliveryReportHttpHandler.makeQueryDRRequest(
							message, this.charset, cst);
				}

				if (postStr != null && !"".equals(postStr.trim())) {
					if(!SUCCESS.equalsIgnoreCase(postStr)){ // for handle class has not parameter and response SUCCESS.
						postData.append("&").append(postStr);
					}
					
				}else {
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
		if(log.isInfoEnabled()){
			log.info(message,"post msg {}", postData.toString());
		}
		return postData;
	}
	
	/**
	 * handle http status code
	 * 
	 * @param msg
	 */
	private void handleHttpStatusCode(GmmsMessage msg) {
		if (msg.getMessageType().equals(GmmsMessage.MSG_TYPE_SUBMIT)) {
			handleHttpStatusCode4Submit(msg);
		} else if (msg.getMessageType().equals(
				GmmsMessage.MSG_TYPE_DELIVERY_REPORT)) {
			handleHttpStatusCode4DR(msg);
		}
	}

	/**
	 * handle http status code for submit
	 * 
	 * @param msg
	 */
	private void handleHttpStatusCode4Submit(GmmsMessage msg) {
		int httpstatus = 0;
		try {
			httpstatus = this.connection.getResponseCode();

			if(log.isDebugEnabled()){
        		log.debug(msg, "httpstatus = {}" , httpstatus);
			}
		} catch (IOException e) {
			log.warn(e, e);
		}
		String strStatus = String.valueOf(httpstatus);
		if (strStatus.startsWith("4")) {
			switch (httpstatus) {
			case 401:
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				break;
			case 403:
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				break;
			case 408:
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				break;
			default:
				msg.setStatus(GmmsStatus.UNDELIVERED);
				break;
			}
		} else if (strStatus.startsWith("5")) {
			switch (httpstatus) {
			case 503:
				msg.setStatus(GmmsStatus.Throttled);
				break;
			case 511:
				msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
				break;
			case 598:
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				break;
			case 599:
				msg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				break;
			default:
				msg.setStatus(GmmsStatus.UNDELIVERED);
				break;
			}
		} else if (strStatus.startsWith("2")) {
			if (httpstatus >= 200 && httpstatus < 300) {
				msg.setStatus(GmmsStatus.SUCCESS);
			} else {
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			}
		} else {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
	}

	/**
	 * handle http status code for DR
	 * 
	 * @param msg
	 */
	private void handleHttpStatusCode4DR(GmmsMessage msg) {
		int httpstatus = 0;
		try {
			httpstatus = this.connection.getResponseCode();
			if(log.isDebugEnabled()){
        		log.debug(msg, "httpstatus = {}" , httpstatus);
			}
		} catch (IOException e) {
			log.warn(e, e);
		}
		String strStatus = String.valueOf(httpstatus);
		if (strStatus.startsWith("4")) {
			switch (httpstatus) {
			case 401:
				msg.setStatusCode(GmmsStatus.REJECTED.getCode());
				break;
			case 403:
				msg.setStatusCode(GmmsStatus.REJECTED.getCode());
				break;
			case 408:
				msg.setStatusCode(GmmsStatus.UNDELIVERABLE.getCode());
				break;
			default:
				msg.setStatusCode(GmmsStatus.UNKNOWN.getCode());
				break;
			}
		} else if (strStatus.startsWith("5")) {
			switch (httpstatus) {
			case 504:
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
				break;
			case 511:
				msg.setStatusCode(GmmsStatus.REJECTED.getCode());
				break;
			case 598:
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
				break;
			case 599:
				msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());
				break;
			default:
				msg.setStatusCode(GmmsStatus.UNKNOWN.getCode());
				break;
			}
		} else {
			msg.setStatusCode(GmmsStatus.UNKNOWN.getCode());
		}
	}
	
	public static void main(String[] args) {
		String plainCredentials = "ACcde14d44c8956736e7e9376bc985b676" + ":" + "4edc48f7b8f45e36ebb42c00d4a16012";
		String base64Credentials = new String(Base64.encodeBase64(plainCredentials.getBytes()));
		System.out.println(base64Credentials);
		String appSercret = "efghG6I3d4TP";
		String nonce = "123456";
		String created = "2023-08-27T17:03:26Z";
		String s = new String(Base64.encodeBase64(HttpUtils.getSHA256(nonce+created+appSercret).getBytes()));
		System.out.println(s);
	}
}
