package com.king.gmms.listener.commonhttpserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.InputSource;

import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.PhoneUtils;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.customerconnectionfactory.CommonHttpServerFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpParam;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.listener.AbstractHttpServer;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.commonhttp.CommonMessageHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.protocol.commonhttp.JxMessage;
import com.king.gmms.protocol.commonhttp.JxSubMessage;
import com.king.gmms.protocol.commonhttp.JxToken;
import com.king.gmms.protocol.smpp.pdu.Response;
import com.king.gmms.protocol.smpp.util.TimeFormatter;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;
import com.king.redis.RedisClient;
import com.king.rest.util.MessageUtils;
import com.king.rest.util.SMSSignatureUtil;
import com.king.rest.util.StringUtility;
import com.sun.mail.handlers.message_rfc822;

public class AliCainiaoJsonHttpServlet extends AbstractHttpServer {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(AliCainiaoJsonHttpServlet.class);
	private static A2PCustomerManager ctm = null;
	private static HttpInterfaceManager him = null;
	private static CommonHttpServerFactory factory = null;
	protected ExecutorServiceManager executorServiceManager;
	protected ExecutorService receiverThreadPool;
	private BillingCounter billingCounter = BillingCounter.getInstance();
	private RedisClient redis = null;
	private int moduleIndex = 0;
	/**
	 * init factory
	 */
	public void init() {
		super.init();
		ctm = gmmsUtility.getCustomerManager();
		redis = gmmsUtility.getRedisClient();
		him = gmmsUtility.getHttpInterfaceManager();
		executorServiceManager = gmmsUtility.getExecutorServiceManager();
		moduleIndex = ModuleManager.getInstance()
				.getModuleIndex(moduleName);
		factory = CommonHttpServerFactory.getInstance();		
		try {
			synchronized (mutex) {
				ArrayList<Integer> alSsid = null;
				if (protocol != null && protocol.trim().length() > 0) {
					alSsid = gmmsUtility.getCustomerManager()
							.getSsidByProtocol(protocol);
				}
				if (alSsid != null && alSsid.size() > 0) {
					for (int i = 0; i < alSsid.size(); i++) {
						int ssid = alSsid.get(i);
						
						// V4.0 Async Routing: Register SSID for DR polling
						com.king.gmms.messagequeue.DRStreamConsumer.getInstance().registerSSID(ssid);
						
						if (ctm.inCurrentA2P(ctm.getConnectedRelay(ssid,
								GmmsMessage.AIC_MSG_TYPE_TEXT))) {
							OperatorMessageQueue queue = factory
									.getOperatorMessageQueue(ssid);
							if (queue == null) {
								queue = factory
										.constructOperatorMessageQueue(ssid);
							}
						}
					}
				} // end of size > 0
				else {
					log.info("No client is started directly.");
				}
				//receiverThreadPool = executorServiceManager.newFixedThreadPool(this, "IntReceiver_commonHttpServer", 10);
				startAgentConnection(factory);
			}
			super.startService();
		} catch (Exception ex) {
			log.debug(ex, ex);

		}
	}		

	public void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		String requestURL = request.getRequestURL().toString();
		if(log.isDebugEnabled()){
			log.debug("Received a message from {}", requestURL);
		}
		A2PCustomerInfo csts = null;
		A2PSingleConnectionInfo sInfo = null;

		String receiveData = recieveData(request);
		
		log.debug("received message is:{}", receiveData);
		GmmsMessage msg = new GmmsMessage();
		try {			
			JSONObject  jsonObj = JSONObject.parseObject(receiveData);			
	         if (jsonObj == null) {
	        	 log.error("convert JxMessage error!");
	 			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
	 			this.response(msg, request, response, 0,"", "");
	 			return;
			}	         
	         String requestId = jsonObj.getString("requestId");
	         String phoneNumber = jsonObj.getString("phoneNumber");
	         String senderId = jsonObj.getString("senderId");
	         String content = jsonObj.getString("content");
	         String smsType = jsonObj.getString("smsType");
	         String supplier = jsonObj.getString("supplier");
	         String method = jsonObj.getString("method");
	         String signature = jsonObj.getString("signature");
	         String timestamp = jsonObj.getString("timestamp");	         
	         String extendRedis = jsonObj.getString("extend");	         
	         String extendValue = null;
	         if(jsonObj.getString("extend")!=null) {
	        	 JSONObject  exJSON = new JSONObject();
	        	 exJSON.put("extend", jsonObj.getString("extend"));	
	        	 extendValue = exJSON.toString().substring(11, exJSON.toString().length()-2);
	         }
	         	         
			 csts = gmmsUtility.getCustomerManager().getCustomerBySpID(supplier);
			 sInfo = (A2PSingleConnectionInfo) csts;
			 int split = MessageUtils.splitNum(content); 
			 if (sInfo == null) {
					log.debug("customerInfo == {} by serverid = {}", sInfo,
							supplier);
					msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					this.response(msg, request, response, 0,requestId, "");
					return;
				}
			  String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo.getSSID(), moduleIndex);
			  String inmsgid = commonMsgID.replace("_", "");
			   if ("false".equalsIgnoreCase(sInfo.getAuthKey())) {
				     msg.setStatus(GmmsStatus.INSUFFICIENT_BALANCE_CODE);
					this.response(msg, request, response, 0,requestId, inmsgid);
					return;
			    }
			    String autkey = sInfo.getChlPasswordr();
				
				String signaturevalue = SMSSignatureUtil.getCainiaoSignature(supplier,
				  method, requestId, timestamp, phoneNumber, senderId, content, smsType,
				  extendValue, autkey); if(signature == null ||
				  !signature.equalsIgnoreCase(signaturevalue)) {
				  msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR); this.response(msg, request,
				  response, 0,requestId, inmsgid); return; }
				 
			    // throttling control process
				try {
					if (!checkIncomingThrottlingControl(csts.getSSID(), msg)) {
						msg.setStatus(GmmsStatus.Throttled);
						this.response(msg, request, response, 0,requestId, inmsgid);
						return;
					}
				} catch (Exception e) {
					log.warn(
							"Error occur when processing throttling control in CommonMessageHttpHandler.parseRequest",
							e);
				}

				if (content!=null && !content.isEmpty()) {
					GmmsMessage gmmsMessage  = new GmmsMessage();
					gmmsMessage.setOSsID(sInfo.getSSID());										
					
					// V4.0 Sticky Routing Context
					gmmsMessage.setInnerTransaction(new com.king.gmms.ha.TransactionURI());
					
					gmmsMessage.setMsgID(commonMsgID);											
					gmmsMessage.setTextContent(content);
					gmmsMessage.setSenderAddress(senderId);						
					gmmsMessage.setRecipientAddress(phoneNumber);
					String type =MessageUtils.getCharset(content);
					gmmsMessage.setContentType(type);
					int len = content.getBytes(gmmsMessage.getContentType()).length;
					gmmsMessage.setMessageSize(len);
					gmmsMessage.setInMsgID(requestId);											
					gmmsMessage.setDeliveryReport(!ctm.isRssidNotSupportDR(sInfo.getSSID()));
					msg.setInMsgID(gmmsMessage.getInMsgID());
                    if (!StringUtility.stringIsNotEmpty(gmmsMessage.getSenderAddress())) {
                    	msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR); 
                    	log.debug("sender error!");
                		this.response(msg, request, response, 0,requestId, inmsgid);
                		return;
					}
                    
                    if (!StringUtility.stringIsNotEmpty(gmmsMessage.getRecipientAddress())
                    		|| !csts.isCheckRecipitLen(gmmsMessage.getRecipientAddress())) {
                    	msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);  
                    	log.debug("recipient error!");
                		this.response(msg, request, response, 0,requestId, inmsgid);
                		return;
					}
                    
                    if (!StringUtility.stringIsNotEmpty(gmmsMessage.getTextContent())) {
                    	msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);       
                    	log.debug("msg content is null error!");
                		this.response(msg, request, response, 0,requestId, inmsgid);
                		return;
					}else {
						if (gmmsMessage.getTextContent().getBytes(gmmsMessage.getContentType()).length > sInfo.getCustomerSupportLength()) {
							msg.setStatus(GmmsStatus.INVALID_MSG_FIELD); 
							log.debug("msg count value error! {}, {}, {}", gmmsMessage.getContentType(), gmmsMessage.getTextContent().getBytes(gmmsMessage.getContentType()).length, sInfo.getCustomerSupportLength());
                    		this.response(msg, request, response, 0,requestId, inmsgid);
                    		return;
						}
					}
					
					//charge recipient 重复次数
					int count = sInfo.getSmsOptionRecipientMaxSendCountIn24H();
					if(count>0){
						String key = "Duplicate:"+gmmsMessage.getRecipientAddress();
						long currentCount = redis.incrString(key);
						if(currentCount == 1){
							redis.setExpire(key, 24*60*60);
						}
						if(currentCount>count){
							msg.setStatus(GmmsStatus.RECIPIENT_ERROR_BY_MAX_COUNT);
							this.response(msg, request, response, 0,requestId, inmsgid);
							return;
						}
					}
					
					//charge the msg
					if (sInfo.isSmsOptionChargeByGateWay()) {
					     int count1 = GmmsUtility.calculateContentSize(gmmsMessage.getTextContent(), gmmsMessage.getContentType());
					     long stock = redis.stock("stock:"+sInfo.getSSID(), count1);
					     if (stock<0) {
					    	 if(log.isInfoEnabled()){
	                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
	                		}
					    	 msg.setStatus(GmmsStatus.INSUFFICIENT_BALANCE_CODE);
							 this.response(msg, request, response, 0,requestId, inmsgid);
							 return;
						 }
				    }
					
					//redis ali extend info for dr, 过期时间设置 					
					int exTime = 345600;
					String expireTime = sInfo.getSMSOptionHttpCustomParameter();
					if(expireTime!=null && !expireTime.isEmpty()) {
						exTime = Integer.parseInt(expireTime.trim());
					}
					
					String splitExtend = System.currentTimeMillis()+"_"+split+"_";
					if(extendRedis!=null && !extendRedis.isEmpty()) {
						splitExtend = splitExtend+ URLEncoder.encode(extendRedis,"UTF-8");
					}
					redis.setString("CainiaoMsg_"+requestId, splitExtend, exTime);
					if(sInfo.isSmsOptionChargeCountryByGateWay()){
                        try {
                        	String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
                        	Long stock = gmmsUtility.getRedisClient().stock("stock:"+sInfo.getSSID()+":"+cc, 1);
      					     if (stock!=null && stock<0) {
      					    	 if(log.isInfoEnabled()){
      		                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
      		                		}					    	 					    	
      					    	 msg.setStatus(GmmsStatus.INSUFFICIENT_BALANCE_CODE);
								 this.response(msg, request, response, 0,requestId, inmsgid);
								 return;
      						 }
						} catch (Exception e) {
							log.warn(msg,"Fail to send response");
							return;
						}       					     
                   	  }                        
					
					gmmsMessage.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
					gmmsMessage.setInTransID(Long.toString(System.currentTimeMillis()));				
					gmmsMessage.setTimeStamp(gmmsUtility.getGMTTime());
					
					// V4.0 Async Submit-MQ: Produce to Redis Stream instead of local memory queue
					if (!com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceSubmitMessage(gmmsMessage)) {
						gmmsUtility.getCdrManager().logInSubmit(gmmsMessage);
						if (gmmsMessage.getDeliveryReport()) {
							gmmsMessage.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
							gmmsMessage.setRSsID(sInfo.getSSID());
							gmmsMessage.setOutMsgID(gmmsMessage.getInMsgID());
							gmmsMessage.setStatus(GmmsStatus.REJECTED);
							// Attempt to produce DR directly (or via report pool)
							com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceDeliveryReport(gmmsMessage);
						}							
					}					
				}
			msg.setStatus(GmmsStatus.SUCCESS);
			this.response(msg, request, response, split,requestId, inmsgid);	
		} catch (Exception e) {
			log.error("receive cainiao msg throw exception", e);
			msg.setStatus(GmmsStatus.SERVER_ERROR);
 			this.response(msg, request, response, 0, "","");
 			return;
		}
		
		
	}

	public void response(GmmsMessage msg,
			HttpServletRequest request, HttpServletResponse response, int split, String requestId, String inmsgid)
			throws ServletException, IOException {		
		String outCharset = "application/json;charset=UTF-8";
		response.setContentType(outCharset);
		String respContent = null;
		respContent = makeAliCainiaoResponse(msg);
		
		if (StringUtility.stringIsNotEmpty(respContent)) {
			respContent = "{\"bizId\":\""+inmsgid+"\",\"requestId\":\""+requestId+"\",\"smsSize\":\""+split+"\","+respContent+"}";
		}else {
			respContent = "{}";
		}
		
		if(log.isInfoEnabled()){
			log.info(msg,"send response {}", respContent);
		}
	
		OutputStream o = null;
		try {
			o = response.getOutputStream();
			o.write((respContent + "").getBytes());
			o.flush();			
		} catch (Exception e) {
			log.error(e, e);
		} finally {
			if (o != null) {
				o.close();
			}
		}
	}

	public void response(String interfaceName, HttpStatus hs, GmmsMessage msg,
			HttpServletRequest request, HttpServletResponse response,
			ServletResponseParameter responseParameter)
			throws ServletException, IOException {		
		String outCharset = "text/html;charset=uft-8";
		response.setContentType(outCharset);
		String respContent = null;
		respContent = makeResponse(msg);
		if(log.isInfoEnabled()){
			log.info(msg,"send response {}", respContent);
		}

		OutputStream o = null;
		try {
			o = response.getOutputStream();
			o.write((respContent + "").getBytes());
			o.flush();			
		} catch (Exception e) {
			log.error(e, e);
		} finally {
			if (o != null) {
				o.close();
			}
		}
	}

	public HttpStatus parseRequest(String interfaceName, List<GmmsMessage> msgs,
			HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		if (hi == null) {
			return null;
		}
		for(GmmsMessage msg: msgs) {
			String msgID = MessageIdGenerator.generateCommonStringID();
			msg.setMsgID(msgID);
			String commonInMsgID = MessageIdGenerator.generateCommonStringID();
			msg.setInMsgID(commonInMsgID);
			msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
			msg.setInTransID(Long.toString(System.currentTimeMillis()));
			msg.setTextContent(null);
			msg.setTimeStamp(gmmsUtility.getGMTTime());
		}
		HttpStatus hs = null;
		CommonMessageHttpHandler commonMsgHandler = hi
				.getCommonMessageHandler();
		hs = commonMsgHandler.parseRequest(msgs, request);
		return hs;
	}
	
	public void response(String interfaceName, HttpStatus hs, GmmsMessage msg,
			HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		int ssid = msg.getOSsID();

		log.debug("ssid = {}", ssid);

		A2PCustomerInfo csts = ctm.getCustomerBySSID(ssid);

		String respContent = null;
		HttpPdu submitResp = hi.getMoSubmitResponse();
		if (submitResp.hasHandlerClass()) {
			String className = submitResp.getHandlerClass();
			Object[] args = { hs, msg, csts };
			respContent = (String) hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_MAKERESPONSE, args);
			if(log.isDebugEnabled()){
        		log.debug(msg, "Invoke handlerclass {}'s method:{}",
					className ,HttpConstants.HANDLER_METHOD_MAKERESPONSE);
			}
		} else {
			CommonMessageHttpHandler commonMsgHandler = hi
					.getCommonMessageHandler();
			respContent = commonMsgHandler.makeResponse(hs, msg, csts);
		}

		if(log.isInfoEnabled()){
			log.info(msg,"send response {}", respContent);
		}
		OutputStream o = null;
		try {
			o = response.getOutputStream();
			o.write((respContent + "").getBytes());
			o.flush();
		} catch (Exception e) {
			log.error(e, e);
		} finally {
			if (o != null) {
				o.close();
			}
		}
	}	
	
	public static String recieveData(HttpServletRequest request){
        String inputLine = null;
        // 接收到的数据
        StringBuffer recieveData = new StringBuffer();
        BufferedReader in = null;
        try
        {
            in = new BufferedReader(new InputStreamReader(
                    request.getInputStream(), "UTF-8"));
            while ((inputLine = in.readLine()) != null)
            {
                recieveData.append(inputLine);
            }
        }
        catch (IOException e)
        {
        }
        finally
        {            
            try
            {
                if (null != in)
                {
                    in.close();
                }
            }
            catch (IOException e)
            {
            }            
        }
        
        return recieveData.toString();
    }		
	
	
}
