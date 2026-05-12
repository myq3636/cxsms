package com.king.gmms.listener.commonhttpserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
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

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.PhoneUtils;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.customerconnectionfactory.CommonHttpServerFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.HttpInterfaceManager;
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
import com.king.gmms.protocol.smpp.util.TimeFormatter;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;
import com.king.redis.RedisClient;
import com.king.rest.util.StringUtility;
import com.sun.mail.handlers.message_rfc822;

public class CommonHttpServlet extends AbstractHttpServer {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonHttpServlet.class);
	private static A2PCustomerManager ctm = null;
	private static HttpInterfaceManager him = null;
	private static CommonHttpServerFactory factory = null;
	protected ExecutorServiceManager executorServiceManager;
	protected ExecutorService receiverThreadPool;
	private BillingCounter billingCounter = BillingCounter.getInstance();
	private RedisClient redis = null;

	/**
	 * init factory
	 */
	public void init() {
		super.init();
		ctm = gmmsUtility.getCustomerManager();
		him = gmmsUtility.getHttpInterfaceManager();
		redis = gmmsUtility.getRedisClient();
		executorServiceManager = gmmsUtility.getExecutorServiceManager();
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
				
				// V4.0 Async Routing: Register all HTTP SSIDs for DR polling
				if (alSsid != null) {
					for (int ssid : alSsid) {
						com.king.gmms.messagequeue.DRStreamConsumer.getInstance().registerSSID(ssid);
					}
				}
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
			/*JAXBContext context = JAXBContext.newInstance(JxMessage.class);  
	         Unmarshaller unmarshaller = context.createUnmarshaller();  
	         JxMessage  jxMessage = (JxMessage) unmarshaller.unmarshal(new StringReader(receiveData));		
	         */
			JxMessage  jxMessage = domXml(receiveData);
			
	         if (jxMessage == null || jxMessage.getAUTHENTICATION() == null || jxMessage.getAUTHENTICATION().getPRODUCTTOKEN() == null) {
	        	 log.error("convert JxMessage error!");
	 			msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
	 			this.response(msg, request, response);
	 			return;
			}
			 String username = jxMessage.getAUTHENTICATION().getPRODUCTTOKEN();

			 csts = gmmsUtility.getCustomerManager().getCustomerBySpID(username);
			 sInfo = (A2PSingleConnectionInfo) csts;
			 if (sInfo == null) {

					log.debug("customerInfo == {} by serverid = {}", sInfo,
							username);
					msg.setStatus(GmmsStatus.AUTHENTICATION_ERROR);
					this.response(msg, request, response);
					return;
				}

			   if ("false".equalsIgnoreCase(sInfo.getAuthKey())) {
				    msg.setStatus(GmmsStatus.INSUFFICIENT_BALANCE_CODE);
					this.response(msg, request, response);
					return;
			    }			   			   
			   
				// throttling control process
				try {
					if (!checkIncomingThrottlingControl(csts.getSSID(), msg)) {
						msg.setStatus(GmmsStatus.Throttled);
						this.response(msg, request, response);
						return;
					}
				} catch (Exception e) {
					log.warn(
							"Error occur when processing throttling control in CommonMessageHttpHandler.parseRequest",
							e);
				}
				

				if (jxMessage.getMSG()!=null && !jxMessage.getMSG().isEmpty()) {
					for (JxSubMessage jxSubMsg : jxMessage.getMSG()) {
						GmmsMessage gmmsMessage  = new GmmsMessage();
						gmmsMessage.setOSsID(sInfo.getSSID());
						
						// V4.0 Sticky Routing Context
						gmmsMessage.setInnerTransaction(new com.king.gmms.ha.TransactionURI());
						
						String commonMsgID = MessageIdGenerator.generateCommonMsgID(sInfo.getSSID());
						gmmsMessage.setMsgID(commonMsgID);
						String content = jxSubMsg.getBODY();						
						gmmsMessage.setTextContent(content);
						gmmsMessage.setSenderAddress(jxSubMsg.getFROM());						
						gmmsMessage.setRecipientAddress(jxSubMsg.getTO());
						if(StringUtility.stringIsNotEmpty(jxSubMsg.getRSSID())){
							gmmsMessage.setRSsID(Integer.parseInt(jxSubMsg.getRSSID()));
						}						
						String type =jxSubMsg.getDCS();
						if(type == null || "0".equals(type.trim())) {
							gmmsMessage.setContentType(GmmsMessage.AIC_CS_ASCII);
						}else {
							gmmsMessage.setContentType(GmmsMessage.AIC_CS_UCS2);
						}
						int len = content.getBytes(gmmsMessage.getContentType()).length;
						gmmsMessage.setMessageSize(len);
						if (jxSubMsg.getREFERENCE() !=null) {
							gmmsMessage.setInMsgID(jxSubMsg.getREFERENCE());							
						}else {
							String commonInMsgID = MessageIdGenerator.generateCommonStringID();
							gmmsMessage.setInMsgID(commonInMsgID);							
						}									
						
						gmmsMessage.setDeliveryReport(!ctm.isRssidNotSupportDR(sInfo.getSSID()));
						msg.setInMsgID(gmmsMessage.getInMsgID());
                        if (!StringUtility.stringIsNotEmpty(gmmsMessage.getSenderAddress())) {
                        	msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);                        	
                    		this.response(msg, request, response);
                    		return;
						}
                        
                        if (!StringUtility.stringIsNotEmpty(gmmsMessage.getRecipientAddress())
                        		|| !csts.isCheckRecipitLen(gmmsMessage.getRecipientAddress())) {
                        	msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);                        	
                    		this.response(msg, request, response);
                    		return;
						}
                        
                        if (!StringUtility.stringIsNotEmpty(gmmsMessage.getTextContent())) {
                        	msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);                        	
                    		this.response(msg, request, response);
                    		return;
						}else {
							if (gmmsMessage.getTextContent().getBytes(gmmsMessage.getContentType()).length > sInfo.getCustomerSupportLength()) {
								msg.setStatus(GmmsStatus.INVALID_MSG_FIELD);                        	
	                    		this.response(msg, request, response);
	                    		return;
							}
						}
						String dateString = jxSubMsg.getVALIDITY();
						if (dateString!=null) {
							try {
								Date date = TimeFormatter.parse(dateString);
								gmmsMessage.setExpiryDate(date);
							} catch (Exception e) {
								log.warn("the Validity {} format is error.", dateString);
							}
						}
						
						gmmsMessage.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
						gmmsMessage.setInTransID(Long.toString(System.currentTimeMillis()));				
						gmmsMessage.setTimeStamp(gmmsUtility.getGMTTime());
						
						
						
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
								this.response(msg, request, response);
								return;
							}
						}
						
						//charge the msg
						if (sInfo.isSmsOptionChargeByGateWay()) {
						     count = GmmsUtility.calculateContentSize(gmmsMessage.getTextContent(), gmmsMessage.getContentType());
						     long stock = redis.stock("stock:"+sInfo.getSSID(), count);
						     if (stock<0) {
						    	 if(log.isInfoEnabled()){
   		                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
   		                		 }
						    	 msg.setStatus(GmmsStatus.INSUFFICIENT_BALANCE_CODE);
								 this.response(msg, request, response);
								 return;
							 }
					    }
						
						if(sInfo.isSmsOptionChargeCountryByGateWay()){
                            try {
                            	String cc = PhoneUtils.getRegionCodeByPhone(msg.getRecipientAddress());
                            	Long stock = gmmsUtility.getRedisClient().stock("stock:"+sInfo.getSSID()+":"+cc, 1);
          					     if (stock!=null && stock<0) {
          					    	 if(log.isInfoEnabled()){
          		                        	log.info(msg, "Request PDU is blocked by billing, and msg is {}", msg);
          		                		}					    	 					    	
          					    	 msg.setStatus(GmmsStatus.INSUFFICIENT_BALANCE_CODE);
    								 this.response(msg, request, response);
    								 return;
          						 }
							} catch (Exception e) {
								log.warn(msg,"Fail to send response");
								return;
							}       					     
                       	  }    
						
						// V4.0 异步化逻辑：将消息提交到 Redis Stream (Submit-MQ)
						boolean produceSuccess = StreamQueueManager.getInstance().produceSubmitMessage(gmmsMessage);
						if (!produceSuccess) {
							log.error(gmmsMessage, "Failed to produce message to Redis Stream!");
							// 如果入队失败（Redis故障），记录 CDR 并尝试返回错误
							gmmsUtility.getCdrManager().logInSubmit(gmmsMessage);
							msg.setStatus(GmmsStatus.SERVER_ERROR);
							this.response(msg, request, response);
							return;
						}
						// 入队成功即视为处理成功
					}
				}
		} catch (Exception e) {
			e.printStackTrace();
			msg.setStatus(GmmsStatus.SERVER_ERROR);
 			this.response(msg, request, response);
 			return;
		}
		
		msg.setStatus(GmmsStatus.SUCCESS);
		this.response(msg, request, response);
	}

	public void response(GmmsMessage msg,
			HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {		
		String outCharset = "text/html;charset=UTF-8";
		response.setContentType(outCharset);
		String respContent = null;
		respContent = makeResponse(msg);
		if (StringUtility.stringIsNotEmpty(respContent)) {
			respContent = msg.getInMsgID()+":"+respContent;
		}else {
			respContent = msg.getInMsgID();
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

	private HttpStatus parseRequest(String interfaceName, GmmsMessage msg,
			HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		if (hi == null) {
			return null;
		}
		String commonInMsgID = MessageIdGenerator.generateCommonStringID();
		msg.setInMsgID(commonInMsgID);
		msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		msg.setInTransID(Long.toString(System.currentTimeMillis()));
		msg.setTextContent(null);
		msg.setTimeStamp(gmmsUtility.getGMTTime());

		HttpStatus hs = null;

		HttpPdu submitReq = hi.getMoSubmitRequest();
		if (submitReq.hasHandlerClass()) {
			String className = submitReq.getHandlerClass();
			Object[] args = { msg, request };
			hs = (HttpStatus) hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSEREQUEST, args);
			if(log.isDebugEnabled()){
        		log.debug(msg, "Invoke handlerclass {}'s method:{}",
					className ,HttpConstants.HANDLER_METHOD_PARSEREQUEST);
			}
		} else {
			CommonMessageHttpHandler commonMsgHandler = hi
					.getCommonMessageHandler();
			hs = commonMsgHandler.parseRequest(msg, request);
		}
		return hs;
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
	
	
	private JxMessage domXml(String xml) {
		StringReader reader = null;
		JxMessage jxMessage= new JxMessage();		 
		try {
			reader = new StringReader(xml);
			InputSource source = new InputSource(reader);
			SAXBuilder sb = new SAXBuilder();
			Document doc = sb.build(source);
			Element root = doc.getRootElement();
			List node = root.getChildren();
			if (node == null) {
				return null;
			}
			Element et = null;
			List<JxSubMessage> messages = new ArrayList<>();
			for (int i = 0; i < node.size(); i++) {
				et = (Element) node.get(i);
				if ("AUTHENTICATION".equalsIgnoreCase(et.getName())) {
					JxToken jxToken = new JxToken();
					jxToken.setPRODUCTTOKEN(et.getChild("PRODUCTTOKEN").getText());
					jxMessage.setAUTHENTICATION(jxToken);
				}else if ("MSG".equalsIgnoreCase(et.getName())){
					JxSubMessage subMessage = new JxSubMessage();
					List<Element> childern = et.getChildren();
					for(Element child: childern) {
						log.debug("msg key: {}:{}",child.getName(), child.getText());
						subMessage.setProperty(child.getName(), child.getText());
					}
					messages.add(subMessage);
				}
				
			}
			jxMessage.setMSG(messages);
		} catch (JDOMException e) {
			log.warn(" dom xml format error and exception is {}", e
					.getMessage());
			return null;
		} catch (IOException e) {
			log.warn("io exception{}", e.getMessage());
			return null;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
				}
			}
		}
		
		return jxMessage;
	}
	
}
