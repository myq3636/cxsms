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

import com.alibaba.fastjson.JSONObject;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
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
import com.king.gmms.protocol.commonhttp.CreditCountrySub;
import com.king.gmms.protocol.commonhttp.CreditCountrySyncRequest;
import com.king.gmms.protocol.commonhttp.CreditSyncRequest;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.protocol.commonhttp.JxMessage;
import com.king.gmms.protocol.commonhttp.JxSubMessage;
import com.king.gmms.protocol.commonhttp.JxToken;
import com.king.gmms.protocol.smpp.util.TimeFormatter;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageBase;
import com.king.message.gmms.MessageIdGenerator;
import com.king.redis.RedisClient;
import com.king.rest.util.StringUtility;
import com.sun.mail.handlers.message_rfc822;

public class CreditCountrySyncHttpServlet extends AbstractHttpServer {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CreditCountrySyncHttpServlet.class);
	private static A2PCustomerManager ctm = null;
	private static HttpInterfaceManager him = null;
	private static CommonHttpServerFactory factory = null;
	protected ExecutorServiceManager executorServiceManager;
	protected ExecutorService receiverThreadPool;
	private BillingCounter billingCounter = BillingCounter.getInstance();

	/**
	 * init factory
	 */
	public void init() {
		super.init();		
		try {
			super.startService();
		} catch (Exception ex) {
			log.debug(ex, ex);

		}
		
		ctm = gmmsUtility.getCustomerManager();
		him = gmmsUtility.getHttpInterfaceManager();
	}

	public void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		String requestURL = request.getRequestURL().toString();
		if (log.isDebugEnabled()) {
			log.debug("Received a message from {}", requestURL);
		}
		String receiveData = recieveData(request);
		log.debug("received message is:{}", receiveData);
		String errorResponse = "{\"code\":301,\"data\":null,\"message\":\"Server Error\",\"tranId\":";
		try {

			JSONObject creditSync = JSONObject.parseObject(receiveData);
			
			if (creditSync == null) {
				log.error("convert CreditSyncRequest error!");
				errorResponse = errorResponse + "\"\"}";
				this.response(errorResponse, request, response);
				return;
			}
			String tranid = creditSync.getString("tranId");
			int requestSsid = creditSync.getIntValue("ssid");
			long ssidTimemark = creditSync.getLongValue("timemark");
			A2PCustomerInfo sInfo = ctm.getCustomerBySSID(requestSsid);
			String successResponse = "{\"code\":100,\"data\":null,\"message\":\"Success\",\"tranId\":\""
					+ tranid + "\"}";
			List<CreditCountrySub> countryCredits = JSONObject.parseArray(creditSync.getString("countryCredit"), CreditCountrySub.class);
			if(countryCredits == null || countryCredits.isEmpty()){
				this.response(successResponse, request, response);
				return;
			}
			boolean result = true;
			errorResponse = errorResponse + "\"" + tranid
					+ "\"}";
			for(CreditCountrySub countryCredit: countryCredits){
				String ccKey = sInfo.getSSID()+":"+countryCredit.getCc();
				String creditKey = "stock:" + ccKey;
				String redisCredit = gmmsUtility.getRedisClient().getString(creditKey);
				String timemarkKey = "stock:" + ccKey + ":timemark";
				long requestCredit = countryCredit.getCredit();
				String redisTimemark = gmmsUtility.getRedisClient().getString(timemarkKey);				
				if (StringUtility.stringIsNotEmpty(redisCredit) 
						&& StringUtility.stringIsNotEmpty(redisTimemark)) {
					long redisCreditL = Long.parseLong(redisCredit);
					
					long addCredit = requestCredit - redisCreditL;
					if (ssidTimemark > Long.parseLong(redisTimemark)) {
						gmmsUtility.getRedisClient().addStock(creditKey, ssidTimemark,
								addCredit);											
					} else {										
						log.info(
								"the credit request time is sync now, not need to sync credit. the tranId is {}",
								tranid);						
					}
				} else {
					if (gmmsUtility.getRedisClient().addStock(creditKey,
							ssidTimemark, requestCredit)) {												
					} else {						
						result = false;
					}
				}
			}
			if(result){
				this.response(successResponse, request, response);
			}else{
				this.response(errorResponse, request, response);
			}
			
		} catch (Exception e) {
			log.error("handle credit sync throw error", e);
			e.printStackTrace();
			String responseString = "{\"code\":301,\"data\":null,\"message\":\"Server Error\",\"tranId\":\"\"}";
			this.response(responseString, request, response);
			return;
		}
	}

	public void response(String respContent, HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		String outCharset = "application/json;charset=UTF-8";
		response.setContentType(outCharset);

		if (log.isInfoEnabled()) {
			log.info("send response {}", respContent);
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
		if (log.isInfoEnabled()) {
			log.info(msg, "send response {}", respContent);
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

	public HttpStatus parseRequest(String interfaceName,
			List<GmmsMessage> msgs, HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		if (hi == null) {
			return null;
		}
		for (GmmsMessage msg : msgs) {
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
			if (log.isDebugEnabled()) {
				log.debug(msg, "Invoke handlerclass {}'s method:{}", className,
						HttpConstants.HANDLER_METHOD_MAKERESPONSE);
			}
		} else {
			CommonMessageHttpHandler commonMsgHandler = hi
					.getCommonMessageHandler();
			respContent = commonMsgHandler.makeResponse(hs, msg, csts);
		}

		if (log.isInfoEnabled()) {
			log.info(msg, "send response {}", respContent);
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
			if (log.isDebugEnabled()) {
				log.debug(msg, "Invoke handlerclass {}'s method:{}", className,
						HttpConstants.HANDLER_METHOD_PARSEREQUEST);
			}
		} else {
			CommonMessageHttpHandler commonMsgHandler = hi
					.getCommonMessageHandler();
			hs = commonMsgHandler.parseRequest(msg, request);
		}
		return hs;
	}

	public static String recieveData(HttpServletRequest request) {
		String inputLine = null;
		// 接收到的数据
		StringBuffer recieveData = new StringBuffer();
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(
					request.getInputStream(), "UTF-8"));
			while ((inputLine = in.readLine()) != null) {
				recieveData.append(inputLine);
			}
		} catch (IOException e) {
		} finally {
			try {
				if (null != in) {
					in.close();
				}
			} catch (IOException e) {
			}
		}

		return recieveData.toString();
	}

	private JxMessage domXml(String xml) {
		StringReader reader = null;
		JxMessage jxMessage = new JxMessage();
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
					jxToken.setPRODUCTTOKEN(et.getChild("PRODUCTTOKEN")
							.getText());
					jxMessage.setAUTHENTICATION(jxToken);
				} else if ("MSG".equalsIgnoreCase(et.getName())) {
					JxSubMessage subMessage = new JxSubMessage();
					List<Element> childern = et.getChildren();
					for (Element child : childern) {
						log.debug("msg key: {}:{}", child.getName(),
								child.getText());
						subMessage
								.setProperty(child.getName(), child.getText());
					}
					messages.add(subMessage);
				}

			}
			jxMessage.setMSG(messages);
		} catch (JDOMException e) {
			log.warn(" dom xml format error and exception is {}",
					e.getMessage());
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
