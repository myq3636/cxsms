package com.king.gmms.listener.commonhttpserver;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.king.framework.SystemLogger;
import com.king.gmms.customerconnectionfactory.CommonHttpServerFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.HttpInterfaceManager;
import com.king.gmms.domain.http.HttpConstants;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.domain.http.HttpPdu;
import com.king.gmms.listener.AbstractHttpServer;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.protocol.commonhttp.CommonMessageRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.gmms.messagequeue.StreamQueueManager;
import com.king.message.gmms.MessageIdGenerator;

/**
 * submit message
 * 
 * @author kevinwang
 * 
 */

public class CommonRESTHttpServlet extends AbstractHttpServer {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonRESTHttpServlet.class);
	private static A2PCustomerManager ctm = null;
	private static HttpInterfaceManager him = null;
	private static CommonHttpServerFactory factory = null;

	public void init() {
		super.init();
		ctm = gmmsUtility.getCustomerManager();
		him = gmmsUtility.getHttpInterfaceManager();
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
								// queue.startMessageQueue();
							}
						}
					}
				} // end of size > 0
				else {
					log.info("No client is started directly.");
				}
				startAgentConnection(factory);
			}
			super.startService();
		} catch (Exception ex) {
			log.debug(ex, ex);

		}
	}

	

	@Override
	public void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {

		String requestURL = request.getRequestURL().toString();
		if (log.isDebugEnabled()) {
			log.debug("Received a message from {}", requestURL);
		}
		String[] args = requestURL.split("/");
		int interfaceIndex = args.length - 2;
		String interfaceName = args[interfaceIndex];
		him.initHttpInterfaceMap(interfaceName);

		GmmsMessage message = new GmmsMessage();

		String msgID = MessageIdGenerator.generateCommonStringID();
		message.setMsgID(msgID);
		message.setInMsgID(msgID);

		HttpStatus hs = this.parseRequest(interfaceName, message, request,
				response);

		String recipientAddr = message.getRecipientAddress();
		if (recipientAddr != null && !"".equals(recipientAddr.trim())) {

			String[] recipientAddrs = recipientAddr.split(",");

			if (recipientAddrs.length == 1) {

				this.sendToCoreEngine4SingleAddrMsg(message, interfaceName,
						request, response, hs);
			} else {
				this.sendToCoreEngine4MultiAddrMsg(message, interfaceName,
						request, response, hs, recipientAddrs);
			}

		} else {
			this.response(interfaceName, hs, message, request, response);
		}

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
			// default xml
			CommonMessageRESTXmlHttpHandler commonMsgHandler = hi
					.getCommonMessageRESTXmlHandler();
			respContent = commonMsgHandler.makeResponse(hs, msg, csts);
		}

		if (log.isInfoEnabled()) {
			log.info(msg, "send response {}", respContent);
		}
		if (respContent == null) {
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
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
			RESTServletResponseParameter responseParameter)
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

			// default xml
			CommonMessageRESTXmlHttpHandler commonMsgHandler = hi
					.getCommonMessageRESTXmlHandler();
			respContent = commonMsgHandler.makeResponse(hs, msg, csts);
		}

		// respContent==null 500 msg gmmsStatus

		if (respContent == null) {
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}

		OutputStream o = null;
		try {
			o = response.getOutputStream();
			if (respContent != null) {
				o.write(respContent.getBytes());
			}
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
		msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
		msg.setInTransID(Long.toString(System.currentTimeMillis()));
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
			CommonMessageRESTXmlHttpHandler commonMsgHandler = hi
					.getCommonMessageRESTXmlHandler();
			hs = commonMsgHandler.parseRequest(msg, request);
		}
		return hs;
	}

	//
	public void sendToCoreEngine4SingleAddrMsg(GmmsMessage message,
			String interfaceName, HttpServletRequest request,
			HttpServletResponse response, HttpStatus hs)
			throws ServletException, IOException {
		String contentTypeStr = request.getContentType();
		String[] contentType = contentTypeStr.split(";");
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);

		String msgID = MessageIdGenerator.generateCommonStringID();
		message.setInMsgID(msgID);

		if (!(HttpConstants.MEDIA_TYPE_XML.equalsIgnoreCase(contentType[0])
				|| HttpConstants.MEDIA_TYPE_JSON
						.equalsIgnoreCase(contentType[0]) || HttpConstants.MEDIA_TYPE_URLENCODE
				.equalsIgnoreCase(contentType[0]))) {
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			if (log.isDebugEnabled()) {
				log.debug(message, "ContentType error!");
			}
			this.response(interfaceName,
					hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN_ERROR),
					message, request, response);
			return;
		}

		ServletResponseParameter responseParameter = new RESTServletResponseParameter(
				interfaceName, hs, request, response, this, contentType[0]);

		if (hi != null) {
			if (hi.isSuccessMessageStatus(hs)) {
				// V4.4 Fast Accept: Directly produce to Redis Stream and respond immediately
				boolean enqueued = StreamQueueManager.getInstance().produceSubmitMessage(message);
				if (!enqueued) {
					message.setStatus(GmmsStatus.INSUBMIT_RESP_FAILED);
					gmmsUtility.getCdrManager().logInSubmit(message);
					this.response(
							interfaceName,
							hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN_ERROR),
							message, request, response);
				} else {
					// Enqueue success, return SUCCESS immediately
					message.setStatus(GmmsStatus.SUCCESS);
					this.response(interfaceName, 
							hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SUCCESS), 
							message, request, response);
				}
			} else {// isn't a success response
				message.setStatus(hi.mapHttpSubStatus2GmmsStatus(hs));
				this.response(interfaceName, hs, message, request, response);
				gmmsUtility.getCdrManager().logInSubmit(message);
			}
		} else {// hi is null
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			gmmsUtility.getCdrManager().logInSubmit(message);
			this.response(interfaceName, hs, message, request, response);
		}
	}

	public void sendToCoreEngine4MultiAddrMsg(GmmsMessage message,
			String interfaceName, HttpServletRequest request,
			HttpServletResponse response, HttpStatus hs, String[] recipientAddrs)
			throws ServletException, IOException {

		Set<GmmsMessage> msgSet = null;
		Map<String, GmmsMessage> recipientMsgMap = new ConcurrentHashMap<String, GmmsMessage>();
		String contentTypeStr = request.getContentType();
		String[] contentType = contentTypeStr.split(";");
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);

		for (String recipient : recipientAddrs) {
			GmmsMessage msg = new GmmsMessage(message);
			msg.setRecipientAddress(recipient);
			String inMsgID = MessageIdGenerator.generateCommonStringID();
			msg.setInMsgID(inMsgID);

			String commonMsgId = MessageIdGenerator.generateCommonMsgID(msg
					.getOSsID());
			msg.setMsgID(commonMsgId);

			recipientMsgMap.put(msg.getRecipientAddress(), msg);
		}

		msgSet = new HashSet<GmmsMessage>(recipientMsgMap.values());

		// add by kevin
		// recipient number max be 100
		if (msgSet != null && msgSet.size() > 100) {
			for (GmmsMessage msg : msgSet) {
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
				if (log.isDebugEnabled()) {
					log.debug(msg, "recipient numbers >100");
				}
			}
			this.response(interfaceName,
					hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN_ERROR),
					msgSet, request, response);
			return;
		}

		if (!(HttpConstants.MEDIA_TYPE_XML.equalsIgnoreCase(contentType[0])
				|| HttpConstants.MEDIA_TYPE_JSON
						.equalsIgnoreCase(contentType[0]) || HttpConstants.MEDIA_TYPE_URLENCODE
				.equalsIgnoreCase(contentType[0]))) {
			for (GmmsMessage msg : msgSet) {
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
				if (log.isDebugEnabled()) {
					log.debug(msg, "ContentType error!");
				}
			}
			this.response(interfaceName,
					hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN_ERROR),
					msgSet, request, response);
			return;
		}

		ServletResponseParameter responseParameter = new RESTMultiAddrServletResponseParameter(
				interfaceName, hs, request, response, this, contentType[0],
				recipientMsgMap);

		int failedCount = 0;
		int successCount = 0;

		if (hi != null) {

			if (hi.isSuccessMessageStatus(hs)) {

				for (GmmsMessage msg : msgSet) {
					if (!StreamQueueManager.getInstance().produceSubmitMessage(msg)) {
						msg.setStatus(GmmsStatus.INSUBMIT_RESP_FAILED);
						gmmsUtility.getCdrManager().logInSubmit(msg);
						failedCount++;
					} else {
						successCount++;
					}
				}
				if (failedCount > 0 && failedCount == msgSet.size()) {
					// all failed
					this.response(
							interfaceName,
							hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN_ERROR),
							msgSet, request, response);
				} else {
					// partial or all success: return SUCCESS
					// Note: client will get MsgIDs in the response and can query status later via DR or Query API
					this.response(
							interfaceName,
							hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SUCCESS),
							msgSet, request, response);
				}

			} else {// isn't a success response

				for (GmmsMessage msg : msgSet) {
					msg.setStatus(hi.mapHttpSubStatus2GmmsStatus(hs));
					gmmsUtility.getCdrManager().logInSubmit(msg);
				}
				this.response(interfaceName, hs, msgSet, request, response);
			}
		} else {// hi is null
			for (GmmsMessage msg : msgSet) {
				msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
				gmmsUtility.getCdrManager().logInSubmit(msg);
			}
			this.response(interfaceName, hs, msgSet, request, response);
		}
	}

	public void response(String interfaceName, HttpStatus hs,
			Set<GmmsMessage> msgSet, HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		if (msgSet == null || msgSet.isEmpty()) {
			return;
		} else {
			GmmsMessage msg = msgSet.iterator().next();
			int ssid = msg.getOSsID();
			if (log.isDebugEnabled()) {
				log.debug("ssid = {}", ssid);
			}
			A2PCustomerInfo csts = ctm.getCustomerBySSID(ssid);

			HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);

			String respContent = null;
			HttpPdu submitResp = hi.getMoSubmitResponse();
			if (submitResp.hasHandlerClass()) {
				String className = submitResp.getHandlerClass();

				Object[] args = new Object[] { hs, msgSet, csts };

				respContent = (String) hi.invokeHandler(className,
						HttpConstants.HANDLER_METHOD_MAKERESPONSE, args);
				if (log.isDebugEnabled()) {
					log.debug(msg, "Invoke handlerclass {}'s method:{}",
							className,
							HttpConstants.HANDLER_METHOD_MAKERESPONSE);
				}
			} else {
				CommonMessageRESTXmlHttpHandler commonMsgHandler = hi
						.getCommonMessageRESTXmlHandler();

				respContent = commonMsgHandler.makeResponse(hs, msgSet, csts);

			}
			if (log.isInfoEnabled()) {
				log.info(msg, "send response {}", respContent);
			}
			if (respContent == null) {
				response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
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
	}

	public void response(String interfaceName, HttpStatus hs,
			Set<GmmsMessage> msgSet, HttpServletRequest request,
			HttpServletResponse response,
			RESTServletResponseParameter responseParameter)
			throws ServletException, IOException {

		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		GmmsMessage msg = msgSet.iterator().next();
		int ssid = msg.getOSsID();

		if (log.isDebugEnabled()) {
			log.debug("ssid = {}", ssid);
		}

		A2PCustomerInfo csts = ctm.getCustomerBySSID(ssid);

		String respContent = null;
		HttpPdu submitResp = hi.getMoSubmitResponse();

		if (submitResp.hasHandlerClass()) {
			String className = submitResp.getHandlerClass();

			Object[] args = new Object[] { hs, msgSet, csts };

			respContent = (String) hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_MAKERESPONSE, args);
		} else {

			CommonMessageRESTXmlHttpHandler commonMsgHandler = hi
					.getCommonMessageRESTXmlHandler();
			respContent = commonMsgHandler.makeResponse(hs, msgSet, csts);
		}

		if (respContent == null) {
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
		}

		OutputStream o = null;
		try {
			o = response.getOutputStream();
			if (respContent != null) {
				o.write(respContent.getBytes());
			}
			o.flush();

		} catch (Exception e) {
			log.error(e, e);
		} finally {
			if (o != null) {
				o.close();
			}
		}
	}

}
