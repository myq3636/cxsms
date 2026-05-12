package com.king.gmms.listener.commonhttpserver;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;

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
import com.king.gmms.protocol.commonhttp.CommonDeliveryReportRESTXmlHttpHandler;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.message.gmms.MessageIdGenerator;

public class CommonRESTHttpDRServlet extends AbstractHttpServer {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommonHttpDRServlet.class);
	private static A2PCustomerManager ctm = null;
	private static HttpInterfaceManager him = null;
	private static CommonHttpServerFactory factory = null;

	/**
	 * init factory
	 */
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
						
						// V4.0 Async Routing: Register SSID for DR support
						com.king.gmms.messagequeue.DRStreamConsumer.getInstance().registerSSID(ssid);
						
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
		log.debug("Received a message from {}", requestURL);
		String[] args = requestURL.split("/");
		int interfaceIndex = args.length - 2;
		String interfaceName = args[interfaceIndex];
		if (log.isDebugEnabled()) {
			log.debug("interfaceName = {}", interfaceName);
		}
		
		him.initHttpInterfaceMap(interfaceName);
		
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		
		GmmsMessage msg = new GmmsMessage();
		String msgID = MessageIdGenerator.generateCommonStringID();
		msg.setMsgID(msgID);

		String contentTypeStr = request.getContentType();

		if (contentTypeStr == null || contentTypeStr.trim().isEmpty()) {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			if (log.isDebugEnabled()) {
				log.debug(msg, "ContentType is null");
			}
			this.response(interfaceName,
					hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN), msg,
					response);
			return;
		}
		String contentType[] = contentTypeStr.split(";");

		if (!(HttpConstants.MEDIA_TYPE_XML.equalsIgnoreCase(contentType[0])
				|| HttpConstants.MEDIA_TYPE_JSON
						.equalsIgnoreCase(contentType[0]) || HttpConstants.MEDIA_TYPE_URLENCODE
				.equalsIgnoreCase(contentType[0]))) {
			msg.setStatus(GmmsStatus.UNKNOWN);
			if (log.isDebugEnabled()) {
				log.debug(msg, "ContentType error!");
			}
			this.response(interfaceName,
					hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.UNKNOWN), msg,
					response);
			return;
		}

		HttpStatus hs = this.parseDrRequest(interfaceName, msg, request,
				response);

		if (log.isInfoEnabled()) {
			log.info(msg, "Receive request {}", msg.toString());
		}
		RESTServletResponseParameter responseParameter = new RESTServletResponseParameter(
				interfaceName, hs, request, response, this, contentType[0]);
		if (msg.getMsgID() == null) {
			msg.setMsgID(msgID);
		}
		if (msg.getOutMsgID() == null) {
			msg.setOutMsgID(msgID);
		}

		if (hi != null) {
			if (hi.isSuccessDRRespStatus(hs)) {
				// V4.1 Async Feedback Loop: Produce result (DR) to Redis Stream
				if (!com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceResult(msg)) {
					msg.setStatus(GmmsStatus.ENROUTE);
					gmmsUtility.getCdrManager().logOutDeliveryReportRes(msg);
				}
				this.response(interfaceName, hs, msg, response);
			} else {// isn't DELEVRED DR status
				msg.setStatus(hi.mapHttpDRRespStatus2GmmsStatus(hs));
				this.response(interfaceName, hs, msg, response);
				gmmsUtility.getCdrManager().logOutDeliveryReportRes(msg);
			}
		} else {// hi is null
			msg.setStatus(GmmsStatus.UNKNOWN);
			this.response(interfaceName, hs, msg, response);
			gmmsUtility.getCdrManager().logOutDeliveryReportRes(msg);
		}

	}

	public void response(String interfaceName, HttpStatus hs, GmmsMessage msg,
			HttpServletResponse response) throws ServletException, IOException {
		int ssid = msg.getRSsID();

		A2PCustomerInfo cst = ctm.getCustomerBySSID(ssid);

		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		HttpPdu drResp = hi.getMtDRResponse();
		String respContent = null;
		if (drResp.hasHandlerClass()) {
			String className = drResp.getHandlerClass();
			Object[] args = { hs, msg, cst };
			respContent = (String) hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_MAKERESPONSE, args);
			if (log.isDebugEnabled()) {
				log.debug(msg, "Invoke handlerclass {}'s method:{}", className,
						HttpConstants.HANDLER_METHOD_MAKERESPONSE);
			}
		} else {
			CommonDeliveryReportRESTXmlHttpHandler commonDRHandler = hi
					.getCommonDeliveryReportRESTXmlHandler();
			respContent = commonDRHandler.makeResponse(hs, msg, cst);
		}

		if (log.isInfoEnabled()) {
			log.info(msg, "send dr response {}", respContent);
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


	private HttpStatus parseDrRequest(String interfaceName, GmmsMessage msg,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		HttpStatus hs = null;
		HttpInterface hi = him.getHttpInterfaceMap().get(interfaceName);
		HttpPdu drRequest = hi.getMtDRRequest();
		msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		if (drRequest.hasHandlerClass()) {
			String className = drRequest.getHandlerClass();
			Object[] args = { msg, request };
			hs = (HttpStatus) hi.invokeHandler(className,
					HttpConstants.HANDLER_METHOD_PARSEREQUEST, args);
			if (log.isDebugEnabled()) {
				log.debug(msg, "Invoke handlerclass {}'s method:{}", className,
						HttpConstants.HANDLER_METHOD_PARSEREQUEST);
			}
		} else {
			CommonDeliveryReportRESTXmlHttpHandler commonDRHandler = hi
					.getCommonDeliveryReportRESTXmlHandler();
			hs = commonDRHandler.parseRequest(msg, request);
		}
		return hs;
	}

}
