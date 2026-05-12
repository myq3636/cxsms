package com.king.gmms.messagequeue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.king.framework.SystemLogger;
import com.king.gmms.customerconnectionfactory.CommonHttpServerFactory;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.http.HttpInterface;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.listener.commonhttpserver.CommonHttpDRServlet;
import com.king.gmms.listener.commonhttpserver.CommonHttpServlet;
import com.king.gmms.listener.commonhttpserver.CommonRESTHttpDRQueryServlet;
import com.king.gmms.listener.commonhttpserver.CommonRESTHttpDRServlet;
import com.king.gmms.listener.commonhttpserver.CommonRESTHttpServlet;
import com.king.gmms.listener.commonhttpserver.RESTMultiAddrServletResponseParameter;
import com.king.gmms.listener.commonhttpserver.RESTServletResponseParameter;
import com.king.gmms.listener.commonhttpserver.ServletResponseParameter;
import com.king.gmms.protocol.commonhttp.HttpStatus;
import com.king.gmms.threadpool.RunnableMsgTask;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class ShortConnectionReceiverMessageQueue extends ReceiverMessageQueue {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ShortConnectionReceiverMessageQueue.class);
	private CommonHttpServerFactory factory = null;

	public ShortConnectionReceiverMessageQueue(A2PCustomerInfo cst,
			int minReceiverNum, int maxReceiverNum) {
		super(cst, minReceiverNum, maxReceiverNum,
				"ShortConnectionReceiverMessageQueue_" + cst.getSSID());
		factory = CommonHttpServerFactory.getInstance();
	}

	/**
	 * queue timeout
	 */
	public void timeout(Object msg) {
		GmmsMessage queuedMsg = (GmmsMessage) msg;
		String routerQueue = null;
		if (log.isInfoEnabled()) {
			log.info(queuedMsg, "{} is timeout in customer queue",
					queuedMsg.getMessageType());
		}
		try {
			if (queuedMsg != null) {
				if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
						.equalsIgnoreCase(queuedMsg.getMessageType())
						|| GmmsMessage.MSG_TYPE_SUBMIT_RESP
								.equalsIgnoreCase(queuedMsg.getMessageType())) {
					queuedMsg.setStatusCode(1);
					queuedMsg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
					TransactionURI innerTransaction = queuedMsg
							.getInnerTransaction();
					routerQueue = innerTransaction.getConnectionName();
					InternalAgentConnectionFactory factory = InternalAgentConnectionFactory
							.getInstance();
					OperatorMessageQueue msgQueue = factory.getMessageQueue(
							queuedMsg, routerQueue);
					msgQueue.putMsg(queuedMsg);
				}
			}
		} catch (Exception ex) {
			log.error(queuedMsg, ex, ex);
		}
	}

	/**
	 * send response to servlet
	 * 
	 * @param msg
	 */
	private void send2Servlet(GmmsMessage msg) {
		if (log.isTraceEnabled()) {
			log.trace(msg,
					"ShortConnectionReceiverMessageQueue send2Servlet:{}", msg);
		}

		String msgType = msg.getMessageType();
		try {
			if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msgType)) {
				//didn't need submit response to customer. so create inner ack to coreEngine.
				//msg.setMessageType(GmmsMessage.MSG_TYPE_INNER_ACK);
				//putGmmsMessage2RouterQueue(msg);
				//return;
				
				ServletResponseParameter servletparam = factory
						.getServletParam(msg.getMsgID());
				
				if (servletparam == null) {
					if (log.isInfoEnabled()) {
						log.info(msg,
								"Do not find the servlet to return response");
					}
					return;
				}
				String interfaceName = servletparam.getInterfaceName();
				HttpStatus hs = servletparam.getHttpStatus();

				// CoreEngine checkMessage may respond failed status, map here
				// as a common operation
				HttpInterface hi = gmmsUtility.getHttpInterfaceManager()
						.getHttpInterfaceMap().get(interfaceName);
				if (!(msg.getStatus().equals(GmmsStatus.UNASSIGNED))) {
					hs = hi.mapGmmsStatus2HttpSubStatus(msg.getStatus());
				} else {
					hs = hi.mapGmmsStatus2HttpSubStatus(GmmsStatus.SUCCESS);
				}

				HttpServletRequest request = servletparam.getRequest();
				HttpServletResponse response = servletparam.getResponse();

				if (servletparam instanceof RESTMultiAddrServletResponseParameter) {
					Map<String, GmmsMessage> recipientAddrMsgMap = servletparam
							.getRecipientMsgMap();

					recipientAddrMsgMap.put(msg.getRecipientAddress(), msg);

					synchronized (servletparam) {
						servletparam.getCdl().countDown();
						long count = servletparam.getCdl().getCount();
						if (count == 0) {
							Set<GmmsMessage> msgSet = new HashSet<GmmsMessage>(
									recipientAddrMsgMap.values());
							CommonRESTHttpServlet servlet = (CommonRESTHttpServlet) servletparam
									.getServlet();
							servlet.response(interfaceName, hs, msgSet,
									request, response,
									(RESTServletResponseParameter) servletparam);
						}
					}

				} else if (servletparam instanceof RESTServletResponseParameter) {

					CommonRESTHttpServlet servlet = (CommonRESTHttpServlet) servletparam
							.getServlet();

					servlet.response(interfaceName, hs, msg, request, response,
							(RESTServletResponseParameter) servletparam);

				} else {
					CommonHttpServlet servlet = (CommonHttpServlet) servletparam
							.getServlet();
					servlet.response(interfaceName, hs, msg, request, response,
							servletparam);
				}
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
					.equalsIgnoreCase(msgType)) {
				ServletResponseParameter servletparam = factory
						.getServletParam(msg.getOutMsgID());
				if (servletparam == null) {
					if (log.isInfoEnabled()) {
						log.info(msg,
								"Do not find the servlet to return response");
					}
					return;
				}
				String interfaceName = servletparam.getInterfaceName();
				HttpStatus hs = servletparam.getHttpStatus();
				HttpServletResponse response = servletparam.getResponse();
				if (servletparam instanceof RESTServletResponseParameter) {

					CommonRESTHttpDRServlet servlet = (CommonRESTHttpDRServlet) servletparam
							.getServlet();
					servlet.response(interfaceName, hs, msg, response,
							(RESTServletResponseParameter) servletparam);

				} else {
					CommonHttpDRServlet servlet = (CommonHttpDRServlet) servletparam
							.getServlet();
					servlet.response(interfaceName, hs, msg, response,
							servletparam);
				}
			}// add by kevin for REST new requirement
			else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP
					.equalsIgnoreCase(msgType)) {
				ServletResponseParameter servletparam = factory
						.getServletParam(msg.getInMsgID());

				if (servletparam == null) {
					if (log.isInfoEnabled()) {
						log.info(msg,
								"Do not find the servlet to return response");
					}
					return;
				}
				if (servletparam instanceof RESTServletResponseParameter) {

					String interfaceName = servletparam.getInterfaceName();

					HttpStatus hs = servletparam.getHttpStatus();
					HttpServletResponse response = servletparam.getResponse();
					CommonRESTHttpDRQueryServlet servlet = (CommonRESTHttpDRQueryServlet) servletparam
							.getServlet();
					servlet.response(interfaceName, hs, msg, response,
							(RESTServletResponseParameter) servletparam);

				} else {
					return;
				}

			}

		} catch (Exception e) {
			log.error(msg, "send2Servlet exception: ", e);
		}
	}

	private final class ShortConnectionReceiver extends RunnableMsgTask {

		public ShortConnectionReceiver(GmmsMessage msg) {
			this.message = msg;
		}

		@Override
		public void run() {
			if (message != null) {
				send2Servlet(message);
			}
		}

	}

	/**
	 * @param msg
	 * @return
	 * @see com.king.gmms.messagequeue.ReceiverMessageQueue#putMsg(com.king.message.gmms.GmmsMessage)
	 */
	@Override
	public boolean putMsg(GmmsMessage msg) {
		if (msg == null) {
			return false;
		}
		if (log.isTraceEnabled()) {
			log.trace(msg, "submit to ShortConnectionReceiver thread pool");
		}

		try {
			receiverThreadPool.execute(new ShortConnectionReceiver(msg));
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info(msg, e, e);
			}

			return false;
		}

		return true;
	}
}
