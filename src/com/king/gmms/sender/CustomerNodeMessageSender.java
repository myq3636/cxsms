package com.king.gmms.sender;

import java.io.IOException;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.messagequeue.NodeManager;
import com.king.gmms.processor.Processor;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;

public class CustomerNodeMessageSender extends Sender {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CustomerNodeMessageSender.class);
	private Processor processor = null;
	private NodeManager nodeManager = null;
	private A2PMultiConnectionInfo cInfo = null;
	private int ssid;

	public CustomerNodeMessageSender(GmmsMessage msg, NodeManager nodeManager, A2PCustomerInfo cst) {
		super.message = msg;
		this.nodeManager = nodeManager;
		this.ssid = cst.getSSID();
		processor = new Processor();
		cInfo = (A2PMultiConnectionInfo)cst;
	}

	public void run() {
		deliver(message);
	}

	public void deliver(GmmsMessage msg) {

		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType()) 
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
			while (cInfo.getOutgoingThrottlingNum() > 0 
					&& !ThrottlingControl.getInstance().isAllowedToSend(ssid)) {
				try {
					Thread.sleep(10L);
				} catch (Exception e) {
					log.error(e, e);
				}
			}
		}
		
		Session session = null;
		Node node = null;
		node = nodeManager.getNode(msg);

		if (node != null) {
			session = node.getSession(msg);
		}

		if (session == null) {
			if(log.isInfoEnabled()){
				log.info(msg, "Can't get available session.");
			}
			processor.logFail(msg);
		} else if (!sendMessage(session, msg)) {
			processor.logFail(msg);
		}

	}

	/**
	 * send message by selected connection
	 * 
	 * **/
	public boolean sendMessage(Session session, GmmsMessage msg) {
		try {
			if (session.submit(msg)) {
				if(log.isDebugEnabled()){
					log.debug(msg, "Submit {} OK",msg.getMessageType());
				}
			} else {
				if(log.isDebugEnabled()){
					log.debug(msg, "Submit failed.");
				}
				return false;
			}
		} catch (IOException e) {
			log.error(msg, "Failed to send this message.", e);
			return false;
		} catch (Exception e) {
			log.error(msg, e, e);
		}
		return true;
	}

	public void setProcessor(Processor processor) {
		if (processor != null) {
			this.processor = processor;
		}
	}

}
