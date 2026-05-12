package com.king.gmms.sender;

import java.io.IOException;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.sessionfactory.SessionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.processor.Processor;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.*;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class ShortConnectionSender extends Sender {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ShortConnectionSender.class);
	private Processor processor = null;
	private int ssid;
	private SessionFactory factory;
	private A2PCustomerInfo cInfo = null;

	public ShortConnectionSender(GmmsMessage msg, SessionFactory factory,
			int ssid) {
		super.message = msg;
		this.factory = factory;
		this.ssid = ssid;
		this.cInfo = GmmsUtility.getInstance().getCustomerManager()
				.getCustomerBySSID(ssid);
		
		this.processor = new Processor();
	}

	public void run() {
		deliver(message);
	}

	/**
	 * deliver
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @todo Implement.gmms.sender.Sender method
	 */
	public void deliver(GmmsMessage msg) {

		if(log.isTraceEnabled()){
			log.trace(msg, msg.toString());
		}

		try {
			while (cInfo.getOutgoingThrottlingNum() > 0
					&& !ThrottlingControl.getInstance().isAllowedToSend(ssid)) {
				try {
					Thread.sleep(10L);
				} catch (Exception e) {
					log.error(e, e);
				}
			}

			Session session = factory.getSession();
			if (session == null) {
				if(log.isInfoEnabled()){
					log.info(msg, "Can't get available session.");
				}
				processor.logFail(msg);
			} else {
				if (!sendMessage(session, msg)) {
					processor.logFail(msg);
				}
			} // end of else
		} catch (Exception ex) {
			log.error(ex, ex);
		} finally {
			// V4.1 Late ACK: Confirm completion to Redis after sending and reporting result
			com.king.gmms.messagequeue.StreamQueueManager.getInstance().ack(
					com.king.gmms.messagequeue.StreamQueueManager.STR_OUTBOUND_HTTP, 
					"cg_http_clients", 
					msg, 
					true);
		}
	}

	/**
	 * send message by selected connection
	 * 
	 * **/
	public boolean sendMessage(Session session, GmmsMessage msg) {
		try {
			if (session.submit(msg)) {
				if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg
						.getMessageType())
						|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg
								.getMessageType())) {
					if(log.isDebugEnabled()){
						log.debug(msg, "Submit ok.");
					}
				} else {
					if(log.isDebugEnabled()){
						log.debug(msg, "Submit {} OK",msg.getMessageType());
					}
				}
				return true;
			} else {
				if(log.isInfoEnabled()){
					log.info(msg, "Submit failed.");
				}
				return false;
			}
		} catch (IOException e) {
			log.error(msg, "Failed to send this message.", e);
			return false;
		} catch (Exception e) {
			log.error(msg, e, e);
			return false;
		}
	}

}
