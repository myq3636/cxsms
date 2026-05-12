package com.king.gmms.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.protocol.udp.nmg.CommandNumberApplicationAck;
import com.king.gmms.protocol.udp.nmg.Pdu;
import com.king.gmms.routing.RouteResponse;
import com.king.gmms.routing.nmg.NMGUtility;
import com.king.gmms.routing.nmg.OTTSMSDispatcher;
import com.king.gmms.util.BufferMonitorWithSafeExit;
import com.king.gmms.util.BufferTimeoutInterface;
import com.king.gmms.util.QueueHandlerInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * thread to handle number application ack, set senderAddr with rent MSISDN
 * 
 * @author levens
 * @version 1.0.0
 */
public class SenderRentThread implements QueueHandlerInterface {

	private OTTSMSDispatcher ottDispatcher = null;
	private DBBackupHandler dbBackup = null;
	private BufferMonitorWithSafeExit buffer = null;
	private MessageProcessorHandler handler = null;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SenderRentThread.class);

	public SenderRentThread() {
		//didn't needs to init OTTSMSDispatcher. king in 2017.10.20
		//ottDispatcher = new OTTSMSDispatcher();
		dbBackup = DBBackupHandler.getInstance();
		handler = MessageProcessorHandler.getInstance();
		int bufferCapacity = Integer.parseInt(GmmsUtility.getInstance()
				.getCommonProperty("DNSBufferCapacity", "100000").trim());
		int bufferTimeout = Integer.parseInt(GmmsUtility.getInstance()
				.getCommonProperty("DNSBufferTimeout", "30000").trim());
		buffer = new BufferMonitorWithSafeExit(bufferCapacity);
		buffer.setListener(new TimeoutHandlerForBuffer());
		buffer.setWaitTime(200, TimeUnit.MILLISECONDS);
		buffer.setTimeout(bufferTimeout, TimeUnit.MILLISECONDS);
		buffer.startMonitor("SenderRentBuffer");
	}

	public boolean handle(Object msg) {
		GmmsMessage message = null;
		try {
			if (msg != null) {
				message = (GmmsMessage) msg;
				while (!buffer.put(message.getInMsgID(), message)) {
					;
				}
				transactNmgApplication(message);
			}

			// handle the result of NMG number application
			transactNmgResult();
		} catch (Exception e) {
			log.error(message,
					"Occur error when SenderRentThread process this message,statuscode="
							+ message.getStatusCode(), e);
			message.setStatus(GmmsStatus.SERVER_ERROR);
			dbBackup.putMsg(message);
			buffer.remove(message.getInMsgID());
		}
		return true;
	}

	protected void transactNmgApplication(GmmsMessage message) {
		try {
			String messageType = message.getMessageType();
			if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_SUBMIT
							.equalsIgnoreCase(messageType)) {
				RouteResponse res = ottDispatcher.applySenderNumber(message);
				if (res == RouteResponse.NMG_RouteFailed) {
					// retry NMG
					message.setStatus(GmmsStatus.SERVER_ERROR);
					dbBackup.putMsg(message);
					buffer.remove(message.getInMsgID());
				}
			} else {
				message.setStatus(GmmsStatus.UNKNOWN_ERROR);
				dbBackup.putMsg(message);
				buffer.remove(message.getInMsgID());
			}
		} catch (Exception e) {
			log.error(message,
					"Occur error when applySenderNumber from NMG, message statuscode="
							+ message.getStatusCode(), e);
		}
	}

	/**
	 * Asynchronous NMG query
	 */
	private void transactNmgResult() {
		try {
			List<GmmsMessage> msgs = new ArrayList<GmmsMessage>();
			List<Pdu> pdus = new ArrayList<Pdu>();
			ottDispatcher.getQueryResults(msgs, pdus);

			int length = msgs.size();
			for (int i = 0; i < length; i++) {
				Pdu pdu = pdus.get(i);
				GmmsMessage msg = msgs.get(i);

				// NMG timeout or failed parsing ack
				if (pdu == null) {
					// retry NMG
					msg.setStatus(GmmsStatus.SERVER_ERROR);
					dbBackup.putMsg(msg);
					buffer.remove(msg.getInMsgID());
					continue;
				}

				if (pdu instanceof CommandNumberApplicationAck) {
					CommandNumberApplicationAck applicationAck = (CommandNumberApplicationAck) pdu;

					// check msgID
					if (!msg.getMsgID().equals(applicationAck.getMsgId())) {
						// retry NMG
						msg.setStatus(GmmsStatus.SERVER_ERROR);
						dbBackup.putMsg(msg);
						buffer.remove(msg.getInMsgID());
						if(log.isInfoEnabled()){
							log.info(msg,
								"msgID is not match, applicationAck msgID={}"
								,applicationAck.getMsgId());
						}
						continue;
					}

					int statusCode = applicationAck.getStatusCode();

					switch (statusCode) {
					case NMGUtility.NMG_OK: {
						// keep original value for CDR
						msg.setOriginalSenderAddr(msg.getSenderAddress());
						msg.setSenderAddress(applicationAck.getMsIsdn());
						if (!handler.putMsg(msg)) {
							msg.setStatus(GmmsStatus.SERVER_ERROR);
							dbBackup.putMsg(msg);
						}
						buffer.remove(msg.getInMsgID());
						break;
					}
					case NMGUtility.NMG_NUMBER_UNAVALABLE:
					case NMGUtility.NMG_NUMBER_NOT_PROVISIONED:
					case NMGUtility.NMG_SYSTEM_FAIL: {
						// retry NMG
						msg.setStatus(GmmsStatus.SERVER_ERROR);
						dbBackup.putMsg(msg);
						buffer.remove(msg.getInMsgID());
						break;
					}
					default: {
						// send out msg directly
						log
								.warn(msg, "applicationAck statusCode="
										+ statusCode);
						if (!handler.putMsg(msg)) {
							msg.setStatus(GmmsStatus.SERVER_ERROR);
							dbBackup.putMsg(msg);
						}
						buffer.remove(msg.getInMsgID());
						break;
					}
					}
				}
			}

			msgs.clear();
			pdus.clear();
		} catch (Exception ex) {
			log.warn("SenderRentThread transactNmgResult error:",ex);
		}
	}

	class TimeoutHandlerForBuffer implements BufferTimeoutInterface {

		public void timeout(Object key, GmmsMessage bufferedMsg) {
	        if(log.isInfoEnabled()){
	        	log.info(bufferedMsg,"{} is timeout in Sender Rent Buffer",bufferedMsg.getMessageType());
	        }
			try {
				if (bufferedMsg != null) {
					if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
							.equalsIgnoreCase(bufferedMsg.getMessageType())) {
						bufferedMsg
								.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
										.getCode());
						dbBackup.putMsg(bufferedMsg);
					} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
							.equalsIgnoreCase(bufferedMsg.getMessageType())) {
						bufferedMsg
								.setStatus(GmmsStatus.FAIL_QUERY_DELIVERREPORT);
						dbBackup.putMsg(bufferedMsg);
					} else {
						bufferedMsg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
						dbBackup.putMsg(bufferedMsg);
					}
				}
			} catch (Exception ex) {
				log.error(bufferedMsg, ex, ex);
			}
		}
	}
}
