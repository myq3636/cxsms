package com.king.gmms.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.processor.DBBackupHandler;
import com.king.gmms.processor.MessageProcessorHandler;
import com.king.gmms.processor.SenderRentHandler;
import com.king.gmms.protocol.udp.nmg.CommandNumberQueryAck;
import com.king.gmms.protocol.udp.nmg.Pdu;
import com.king.gmms.routing.nmg.NMGUtility;
import com.king.gmms.routing.nmg.OTTSMSDispatcher;
import com.king.gmms.util.BufferMonitorWithSafeExit;
import com.king.gmms.util.BufferTimeoutInterface;
import com.king.gmms.util.QueueHandlerInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class DeliveryRouterThread implements QueueHandlerInterface {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(DeliveryRouterThread.class);
	private IOSMSDispatcher smsDispatcher = null;
	private OTTSMSDispatcher ottDispatcher = null;
	private DBBackupHandler dbBackup = null;
	private MessageProcessorHandler handler = null;
	private SenderRentHandler senderRentHandler = null;
	//private BufferMonitorWithSafeExit buffer = null;

	public DeliveryRouterThread() {
		smsDispatcher = new IOSMSDispatcher();
		//didn't needs to init OTTSMSDispatcher. king in 2017.10.20
		//ottDispatcher = new OTTSMSDispatcher();
		dbBackup = DBBackupHandler.getInstance();
		handler = MessageProcessorHandler.getInstance();
		//senderRentHandler = SenderRentHandler.getInstance();
		/*
		 * int bufferCapacity = Integer.parseInt(GmmsUtility.getInstance()
		 * .getCommonProperty("DNSBufferCapacity", "100000").trim()); int bufferTimeout
		 * = Integer.parseInt(GmmsUtility.getInstance()
		 * .getCommonProperty("DNSBufferTimeout", "90000").trim()); buffer = new
		 * BufferMonitorWithSafeExit(bufferCapacity); buffer.setListener(new
		 * TimeoutHandlerForBuffer()); buffer.setWaitTime(200, TimeUnit.MILLISECONDS);
		 * buffer.setTimeout(bufferTimeout, TimeUnit.MILLISECONDS);
		 * buffer.startMonitor("DeliveryRouterBuffer");
		 */
	}

	public boolean handle(Object msg) {
		GmmsMessage message = null;
		try {
			if (msg != null) {
				message = (GmmsMessage) msg;
				/*
				 * while (!buffer.put(message.getInMsgID(), message)) { ; }
				 */
				
				// 6.10 Optimization: use {} placeholder instead of StringBuilder
				if(log.isTraceEnabled()){
					log.trace(message, "Delivery Router Thread transact this message,status:{},messageType:{}",
						message.getStatusCode(), message.getMessageType());
				}
				
				/*if (NMGUtility.needNmgRoute(message)) {
					// NMG routing, set rop, rssid, ra2p
					//transactNmgQuery(message);
				} else {*/
					// DNS routing directly
					transact(message);
				//}
			}
			// handle the result of DNS Query.
			//transactDnsQuery();

			// handle the result of NMG query/application
			//transactNmgResult();
		} catch (Exception e) {
			log.warn(e, e);
			if (message != null) {
				message.setStatus(GmmsStatus.SERVER_ERROR);
				dbBackup.putMsg(message);
				//buffer.remove(message.getInMsgID());
			}
		}
		return true;
	}

	/**
	 * Asynchronous DNS query
	 */
	/*
	 * private void transactDnsQuery() { try { List<GmmsMessage> msgs =
	 * smsDispatcher.getQueryResults(buffer); for (GmmsMessage msg : msgs) {
	 * if(log.isInfoEnabled()){ log.info(msg, "get Operator ok! OOP:{}  ROP:{}",
	 * msg.getOoperator() , msg.getRoperator()); } transact(msg); } msgs.clear(); }
	 * catch (Exception ex) { if(log.isInfoEnabled()){
	 * log.info("transact dns query error:{}", ex); } } }
	 */

	/**
	 * Asynchronous NMG query
	 */
	/*
	 * private void transactNmgResult() { try { List<GmmsMessage> msgs = new
	 * ArrayList<GmmsMessage>(); List<Pdu> pdus = new ArrayList<Pdu>();
	 * ottDispatcher.getQueryResults(msgs, pdus);
	 * 
	 * // NMG routing, set rop, rssid, ra2p // need go back to DNS routing for oop
	 * setting int length = msgs.size(); for (int i = 0; i < length; i++) { Pdu pdu
	 * = pdus.get(i); GmmsMessage msg = msgs.get(i);
	 * 
	 * // NMG timeout or failed parsing ack if (pdu == null) { // retry NMG
	 * msg.setStatus(GmmsStatus.SERVER_ERROR); dbBackup.putMsg(msg);
	 * buffer.remove(msg.getInMsgID()); continue; }
	 * 
	 * if (pdu instanceof CommandNumberQueryAck) { CommandNumberQueryAck queryAck =
	 * (CommandNumberQueryAck) pdu;
	 * 
	 * // check msgID if (!msg.getMsgID().equals(queryAck.getMsgId())) { // retry
	 * NMG msg.setStatus(GmmsStatus.SERVER_ERROR); dbBackup.putMsg(msg);
	 * buffer.remove(msg.getInMsgID()); if(log.isInfoEnabled()){ log.info(msg,
	 * "msgID is not match, queryAck msgID = {}",queryAck.getMsgId()); } continue; }
	 * 
	 * int statusCode = queryAck.getStatusCode(); switch (statusCode) { case
	 * NMGUtility.NMG_OK: { // ott/1.5 way // ott rssid and ra2p will set by
	 * channelRouter.dispatch A2PCustomerManager ctm = GmmsUtility.getInstance()
	 * .getCustomerManager(); int rOp = ctm
	 * .getSsidByCustID(queryAck.getOttCustomer());
	 * 
	 * // check if rop valid, avoid flood CommandNumberQuery if (rOp <= 0) {
	 * log.warn(msg, "can't get rop by OttCustomer in CommandNumberQueryAck:" +
	 * queryAck.getOttCustomer()); msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
	 * dbBackup.putMsg(msg); buffer.remove(msg.getInMsgID()); break; }
	 * 
	 * msg.setRoperator(rOp); if(log.isInfoEnabled()){ log.info(msg,
	 * "set rop by CommandNumberQueryAck, rop={}",rOp); }
	 * 
	 * // 1.5 way if (NMGUtility.isRentAddrMsg(msg)) { // set rop, rssid, ra2p //
	 * msg.setRoperator(queryAck.getO_op()); msg.setRSsID(queryAck.getO_relay());
	 * msg.setRA2P(queryAck.getO_hub());
	 * 
	 * // keep original value for CDR msg.setOriginalRecipientAddr(msg
	 * .getRecipientAddress()); // recover addr
	 * msg.setRecipientAddress(queryAck.getRecipientAddr());
	 * 
	 * if(log.isInfoEnabled()){
	 * log.info(msg,"set rssid, rop by CommandNumberQueryAck"); }
	 * 
	 * } transactNmgQuery(msg);
	 * 
	 * break; }
	 * 
	 * case NMGUtility.NMG_NUMBER_UNAVALABLE: case
	 * NMGUtility.NMG_NUMBER_NOT_PROVISIONED: case NMGUtility.NMG_SYSTEM_FAIL: { //
	 * retry NMG msg.setStatus(GmmsStatus.SERVER_ERROR); dbBackup.putMsg(msg);
	 * buffer.remove(msg.getInMsgID()); break; }
	 * 
	 * default: { // NMG failed, query DNS transact(msg); break; } } } }
	 * 
	 * msgs.clear(); pdus.clear(); } catch (Exception ex) {
	 * log.warn("DeliveryRouterThread transactNmgResult error:{}", ex); } }
	 */

	/**
	 * DNS routing dispatch
	 * 
	 * @param message
	 */
	protected void transact(GmmsMessage message) {
		try {
			String messageType = message.getMessageType();
			if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_SUBMIT
							.equalsIgnoreCase(messageType)) {
				transactNewMessage(message);
			} else {
				if(log.isInfoEnabled()){
					log.info(message, "Message type error:{}", message.getMessageType());
				}
				message.setStatus(GmmsStatus.UNKNOWN_ERROR);
				dbBackup.putMsg(message);
				//buffer.remove(message.getInMsgID());
			}
		} catch (Exception e) {
			log.error(message,
					"Occur error when DNS routing this message,statuscode="
							+ message.getStatusCode(), e);
		}
	}

	/**
	 * NMG routing dispatch
	 * 
	 * @param message
	 */
	protected void transactNmgQuery(GmmsMessage message) {
		try {
			String messageType = message.getMessageType();
			if (GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(messageType)
					|| GmmsMessage.MSG_TYPE_SUBMIT
							.equalsIgnoreCase(messageType)) {
				transactNewMessageNmgQuery(message);
			} else {
				if(log.isInfoEnabled()){
					log.info(message, "Message type error:{}", message.getMessageType());
				}
				message.setStatus(GmmsStatus.UNKNOWN_ERROR);
				dbBackup.putMsg(message);
				//buffer.remove(message.getInMsgID());
			}
		} catch (Exception e) {
			log.error(message,
					"Occur error when NMG routing this message,statuscode="
							+ message.getStatusCode(), e);
		}
	}

	/**
	 * transactNewMessage by DNS routing return -1 failed, 0 asy
	 */
	private RouteResponse transactNewMessage(GmmsMessage message) {
		RouteResponse res = smsDispatcher.dispatch(message);
		switch (res) {
		case RouteFailed: {
			dbBackup.putMsg(message);
			//buffer.remove(message.getInMsgID());
			return res;
		}
		case ASYQueryOP: {
			return res;
		}
		}
		if (message.getRSsID() <= 0) {
			message.setStatus(GmmsStatus.SERVER_ERROR);
			dbBackup.putMsg(message);
			//buffer.remove(message.getInMsgID());
			return RouteResponse.RouteFailed;
		}
		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		if (ctm.isA2P(message.getRA2P()) || ctm.isPartition(message.getRA2P())) {
			if (NMGUtility.needRentAddr(message)) {
				if (!senderRentHandler.putMsg(message)) {
					message.setStatus(GmmsStatus.SERVER_ERROR);
					dbBackup.putMsg(message);
				}
			} else {
				if (!handler.putMsg(message)) {
					message.setStatus(GmmsStatus.SERVER_ERROR);
					dbBackup.putMsg(message);
				}
			}
			//buffer.remove(message.getInMsgID());
		} else {
			if(log.isInfoEnabled()){
				log.info(message, "Can't get RA2P for message.");
			}
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			dbBackup.putMsg(message);
			//buffer.remove(message.getInMsgID());
			return RouteResponse.RouteFailed;
		}
		return RouteResponse.RouteOK;
	}

	/**
	 * transactNewMessage by NMG routing NMG routing, set rop, rssid, ra2p
	 */
	private RouteResponse transactNewMessageNmgQuery(GmmsMessage message) {
		RouteResponse res = ottDispatcher.dispatch(message);

		switch (res) {
		case NMG_RouteFailed: {
			dbBackup.putMsg(message);
			//buffer.remove(message.getInMsgID());
			return res;
		}
		case NMG_ASYQueryOP: {
			return res;
		}
		}
		if (message.getRSsID() <= 0) {
			message.setStatus(GmmsStatus.SERVER_ERROR);
			dbBackup.putMsg(message);
			//buffer.remove(message.getInMsgID());
			return RouteResponse.NMG_RouteFailed;
		}
		A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
		if (ctm.isA2P(message.getRA2P()) || ctm.isPartition(message.getRA2P())) {
			if (!handler.putMsg(message)) {
				message.setStatus(GmmsStatus.SERVER_ERROR);
				dbBackup.putMsg(message);
			}
			//buffer.remove(message.getInMsgID());
		} else {
			if(log.isInfoEnabled()){
				log.info(message, "NmgQuery can't get RA2P for message.");
			}
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
			dbBackup.putMsg(message);
			//buffer.remove(message.getInMsgID());
			return RouteResponse.NMG_RouteFailed;
		}
		return RouteResponse.NMG_RouteOK;
	}

	class TimeoutHandlerForBuffer implements BufferTimeoutInterface {

		public void timeout(Object key, GmmsMessage bufferedMsg) {
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
