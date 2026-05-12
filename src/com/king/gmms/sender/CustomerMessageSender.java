package com.king.gmms.sender;

import java.io.IOException;
import java.util.Map;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.processor.Processor;
import com.king.gmms.strategy.ConnectionStrategy;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CustomerMessageSender extends Sender {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(CustomerMessageSender.class);

	private Processor processor = null;
//	private ConnectionManager connManager = null;
	private A2PMultiConnectionInfo cInfo = null;
	protected ConnectionStrategy newMsgStrategy;
	protected ConnectionStrategy drStrategy;
	protected ConnectionStrategy responseStrategy;
	protected ConnectionStrategy sameSessionStrategy;
	private int ssid = -1;
	private Map connectionMap;
	protected boolean isEnableSysMgt = false;
	protected boolean isRespFirst = false;
	protected GmmsUtility gmmsUtility = null;
	
	public CustomerMessageSender(GmmsMessage msg, ConnectionManager connManager, A2PCustomerInfo cst) {
		super.message = msg;
//		this.connManager = connManager;
		this.ssid = cst.getSSID();
		processor = new Processor();
		gmmsUtility = GmmsUtility.getInstance();
		
		cInfo = (A2PMultiConnectionInfo) cst;
		String newMsgPolicy = cInfo.getSubmitConnectionPolicy();
		String newMsgOpt = cInfo.getPrimarySubmitConnection();
		String drPolicy = cInfo.getDrConnectionPolicy();
		String drOpt = cInfo.getPrimaryDRConnection();
		newMsgStrategy = getStrategy(newMsgPolicy, connManager, newMsgOpt);
		SingleNodeCustomerInfo nInfo = (SingleNodeCustomerInfo) cInfo;
		connectionMap = nInfo.getConnectionMap(isServer);
		newMsgStrategy.setConnectionMap(connectionMap);
		drStrategy = getStrategy(drPolicy, connManager, drOpt);
		drStrategy.setConnectionMap(connectionMap);
		String responsePolicy = cInfo.getResponseConnectionPolicy();
		responseStrategy = getStrategy(responsePolicy, connManager, null);
		sameSessionStrategy = getStrategy("SameSession", connManager, null);
		isEnableSysMgt = GmmsUtility.getInstance().isSystemManageEnable();
		
	}

	public void run() {
		deliver(message);
	}
	
	
	public void deliver(GmmsMessage msg) {
		// Outgoing throttling control
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg.getMessageType()) 
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())) {
			while (cInfo.getOutgoingThrottlingNum() > 0 
					&& cInfo.getPriorityPercentMap().isEmpty()
					&& !ThrottlingControl.getInstance().isAllowedToSend(ssid)) {
				try {
					Thread.sleep(10L);
				} catch (Exception e) {
					log.error(e, e);
				}
			}
		}
		
		Session session = null;
		long beginTime = System.currentTimeMillis();
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY.equalsIgnoreCase(msg
						.getMessageType())) {
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					session = sameSessionStrategy.execute(msg);
					if (session == null) {
						session = newMsgStrategy.execute(msg);
					}
				} else {
					session = newMsgStrategy.execute(msg);
				}
			} else {
				session = newMsgStrategy.execute(msg);
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg
				.getMessageType())) {
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					session = sameSessionStrategy.execute(msg);
					if (session == null) {
						session = drStrategy.execute(msg);
					}
				} else {
					session = drStrategy.execute(msg);
				}
			} else {
				session = drStrategy.execute(msg);
			}
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(msg.getMessageType())) {
			if (isEnableSysMgt) {
				TransactionURI transaction = msg.getTransaction();
				if (transaction != null) {
					session = sameSessionStrategy.execute(msg);
					if (session == null) {
						session = drStrategy.execute(msg);
					}
				} else {
					session = drStrategy.execute(msg);
				}
			} else {
				session = drStrategy.execute(msg);
			}
		} else if (GmmsMessage.MSG_TYPE_SUBMIT_RESP.equalsIgnoreCase(msg
				.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP
						.equalsIgnoreCase(msg.getMessageType())
				|| GmmsMessage.MSG_TYPE_DELIVERY_RESP.equalsIgnoreCase(msg
						.getMessageType())) {
			session = responseStrategy.execute(msg);
		} else {
			msg.setStatus(GmmsStatus.UNKNOWN_ERROR);
			log.warn(msg, "Unknown Message Type:{}, statuscode:{}", msg.getMessageType(), msg.getStatusCode());
			return;
		}

		//log.debug(msg, "The messsage start to send. {}", (System.currentTimeMillis() - beginTime));
		
		if (session == null) {
			if(log.isInfoEnabled()){
				log.info(msg, "Can't get available session.");
			}
			processor.logFail(msg);
		} else if (!sendMessage(session, msg)) {
			processor.logFail(msg);
		}
		 if(log.isDebugEnabled()){
			 log.debug(msg, "The during time to send message is {}", (System.currentTimeMillis() - beginTime));
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
					log.debug(msg, "Submit {} OK.",	msg.getMessageType());
				}
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
		}
		return true;
	}

	public int getSsid() {
		return ssid;
	}

	public Map getConnectionMap() {
		return connectionMap;
	}

	public void setSsid(int ssid) {
		this.ssid = ssid;
	}

	public void setConnectionMap(Map connectionMap) {
		this.connectionMap = connectionMap;
	}

	public void setProcessor(Processor processor) {
		if (processor != null) {
			this.processor = processor;
		}
	}

}
