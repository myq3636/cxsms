package com.king.gmms.connectionpool.session;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.sessionthread.InternalClientSessionThread;
import com.king.gmms.connectionpool.sessionthread.InternalServerSessionThread;
import com.king.gmms.customerconnectionfactory.InternalMQMConnectionFactory;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.CommandBind;
import com.king.gmms.protocol.tcp.internaltcp.CommandBindAck;
import com.king.gmms.protocol.tcp.internaltcp.CommandKeepAliveAck;
import com.king.gmms.protocol.tcp.internaltcp.InternalPdu4MQM;
import com.king.gmms.protocol.tcp.internaltcp.Pdu;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.util.BufferMonitorWithSafeExit;
import com.king.gmms.util.BufferTimeoutInterface;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class InternalMQMSession extends AbstractInternalSession implements
		BufferTimeoutInterface {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(InternalMQMSession.class);
	private A2PCustomerManager ctm = null;
	private int currentVersion = Pdu.VERSION_2_0;
	private int gmdVersion = Pdu.VERSION_2_0;
	private BufferMonitorWithSafeExit bufferMonitor = null;
	private int windowsSize = 100000;
	private int bufferTimeout = 1000000;

	public InternalMQMSession() {
		super();
		ctm = gmmsUtility.getCustomerManager();
	}

	/**
	 * for client side
	 * 
	 * @param connectionInfo
	 */
	public InternalMQMSession(ConnectionInfo connectionInfo) {
		super();
		if (connectionInfo == null) {
			return;
		}
		isServer = false;
		msgQueue = new InternalConnectionMessageQueue(isServer);
		
		transaction = new TransactionURI();
		transaction.setConnectionName(connectionInfo.getConnectionName());
		ctm = gmmsUtility.getCustomerManager();
		int connectionSilentTime = Integer.parseInt(gmmsUtility
				.getCommonProperty("ConnectionMaxSilentTime", "600")) * 1000;
		this.connectionInfo = connectionInfo;
		super.initReceivers();
		super.initSenders();
		setSessionName(connectionInfo.getConnectionName());
		sessionThread = new InternalClientSessionThread(this,
				connectionSilentTime);
		startBufferMonitor();
		start();
	}

	/**
	 * for server side
	 * 
	 * @param socket
	 */
	public InternalMQMSession(Socket socket) {
		super();
		// create a single thread pool first, then update after binded
        super.initReceivers();
		isServer = true;
		msgQueue = new InternalConnectionMessageQueue(isServer);
		
		ctm = gmmsUtility.getCustomerManager();
		int connectionSilentTime = Integer.parseInt(gmmsUtility
				.getCommonProperty("ConnectionMaxSilentTime", "600")) * 1000;
		transaction = new TransactionURI();
		if (ctm.getPeeringTcpVersion() > 0) {
			gmdVersion = ctm.getPeeringTcpVersion();
		}
		try {
			if (socket != null) {
				createConnection(socket);
			}
		} catch (IOException ex) {
			log.error(ex, ex);
		}
		sessionThread = new InternalServerSessionThread(this,
				connectionSilentTime);
		start();
	}

	/**
	 * start BufferMonitor
	 * 
	 * @return
	 */
	public boolean startBufferMonitor() {
		bufferMonitor = new BufferMonitorWithSafeExit(windowsSize);
		bufferMonitor.setListener(this);
		bufferMonitor.setWaitTime(200, TimeUnit.MILLISECONDS);
		bufferMonitor.setTimeout(bufferTimeout, TimeUnit.MILLISECONDS);
		bufferMonitor.startMonitor(sessionName+"_Buffer");
		return true;
	}

	/**
	 * receive pdu
	 */
	public boolean receive(Object obj) {
		boolean result = false;
		if (obj == null) {
			return true;
		}
		Pdu pdu = (Pdu) obj;
		GmmsMessage msg = null;
		if(log.isDebugEnabled()){
			log.debug("PeeringTcp Receive a PDU {}", pdu.getCommandId());
		}
		if (Pdu.COMMAND_ACK4MQM == pdu.getCommandId()) {
			msg = pdu.convertToMsg(null);
    		if(log.isDebugEnabled()){
    			log.debug("processAck4MQM before:{}", msg);
    		}
			if (msg != null) {
				this.processAck(msg);
			}
		}
//		else if (Pdu.COMMAND_DELIVERY_REPORT_QUERY_ACK == pdu.getCommandId()) {
//			msg = pdu.convertToMsg(null);
//    		if(log.isDebugEnabled()){
//    			log.debug("processAck4MQM before:{}", msg);
//    		}
//			if (msg != null) {
//				this.processAck(msg);
//			}
//		} 
		else if (Pdu.COMMAND_ALIVE == pdu.getCommandId()) {
			CommandKeepAliveAck ack = new CommandKeepAliveAck();
			try {
				super.submit(ack.toByteBuffer().getBuffer());
			} catch (Exception ex) {
				log.warn("IOException occured when sending KeepAlive ACK", ex);
				stop();
			}
		} else if (Pdu.COMMAND_ALIVE_ACK == pdu.getCommandId()) {
			super.clearActiveTestCount();
		} else if (Pdu.COMMAND_BIND == pdu.getCommandId()) {
			result = processBind(pdu);
			if (!result) {
				stop();
			}
		}
		return result;
	}

	/**
	 * buffer timeout
	 */
	public void timeout(Object key, GmmsMessage bufferedMsg) {
		try {
			if (bufferedMsg != null) {
				if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(bufferedMsg
						.getMessageType())) {
					bufferedMsg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
					gmmsUtility.getMessageStoreManager().handleOutSubmitRes(
							bufferedMsg);
				} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
						.equalsIgnoreCase(bufferedMsg.getMessageType())) {
					bufferedMsg
							.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
									.getCode());
					gmmsUtility.getMessageStoreManager()
							.handleInDeliveryReportRes(bufferedMsg);
				} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
						.equalsIgnoreCase(bufferedMsg.getMessageType())) {
					bufferedMsg
							.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
									.getCode());
					gmmsUtility.getMessageStoreManager()
							.handleOutDeliveryReportRes(bufferedMsg);
				} else {
					bufferedMsg.setStatus(GmmsStatus.COMMUNICATION_ERROR);
				}
			}
		} catch (Exception ex) {
			log.error(bufferedMsg, ex, ex);
		}
	}

	/**
	 * queue timeout
	 */
	public void timeout(Object msg) {
		this.timeout(null, (GmmsMessage) msg);
	}

	public boolean submit(GmmsMessage message) {
		boolean result = false;
		String messageType = message.getMessageType();
		if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {
			result = sendNewMessage(message);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT
				.equalsIgnoreCase(messageType)) {
			result = sendReport(message);
		} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY
				.equalsIgnoreCase(messageType)) {
			result = sendReportQuery(message);
		} else {
			log.warn(message, "Unknown Message Type:"
					+ message.getMessageType());
			message.setStatus(GmmsStatus.UNKNOWN_ERROR);
		}
		return result;
	}

	/**
	 * 
	 * @param msg
	 * @return
	 */
	private boolean processAck(GmmsMessage resp) {
		boolean result = false;
		if (resp == null) {
			log.warn("Message is null in processAck4MQM method.");
			return result;
		}
		try {
			GmmsMessage msg = bufferMonitor.remove(resp.getMsgID() + resp.getSarSegmentSeqNum());
			String messageType = msg.getMessageType();
			if (GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(messageType)) {
				if(log.isInfoEnabled()){
					log.info(msg,"CoreEngine has already processed submit message from MQM successfully!");
				}
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(messageType)) {
				if(log.isInfoEnabled()){
					log.info(msg,"CoreEngine has already processed DR message from MQM successfully!");
				}
			} else if (GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY.equalsIgnoreCase(messageType)) {
				if(log.isInfoEnabled()){
					log.info(msg,"CoreEngine has already processed DR query message from MQM successfully!");
				}
			}
			result = true;
		} catch (Exception ex) {
			log.warn(ex, ex);
		}
		return result;
	}

	public boolean processBind(Pdu pdu) {
		boolean result = false;
		CommandBind bind = (CommandBind) pdu;
		String userName = bind.getUserName();
		CommandBindAck ack = new CommandBindAck();

		if (userName != null) {
			try {
				ConnectionInfo connInfo = ModuleManager.getInstance()
						.getConnectionInfo(userName);

				if (connInfo != null) {
					connectionInfo = connInfo;
					super.initSenders();
					super.updateReceivers();
					if (gmdVersion > bind.getVersion()) {
						currentVersion = bind.getVersion();
					} else {
						currentVersion = gmdVersion;
					}
					ack.setVersion(currentVersion);
					transaction.setConnectionName(connectionInfo
							.getConnectionName());
					setSessionName(connectionInfo.getConnectionName());
					if (insertSession(userName)) {
						setStatus(ConnectionStatus.CONNECT);
						startBufferMonitor();
						ack.setStatusCode(0);
						result = true;
					} else {
						ack.setStatusCode(1);
					}

				} else {
					log.warn("Can not find Server for module: {}" , userName);
					ack.setStatusCode(1);
				}
			} catch (Exception e) {
				log.error(e, e);
				setStatus(ConnectionStatus.DISCONNECT);
				ack.setStatusCode(1);
			}
		} else {
			log.warn("userName in BIND is null.");
			ack.setStatusCode(1);
		}
		try {
			submit(ack.toByteBuffer().getBuffer());
		} catch (IOException ex) {
			log.warn(ex, ex);
		} catch (Exception ex) {
			log.warn(ex, ex);
		}
		return result;
	}

	/**
	 * send SUBMIT request
	 * 
	 * @param message
	 *            GmmsMessage
	 * @return boolean
	 */
	private boolean sendNewMessage(GmmsMessage message) {
		boolean result = false;
		TcpByteBuffer submitBuffer = null;

		try {
			InternalPdu4MQM pdu = new InternalPdu4MQM(Pdu.COMMAND_SUBMIT4MQM);
			pdu.convertFromMsg(message);
			submitBuffer = pdu.toByteBuffer();
		} catch (Exception e) {
			log.error(message, e, e);
			return result;
		}
		if (submitBuffer == null) {
			message.setStatus(GmmsStatus.INVALID_MSG_FIELD);
			return result;
		}
		while (!bufferMonitor.put(message.getMsgID() + message.getSarSegmentSeqNum(), message)) {
			;
		}
		try {
			super.submit(submitBuffer.getBuffer());
			message.setOutTransID(String.valueOf(System.currentTimeMillis()));
			result = true;
		} catch (IOException ex) {
			log.warn(message,
					"IOException occured when submit msg to PeerTcp Server: ",
					ex);
			bufferMonitor.remove(message.getMsgID() + message.getSarSegmentSeqNum());
			message.setStatus(GmmsStatus.COMMUNICATION_ERROR);
			stop();
		}
		return result;
	}

	/**
	 * send REPORT2 to the client
	 * 
	 * @param message
	 *            GmmsMessage
	 * @return boolean
	 */
	private boolean sendReportQuery(GmmsMessage message) {
		if (message == null) {
			log.trace("No DR send to Client");
			return false;
		}
		boolean result = false;
		TcpByteBuffer reportBuffer = null;
		try {
//			InternalPdu4MQM pdu = new InternalPdu4MQM(
//					Pdu.COMMAND_DELIVERY_REPORT4MQM);// TODO:
			InternalPdu4MQM pdu = new InternalPdu4MQM(
					Pdu.COMMAND_DELIVERY_REPORT_QUERY4MQM);
			pdu.convertFromMsg(message);
			reportBuffer = pdu.toByteBuffer();
		} catch (Exception e) {
			log.error(message, e, e);
			message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
					.getCode());
			return result;
		}

		if (reportBuffer == null) {
			return result;
		}
		while (!bufferMonitor.put(message.getMsgID() + message.getSarSegmentSeqNum(), message)) {
			;
		}
		try {
			super.submit(reportBuffer.getBuffer());
			result = true;
		} catch (IOException ex) {
			log.warn(message, ex, ex);
//			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
//					.getCode());
			message.setStatusCode(GmmsStatus.FAIL_QUERY_DELIVERREPORT
					.getCode());
			bufferMonitor.remove(message.getMsgID() + message.getSarSegmentSeqNum());
			stop();
		}
		return result;
	}

	/**
	 * send REPORT2 to the client
	 * 
	 * @param message
	 *            GmmsMessage
	 * @return boolean
	 */
	private boolean sendReport(GmmsMessage message) {
		if (message == null) {
			log.trace("No DR send to Client");
			return false;
		}
		boolean result = false;
		TcpByteBuffer reportBuffer = null;
		try {
			InternalPdu4MQM pdu = new InternalPdu4MQM(
					Pdu.COMMAND_DELIVERY_REPORT4MQM);
			pdu.convertFromMsg(message);
			reportBuffer = pdu.toByteBuffer();
		} catch (Exception e) {
			log.error(message, e, e);
			return result;
		}

		if (reportBuffer == null) {
			return result;
		}
		while (!bufferMonitor.put(message.getMsgID() + message.getSarSegmentSeqNum(), message)) {
			;
		}
		try {
			super.submit(reportBuffer.getBuffer());
			result = true;
		} catch (IOException ex) {
			log.warn(message, ex, ex);
			message.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT
					.getCode());
			bufferMonitor.remove(message.getMsgID() + message.getSarSegmentSeqNum());
			stop();
		}
		return result;
	}

	/**
	 * insert internal session
	 * 
	 * @param moduleName
	 * @return
	 */
	private synchronized boolean insertSession(String moduleName) {
		boolean result = false;
		try {
			result = InternalMQMConnectionFactory.getInstance()
					.manageConnection(moduleName, this);
		} catch (Exception ex) {
			log.error(ex, ex);
		}
		log.debug("processBind result:{} with moduleName:{}", result,
						moduleName);
		return result;
	}

	/**
	 * connection broken
	 */
	public void connectionUnavailable() {
		super.connectionUnavailable();
		InternalMQMConnectionFactory factory = InternalMQMConnectionFactory
				.getInstance();
		if (isServer && connectionInfo != null) {
			factory.connectionBroken(connectionInfo.getUserName(), this);
		}
	}

	/**
	 * stop bufferMonitor
	 */
	public void stop() {
		super.stop();
		if (isServer) {
			if (msgQueue != null) {
				msgQueue.stopMessageQueue();
			}
			
			if (executorServiceManager != null) {
				executorServiceManager.shutdown(receiverThreadPool);
			}
			
			if (bufferMonitor != null) {
				bufferMonitor.writeAllToDB();
				bufferMonitor.stopMonitor();
				bufferMonitor = null;
			}
			
		}
	}
}
