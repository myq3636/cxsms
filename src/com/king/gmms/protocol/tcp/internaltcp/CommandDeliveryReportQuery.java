package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CommandDeliveryReportQuery extends Request {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(CommandDeliveryReportQuery.class);
	protected SimpleDateFormat dateFormat = new SimpleDateFormat(
			"yyyyMMddHHmmss");
	A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
	private String msgId;
	private String sessionID;
	private String sender;
	private String recipient;
	private int o_op = -1;
	private int r_op = -1;
	private int o_hub = -1;
	private int r_hub = -1;
	private int o_relay = -1;
	private int r_relay = -1;
	private String transactionId;
	private TransactionURI transaction = null;
	private int statusCode = -1;
	private int statusCodeInternal = -1;
	private String originalQueue = null;

	private String deliveryChannel;

	public CommandDeliveryReportQuery() {
		if (header == null) {
			header = new PduHeader();
		}
		header.setCommandId(this.COMMAND_DELIVERY_REPORT_QUERY);
	}

	public String getMsgId() {
		return msgId;
	}

	public void parsePduCommand(TcpByteBuffer buffer)
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException, UnknownParameterIdException {
		if (buffer == null) {
			return;
		}
		TcpByteBuffer tempBuffer = null;
		while (buffer.length() > 0) {
			int tag = buffer.removeBytesAsInt(1);
			int length = buffer.removeBytesAsInt(2);
			switch (tag) {
			case FIELD_MSGID:
				msgId = buffer.removeString(length);
				break;
			case FIELD_SESSIONID:
				sessionID = buffer.removeString(length);
				break;
			case FIELD_SENDER:
				sender = buffer.removeString(length);
				break;
			case FIELD_RECIPIENT:
				recipient = buffer.removeString(length);
				break;
			case FIELD_OOPERATOR:
				o_op = buffer.removeBytesAsInt(length);
				break;
			case FIELD_ROPERATOR:
				r_op = buffer.removeBytesAsInt(length);
				break;
			case FIELD_OHUB:
				o_hub = buffer.removeBytesAsInt(length);
				break;
			case FIELD_RHUB:
				r_hub = buffer.removeBytesAsInt(length);
				break;
			case FIELD_ORELAY:
				o_relay = buffer.removeBytesAsInt(length);
				break;
			case FIELD_RRELAY:
				r_relay = buffer.removeBytesAsInt(length);
				break;
			case FIELD_STATUS:
				statusCode = buffer.removeBytesAsInt(length);
				break;
			case FIELD_STATUS_INTERNAL:
				statusCodeInternal = buffer.removeBytesAsInt(length);
				break;
			case FIELD_TRANSACTIONURI:
				String uriString = buffer.removeString(length);
				if (uriString != null) {
					if (log.isDebugEnabled()) {
						log.debug("convertFromString uriString:{}", uriString);
					}
					transaction = TransactionURI.fromString(uriString);
				}
				break;
			case FIELD_TRANSACTIONID:
				transactionId = buffer.removeString(length);
				break;
			case FIELD_ORIGINALQUEUE:
				originalQueue = buffer.removeString(length);
				break;
			case FIELD_DELIVERYCHANNEL:
				deliveryChannel = buffer.removeString(length);
				break;
			default:
				log.warn("Cant find field with tag: {},len:{}", tag, length);
				buffer.removeBytes(length);
				break;
			}
		}
	}

	public GmmsMessage convertToMsg(BufferMonitor buffer) {
		if (header == null) {
			return null;
		}
		GmmsMessage msg = null;
		try {
			msg = new GmmsMessage();
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY);
			if (msgId != null) {
				msg.setOutMsgID(msgId);
			}
			if (sessionID != null) {
				msg.setMsgID(sessionID);
			}
			if (transaction != null) {
				msg.setTransaction(transaction);
			}
			msg.setOutTransID(transactionId);
			msg.setOoperator(o_op);
			msg.setRoperator(r_op);
			msg.setOSsID(o_relay);
			msg.setRSsID(r_relay);
			if (o_hub > 0) {
				msg.setOA2P(o_hub);
			}
			if (r_hub > 0) {
				msg.setRA2P(r_hub);
			}
			int oA2P = gmmsUtility.getCustomerManager().getConnectedRelay(
					o_relay, GmmsMessage.MSG_TYPE_SUBMIT);
			if (gmmsUtility.getCustomerManager().inCurrentA2P(oA2P)) {
				msg.setCurrentA2P(oA2P);
			} else {
				msg.setCurrentA2P(r_hub);
			}
			msg.setSenderAddress(sender);
			msg.setRecipientAddress(recipient);
			msg.setDateIn(new Date());
			if (statusCode != -1) {
				switch (statusCode) {
				case 0:
					msg.setStatus(GmmsStatus.DELIVERED);
					break; // 10000
				case 1:
					msg.setStatus(GmmsStatus.EXPIRED);
					break; // 10200
				case 2:
					msg.setStatus(GmmsStatus.EXPIRED);
					break; // 10200
				case 3:
					msg.setStatus(GmmsStatus.UNDELIVERABLE);
					break; // 10400
				case 4:
					msg.setStatus(GmmsStatus.ENROUTE);
					break; // 10105
				case 5:
					msg.setStatus(GmmsStatus.UNKNOWN);
					break; // 10900
				case 6:
					msg.setStatus(GmmsStatus.REJECTED);
					break; // 10500
				default:
					msg.setStatus(GmmsStatus.UNKNOWN);
					break; // 10900
				}
			} else if (statusCodeInternal != -1) {
				msg.setStatusCode(statusCodeInternal);
			}
			msg.setOriginalQueue(originalQueue);
			msg.setDeliveryChannel(deliveryChannel);
		} catch (Exception e) {
			log.warn(e, e);
		}
		return msg;
	}

	public TcpByteBuffer pduCommandToByteBuffer()
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {

		TcpByteBuffer buffer = new TcpByteBuffer();

		appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
		appendParameterToBuffer(buffer, FIELD_SESSIONID, sessionID);
		appendParameterToBuffer(buffer, FIELD_OOPERATOR, o_op);
		appendParameterToBuffer(buffer, FIELD_ROPERATOR, r_op);
		appendParameterToBuffer(buffer, FIELD_OHUB, o_hub);
		appendParameterToBuffer(buffer, FIELD_RHUB, r_hub);
		appendParameterToBuffer(buffer, FIELD_ORELAY, o_relay);
		appendParameterToBuffer(buffer, FIELD_RRELAY, r_relay);
		appendParameterToBuffer(buffer, FIELD_SENDER, sender);
		appendParameterToBuffer(buffer, FIELD_RECIPIENT, recipient);
		appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
		appendParameterToBuffer(buffer, FIELD_STATUS_INTERNAL,
				statusCodeInternal);
		appendParameterToBuffer(buffer, FIELD_TRANSACTIONID, transactionId);
		appendParameterToBuffer(buffer, FIELD_ORIGINALQUEUE, originalQueue);
		appendParameterToBuffer(buffer, FIELD_DELIVERYCHANNEL, deliveryChannel);
		if (transaction != null) {
			appendParameterToBuffer(buffer, FIELD_TRANSACTIONURI,
					transaction.toString());
		}

		return buffer;
	}

	public String toString() {
		return new StringBuffer("COMMAND_DELIVERY_REPORT_QUERY:").append("msgId:")
				.append(msgId).append(",").append("sessionID:")
				.append(sessionID).append(",").append("statusCode:")
				.append(statusCode).append(",").append("statusCodeInternal:")
				.append(statusCodeInternal).append(",")
				.append("deliveryChannel:").append(deliveryChannel).append(",")
				.append("transaction:").append(transaction).toString();
	}

	protected Respond createResponse() {
		return null;
	}

	/**
	 * 
	 * @param msg
	 * @param in
	 * @throws NotEnoughDataInByteBufferException
	 * @throws UnsupportedEncodingException
	 */
	public void convertFromMsg(GmmsMessage msg)
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {
		if (msg == null) {
			return;
		}
		msgId = msg.getOutMsgID();
		sessionID = msg.getMsgID();
		transactionId = msg.getOutTransID();
		transaction = msg.getTransaction();
		statusCode = getStatus(msg.getStatusCode());
		o_op = msg.getOoperator();
		r_op = msg.getRoperator();
		o_hub = msg.getOA2P();
		r_hub = msg.getRA2P();
		o_relay = msg.getOSsID();
		r_relay = msg.getRSsID();
		sender = msg.getSenderAddress();
		recipient = msg.getRecipientAddress();
		originalQueue = msg.getOriginalQueue();
		deliveryChannel = msg.getDeliveryChannel();
	}

	private int getStatus(int statusCode) {
		int status = -1;
		switch (statusCode) {
		case 0:
		case 10000:
		case 10105:
			status = 0;
			break;
		// case 10105:
		case 12000:
		case 13000:
			status = 4;
			break;
		case 2100:
		case 2110:
		case 2120:
		case 10400:
		case 11005:
			status = 3;
			break;
		case 10200:
			status = 1;
			break;
		case 10300:
			status = 2;
			break;
		case 10500:
			status = 6;
			break;
		case 10700:
			status = 6;
			break;
		case 9000:
		case 10900:
		default:
			status = 5;
		}
		return status;
	}

	public String getSender() {
		return sender;
	}

	public void setSender(String sender) {
		this.sender = sender;
	}

	public String getRecipient() {
		return recipient;
	}

	public void setRecipient(String recipient) {
		this.recipient = recipient;
	}

	public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public TransactionURI getTransaction() {
		return transaction;
	}

	public void setTransaction(TransactionURI transaction) {
		this.transaction = transaction;
	}

	public String getDeliveryChannel() {
		return deliveryChannel;
	}

	public void setDeliveryChannel(String deliveryChannel) {
		this.deliveryChannel = deliveryChannel;
	}

	// add by kevin for REST

	public GmmsMessage convertToMsg() {
		if (header == null) {
			return null;
		}
		GmmsMessage msg = null;
		try {
			msg = new GmmsMessage();
			msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY);
			if (msgId != null) {
				msg.setInMsgID(msgId);
			}
			if (sessionID != null) {
				msg.setMsgID(sessionID);
			}
			if (transaction != null) {
				msg.setTransaction(transaction);
			}
			msg.setOutTransID(transactionId);
			msg.setOoperator(o_op);
			msg.setRoperator(r_op);
			msg.setOSsID(o_relay);
			msg.setRSsID(r_relay);
			if (o_hub > 0) {
				msg.setOA2P(o_hub);
			}
			if (r_hub > 0) {
				msg.setRA2P(r_hub);
			}
			int oA2P = gmmsUtility.getCustomerManager().getConnectedRelay(
					o_relay, GmmsMessage.MSG_TYPE_SUBMIT);
			if (gmmsUtility.getCustomerManager().inCurrentA2P(oA2P)) {
				msg.setCurrentA2P(oA2P);
			} else {
				msg.setCurrentA2P(r_hub);
			}
			msg.setSenderAddress(sender);
			msg.setRecipientAddress(recipient);
			msg.setDateIn(new Date());
			if (statusCode != -1) {
				switch (statusCode) {
				case 0:
					msg.setStatus(GmmsStatus.DELIVERED);
					break; // 10000
				case 1:
					msg.setStatus(GmmsStatus.EXPIRED);
					break; // 10200
				case 2:
					msg.setStatus(GmmsStatus.EXPIRED);
					break; // 10200
				case 3:
					msg.setStatus(GmmsStatus.UNDELIVERABLE);
					break; // 10400
				case 4:
					msg.setStatus(GmmsStatus.ENROUTE);
					break; // 10105
				case 5:
					msg.setStatus(GmmsStatus.UNKNOWN);
					break; // 10900
				case 6:
					msg.setStatus(GmmsStatus.REJECTED);
					break; // 10500
				default:
					msg.setStatus(GmmsStatus.UNKNOWN);
					break; // 10900
				}
			} else if (statusCodeInternal != -1) {
				msg.setStatusCode(statusCodeInternal);
			}
			msg.setOriginalQueue(originalQueue);
			msg.setDeliveryChannel(deliveryChannel);
		} catch (Exception e) {
			log.warn(e, e);
		}
		return msg;
	}

	public void convertFromMsgForREST(GmmsMessage msg)
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {
		if (msg == null) {
			return;
		}
		msgId = msg.getInMsgID();
		sessionID = msg.getMsgID();
		transactionId = msg.getOutTransID();
		transaction = msg.getTransaction();
		statusCode = getStatus(msg.getStatusCode());
		o_op = msg.getOoperator();
		r_op = msg.getRoperator();
		o_hub = msg.getOA2P();
		r_hub = msg.getRA2P();
		o_relay = msg.getOSsID();
		r_relay = msg.getRSsID();
		sender = msg.getSenderAddress();
		recipient = msg.getRecipientAddress();
		originalQueue = msg.getOriginalQueue();
		deliveryChannel = msg.getDeliveryChannel();
	}

}
