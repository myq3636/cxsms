package com.king.message.gmms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;






import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.ByteBuffer;

/**
 * <p>
 * Title: GmmsMessage
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Copyright: Copyright (c) 2004
 * </p>
 * <p>
 * Company: King
 * </p>
 * 
 * @version 6.1
 * @author: Jesse Duan
 */
public class GmmsMessage extends MessageBase {
	public static final String MSG_TYPE_SUBMIT = "Submit";
	public static final String MSG_TYPE_SUBMIT_RESP = "Submit Resp";
	public static final String MSG_TYPE_INNER_ACK = "Inner Ack";
	public static final String MSG_TYPE_DELIVERY = "Delivery";
	public static final String MSG_TYPE_DELIVERY_RESP = "Delivery Resp";
	public static final String MSG_TYPE_DELIVERY_REPORT = "Delivery Report";
	public static final String MSG_TYPE_DELIVERY_REPORT_RESP = "Delivery Report Resp";
	public static final String MSG_TYPE_DELIVERY_REPORT_QUERY = "Delivery Report Query";
	public static final String MSG_TYPE_DELIVERY_REPORT_QUERY_RESP = "Delivery Report Query Resp";
	/* Added by Bill, August, 2004 */
	public static final String MSG_TYPE_READ_REPLY_REPORT = "Read Reply Report";
	public static final String MSG_TYPE_READ_REPLY_REPORT_QUERY = "Read Reply Report Query";
	// Added by Neal, June,2005
	public static final String MSG_TYPE_FORWARD_REQ = "Forward Req";
	public static final String MSG_TYPE_FORWARD_RES = "Forward Res";

	protected String inMsgID = null;
	protected String outMsgID = null;

	// added by Jesse for GMD6.0 2005-3-21
	protected String msgID = null;

	protected String inTransID = null;
	protected String outTransID = null;
	protected int oSsID = -1;
	protected int rSsID = -1;
	
	protected String deliveryChannel = null;
	protected String routingSsIDs = null;
	protected String redisStreamID = null; // V4.0 Redis Stream ID for ACK
	
	
//	protected boolean inClientPull = false;
	
	private int inClientPull=0;
	
	
	
	protected boolean outClientPull = false;
	/** indicate whether IN_SUBMIT is Concatenated SMS */
	protected boolean inCsm = false;
	protected String connectionID = null;

	// added by Neal for MNP 2005-8-3
	protected int oOperator = -1;
	protected int rOperator = -1;
	public static final String AIC_CS_ASCII = "ASCII";
	public static final String AIC_CS_ISO8859_1 = "ISO-8859-1";
	public static final String AIC_CS_ISO8859_5 = "ISO-8859-5";
	public static final String AIC_CS_ISO8859_8 = "ISO-8859-8";
	public static final String AIC_CS_UCS2 = "UnicodeBigUnmarked";
	public static final String AIC_CS_GBK = "GBK";
	public static final String AIC_CS_UTF8 = "UTF-8";
	public static final String AIC_CS_BIG5 = "Big5";
	public static final String AIC_CS_EUCJP = "EUC-JP";
	public static final String AIC_CS_EUCKR = "EUC-KR";
	public static final String AIC_CS_KSC5601 = "KSC5601";
	public static final String AIC_CS_ISO2022_JP = "ISO-2022-JP";

	// added by jesse
	public static final String AIC_CT_ASCII = "ASCII";
	public static final String AIC_CT_USC2 = "USC2";
	public static final String AIC_CT_NOKIA_OLOGO = "SMS_NOKIA_OLOGO";
	public static final String AIC_CT_NOKIA_CLI = "SMS_NOKIA_CLI";
	public static final String AIC_CT_NOKIA_PICTURE = "SMS_NOKIA_PICTURE";
	public static final String AIC_CT_NOKIA_RINGTONE = "SMS_NOKIA_RINGTONE";
	public static final String AIC_CT_NOKIA_RTTTL = "SMS_NOKIA_RTTTL";
	public static final String AIC_CT_NOKIA_VCARD = "SMS_NOKIA_VCARD";
	public static final String AIC_CT_NOKIA_VCAL = "SMS_NOKIA_VCAL";

	public static final String AIC_PHONESET_NOKIA = "NOKIA";
	public static final String AIC_PHONESET_SONYERICSSON = "SONY ERICSSON";
	public static final String AIC_PHONESET_MOTOROLA = "MOTOROLA";
	public static final String AIC_PHONESET_ALCATEL = "ALCATEL";
	public static final String AIC_PHONESET_SIEMENS = "SIEMENS";
	public static final String AIC_PHONESET_OTHER = "OTHER";

	// added by jesse for milter feature. 2005-2-16.
	public static final int HAVE_ANTISPAM = 1;
	public static final int HAVE_TEXT2IMAGE = 2;
	public static final int Have_Anti_T2I = 10;
	/**
	 * Creates a new instance of GmmsMessage
	 */
	// added by linda to support cumstom retry
	protected int retriedNumber = 0;
	protected Date nextRetryTime;
	// end add
	private TransactionURI transaction;
	private TransactionURI innerTransaction; // used for internal communication
	private Serializable attachment;
	// private MessageMode messageMode = MessageMode.STORE_FORWARD;
	private String oMncMcc;
	private String rMncMcc;
	private boolean contentIsChanged = false;
	private String sarMsgRefNum = null;
	private int sarTotalSegments;
	private int sarSegmentSeqNum;
	private byte[] udh;
	private int operatorPriority;
	private String originalQueue;

	// private Date dateIn4StroeDR = null;
	private String specialDataCodingScheme;// added by Jianming in v1.0.1 for
											// Polar wireless

	/**
	 * Only for incoming messages.
	 * 0: default 
	 * 1: Password messages 
	 * 2: 2- factor authentication messages 
	 * 3: 2 way chat messages
	 */
	protected int serviceTypeID = 0;
	private String outsender;
	
	
	/**
	 * constructor
	 */
	public GmmsMessage() {
		super.textContent = "";
		super.inTextContent = "";
		// super.subject = null;
		// messageMode = MessageMode.STORE_FORWARD;
	}

	public String getMsgID() {
		return this.msgID;
	}

	public void setMsgID(String msgid) {
		this.msgID = msgid;
	}

	public String getInMsgID() {
		return inMsgID;
	}

	public void setInMsgID(String inMsgID) {
		this.inMsgID = inMsgID;
	}

	public String getOutMsgID() {
		return outMsgID;
	}

	public void setOutMsgID(String outMsgID) {
		this.outMsgID = outMsgID;
	}

	public String getInTransID() {
		return inTransID;
	}

	public void setInTransID(String inTransID) {
		this.inTransID = inTransID;
	}

	public String getOutTransID() {
		return outTransID;
	}

	public void setOutTransID(String outTransID) {
		this.outTransID = outTransID;
	}

	public int getOSsID() {
		return oSsID;
	}

	public void setOSsID(int o_ssid) {
		this.oSsID = o_ssid;
		this.oOperator = o_ssid;
	}

	public int getRSsID() {
		return rSsID;
	}

	public void setRSsID(int r_ssid) {
		this.rSsID = r_ssid;
		this.rOperator = r_ssid;
	}

	public int getOoperator() {
		return oOperator;
	}

	public void setOoperator(int oOperator) {
		this.oOperator = oOperator;
	}

	public int getRoperator() {
		return rOperator;
	}

	public void setRoperator(int rOperator) {
		this.rOperator = rOperator;
	}

	public int getOperatorPriority() {
		return operatorPriority;
	}

	public void setOperatorPriority(int operatorPriority) {
		this.operatorPriority = operatorPriority;
	}

	public void setDeliveryChannel(String channelName) {
		this.deliveryChannel = channelName;
	}

	public String getDeliveryChannel() {
		return this.deliveryChannel;
	}

//	public boolean inClientPull() {
//		return this.inClientPull;
//	}
//
//	public void setInClientPull(boolean inClientPull) {
//		this.inClientPull = inClientPull;
//	}

	public boolean isInCsm() {
		return inCsm;
	}

	public void setInCsm(boolean inCsm) {
		this.inCsm = inCsm;
	}

	public boolean outClientPull() {
		return this.outClientPull;
	}

	public void setOutClientPull(boolean outClientPull) {
		this.outClientPull = outClientPull;
	}

	// public Date getLocalTimeStamp() {
	// return this.localTimeStamp;
	// }
	//
	// public void setLocalTimeStamp(Date localTimeStamp) {
	// this.localTimeStamp = localTimeStamp;
	// }

	public GmmsStatus getStatus() {
		return super.getStatus();
	}

	public void setStatus(GmmsStatus status) {
		super.setStatus(status);
	}

	public String getOMncMcc() {
		return this.oMncMcc;
	}

	public void setOMncMcc(String mncMcc) {
		this.oMncMcc = mncMcc;
	}

	public void setOMncMcc(String mnc, String mcc) {
		this.oMncMcc = mnc + mcc;
	}

	public String getRMncMcc() {
		return this.rMncMcc;
	}

	public void setRMncMcc(String mncMcc) {
		this.rMncMcc = mncMcc;
	}

	public void setRMncMcc(String mnc, String mcc) {
		this.rMncMcc = mnc + mcc;
	}

	public void serializeMimeParts(MimeMultipart mimeMultiPart)
			throws MessagingException, IOException {
		super.serializeMimeParts(mimeMultiPart);
	}

	public MimeMultipart parseMimeParts() throws MessagingException {
		return super.parseMimeParts();
	}

	public GmmsMessage(MessageBase msg) {
		super(msg);
		if (super.textContent == null) {
			super.textContent = "";
		}
		if (super.inTextContent == null) {
			super.inTextContent = "";
		}
		// if(super.subject == null) {
		// super.subject = null;
		// }
		// messageMode = MessageMode.STORE_FORWARD;
	}

	/**
	 * constructor
	 * 
	 * @param oneMsg
	 *            GmmsMessage
	 */
	public GmmsMessage(GmmsMessage oneMsg) {
		super(oneMsg);
		this.inTransID = oneMsg.inTransID;
		this.outTransID = oneMsg.outTransID;
		this.msgID = oneMsg.msgID;
		this.inMsgID = oneMsg.inMsgID;
		this.outMsgID = oneMsg.outMsgID;
		this.rSsID = oneMsg.rSsID;
		this.oSsID = oneMsg.oSsID;
		this.inClientPull = oneMsg.inClientPull;
		this.outClientPull = oneMsg.outClientPull;
		this.deliveryChannel = oneMsg.deliveryChannel;
		this.oOperator = oneMsg.oOperator;
		this.rOperator = oneMsg.rOperator;
		// this.localTimeStamp = oneMsg.localTimeStamp;
		this.oMncMcc = oneMsg.oMncMcc;
		this.rMncMcc = oneMsg.rMncMcc;
		// this.messageMode = oneMsg.messageMode;
		this.retriedNumber = oneMsg.retriedNumber;
		this.nextRetryTime = oneMsg.nextRetryTime;
		this.sarMsgRefNum = oneMsg.getSarMsgRefNum();
		this.sarSegmentSeqNum = oneMsg.getSarSegmentSeqNum();
		this.sarTotalSegments = oneMsg.getSarTotalSeqments();
		this.connectionID = oneMsg.getConnectionID();
		this.operatorPriority = oneMsg.getOperatorPriority();
		this.originalQueue = oneMsg.getOriginalQueue();
		this.innerTransaction = oneMsg.getInnerTransaction();

		if (oneMsg.getUdh() != null) {
			int udhLen = oneMsg.getUdh().length;
			byte[] udhData = new byte[udhLen];
			System.arraycopy(oneMsg.getUdh(), 0, udhData, 0, udhLen);
			this.setUdh(udhData);
		}
		this.contentIsChanged = oneMsg.isContentIsChanged();
		this.specialDataCodingScheme = oneMsg.getSpecialDataCodingScheme();
		this.transaction = oneMsg.getTransaction();
		this.attachment = oneMsg.getAttachment();
		this.deliveryReport = oneMsg.getDeliveryReport();
		this.inCsm = oneMsg.inCsm;
		this.serviceTypeID = oneMsg.getServiceTypeID();
		this.routingSsIDs = oneMsg.getRoutingSsIDs();
		this.outsender = oneMsg.getOutsender();
		this.redisStreamID = oneMsg.redisStreamID;
	}

	// public String getOwnerA2P() {
	// return ownerA2P;
	// }
	//
	// public void setOwnerA2P(String ownerA2P) {
	// this.ownerA2P = ownerA2P;
	// }

	public void setRetriedNumber(int number) {
		this.retriedNumber = number;
	}

	public int getRetriedNumber() {
		return this.retriedNumber;
	}

	public void setNextRetryTime(Date nextTime) {
		this.nextRetryTime = nextTime;
	}

	public Date getNextRetryTime() {
		return this.nextRetryTime;
	}

	public String toString() {
		String prefix = ",";
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		StringBuilder udhBuilder = new StringBuilder(160);
		if (udh != null && udh.length > 0) { // has UDH
			for (byte one : udh) {
				udhBuilder.append(format2Digits(Integer.toHexString(one)));
			}
		}
		StringBuilder mimeBuilder = new StringBuilder(255);
		if (mimeMultiPartData != null && mimeMultiPartData.length > 0) {
			for (byte one : mimeMultiPartData) {
				mimeBuilder.append(format2Digits(Integer.toHexString(one)));
			}
		}

		return new StringBuilder(2048).append("[").append("InMsgID:").append(
				inMsgID).append(prefix).append("InTransID:").append(inTransID)
				.append(prefix).append("OutMsgID:").append(outMsgID).append(
						prefix).append("OutTransID:").append(outTransID)
				.append(prefix).append("SenderAddress:").append(senderAddress)
				.append(prefix).append("RecipientAddress:").append(
						recipientAddress).append(prefix).append(
						"originalSenderAddr:").append(originalSenderAddr)
				.append(prefix).append("originalRecipientAddr:").append(
						originalRecipientAddr).append(prefix).append("OSsID:")
				.append(oSsID).append(prefix).append("RSsID:").append(rSsID)
				.append(prefix).append("O_Operator:").append(oOperator).append(
						prefix).append("R_Operator:").append(rOperator).append(
						prefix).append("OA2P:").append(oA2P)
				.append(prefix)
				.append("CurrentA2P:")
				.append(currentA2P)
				.append(prefix)
				.append("RA2P:")
				.append(rA2P)
				.append(prefix)
				.append("MessageType:")
				.append(messageType)
				.append(prefix)
				.append("GmmsMsgType:")
				.append(gmmsMsgType)
				// .append(prefix).append("MessageMode:").append(messageMode)
				.append(prefix).append("StatusCode:").append(statusCode)
				.append(prefix).append("StatusText:").append(statusText)
				.append(prefix).append("Split:").append(splitStatus).append(
						prefix).append("udh:").append(udhBuilder)
				.append(prefix).append("ContentType:").append(contentType)
				.append(prefix).append("TextContent:").append(textContent)
				.append(prefix).append("inTextContent:").append(inTextContent)
				.append(prefix).append("mimeMultiPartData:")
				.append(mimeBuilder).append(prefix).append("Message size:")
				.append(messageSize).append(prefix).append("Delivery report:")
				.append(deliveryReport).append(prefix).append("Expiry date:").append(expiryDate==null? null : dateFormat.format(expiryDate))
				.append(prefix).append("OriginalQueue:").append(originalQueue)
				.append(prefix).append("ServiceTypeID:").append(serviceTypeID)
				.append(prefix).append("RoutingSsID:").append(routingSsIDs)
				.append(prefix).append("ScheduleDeliveryTime:").append(scheduleDeliveryTime==null? null : dateFormat.format(scheduleDeliveryTime))
				.append("]").toString();
	}

	public String toString4NewMsg() {
		String prefix = ",";
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		StringBuilder udhBuilder = new StringBuilder(160);
		if (udh != null && udh.length > 0) { // has UDH
			for (byte one : udh) {
				udhBuilder.append(format2Digits(Integer.toHexString(one)));
			}
		}

		StringBuilder mimeBuilder = new StringBuilder(255);
		if (mimeMultiPartData != null && mimeMultiPartData.length > 0) {
			for (byte one : mimeMultiPartData) {
				mimeBuilder.append(format2Digits(Integer.toHexString(one)));
			}
		}

		return new StringBuilder(2048).append("[").append("InMsgID:").append(
				inMsgID).append(prefix).append("InTransID:").append(inTransID)
				.append(prefix).append("SenderAddress:").append(senderAddress)
				.append(prefix).append("RecipientAddress:").append(recipientAddress)
				.append(prefix).append("originalSenderAddr:").append(originalSenderAddr)
				.append(prefix).append("originalRecipientAddr:").append(
						originalRecipientAddr)
				.append(prefix).append("OSsID:").append(oSsID).
				append(prefix).append("O_Operator:").append(
						oOperator).append(prefix).append("OA2P:").append(oA2P)
				.append(prefix).append("CurrentA2P:").append(currentA2P)
				.append(prefix).append("MessageType:").append(messageType)
				.append(prefix).append("GmmsMsgType:").append(gmmsMsgType)
				.append(prefix).append("Statuscode:").append(statusCode)
				.append(prefix).append("TimeMark:").append(
						dateFormat.format(timeStamp)).append(prefix).append(
						"udh:").append(udhBuilder).append(prefix).append(
						"ContentType:").append(contentType).append(prefix)
				.append("TextContent:").append(textContent).append(prefix)
				.append("mimeMultiPartData:").append(mimeBuilder)
				.append(prefix).append("Message size:").append(messageSize)
				.append(prefix).append("Delivery report:").append(deliveryReport)
				.append(prefix).append("Expiry date:").append(expiryDate==null? null : dateFormat.format(expiryDate))
				.append(prefix).append("ServiceTypeID:").append(serviceTypeID)
				.append(prefix).append("RoutingSsID:").append(routingSsIDs)
				.append(prefix).append("ScheduleDeliveryTime:").append(scheduleDeliveryTime==null? null : dateFormat.format(scheduleDeliveryTime))
				.append("]").toString();
	}

	/**
	 * 
	 * @param s
	 * @return
	 */
	private String format2Digits(String s) {
		if (s == null) {
			return s;
		}
		s = s.toUpperCase();
		if (s.length() == 2) {
			return s;
		}
		if (s.length() == 1) {
			return "0" + s;
		}
		return s.substring(s.length() - 2);
	}

	/**
	 * Added by Jesse 2005-06-22
	 * 
	 * @return String
	 */
	public String printMsg() {
		return " Message msgID:" + this.msgID + " From:" + this.senderAddress
				+ " To:" + this.recipientAddress;
	}

	// public static void main(String [] args) {
	// GmmsMessage message = new GmmsMessage();
	// message.setTimeStamp(new Date());
	// message.setTextContent("King SMPP TEXT");
	// System.out.println(message);
	// }

	public TransactionURI getTransaction() {
		return transaction;
	}

	public void setTransaction(TransactionURI transaction) {
		this.transaction = transaction;
	}

	public Serializable getAttachment() {
		return attachment;
	}

	public void setAttachment(Serializable attachment) {
		this.attachment = attachment;
	}

	// public MessageMode getMessageMode() {
	// return messageMode;
	// }
	//
	// public void setMessageMode(MessageMode messageMode) {
	// this.messageMode = messageMode;
	// }

	public void setSarMsgRefNum(String refNum) {
		this.sarMsgRefNum = refNum;
	}

	public String getSarMsgRefNum() {
		return this.sarMsgRefNum;
	}

	public void setSarTotalSegments(int total) {
		this.sarTotalSegments = total;
	}

	public int getSarTotalSeqments() {
		return this.sarTotalSegments;
	}

	public void setSarSegmentSeqNum(int seq) {
		this.sarSegmentSeqNum = seq;
	}

	public int getSarSegmentSeqNum() {
		return this.sarSegmentSeqNum;
	}

	public byte[] getUdh() {
		return this.udh;
	}

	public String getConnectionID() {
		return connectionID;
	}

	public boolean isContentIsChanged() {
		return contentIsChanged;
	}

	public void setUdh(byte[] udh) {
		this.udh = udh;
	}

	public void setConnectionID(String connectionID) {
		this.connectionID = connectionID;
	}

	public void setContentIsChanged(boolean contentIsChanged) {
		this.contentIsChanged = contentIsChanged;
	}

	public void setOriginalQueue(String originalQueue) {
		this.originalQueue = originalQueue;
	}

	public String getOriginalQueue() {
		return this.originalQueue;
	}

	public String getSpecialDataCodingScheme() {
		return specialDataCodingScheme;
	}

	public void setSpecialDataCodingScheme(String specialDataCodingScheme) {
		this.specialDataCodingScheme = specialDataCodingScheme;
	}

	public TransactionURI getInnerTransaction() {
		return innerTransaction;
	}

	public void setInnerTransaction(TransactionURI innerTransaction) {
		this.innerTransaction = innerTransaction;
	}

	// Add by Will for http common module on 2012-02-08
	public boolean setProperty(String propertyName, Object value) {
		boolean bret = false;
		Method[] methods = GmmsMessage.class.getMethods();
		for (Method f : methods) {
			if (f.getName().equalsIgnoreCase("set" + propertyName)) {
				try {
					f.invoke(this, value);
					bret = true;
					break;
				} catch (Exception e) {
					System.out.println("set properyt " + propertyName + " error:" + e.getMessage());
					continue;
				}
				
			}
		}
		return bret;
	}

	public Object getProperty(String propertyName) {
		Method[] methods = GmmsMessage.class.getMethods();
		Object obj = null;
		try {
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("get" + propertyName)) {
					obj = f.invoke(this);
				}
			}
		} catch (Exception e) {
			System.out.println("get properyt " + propertyName + " error!");
		}
		return obj;
	}
	
	

	public String getRoutingSsIDs() {
		return routingSsIDs;
	}

	public void setRoutingSsIDs(String routingSsIDs) {
		this.routingSsIDs = routingSsIDs;
	}

	/**
	 * 
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public ByteBuffer serialize() throws Exception, ClassNotFoundException {
		// object to bytearray
		ByteArrayOutputStream bo = new ByteArrayOutputStream();
		ObjectOutputStream oo = null;
		ByteBuffer byteBuffer = null;
		try {
			oo = new ObjectOutputStream(bo);
			oo.writeObject(this);
			byteBuffer = new ByteBuffer(bo.toByteArray());
		} finally {
			try {
				if (oo != null) {
					oo.close();
				}
			} catch (Exception e) {
				throw e;
			}
		}
		return byteBuffer;
	}
	
	public int getInClientPull() {
		return inClientPull;
	}

	public void setInClientPull(int inClientPull) {
		this.inClientPull = inClientPull;
	}

	public int getServiceTypeID() {
		return serviceTypeID;
	}

	public void setServiceTypeID(String serviceTypeID) {
		this.serviceTypeID = Integer.valueOf(serviceTypeID);
	}
	
	public void setServiceTypeID(int serviceTypeID) {
		this.serviceTypeID = serviceTypeID;
	}

	public String getOutsender() {
		return outsender;
	}

	public void setOutsender(String outsender) {
		this.outsender = outsender;
	}

	public String getRedisStreamID() {
		return redisStreamID;
	}

	public void setRedisStreamID(String redisStreamID) {
		this.redisStreamID = redisStreamID;
	}
	
	
}
