package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.*;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CommandDeliveryReport
    extends Request {
    private static SystemLogger log = SystemLogger.getSystemLogger(
        CommandDeliveryReport.class);
    protected SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
    private String msgId;
    private String sessionID;
    private int o_op = -1;
    private int r_op = -1;
    private int o_hub = -1;
    private int r_hub = -1;
    private int c_hub = -1;
    private int o_relay = -1;
    private int r_relay = -1;
	private String timeMark;
    private String timeExpiry;
    private String sender;
	private String recipient;
    private int data_coding;
    private int statusCode = -1;
    private String  statusText;
    private byte[] content;
    private byte[] binaryContent;
    private String transactionId;
    private TransactionURI transaction = null;
    private String connectionID;
    private int priority = -1;
    private String senderAddrType = null;
    private String recipientAddrType = null;
    private String senderAddrTon = null;
    private String recipientAddrTon = null;
    
    /**
     * for cdr
     */
    private String deliveryChannel;

    public CommandDeliveryReport() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(this.COMMAND_DELIVERY_REPORT);
    }

    public String getMsgId() {
        return msgId;
    }

    public void parsePduCommand(TcpByteBuffer buffer) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException {
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
                case FIELD_TIMEMARK:
                    timeMark = buffer.removeString(length);
                    break;
                case FIELD_TIMEEXPIRY:
                    timeExpiry = buffer.removeString(length);
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
                case FIELD_CHUB:
                    c_hub = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_ORELAY:
                    o_relay = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_RRELAY:
                    r_relay = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_DATACODING:
                    data_coding = buffer.removeBytesAsInt(length);
                    break;    
                case FIELD_STATUS:
                    statusCode = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_CONTENT:
                    tempBuffer = buffer.removeBytes(length);
                    if (tempBuffer != null) {
                        content = tempBuffer.getBuffer();
                    }
                    break;  
                case FIELD_BINARYCONTENT:
                    tempBuffer = buffer.removeBytes(length);
                    if (tempBuffer != null) {
                        binaryContent = tempBuffer.getBuffer();
                    }
                    break;
                case FIELD_TRANSACTIONURI:
                	String uriString = buffer.removeString(length);
                    if (uriString != null) {
                    	transaction = TransactionURI.fromString(uriString);
                    }
                	break;
                case FIELD_TRANSACTIONID:
                    transactionId = buffer.removeString(length);
                    break;
                case FIELD_CONNECTIONID:
                	connectionID = buffer.removeString(length);
                	break;
                case FIELD_PRIORITY:
                	priority = buffer.remove1ByteAsSignInt();;
                	break;
                case FIELD_SENDERADDRTYPE:
                    senderAddrType = buffer.removeString(length);
                    break;
                case FIELD_RECIPIENTADDRTYPE:
                    recipientAddrType = buffer.removeString(length);
                    break;
                case FIELD_SENDERADDRTON:
                	senderAddrTon = buffer.removeString(length);
                    break;
                case FIELD_RECIPIENTADDRTON:
                    recipientAddrTon = buffer.removeString(length);
                    break;
                case FIELD_DELIVERYCHANNEL:
                	deliveryChannel = buffer.removeString(length);
                	break;
                /*case FIELD_STATUSTEXT:
                	statusText = buffer.removeString(length);
                	break;*/
                default:
                    log.warn("Cant find field with tag: {},len:{}"
                             ,tag,length);
                    buffer.removeBytes(length);
                    break;
            }
        }
    }
    
    private void initGmmsMessage4Server(GmmsMessage msg){
    	msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		/*
		 * if(log.isDebugEnabled()){
		 * log.debug("dr pdu deliveryChannel: {}, msg orgi deli:{}",deliveryChannel,msg.
		 * getDeliveryChannel()); }
		 */
    	if(deliveryChannel!=null){
    		msg.setDeliveryChannel(deliveryChannel);
    	}
    	
    	if(msgId!=null){
        	msg.setInMsgID(msgId);
        }
        if(sessionID!=null){
        	msg.setMsgID(sessionID);
        }
        if(transaction!=null){
        	msg.setTransaction(transaction);
        }
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
        if (c_hub > 0) {
            msg.setCurrentA2P(c_hub);
        }
        try{
        	TimeZone local = TimeZone.getDefault();
            if(timeMark != null){
                Date gmtTimemark = dateFormat.parse(timeMark);
                msg.setTimeStamp(gmtTimemark);
            }

            if(timeExpiry != null) {
                msg.setExpiryDate(dateFormat.parse(timeExpiry));
            }
        }catch(Exception e){
        	log.warn(e,e);
        }
        msg.setSenderAddress(sender);
        msg.setRecipientAddress(recipient);

        msg.setContentType(DataCoding.handleDatecoding(data_coding));
        if(content != null && content.length > 0){
            try {
				msg.setTextContent(new String(content,msg.getContentType()));
			} catch (UnsupportedEncodingException e) {
				log.warn(msg, "Fail to parse the text content");
			}
        }else if(binaryContent != null && binaryContent.length > 0){
        	msg.setMimeMultiPartData(binaryContent);
        }
        msg.setDateIn(new Date());
        msg.setStatus(GmmsStatus.getStatus(statusCode));
        msg.setConnectionID(connectionID);
        msg.setSenderAddrType(senderAddrType);
        msg.setRecipientAddrType(recipientAddrType);
        msg.setSenderAddrTon(senderAddrTon);
        msg.setRecipientAddrTon(recipientAddrTon);
        if (priority != -1) {
			msg.setPriority(this.priority);
		}
    }
    
    private void initGmmsMessage4Core(GmmsMessage msg){
    	msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
		/*
		 * if(log.isDebugEnabled()){
		 * log.debug("dr pdu deliveryChannel: {}, msg orgi deli:{}",deliveryChannel,msg.
		 * getDeliveryChannel()); }
		 */
    	msg.setDeliveryChannel(deliveryChannel);
        msg.setOutTransID(transactionId);
        if(transaction!=null){
        	msg.setTransaction(transaction);
        }
		msg.setRSsID(r_relay);
		msg.setOSsID(o_relay);
		msg.setOutMsgID(msgId);
        msg.setStatus(GmmsStatus.getStatus(statusCode));
        msg.setConnectionID(connectionID);
    }
    

    public GmmsMessage convertToMsg(boolean isCore) {
        if (header == null) {
            return null;
        }
        GmmsMessage msg = new GmmsMessage();
        try {
        	if(isCore){
        		initGmmsMessage4Core(msg);
            }else {
        		initGmmsMessage4Server(msg);
            }
        } catch (Exception e) {
            log.warn(e, e);
        }
        return msg;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {

        TcpByteBuffer buffer = new TcpByteBuffer();

        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_SESSIONID, sessionID);
        appendParameterToBuffer(buffer, FIELD_OOPERATOR, o_op);
        appendParameterToBuffer(buffer, FIELD_ROPERATOR, r_op);
        appendParameterToBuffer(buffer, FIELD_OHUB, o_hub);
        appendParameterToBuffer(buffer, FIELD_RHUB, r_hub);
        appendParameterToBuffer(buffer, FIELD_CHUB, c_hub);
        appendParameterToBuffer(buffer, FIELD_ORELAY, o_relay);
        appendParameterToBuffer(buffer, FIELD_RRELAY, r_relay);
        appendParameterToBuffer(buffer, FIELD_TIMEMARK, timeMark);
        appendParameterToBuffer(buffer, FIELD_TIMEEXPIRY, timeExpiry);
        appendParameterToBuffer(buffer, FIELD_SENDER, sender);
        appendParameterToBuffer(buffer, FIELD_RECIPIENT, recipient);
        
        appendParameterToBuffer(buffer, FIELD_DATACODING, data_coding, 1);
        if(binaryContent != null && binaryContent.length > 0){
	        if (data_coding == 2 || data_coding == 4) {
	            appendParameterToBuffer(buffer, FIELD_BINARYCONTENT, binaryContent);
	        }
        }
        if(content != null && content.length > 0){
	        if (data_coding != 2 && data_coding != 4) {
	            appendParameterToBuffer(buffer, FIELD_CONTENT, content);
	        }
        }
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
        appendParameterToBuffer(buffer, FIELD_TRANSACTIONID, transactionId);
        appendParameterToBuffer(buffer, FIELD_CONNECTIONID,connectionID);
        appendParameterToBuffer(buffer, FIELD_PRIORITY, priority, 1);
        appendParameterToBuffer(buffer, FIELD_SENDERADDRTYPE, senderAddrType);
        appendParameterToBuffer(buffer, FIELD_RECIPIENTADDRTYPE, recipientAddrType);
        appendParameterToBuffer(buffer, FIELD_SENDERADDRTON, senderAddrTon);
        appendParameterToBuffer(buffer, FIELD_RECIPIENTADDRTON, recipientAddrTon);        
        appendParameterToBuffer(buffer, FIELD_DELIVERYCHANNEL, deliveryChannel);
        if(transaction!=null){
        	appendParameterToBuffer(buffer, FIELD_TRANSACTIONURI,
            		transaction.toString());
        }

        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_DELIVERY_REPORT:")
            .append("msgId:").append(msgId).append(",")
            .append("sessionID:").append(sessionID).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("TransactionID:").append(transactionId).append(",")
            .append("transaction:").append(transaction).append(",")
            .append("r_relay:").append(r_relay).append(",")
            .append("r_hub:").append(r_hub).append(",")
            .append("c_hub:").append(c_hub)
            .toString();
    }

    protected Respond createResponse() {
        return null;
    }

    public void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    	convertFromMsg(msg,true);
    }
    /**
     * 
     * @param msg
     * @param in
     * @throws NotEnoughDataInByteBufferException
     * @throws UnsupportedEncodingException
     */
    public void convertFromMsg(GmmsMessage msg,boolean in) throws
    	NotEnoughDataInByteBufferException, UnsupportedEncodingException {
	    if (msg == null) {
	        return;
	    }
	    if(in){
	    	msgId = msg.getInMsgID();
	    	sessionID = msg.getMsgID();
	    	transactionId = msg.getInTransID();
	        TcpByteBuffer tempBuffer = null;
	        if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg.getGmmsMsgType())) {
		        if (tempBuffer == null) {
		                tempBuffer = new TcpByteBuffer(msg.getMimeMultiPartData());
		        } else {
		                tempBuffer.appendBytes(new TcpByteBuffer(msg.
		                    getMimeMultiPartData()));
		        }
		        data_coding = 2;
		        if (tempBuffer != null) {
		            binaryContent = tempBuffer.getBuffer();
		        }
	         } else if(msg.getContentType()!=null){
	            data_coding = DataCoding.getDataCoding(msg.getContentType());
	            if (msg.getTextContent() != null) {
	                content = msg.getTextContent().getBytes(msg.getContentType());
	         }
	       }
        }else{
        	msgId = msg.getOutMsgID();
        	sessionID = msg.getMsgID();
        	transactionId = msg.getOutTransID();
        }
	    transaction = msg.getTransaction();
	    statusCode = msg.getStatusCode();
	    o_op = msg.getOoperator();
        r_op = msg.getRoperator();
        o_hub = msg.getOA2P();
        r_hub = msg.getRA2P();
        c_hub = msg.getCurrentA2P();
        o_relay = msg.getOSsID();
        r_relay = msg.getRSsID();
        if(msg.getTimeStamp()!=null){
        	timeMark = dateFormat.format(msg.getTimeStamp());
        }
        sender = msg.getSenderAddress();
        recipient = msg.getRecipientAddress();
       deliveryChannel = msg.getDeliveryChannel();
       connectionID  = msg.getConnectionID();
       priority = msg.getPriority();
       senderAddrType = msg.getSenderAddrType();
       recipientAddrType = msg.getRecipientAddrType();
       senderAddrTon = msg.getSenderAddrTon();
       recipientAddrTon = msg.getRecipientAddrTon();
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
	
	public int getO_relay() {
		return o_relay;
	}

	public void setO_relay(int o_relay) {
		this.o_relay = o_relay;
	}

	public int getR_relay() {
		return r_relay;
	}

	public void setR_relay(int r_relay) {
		this.r_relay = r_relay;
	}

	public TransactionURI getTransaction() {
		return transaction;
	}

	public void setTransaction(TransactionURI transaction) {
		this.transaction = transaction;
	}

	public GmmsMessage convertToMsg(BufferMonitor buffer) {
        return convertToMsg(false);
	}

	public String getDeliveryChannel() {
		return deliveryChannel;
	}

	public void setDeliveryChannel(String deliveryChannel) {
		this.deliveryChannel = deliveryChannel;
	}

	public int getO_hub() {
		return o_hub;
	}

	public int getR_hub() {
		return r_hub;
	}

	public int getC_hub() {
		return c_hub;
	}

	public void setO_hub(int o_hub) {
		this.o_hub = o_hub;
	}

	public void setR_hub(int r_hub) {
		this.r_hub = r_hub;
	}

	public void setC_hub(int c_hub) {
		this.c_hub = c_hub;
	}
	
}

