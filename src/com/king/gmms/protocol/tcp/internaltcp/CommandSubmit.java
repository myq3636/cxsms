package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageIdGenerator;


public class CommandSubmit
    extends Request {
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
    private String scheduleDeliveryTime;
    private String sender;
    private String oMncMcc;
    private String rMncMcc;
    private String senderAddrType = null;
    private String recipientAddrType = null;
    private String senderAddrTon = null;
    private String recipientAddrTon = null;
	private String recipient;
    private int data_coding;
    private byte[] content;
    private int deliveryReport;
    private String refNum;
    private int totalSegments;
    private int seqNum;
    private int milterActionCode;
    private int udhIndicator = 0;
    private String transactionId;
    private byte[] binaryContent;
    protected int statusCode = -1;
    private TransactionURI transaction = null;
    private String connectionID;
    private String specialDCS;
    private int priority = -1;
    private int messageSize = 0;
    
    /**
     * for 1.5way/ott to keep original value, CDR required
     */
    private String original_sender;
    private String original_recipient;
    
    /**
     * for cdr
     */
    private String deliveryChannel;
    
    private int serviceTypeID = -1;
    
    public CommandSubmit() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(COMMAND_SUBMIT);
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
                case FIELD_SESSIONID:
                    sessionID = buffer.removeString(length);
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
                case FIELD_CONTENT:
                    tempBuffer = buffer.removeBytes(length);
                    if (tempBuffer != null) {
                        content = tempBuffer.getBuffer();
                    }
                    break;
                case FIELD_NEED_DR:
                    deliveryReport = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_SAR_ReferenceNumber:
                    refNum = buffer.removeString(length);
                    break;
                case FIELD_SAR_TotalSegments:
                    totalSegments = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_SAR_SegementsSeqnum:
                    seqNum = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_UDH_INDICATOR:
                    udhIndicator = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_MILTERACTIONCODE:
                    milterActionCode = buffer.removeBytesAsInt(length);
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
                    	if(log.isDebugEnabled()){
            				log.debug("convertFromString uriString:{}",uriString);
                    	}
                    	transaction = TransactionURI.fromString(uriString);
                    }
                	break;
                case FIELD_TRANSACTIONID:
                    transactionId = buffer.removeString(length);
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
                case FIELD_OMNCMCC:
                    oMncMcc = buffer.removeString(length);
                    break;
                case FIELD_RMNCMCC:
                    rMncMcc = buffer.removeString(length);
                    break;
                case FIELD_STATUS:
                    statusCode = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_CONNECTIONID:
                	connectionID = buffer.removeString(length);
                	break;
                case FIELD_SPECIALDCS:
                	specialDCS  = buffer.removeString(length);
                	break;
                case FIELD_ORIGINAL_SENDER:
                	original_sender = buffer.removeString(length);
                    break;
                case FIELD_ORIGINAL_RECIPIENT:
                	original_recipient = buffer.removeString(length);
                    break;
                case FIELD_PRIORITY:
                	priority = buffer.remove1ByteAsSignInt();
                    break;
                case FIELD_MESSAGESIZE:
                	messageSize = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_DELIVERYCHANNEL:
                	deliveryChannel = buffer.removeString(length);
                	break;
                case FIELD_SERVICETYPEID:
                	serviceTypeID = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_SCHEDULE_DELIVERY_TIME:
                	scheduleDeliveryTime = buffer.removeString(length);
                	break;
                default:
                    log.warn("Cant find field with tag: {},len:{}"
                             ,tag,length);
                    buffer.removeBytes(length);
                    break;
            }
        }
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
        if (header == null) {
            return null;
        }

        GmmsMessage msg = new GmmsMessage();
        try {
            msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
            msg.setMsgID(msgId);
            msg.setInMsgID(sessionID);
            msg.setInTransID(transactionId);
            msg.setOoperator(o_op);
            msg.setRoperator(r_op);
            msg.setOSsID(o_relay);
            msg.setRSsID(r_relay);
            int oA2P = gmmsUtility.getCustomerManager().getConnectedRelay(o_relay,GmmsMessage.MSG_TYPE_SUBMIT);
            if (o_hub > 0) {
                msg.setOA2P(o_hub);
            }else{
            	msg.setOA2P(oA2P);
            }
            if (r_hub > 0) {
                msg.setRA2P(r_hub);
            }
            if (c_hub > 0) {
                msg.setCurrentA2P(c_hub);
            }
            TimeZone local = TimeZone.getDefault();
            if(timeMark != null){
                Date gmtTimemark = dateFormat.parse(timeMark);
                msg.setTimeStamp(gmtTimemark);
            }

            if(timeExpiry != null) {
                msg.setExpiryDate(dateFormat.parse(timeExpiry));
            }
            
            if (scheduleDeliveryTime != null) {
            	msg.setScheduleDeliveryTime(dateFormat.parse(scheduleDeliveryTime));
            }

            msg.setSenderAddress(sender);
            msg.setRecipientAddress(recipient);
            
            msg.setOriginalSenderAddr(original_sender);
            msg.setOriginalRecipientAddr(original_recipient);

            msg.setContentType(DataCoding.handleDatecoding(data_coding));

            
            boolean hasUdh = udhIndicator == 1 ? true : false;

            if (hasUdh || data_coding == 2 || data_coding == 4) {
                TcpByteBuffer binaryContentData = new TcpByteBuffer(
                    binaryContent);
                int len = binaryContentData.length();
                int udhLen = 0;
                if (hasUdh) {
                    udhLen = binaryContentData.read1ByteAsInt();
                    byte[] udh = binaryContentData.removeBytes(udhLen + 1).
                        getBuffer();
                    msg.setUdh(udh);
                    len = len - udhLen - 1;
                }
                if ( (data_coding == 2 || data_coding == 4) && len > 0) {
                    msg.setMimeMultiPartData(binaryContentData.removeBytes(
                        len).getBuffer());
                    msg.setGmmsMsgType(GmmsMessage.
                                       AIC_MSG_TYPE_BINARY);
                    msg.setContentType(GmmsMessage.
                                       AIC_MSG_TYPE_BINARY);
                    //msg.setMessageSize(len);
                }
            }

            switch (data_coding) {
                case 2:
                case 4:
                    break;
                case 0: {
                    msg.setTextContent(new String(content));
                    msg.setContentType(GmmsMessage.AIC_CS_ASCII);
                    //msg.setMessageSize(msg.getTextContent().getBytes(msg.getContentType()).length);
                    break;
                }
                default: {
                    TcpByteBuffer contentData = new TcpByteBuffer(content);
                    int len = contentData.length();
                    if(len <= 0) {
                        msg.setTextContent("");
                        msg.setMessageSize(0);
                    } else {
                        String content = contentData.removeString(len,
                            msg.getContentType());
                        msg.setTextContent(content);
                        //msg.setMessageSize(len);
                    }
                    break;
                }
            }

            msg.setDeliveryReport(deliveryReport == 1 ? true : false);

            if (msg.getTimeStamp() == null) {
                Date now = new Date();
                long diff = local.getRawOffset();
                if (local.inDaylightTime(now)) {
                    diff += local.getDSTSavings();
                }
                msg.setTimeStamp(new Date(now.getTime() - diff));
            }
            
            A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();           
            if (msg.getCurrentA2P() <= 0) {
            	if(ctm.inCurrentA2P(oA2P)){
            		 msg.setCurrentA2P(oA2P);
            	}else{
            		msg.setCurrentA2P(ctm.getCurrentA2P());
            	}
            }
            
            msg.setTransaction(transaction);
            msg.setSenderAddrType(senderAddrType);
            msg.setRecipientAddrType(recipientAddrType);
            msg.setSenderAddrTon(senderAddrTon);
            msg.setRecipientAddrTon(recipientAddrTon);
            msg.setOMncMcc(oMncMcc);
            msg.setRMncMcc(rMncMcc);
            msg.setStatusCode(statusCode);
            msg.setConnectionID(connectionID);
            msg.setSpecialDataCodingScheme(specialDCS);
            msg.setSarMsgRefNum(refNum);
            msg.setSarSegmentSeqNum(seqNum);
            msg.setSarTotalSegments(totalSegments);
            msg.setPriority(priority);
            msg.setDeliveryChannel(deliveryChannel);
            msg.setMilterActionCode(milterActionCode);
            msg.setMessageSize(messageSize);
            msg.setServiceTypeID(serviceTypeID);
            
        }
        catch (Exception e) {
            log.error(msg,e, e);
            return null;
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
        appendParameterToBuffer(buffer, FIELD_SENDERADDRTYPE, senderAddrType);
        appendParameterToBuffer(buffer, FIELD_SENDERADDRTON,senderAddrTon);
        appendParameterToBuffer(buffer, FIELD_RECIPIENT, recipient);
        appendParameterToBuffer(buffer, FIELD_RECIPIENTADDRTYPE, recipientAddrType);
        appendParameterToBuffer(buffer, FIELD_RECIPIENTADDRTON, recipientAddrTon);
        appendParameterToBuffer(buffer, FIELD_UDH_INDICATOR, udhIndicator, 1);
        appendParameterToBuffer(buffer, FIELD_DATACODING, data_coding, 1);
        appendParameterToBuffer(buffer, FIELD_TRANSACTIONID, transactionId);
        if (udhIndicator > 0 || data_coding == 2 || data_coding == 4) {
            appendParameterToBuffer(buffer, FIELD_BINARYCONTENT, binaryContent);
        }

        if (data_coding != 2 && data_coding != 4) {
            appendParameterToBuffer(buffer, FIELD_CONTENT, content);
        }
        appendParameterToBuffer(buffer, FIELD_OMNCMCC, oMncMcc);
        appendParameterToBuffer(buffer, FIELD_RMNCMCC, rMncMcc);
        appendParameterToBuffer(buffer, FIELD_NEED_DR, deliveryReport, 1);
        appendParameterToBuffer(buffer, FIELD_SAR_ReferenceNumber, refNum);
        appendParameterToBuffer(buffer, FIELD_SAR_TotalSegments,totalSegments,1);
        appendParameterToBuffer(buffer, FIELD_SAR_SegementsSeqnum,seqNum, 1);
        appendParameterToBuffer(buffer, FIELD_MILTERACTIONCODE,milterActionCode);
        appendParameterToBuffer(buffer, FIELD_CONNECTIONID,connectionID);
        appendParameterToBuffer(buffer, FIELD_SPECIALDCS,specialDCS);
        if(statusCode > 0){
        	appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
        }
        if(transaction!=null){
        	appendParameterToBuffer(buffer, FIELD_TRANSACTIONURI,
            		transaction.toString());
        }
        
        appendParameterToBuffer(buffer, FIELD_ORIGINAL_SENDER, original_sender);
        appendParameterToBuffer(buffer, FIELD_ORIGINAL_RECIPIENT, original_recipient);
        appendParameterToBuffer(buffer, FIELD_PRIORITY, priority, 1);
        appendParameterToBuffer(buffer, FIELD_DELIVERYCHANNEL, deliveryChannel);
        appendParameterToBuffer(buffer, FIELD_MESSAGESIZE, messageSize);
        appendParameterToBuffer(buffer, FIELD_SERVICETYPEID, serviceTypeID, 4);
        appendParameterToBuffer(buffer, FIELD_SCHEDULE_DELIVERY_TIME, scheduleDeliveryTime);
        return buffer;
    }
    
    public void convertFromMsg(GmmsMessage msg) throws
    			NotEnoughDataInByteBufferException, UnsupportedEncodingException{
    	convertFromMsg(msg,true);
    }
    
    
    public void convertFromMsg(GmmsMessage msg, boolean in) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException{
        if (msg == null) {
            return;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        String msgId = msg.getMsgID();
        if (msgId == null) {
        	this.msgId = MessageIdGenerator.generateCommonMsgID(msg.getCurrentA2P());
        }
        else {
            this.msgId = msg.getMsgID();
        }

        if(in){
            sessionID = msg.getInMsgID();
            transactionId = msg.getInTransID();
        }        
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
        if(msg.getExpiryDate()!=null){
            timeExpiry = dateFormat.format(msg.getExpiryDate());
        }
        if (msg.getScheduleDeliveryTime() != null) {
        	scheduleDeliveryTime = dateFormat.format(msg.getScheduleDeliveryTime());
        }
        sender = msg.getSenderAddress();
        senderAddrType = msg.getSenderAddrType();
        senderAddrTon = msg.getSenderAddrTon();
        recipient = msg.getRecipientAddress();
        recipientAddrType = msg.getRecipientAddrType();
        recipientAddrTon = msg.getRecipientAddrTon();
        
        original_sender = msg.getOriginalSenderAddr();
        original_recipient = msg.getOriginalRecipientAddr();

        oMncMcc = msg.getOMncMcc();
        rMncMcc = msg.getRMncMcc();
        
        if (msg.getUdh() != null && msg.getUdh().length > 0) {
            udhIndicator = 1;
        }

        TcpByteBuffer tempBuffer = null;
        if (msg.getUdh() != null) {
            tempBuffer = new TcpByteBuffer(msg.getUdh());
        }

        if (GmmsMessage.AIC_MSG_TYPE_BINARY.equalsIgnoreCase(msg.
            getGmmsMsgType())) {
            if (tempBuffer == null) {
                tempBuffer = new TcpByteBuffer(msg.getMimeMultiPartData());
            }
            else {
                tempBuffer.appendBytes(new TcpByteBuffer(msg.
                    getMimeMultiPartData()));
            }
            data_coding = 2;
        }
        else {
            data_coding = DataCoding.getDataCoding(msg.getContentType());
            if (msg.getTextContent() != null) {
                content = msg.getTextContent().getBytes(msg.getContentType());
            }
        }

        if (tempBuffer != null) {
            binaryContent = tempBuffer.getBuffer();
        }

        milterActionCode = msg.getMilterActionCode();
        boolean isNeedDR = msg.getDeliveryReport();
        if(isNeedDR){
            deliveryReport=1;
        }else{
            deliveryReport=0;
        }

        transaction = msg.getTransaction(); 
        statusCode = msg.getStatusCode();
        connectionID  = msg.getConnectionID();
        specialDCS = msg.getSpecialDataCodingScheme();
        refNum = msg.getSarMsgRefNum();
        seqNum= msg.getSarSegmentSeqNum();
        totalSegments = msg.getSarTotalSeqments();
        priority = msg.getPriority();
        deliveryChannel = msg.getDeliveryChannel();
        messageSize = msg.getMessageSize();
        serviceTypeID = msg.getServiceTypeID();
        
    }

    public String toString() {
        return new StringBuffer("COMMAND_SUBMIT:")
            .append("msgId:").append(msgId).append(",")
            .append("sessionID:").append(sessionID).append(",")
            .append("o_op:").append(o_op).append(",")
            .append("r_op:").append(r_op).append(",")
            .append("o_relay:").append(o_relay).append(",")
            .append("r_relay:").append(r_relay).append(",")
            .append("o_a2p:").append(o_hub).append(",")
            .append("r_a2p:").append(r_hub).append(",")
            .append("sender:").append(sender).append(",")
            .append("recipient:").append(recipient).append(",")
            .append("original_sender:").append(original_sender).append(",")
            .append("original_recipient:").append(original_recipient).append(",")
            .append("timeMark:").append(timeMark).append(",")
            .append("datacoding:").append(data_coding).append(",")
            .append("TransactionURI:").append(transaction).append(",")
            .append("DeliveryChannel:").append(deliveryChannel).append(",")
            .append("TransactionID:").append(transactionId).append(",")
            .append("milterActionCode:").append(milterActionCode).append(",")
            .append("messageSize:").append(messageSize).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("ServiceTypeID:").append(serviceTypeID).append(",")
            .append("deliveryReport:").append(deliveryReport).append(",")
            .append("ScheduleDeliveryTime:").append(scheduleDeliveryTime)
            .toString();
    }

    protected Respond createResponse() {
        return null;
    }

	public TransactionURI getTransaction() {
		return transaction;
	}

	public void setTransaction(TransactionURI transaction) {
		this.transaction = transaction;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}

	public String getTransactionId() {
		return transactionId;
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

	public int getMessageSize() {
		return messageSize;
	}

	public void setMessageSize(int messageSize) {
		this.messageSize = messageSize;
	}

	public int getServiceTypeID() {
		return serviceTypeID;
	}

	public void setServiceTypeID(int serviceTypeID) {
		this.serviceTypeID = serviceTypeID;
	}
	
	
}
