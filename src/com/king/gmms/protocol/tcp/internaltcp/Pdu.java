package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.protocol.tcp.internaltcp.exception.HeaderIncompleteException;
import com.king.gmms.protocol.tcp.internaltcp.exception.InvalidPduException;
import com.king.gmms.protocol.tcp.internaltcp.exception.MessageIncompleteException;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownCommandIdException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnsupportedCommandIdException;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;


/**
 * Insert the type's description here.
 * Creation date: (1/3/2003 3:14:41 PM)
 * @author: Administrator
 */
public abstract class Pdu {
    protected static SystemLogger log = SystemLogger.getSystemLogger(Pdu.class);
    protected GmmsUtility gmmsUtility = GmmsUtility.getInstance();
    protected SimpleDateFormat dateFormat = new SimpleDateFormat(
        "yyyyMMddHHmmss");
    public static String datetimeZonePattern = "yyyyMMddHHmmsszzz";
    protected PduHeader header = null;
    protected A2PMultiConnectionInfo customerInfo = null;
    public static int PDU_HEADER_SIZE = 5;

    public static final int VERSION_1_0 = 10;
    public static final int VERSION_2_0 = 20;

    //Command Type
    public static final int COMMAND_SUBMIT = 0x01;
    public static final int COMMAND_SUBMIT_ACK = 0x02;
    public static final int COMMAND_DELIVERY_REPORT = 0x03;
    public static final int COMMAND_DELIVERY_REPORT_ACK = 0x04;
    public static final int COMMAND_ALIVE = 0x05;
    public static final int COMMAND_ALIVE_ACK = 0x06;
    public static final int COMMAND_BIND = 0x10;
    public static final int COMMAND_BIND_ACK = 0x11;
    public static final int COMMAND_INNER_ACK = 0x12;
    public static final int COMMAND_SUBMIT4MQM = 0x13;
    public static final int COMMAND_DELIVERY_REPORT4MQM = 0x14;
    public static final int COMMAND_ACK4MQM = 0x15;
    public static final int COMMAND_DELIVERY_REPORT_QUERY = 0x16;
    public static final int COMMAND_DELIVERY_REPORT_QUERY_ACK = 0x17;
    
    public static final int COMMAND_DELIVERY_REPORT_QUERY4MQM = 0x18;

    //Parameter Tag Identifier
    public static final int FIELD_MSGID = 0x01;
    public static final int FIELD_SESSIONID = 0x02;
    public static final int FIELD_OOPERATOR = 0x03;
    public static final int FIELD_ROPERATOR = 0x04;
    public static final int FIELD_ORELAY = 0x05;
    public static final int FIELD_RRELAY = 0x06;
    public static final int FIELD_OHUB = 0x07;
    public static final int FIELD_RHUB = 0x08;
    public static final int FIELD_TIMEMARK = 0x09;
    public static final int FIELD_TIMEEXPIRY = 0x0a;
    public static final int FIELD_SENDER = 0x0b;
    public static final int FIELD_RECIPIENT = 0x0c;
    public static final int FIELD_CONTENT = 0x0d;
    public static final int FIELD_DATACODING = 0x0e;
    public static final int FIELD_STATUS = 0x0f;
    public static final int FIELD_STATUS_INTERNAL = 0x10;
    public static final int FIELD_NEED_DR = 0x11;
    public static final int FIELD_SAR_ReferenceNumber = 0x12;
    public static final int FIELD_SAR_TotalSegments = 0x13;
    public static final int FIELD_SAR_SegementsSeqnum = 0x14;
    public static final int FIELD_UDH_INDICATOR = 0x15;
    public static final int FIELD_GEMD = 0x17;
    public static final int FIELD_TRANSACTIONID = 0x1a;
    public static final int FIELD_PRIORITY = 0x1c;
    public static final int FIELD_VERSION = 0x23;
    public static final int FIELD_USERNAME = 0x24;
    public static final int FIELD_PASSWORD = 0x25;
    public static final int FIELD_TIMESTAMP = 0x26;
    public static final int FIELD_CUSTMSGID = 0x27;
    public static final int FIELD_VASPTOKEN = 0x28;
    public static final int FIELD_MILTERACTIONCODE = 0x29;
    public static final int FIELD_BINARYCONTENT = 0x2A;
    public static final int FIELD_TRANSACTIONURI = 0x2B;
    public static final int FIELD_ORIGINALQUEUE = 0x2c;
    public static final int FIELD_OMNCMCC = 0x2d;
    public static final int FIELD_RMNCMCC = 0x2e;
    public static final int FIELD_SENDERADDRTYPE = 0x2f;
    public static final int FIELD_RECIPIENTADDRTYPE = 0x30;
    public static final int FIELD_CONNECTIONID = 0x31;
    public static final int FIELD_RETRIEDNUM = 0x32;
    public static final int FIELD_SPLITSTATUS = 0x33;
    public static final int FIELD_SPECIALDCS  = 0x34;
    public static final int FIELD_CHUB = 0x35;
    public static final int FIELD_MESSAGESIZE = 0x36;
    public static final int FIELD_SENDERADDRTON = 0x37;
    public static final int FIELD_RECIPIENTADDRTON = 0x38;
    /**
     * for cdr  
     */
    public static final int FIELD_DELIVERYCHANNEL = 0x39;
    public static final int FIELD_STATUSTEXT = 0x3A;
    
    
    /**
     * Reserved for internal PDU fields which are not consistent with Peering2
     * 0x3a ~ 0x3f
     */
    
    /**
     * for 1.5way/ott to keep original value, CDR required
     */
    public static final int FIELD_ORIGINAL_SENDER = 0x40;
    public static final int FIELD_ORIGINAL_RECIPIENT = 0x41;
    
    public static final int FIELD_SERVICETYPEID = 0x42;
    public static final int FIELD_SCHEDULE_DELIVERY_TIME = 0x43;

    public static int[] commandIdList = {
        COMMAND_SUBMIT,
        COMMAND_SUBMIT_ACK,
        COMMAND_DELIVERY_REPORT,
        COMMAND_DELIVERY_REPORT_ACK,
        COMMAND_ALIVE,
        COMMAND_ALIVE_ACK,
        COMMAND_BIND,
        COMMAND_BIND_ACK,
        COMMAND_INNER_ACK,
        COMMAND_SUBMIT4MQM,
        COMMAND_DELIVERY_REPORT4MQM,
        COMMAND_ACK4MQM,
        COMMAND_DELIVERY_REPORT_QUERY,
        COMMAND_DELIVERY_REPORT_QUERY_ACK,
        COMMAND_DELIVERY_REPORT_QUERY4MQM
    };

    public static int[] parameterIdList = {
        FIELD_MSGID,
        FIELD_SESSIONID,
        FIELD_OOPERATOR,
        FIELD_ROPERATOR,
        FIELD_ORELAY,
        FIELD_RRELAY,
        FIELD_OHUB,
        FIELD_RHUB,
        FIELD_CHUB,
        FIELD_TIMEMARK,
        FIELD_TIMEEXPIRY,
        FIELD_SENDER,
        FIELD_RECIPIENT,
        FIELD_CONTENT,
        FIELD_DATACODING,
        FIELD_STATUS,
        FIELD_STATUS_INTERNAL,
        FIELD_NEED_DR,
        FIELD_SAR_ReferenceNumber,
        FIELD_SAR_TotalSegments,
        FIELD_SAR_SegementsSeqnum,
        FIELD_UDH_INDICATOR,
        FIELD_GEMD,
        FIELD_TRANSACTIONID,
        FIELD_VERSION,
        FIELD_USERNAME,
        FIELD_PASSWORD,
        FIELD_TIMESTAMP,
        FIELD_CUSTMSGID,
        FIELD_VASPTOKEN,
        FIELD_BINARYCONTENT,
        FIELD_TRANSACTIONURI,
        FIELD_ORIGINALQUEUE,
        FIELD_OMNCMCC,
        FIELD_RMNCMCC,
        FIELD_SENDERADDRTYPE,
        FIELD_RECIPIENTADDRTYPE,
        FIELD_CONNECTIONID,
        FIELD_RETRIEDNUM,
        FIELD_SPLITSTATUS,
        FIELD_SPECIALDCS
    };

    public PduHeader getHeader() {
        return header;
    }

    public void setHeader(PduHeader newHeader) {
        header = newHeader;
    }

    public int getCommandId() {
        if (header == null) {
            return -1;
        }

        return header.getCommandId();
    }

    /**
     * Insert the method's description here.
     * Creation date: (1/8/2003 10:29:32 AM)
     * @param totalLength long
     */
    public void setTotalLength(long totalLength) {
        header.setTotalLength(totalLength);
    }

    public TcpByteBuffer toByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {

        TcpByteBuffer body = pduCommandToByteBuffer();
        if (body != null) {
            header.setTotalLength(body.length() + this.PDU_HEADER_SIZE);
        }
        else {
            header.setTotalLength(this.PDU_HEADER_SIZE);
        }

        TcpByteBuffer buffer = header.toByteBuffer();

        if (body != null) {
            buffer.appendBytes(body);
        }
        return buffer;
    }

    public static final Pdu createPdu(PduHeader header) {
        if (header == null) {
            return null;
        }
        Pdu newInstance = null;

        int commId = header.getCommandId();
        switch (commId) {
            case COMMAND_SUBMIT:
                newInstance = new CommandSubmit();
                break;
            case COMMAND_SUBMIT_ACK:
                newInstance = new CommandSubmitAck();
                break;
            case COMMAND_DELIVERY_REPORT:
                newInstance = new CommandDeliveryReport();
                break;
            case COMMAND_DELIVERY_REPORT_ACK:
                newInstance = new CommandDeliveryReportAck();
                break;
            case COMMAND_BIND:
                newInstance = new CommandBind();
                break;
            case COMMAND_BIND_ACK:
                newInstance = new CommandBindAck();
                break;
            case COMMAND_ALIVE:
                newInstance = new CommandKeepAlive();
                break;
            case COMMAND_ALIVE_ACK:
                newInstance = new CommandKeepAliveAck();
                break;
            case COMMAND_INNER_ACK:
                newInstance = new CommandInnerAck();
                break;
            case COMMAND_SUBMIT4MQM:
                newInstance = new InternalPdu4MQM(COMMAND_SUBMIT4MQM);
                break;
            case COMMAND_DELIVERY_REPORT4MQM:
                newInstance = new InternalPdu4MQM(COMMAND_DELIVERY_REPORT4MQM);
                break;
            case COMMAND_ACK4MQM:
                newInstance = new InternalPdu4MQM(COMMAND_ACK4MQM);
                break;
            case COMMAND_DELIVERY_REPORT_QUERY:
                newInstance = new CommandDeliveryReportQuery();
                break;
            case COMMAND_DELIVERY_REPORT_QUERY_ACK:
                newInstance = new CommandDeliveryReportQueryAck();
                break;
            case COMMAND_DELIVERY_REPORT_QUERY4MQM:
                newInstance = new InternalPdu4MQM(COMMAND_DELIVERY_REPORT_QUERY4MQM);
                break;
            default:
                log.warn("Error Command Type found in createPdu() method:{}",
                         commId);
        }
        if (newInstance != null)
        {
            newInstance.setHeader(header);
        }
        return newInstance;
    }

    private void parsePduBody(TcpByteBuffer buffer) throws
        InvalidPduException,
        NotEnoughDataInByteBufferException,
        MessageIncompleteException,
        UnknownCommandIdException,
        UnknownParameterIdException,
        UnsupportedEncodingException {

        if (header == null) {
            log.error("Header is null in parsePduBody!");
            return;
        }

        long bodyLength = header.getTotalLength() - PDU_HEADER_SIZE;
        if (buffer.length() < bodyLength) {
            throw new MessageIncompleteException();
        }
        TcpByteBuffer bodyBuffer = buffer.removeBytes( (int) bodyLength);
        parsePduCommand(bodyBuffer);
    }

    public static final Pdu createPdu(TcpByteBuffer buffer, int version) throws
        MessageIncompleteException,
        UnknownCommandIdException,
        NotEnoughDataInByteBufferException,
        HeaderIncompleteException,
        UnsupportedCommandIdException,
        UnknownParameterIdException,
        InvalidPduException,
        UnsupportedEncodingException {
        TcpByteBuffer headerBuf = null;
        try {
           PDU_HEADER_SIZE = 5;
           headerBuf = buffer.readBytes(PDU_HEADER_SIZE);
        }
        catch (NotEnoughDataInByteBufferException e) {
            log.warn(e, e);
            throw new HeaderIncompleteException();
        }

        PduHeader header = null;
        try {
            header = PduHeader.parseHeader(headerBuf);
        }
        catch (NotEnoughDataInByteBufferException e) {
            log.warn(e, e);
            throw new NotEnoughDataInByteBufferException(e.getMessage());
        }

        if (buffer.length() < header.getTotalLength()) {
            throw new MessageIncompleteException();
        }

        buffer.removeBytes(PDU_HEADER_SIZE);
        Pdu pdu = createPdu(header);
        if (pdu != null) {
            pdu.parsePduBody(buffer);
            if(log.isTraceEnabled()){
            	log.trace("parse PDU:{}", pdu);
            }
            return pdu;
        }
        else {
            throw new UnsupportedCommandIdException(header);
        }
    }
    
    public static final TcpByteBuffer getPduByteBuffer(TcpByteBuffer buffer, int version) throws
	    MessageIncompleteException,
	    UnknownCommandIdException,
	    NotEnoughDataInByteBufferException,
	    HeaderIncompleteException,
	    UnsupportedCommandIdException,
	    UnknownParameterIdException,
	    InvalidPduException,
	    UnsupportedEncodingException {
	    TcpByteBuffer headerBuf = null;
	    try {
	        if (version == Pdu.VERSION_1_0) {
	            PDU_HEADER_SIZE = 3;
	        }
	        else {
	            PDU_HEADER_SIZE = 5;
	        }
	
	        headerBuf = buffer.readBytes(PDU_HEADER_SIZE);
	    }
	    catch (NotEnoughDataInByteBufferException e) {
	        log.warn(e, e);
	        throw new HeaderIncompleteException();
	    }
	
	    PduHeader header = null;
	    try {
	        header = PduHeader.parseHeader(headerBuf);
	    }
	    catch (NotEnoughDataInByteBufferException e) {
	        log.warn(e, e);
	        throw new MessageIncompleteException(e.getMessage());
	    }
	    
	    int totalLength = (int)header.getTotalLength();
	
	    if (buffer.length() < totalLength) {
	        throw new MessageIncompleteException();
	    }else{
	    	return buffer.removeBytes(totalLength);
	    }
	}

    
    
    public static final TcpByteBuffer createPduBuffer(TcpByteBuffer buffer, int version) throws
	    MessageIncompleteException,
	    NotEnoughDataInByteBufferException,
	    HeaderIncompleteException,
	    InvalidPduException,
	    UnknownCommandIdException,
	    UnsupportedEncodingException {
	    TcpByteBuffer headerBuf = null;
	    try {
	        if (version == Pdu.VERSION_1_0) {
	            PDU_HEADER_SIZE = 3;
	        }
	        else {
	            PDU_HEADER_SIZE = 5;
	        }
	
	        headerBuf = buffer.readBytes(PDU_HEADER_SIZE);
	    }
	    catch (NotEnoughDataInByteBufferException e) {
	        log.warn(e, e);
	        throw new HeaderIncompleteException();
	    }
	
	    PduHeader header = null;
	    try {
	        header = PduHeader.parseHeader(headerBuf);
	    }
	    catch (NotEnoughDataInByteBufferException e) {
	        log.warn(e, e);
	        throw new MessageIncompleteException(e.getMessage());
	    }
	    int total = (int)header.getTotalLength();
	    if (buffer.length() < total) {
	        throw new MessageIncompleteException();
	    }else {
	    	return buffer.removeBytes(total);
	    }
	}

    public void setCustomerInfo(A2PMultiConnectionInfo customerInfo) {
        this.customerInfo = customerInfo;

    }
    public abstract void parsePduCommand(TcpByteBuffer buffer) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException;

    public abstract void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException;

    public abstract GmmsMessage convertToMsg(BufferMonitor buffer);

    public abstract TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException;

    public abstract boolean isRequest();

    protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
                                           String value) throws
        NotEnoughDataInByteBufferException {
        if (value == null) {
            return;
        }

        buffer.appendBytes(
            new Parameter(id, value).toByteBuffer());
    }

    protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
                                           int value) throws
        NotEnoughDataInByteBufferException {
        appendParameterToBuffer(buffer, id, value, 2);
    }

    protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
                                           int value, int length) throws
        NotEnoughDataInByteBufferException {
        if (value == -1) {
            return;
        }

        buffer.appendBytes(
            new Parameter(id, value, length).toByteBuffer());
    }

    protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
                                           byte[] byteValue, int length) throws
        NotEnoughDataInByteBufferException {
        if (byteValue == null) {
            return;
        }

        buffer.appendBytes(
            new Parameter(id, byteValue, length).toByteBuffer());
    }

    protected void appendParameterToBuffer(TcpByteBuffer buffer, int id,
                                           byte[] byteValue) throws
        NotEnoughDataInByteBufferException {
        appendParameterToBuffer(buffer, id, byteValue, byteValue.length);
    }
}
