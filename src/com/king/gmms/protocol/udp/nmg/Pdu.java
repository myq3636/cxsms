/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.protocol.udp.nmg.exception.*;
import com.king.message.gmms.GmmsMessage;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public abstract class Pdu {
	protected static SystemLogger log = SystemLogger.getSystemLogger(Pdu.class);
	
	protected PduHeader header = null;
	protected GmmsUtility gmmsUtility = GmmsUtility.getInstance();
	
	
	public static int PDU_HEADER_SIZE = 5;

    public static final int VERSION_1_0 = 10;
    public static final int VERSION_2_0 = 20;
    
    //Command Type
    public static final int COMMAND_NUMBER_APPLICATION = 0x01;
    public static final int COMMAND_NUMBER_APPLICATION_ACK = 0x02;
    public static final int COMMAND_NUMBER_QUERY = 0x04;
    public static final int COMMAND_NUMBER_QUERY_ACK = 0x05;
    
    //Parameter Tag Identifier
    public static final int FIELD_MSGID = 0x01;
    public static final int FIELD_SERVICE = 0x02; 
    public static final int FIELD_OPERATORPARTNER = 0x03;
    public static final int FIELD_OTTCUSTOMER = 0x04;
    public static final int FIELD_SENDERADDRESS = 0x05;
    public static final int FIELD_RECIPIENTADDRESS = 0x06;
    public static final int FIELD_MSISDN = 0x07;
    public static final int FIELD_NUMBERTYPE = 0x08;
    public static final int FIELD_STATUSCODE = 0x09;
    public static final int FIELD_OOPERATOR = 0x21;
    public static final int FIELD_ROPERATOR = 0x22;
    /**
     * OSSID
     */
    public static final int FIELD_ORELAY = 0x23;
    
    /**
     * RSSID
     */
    public static final int FIELD_RRELAY = 0x24;
    
    /**
     * O_A2P
     */
    public static final int FIELD_OHUB = 0x25;
    
    /**
     * R_A2P
     */
    public static final int FIELD_RHUB = 0x26;
    
    public static int[] commandIdList = {
    	COMMAND_NUMBER_APPLICATION,
    	COMMAND_NUMBER_APPLICATION_ACK,
    	COMMAND_NUMBER_QUERY,
    	COMMAND_NUMBER_QUERY_ACK
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

    public UdpByteBuffer toByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {

    	UdpByteBuffer body = pduCommandToByteBuffer();
        if (body != null) {
            header.setTotalLength(body.length() + Pdu.PDU_HEADER_SIZE);
        }
        else {
            header.setTotalLength(Pdu.PDU_HEADER_SIZE);
        }

        UdpByteBuffer buffer = header.toByteBuffer();

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
            case COMMAND_NUMBER_APPLICATION:
                newInstance = new CommandNumberApplication(header.getVersion());
                break;
            case COMMAND_NUMBER_APPLICATION_ACK:
            	newInstance = new CommandNumberApplicationAck(header.getVersion());
                break;
            case COMMAND_NUMBER_QUERY:
                newInstance = new CommandNumberQuery(header.getVersion());
                break;
            case COMMAND_NUMBER_QUERY_ACK:
            	newInstance = new CommandNumberQueryAck(header.getVersion());
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

    private void parsePduBody(UdpByteBuffer buffer) throws
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
        UdpByteBuffer bodyBuffer = buffer.removeBytes( (int) bodyLength);
        parsePduCommand(bodyBuffer);
    }

    public static final Pdu createPdu(UdpByteBuffer buffer, int version) throws
        MessageIncompleteException,
        UnknownCommandIdException,
        NotEnoughDataInByteBufferException,
        HeaderIncompleteException,
        UnsupportedCommandIdException,
        UnknownParameterIdException,
        InvalidPduException,
        UnsupportedEncodingException {
    	UdpByteBuffer headerBuf = null;
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
            header = PduHeader.parseHeader(headerBuf, version);
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
            if(log.isInfoEnabled()){
				log.info("parse PDU:{}", pdu);
            }
            return pdu;
        }
        else {
            throw new UnsupportedCommandIdException(header);
        }
    }

    public abstract void parsePduCommand(UdpByteBuffer buffer) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException;

    public abstract UdpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException;

    public abstract boolean isRequest();

    protected void appendParameterToBuffer(UdpByteBuffer buffer, int id,
                                           String value) throws
        NotEnoughDataInByteBufferException {
        if (value == null) {
            return;
        }

        buffer.appendBytes(
            new Parameter(id, value).toByteBuffer());
    }

    protected void appendParameterToBuffer(UdpByteBuffer buffer, int id,
                                           int value) throws
        NotEnoughDataInByteBufferException {
        appendParameterToBuffer(buffer, id, value, 2);
    }

    protected void appendParameterToBuffer(UdpByteBuffer buffer, int id,
                                           int value, int length) throws
        NotEnoughDataInByteBufferException {
        if (value == -1) {
            return;
        }

        buffer.appendBytes(
            new Parameter(id, value, length).toByteBuffer());
    }

    protected void appendParameterToBuffer(UdpByteBuffer buffer, int id,
                                           byte[] byteValue, int length) throws
        NotEnoughDataInByteBufferException {
        if (byteValue == null) {
            return;
        }

        buffer.appendBytes(
            new Parameter(id, byteValue, length).toByteBuffer());
    }

    protected void appendParameterToBuffer(UdpByteBuffer buffer, int id,
                                           byte[] byteValue) throws
        NotEnoughDataInByteBufferException {
        appendParameterToBuffer(buffer, id, byteValue, byteValue.length);
    }
    
    public abstract void convertFromMsg(GmmsMessage msg) throws
                NotEnoughDataInByteBufferException, UnsupportedEncodingException;

}
