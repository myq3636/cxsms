/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.udp.nmg.exception.*;
import com.king.message.gmms.GmmsMessage;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class CommandNumberApplicationAck
    extends Respond {
    private static SystemLogger log = SystemLogger.getSystemLogger(CommandNumberApplicationAck.class);
    private String msgId;
    private String msIsdn;
    /**
     * CustomerID of Operator Partner
     */
    private int operatorPartner = -1;
    private int statusCode = -1;

    public CommandNumberApplicationAck() {
        this(Pdu.VERSION_2_0);
    }

    public CommandNumberApplicationAck(int version) {
        if (header == null) {
            header = new PduHeader(version);
        }
        header.setCommandId(Pdu.COMMAND_NUMBER_APPLICATION_ACK);
    }

    public void parsePduCommand(UdpByteBuffer buffer) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException {
        if (buffer == null) {
            return;
        }
        while (buffer.length() > 0) {
            int tag = buffer.removeBytesAsInt(1);
            int length = buffer.removeBytesAsInt(2);
            switch (tag) {
                case FIELD_MSGID:
                    msgId = buffer.removeString(length);
                    break;
                case FIELD_MSISDN:
                	msIsdn = buffer.removeString(length);
                	break;
                case FIELD_OPERATORPARTNER:
                	operatorPartner = buffer.removeBytesAsInt(length);
                	break;
                case FIELD_STATUSCODE:
                    statusCode = buffer.removeBytesAsInt(length);
                    break;
                default:
                    log.warn("Cant find field with tag: {},len:{}",tag, length);
                    buffer.removeBytes(length);
                    break;
            }
        }
    }

    public UdpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    	
    	UdpByteBuffer buffer = new UdpByteBuffer();
        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_MSISDN, msIsdn);
        appendParameterToBuffer(buffer, FIELD_OPERATORPARTNER, operatorPartner);
        appendParameterToBuffer(buffer, FIELD_STATUSCODE, statusCode);
        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_NUMBER_APPLICATION_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("msIsdn:").append(msIsdn).append(",")
            .append("operatorPartner:").append(operatorPartner).append(",")
            .append("statusCode:").append(statusCode)
            .toString();
    }

	/** 
	 * @param msg
	 * @throws NotEnoughDataInByteBufferException
	 * @throws UnsupportedEncodingException
	 * @see com.king.gmms.protocol.udp.nmg.Pdu#convertFromMsg(com.king.message.gmms.GmmsMessage)
	 */
	@Override
	public void convertFromMsg(GmmsMessage msg) throws NotEnoughDataInByteBufferException, UnsupportedEncodingException {
		return;
	}

	public String getMsgId() {
		return msgId;
	}

	public void setMsgId(String msgId) {
		this.msgId = msgId;
	}

	public String getMsIsdn() {
		return msIsdn;
	}

	public void setMsIsdn(String msIsdn) {
		this.msIsdn = msIsdn;
	}

	public int getOperatorPartner() {
		return operatorPartner;
	}

	public void setOperatorPartner(int operatorPartner) {
		this.operatorPartner = operatorPartner;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
}
