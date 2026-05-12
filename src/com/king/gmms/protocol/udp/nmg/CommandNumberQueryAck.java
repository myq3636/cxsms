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
public class CommandNumberQueryAck
    extends Respond {
    private static SystemLogger log = SystemLogger.getSystemLogger(CommandNumberQueryAck.class);
    private String msgId;
    
    /**
     * CustomerID of OTT/1.5way Customer
     */
    private int ottCustomer = -1;
    
    private String recipientAddr;
    
    private int o_op = -1;
    
    /**
     * ossid
     */
    private int o_relay = -1;
    
    /**
     * o_A2P
     */
    private int o_hub = -1;

    
    private int statusCode = -1;

    public CommandNumberQueryAck() {
        this(Pdu.VERSION_2_0);
    }

    public CommandNumberQueryAck(int version) {
        if (header == null) {
            header = new PduHeader(version);
        }
        header.setCommandId(Pdu.COMMAND_NUMBER_QUERY_ACK);
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
                case FIELD_OTTCUSTOMER:
                	ottCustomer = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_RECIPIENTADDRESS:
                	recipientAddr = buffer.removeString(length);
                	break;
                case FIELD_STATUSCODE:
                    statusCode = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_OOPERATOR:
                	o_op = buffer.removeBytesAsInt(length);
                	break;
                case FIELD_ORELAY:
                    o_relay = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_OHUB:
                    o_hub = buffer.removeBytesAsInt(length);
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
    	return null;
    }

    public String toString() {
        return new StringBuffer("COMMAND_NUMBER_QUERY_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("ottCustomer:").append(ottCustomer).append(",")
            .append("recipientAddr:").append(recipientAddr).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("o_op:").append(o_op).append(",")
            .append("o_relay:").append(o_relay).append(",")
            .append("o_hub:").append(o_hub)
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

	public int getOttCustomer() {
		return ottCustomer;
	}

	public void setOttCustomer(int ottCustomer) {
		this.ottCustomer = ottCustomer;
	}

	public String getRecipientAddr() {
		return recipientAddr;
	}

	public void setRecipientAddr(String recipientAddr) {
		this.recipientAddr = recipientAddr;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public int getO_op() {
		return o_op;
	}

	public void setO_op(int o_op) {
		this.o_op = o_op;
	}

	public int getO_relay() {
		return o_relay;
	}

	public void setO_relay(int o_relay) {
		this.o_relay = o_relay;
	}

	public int getO_hub() {
		return o_hub;
	}

	public void setO_hub(int o_hub) {
		this.o_hub = o_hub;
	}
}
