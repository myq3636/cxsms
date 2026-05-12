/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

import java.io.UnsupportedEncodingException;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.protocol.udp.nmg.exception.*;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageIdGenerator;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class CommandNumberQuery
    extends Request {
	
    private String msgId;
    
    /**
     * IOSMS/IOMMS/SMSA2P/MMSA2P/Voice
     */
    private String service = "SMSA2P";
    
    /**
     * CustomerID of Operator Partner
     */
    private int operatorPartner = -1;
    
    private String senderAddr;
    
    /**
     * MSISDN
     */
    private String recipientAddr;
    
    public CommandNumberQuery() {
        this(Pdu.VERSION_2_0);
    }

    public CommandNumberQuery(int version) {
        if (header == null) {
            header = new PduHeader(version);
        }
        header.setCommandId(COMMAND_NUMBER_QUERY);
    }

    public void parsePduCommand(UdpByteBuffer buffer) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException {
        return;
    }

    public UdpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {

    	UdpByteBuffer buffer = new UdpByteBuffer();
        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_SERVICE, service);
        appendParameterToBuffer(buffer, FIELD_OPERATORPARTNER, operatorPartner);
        appendParameterToBuffer(buffer, FIELD_SENDERADDRESS, senderAddr);
        appendParameterToBuffer(buffer, FIELD_RECIPIENTADDRESS, recipientAddr);
        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_NUMBER_QUERY:")
            .append("msgId:").append(msgId).append(",")
            .append("service:").append(service).append(",")
            .append("operatorPartner:").append(operatorPartner).append(",")
            .append("senderAddr:").append(senderAddr).append(",")
            .append("recipientAddr:").append(recipientAddr)
            .toString();
    }

    protected Respond createResponse() {
        return null;
    }

	/** 
	 * @param msg
	 * @throws NotEnoughDataInByteBufferException
	 * @throws UnsupportedEncodingException
	 * @see com.king.gmms.protocol.udp.nmg.Pdu#convertFromMsg(com.king.message.gmms.GmmsMessage)
	 */
	@Override
	public void convertFromMsg(GmmsMessage msg) throws NotEnoughDataInByteBufferException, UnsupportedEncodingException {
		// TODO CSM msgid too long
        if (msg.getMsgID() == null) {
            String tmpId = MessageIdGenerator.generateCommonMsgID(msg.getCurrentA2P());
            msg.setMsgID(tmpId);
        }
        this.msgId = msg.getMsgID();
        
        A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
        A2PCustomerInfo ossidCust = ctm.getCustomerBySSID(msg.getOSsID());
        if (ossidCust != null) {
        	this.operatorPartner = ossidCust.getCustomerId();
        }
        
        this.senderAddr = msg.getSenderAddress();
        this.recipientAddr = msg.getRecipientAddress();
	}
}
