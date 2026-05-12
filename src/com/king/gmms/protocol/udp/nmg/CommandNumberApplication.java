/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

import java.io.UnsupportedEncodingException;

import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.protocol.udp.nmg.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.udp.nmg.exception.UnknownParameterIdException;
import com.king.gmms.routing.nmg.NMGUtility;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.MessageIdGenerator;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class CommandNumberApplication
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
    
    /**
     * CustomerID of OTT/1.5way Customer
     */
    private int ottCustomer = -1;
    
    private String senderAddr;
    private String recipientAddr;
    
    /**
     * MobileNumber: 0x01 /FixNumber: 0x02
     */
    private int numberType = 1;
    
    private int o_op = -1;
    
    /**
     * ossid
     */
    private int o_relay = -1;
    
    /**
     * o_A2P
     */
    private int o_hub = -1;

    public CommandNumberApplication() {
        this(Pdu.VERSION_2_0);
    }

    public CommandNumberApplication(int version) {
        if (header == null) {
            header = new PduHeader(version);
        }
        header.setCommandId(COMMAND_NUMBER_APPLICATION);
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
        appendParameterToBuffer(buffer, FIELD_OTTCUSTOMER, ottCustomer);
        appendParameterToBuffer(buffer, FIELD_SENDERADDRESS, senderAddr);
        appendParameterToBuffer(buffer, FIELD_RECIPIENTADDRESS, recipientAddr);
        appendParameterToBuffer(buffer, FIELD_NUMBERTYPE, numberType);
        appendParameterToBuffer(buffer, FIELD_OOPERATOR, o_op);
        appendParameterToBuffer(buffer, FIELD_ORELAY, o_relay);
        appendParameterToBuffer(buffer, FIELD_OHUB, o_hub);
        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_NUMBER_APPLICATION:")
            .append("msgId:").append(msgId).append(",")
            .append("service:").append(service).append(",")
            .append("operatorPartner:").append(operatorPartner).append(",")
            .append("ottCustomer:").append(ottCustomer).append(",")
            .append("senderAddr:").append(senderAddr).append(",")
            .append("recipientAddr:").append(recipientAddr).append(",")
            .append("numberType:").append(numberType).append(",")
            .append("o_op:").append(o_op).append(",")
            .append("o_relay:").append(o_relay).append(",")
            .append("o_hub:").append(o_hub)
            .toString();
    }

    /**
     * 
     * @return
     * @see com.king.gmms.protocol.udp.nmg.Request#createResponse()
     */
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
		
        this.operatorPartner = NMGUtility.getPartnerCustId(msg);
        
        A2PCustomerManager ctm = GmmsUtility.getInstance().getCustomerManager();
        A2PCustomerInfo oopCust = ctm.getCustomerBySSID(msg.getOoperator());
        if (oopCust != null) {
        	this.ottCustomer = oopCust.getCustomerId();
        }
        
        this.senderAddr = msg.getSenderAddress();
        this.recipientAddr = msg.getRecipientAddress();
        this.o_op = msg.getOoperator();
        this.o_relay = msg.getOSsID();
        this.o_hub = msg.getOA2P();
	}
}
