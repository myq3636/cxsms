package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.*;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.smpp.SMPPRespStatus;
import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CommandSubmitAck
    extends Respond {
    private static SystemLogger log = SystemLogger.getSystemLogger(CommandSubmitAck.class);
    A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
    protected String msgId;
    protected String custMsgId;
    protected String transactionId;
    private int o_hub = -1;
    private int r_hub = -1;
    protected int statusCode = -1;
    protected String statusText;
    private TransactionURI transaction = null;
    private String oMncMcc;
    private String rMncMcc;
    private int sequenceNum = -1;
    private int o_relay = 0;
  
    //add by Elton for 4.3.1
    private int o_op = -1;
    private int r_op = -1;
    
  //add by kevin for Multi-recipient
    private String recipientAddr;
    //add by king
    private String sender;

    public CommandSubmitAck() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(this.COMMAND_SUBMIT_ACK);
    }

    public void parsePduCommand(TcpByteBuffer buffer) throws
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
                case FIELD_STATUS:
                    statusCode = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_TRANSACTIONID:
                    transactionId = buffer.removeString(length);
                    break;
                case FIELD_CUSTMSGID:
                    custMsgId = buffer.removeString(length);
                    break;
                case FIELD_ORELAY:
                    o_relay = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_OHUB:
                    o_hub = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_RHUB:
                    r_hub = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_OOPERATOR:
                	o_op = buffer.removeBytesAsInt(length);
                	break;
                case FIELD_ROPERATOR:
                	r_op = buffer.removeBytesAsInt(length);
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
                case FIELD_OMNCMCC:
                    oMncMcc = buffer.removeString(length);
                    break;
                case FIELD_RMNCMCC:
                    rMncMcc = buffer.removeString(length);
                    break;
                case FIELD_SAR_SegementsSeqnum:
                	sequenceNum = buffer.removeBytesAsInt(length);
                	break;
                	//add by kevin for Multi-recipient
                case FIELD_RECIPIENT:
                	recipientAddr=buffer.removeString(length);
                	break;
                case FIELD_SENDER:
                    sender = buffer.removeString(length);
                    break;
                default:
                    log.warn("Cant find field with tag: {},len:{}" 
                             ,tag ,length);
                    buffer.removeBytes(length);
                    break;
            }
        }
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
    	GmmsMessage msg = null;
        if (buffer == null) {
           msg = new GmmsMessage();
           msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
           msg.setMsgID(msgId);
           msg.setInMsgID(custMsgId);
           if (o_relay > 0) {
               msg.setOSsID(o_relay);
           }
           if (o_hub > 0) {
               msg.setOA2P(o_hub);
           }
           if (r_hub > 0) {
               msg.setRA2P(r_hub);
           }
           if (o_op > 0){
        	   msg.setOoperator(o_op);
           }
           if (r_op > 0){
        	   msg.setRoperator(r_op);
           }
           int oA2P = gmmsUtility.getCustomerManager().getConnectedRelay(o_relay,GmmsMessage.MSG_TYPE_SUBMIT);
           if (gmmsUtility.getCustomerManager().inCurrentA2P(oA2P)) {
        	   msg.setCurrentA2P(oA2P);
			} else {
				msg.setCurrentA2P(r_hub);
			}
           msg.setInTransID(transactionId);
           msg.setOMncMcc(oMncMcc);
           msg.setRMncMcc(rMncMcc);
           msg.setSarSegmentSeqNum(sequenceNum);
           
           //add by kevin for 4.0.1
           msg.setRecipientAddress(recipientAddr);
           msg.setOutsender(sender);
            
        }else{
        	msg = buffer.remove(msgId+sequenceNum);
           if (msg != null) {
               msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT);
        	   msg.setOutMsgID(custMsgId);
               msg.setOutTransID(transactionId);
               msg.setOutsender(sender);
           } else {
               log.error("can't find message with MsgId:{}", msgId);
               return msg;
           }	   
        }
        msg.setStatus(GmmsStatus.getStatus(statusCode));
        msg.setTransaction(transaction);
        return msg;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        TcpByteBuffer buffer = new TcpByteBuffer();

        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_CUSTMSGID, custMsgId);
        appendParameterToBuffer(buffer, FIELD_TRANSACTIONID, transactionId);
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
        appendParameterToBuffer(buffer, FIELD_ORELAY, o_relay);
        appendParameterToBuffer(buffer, FIELD_OHUB, o_hub);
        appendParameterToBuffer(buffer, FIELD_RHUB, r_hub);
        appendParameterToBuffer(buffer, FIELD_OOPERATOR, o_op);
        appendParameterToBuffer(buffer, FIELD_ROPERATOR, r_op);        
        appendParameterToBuffer(buffer, FIELD_OMNCMCC, oMncMcc);
        appendParameterToBuffer(buffer, FIELD_RMNCMCC, rMncMcc);
        appendParameterToBuffer(buffer,FIELD_SAR_SegementsSeqnum,sequenceNum,1);
        if(transaction!=null){
        	appendParameterToBuffer(buffer, FIELD_TRANSACTIONURI,
            		transaction.toString());
        }
        
        //add by kevin for Multi-recipient
        appendParameterToBuffer(buffer,FIELD_RECIPIENT,recipientAddr);
        appendParameterToBuffer(buffer, FIELD_SENDER, sender);
        
        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_SUBMIT_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("custMsgId:").append(custMsgId).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("TransactionURI:").append((transaction == null)?"": transaction.toString()).append(",")
            .append("TransactionID:").append(transactionId)
            .toString();
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
	    msgId = msg.getMsgID();
	    if(in){
	    	custMsgId = msg.getInMsgID();
		    transactionId = msg.getInTransID();
	    }else{
	    	custMsgId = msg.getOutMsgID();
		    transactionId = msg.getOutTransID();
	    }
	    transaction = msg.getTransaction();
	    o_relay = msg.getOSsID();
	    o_hub = msg.getOA2P();
        r_hub = msg.getRA2P();
        oMncMcc = msg.getOMncMcc();
        rMncMcc = msg.getRMncMcc();
        statusCode = msg.getStatusCode();
        sequenceNum = msg.getSarSegmentSeqNum();
        
        recipientAddr=msg.getRecipientAddress();
        sender = msg.getSenderAddress();
        o_op = msg.getOoperator();
        r_op = msg.getRoperator();
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}
}
