package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CommandDeliveryReportQueryAck extends Respond {
    private static SystemLogger log = SystemLogger.getSystemLogger(CommandDeliveryReportAck.class);
    private String msgId;
    protected String custMsgId;
    private int statusCode = -1;
    private TransactionURI transaction = null;
    protected String transactionId;
	private int o_relay = 0;
    private int r_relay = 0;
    
    private String deliveryChannel;
    private String  statusText;

    public CommandDeliveryReportQueryAck() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(this.COMMAND_DELIVERY_REPORT_QUERY_ACK);
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
    
    public String getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(String transactionId) {
		this.transactionId = transactionId;
	}


    public void setCustMsgId(String custMsgId) {
		this.custMsgId = custMsgId;
	}

	public void setTransaction(TransactionURI transaction) {
		this.transaction = transaction;
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
                case FIELD_ORELAY:
                    o_relay = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_RRELAY:
                    r_relay = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_CUSTMSGID:
                    custMsgId = buffer.removeString(length);
                    break;
                case FIELD_TRANSACTIONID:
                    transactionId = buffer.removeString(length);
                    break;
                case FIELD_STATUSTEXT:
                	statusText = buffer.removeString(length);
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
                case FIELD_DELIVERYCHANNEL:
                	deliveryChannel = buffer.removeString(length);
                	break;
                default:
                	if(log.isInfoEnabled()){
    					log.info("Cant find field with tag: {},len:{}"
                             ,tag,length);
                	}
                    buffer.removeBytes(length);
                    break;
            }
        }
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
        if (header == null) {
            return null;
        }
        GmmsMessage msg = null;
        if (buffer == null){
        	msg = new GmmsMessage();
        	msg.setMsgID(msgId);
        	msg.setOutMsgID(custMsgId);
            msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
            msg.setOutTransID(transactionId);
            if(deliveryChannel!=null){
        		msg.setDeliveryChannel(deliveryChannel);
        	}
            log.trace("transactionId={}",transactionId);
            if (r_relay > 0) {
                msg.setRSsID(r_relay);
            }
            if (o_relay > 0) {
                msg.setOSsID(o_relay);
            }
            msg.setStatus(GmmsStatus.getStatus(statusCode));
            msg.setTransaction(transaction);
        }
        else{
	        msg = buffer.remove(msgId);
	        if (msg != null) {
	            msg.setStatus(GmmsStatus.getStatus(statusCode));
	            msg.setTransaction(transaction);
	            if (r_relay > 0) {
	                msg.setRSsID(r_relay);
	            }
	            if (o_relay > 0) {
	                msg.setOSsID(o_relay);
	            }
	            if(deliveryChannel!=null){
	        		msg.setDeliveryChannel(deliveryChannel);
	        	}
	        }
	        else {
	            log.warn("Can't found the message by inMsgId:{}", msgId);
	        }
        }
        return msg;
    }
    
	//add by kevin for REST
	public GmmsMessage convertToMsg() {
		if (header == null) {
			return null;
		}
		GmmsMessage msg = null;

		msg = new GmmsMessage();
		msg.setMsgID(msgId);
		msg.setInMsgID(custMsgId);
		msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_QUERY_RESP);
		msg.setOutTransID(transactionId);
		if (deliveryChannel != null) {
			msg.setDeliveryChannel(deliveryChannel);
		}
		log.trace("transactionId={}", transactionId);
		if (r_relay > 0) {
			msg.setRSsID(r_relay);
		}
		if (o_relay > 0) {
			msg.setOSsID(o_relay);
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
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
        appendParameterToBuffer(buffer, FIELD_ORELAY, o_relay);
        appendParameterToBuffer(buffer, FIELD_RRELAY, r_relay);
        appendParameterToBuffer(buffer, FIELD_TRANSACTIONID, transactionId);
        appendParameterToBuffer(buffer, FIELD_DELIVERYCHANNEL, deliveryChannel);
        if(transaction!=null){
        	appendParameterToBuffer(buffer, FIELD_TRANSACTIONURI,
            		transaction.toString());
        }

        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_DELIVERY_REPORT_QUERY_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("custMsgId:").append(custMsgId).append(",")
            .append("deliveryChannel:").append(deliveryChannel).append(",")
            .append("statusCode:").append(statusCode)
            .toString();
    }

    protected Respond createResponse() {
        return null;
    }

    public void convertFromMsg(GmmsMessage msg) throws
    		NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    	convertFromMsg(msg, true);
    }
    
    
    public void convertFromMsg(GmmsMessage msg, boolean in) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        if (msg == null) {
            return;
        }
        msgId = msg.getMsgID();
        if(in){
	        custMsgId = msg.getInMsgID();
	        transactionId = msg.getInTransID();
        }else{
        	custMsgId = msg.getOutMsgID();
            transactionId = msg.getOutTransID();
        }
        statusCode = msg.getStatusCode();
	    o_relay = msg.getOSsID();
	    r_relay = msg.getRSsID();
        transaction = msg.getTransaction();
        deliveryChannel = msg.getDeliveryChannel();
    }
}
