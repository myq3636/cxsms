package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CommandDeliveryReportAck extends Respond {
    private static SystemLogger log = SystemLogger.getSystemLogger(CommandDeliveryReportAck.class);
    private String msgId;
    protected String custMsgId;
    private int statusCode = -1;
    private TransactionURI transaction = null;
	protected String transactionId;
	private int o_relay = 0;
    private int r_relay = 0;
    private int o_hub = 0;
    private int r_hub = 0;
    private int c_hub = 0;
    private String  statusText;
    
    public CommandDeliveryReportAck() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(this.COMMAND_DELIVERY_REPORT_ACK);
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

    public void setO_relay(int o_relay) {
		this.o_relay = o_relay;
	}

	public void setR_relay(int r_relay) {
		this.r_relay = r_relay;
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
                case FIELD_OHUB:
                    o_hub = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_RHUB:
                    r_hub = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_CHUB:
                    c_hub = buffer.removeBytesAsInt(length);
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
        GmmsMessage msg = null;
        if (buffer == null){
        	msg = new GmmsMessage();
        	msg.setMsgID(msgId);
        	msg.setOutMsgID(custMsgId);
            msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
            msg.setOutTransID(transactionId);
            if (r_relay > 0) {
                msg.setRSsID(r_relay);
            }
            if (o_relay > 0) {
                msg.setOSsID(o_relay);
            }
            if (o_hub > 0) {
                msg.setOA2P(o_hub);
            }
            if (r_hub > 0) {
                msg.setRA2P(r_hub);
            }
            if (c_hub > 0) {
                msg.setCurrentA2P(c_hub);
            }
        	if(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode() == statusCode){
        		msg.setStatusCode(statusCode);
        	}else{
        		msg.setStatus(GmmsStatus.getStatus(statusCode));
        	}
        	if(transaction!=null){
        		msg.setTransaction(transaction);
        	}
        	
        	if (statusText!=null) {
				msg.setStatusText(statusText);
			}
        }
        else{
        	log.debug("get the message from outbuffer, the key is:{}_{}", custMsgId, statusText);
	        msg = buffer.remove(custMsgId+"_"+statusText);
	        if (msg != null) {
	        	if(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode() == statusCode){
	        		msg.setStatusCode(statusCode);
	        	}else if ((GmmsStatus.DELIVERED.getCode() == statusCode)
	        			|| (GmmsStatus.ACCEPT.getCode() == msg.getStatusCode())) {
					//do nothing, not replace status code for accept in dr
				}else{
	        		msg.setStatus(GmmsStatus.getStatus(statusCode));
	        	}
	        	if(transaction!=null){
	        		msg.setTransaction(transaction);
	        	}
	        	
	        	if (statusText!=null) {
					msg.setStatusText(statusText);
				}
	        	if (r_relay > 0) {
	                msg.setRSsID(r_relay);
	            }
	            if (o_relay > 0) {
	                msg.setOSsID(o_relay);
	            }
	        }
	        else {
	            log.warn("Can't found the message by InmsgId:{}", custMsgId);
	        }
        }
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
        appendParameterToBuffer(buffer, FIELD_OHUB, o_hub);
        appendParameterToBuffer(buffer, FIELD_RHUB, r_hub);
        appendParameterToBuffer(buffer, FIELD_CHUB, c_hub);
        appendParameterToBuffer(buffer, FIELD_TRANSACTIONID, transactionId);
        if(transaction!=null){
        	appendParameterToBuffer(buffer, FIELD_TRANSACTIONURI,
            		transaction.toString());
        }

        if (statusText!=null) {
        	appendParameterToBuffer(buffer, FIELD_STATUSTEXT,
            		statusText);
		}
        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_DELIVERY_REPORT_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("custMsgId:").append(custMsgId).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("statusText:").append(statusText).append(",")
            .append("transactionID:").append(transactionId).append(",")
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
	    o_hub = msg.getOA2P();
        r_hub = msg.getRA2P();
        c_hub = msg.getCurrentA2P();
        transaction = msg.getTransaction();
        statusText = msg.getStatusText();
    }

	public int getO_hub() {
		return o_hub;
	}

	public void setO_hub(int o_hub) {
		this.o_hub = o_hub;
	}

	public int getR_hub() {
		return r_hub;
	}

	public void setR_hub(int r_hub) {
		this.r_hub = r_hub;
	}

	public int getC_hub() {
		return c_hub;
	}

	public void setC_hub(int c_hub) {
		this.c_hub = c_hub;
	}
}
