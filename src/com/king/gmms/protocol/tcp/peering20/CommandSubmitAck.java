package com.king.gmms.protocol.tcp.peering20;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.protocol.smpp.SMPPRespStatus;
import com.king.gmms.protocol.tcp.peering20.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.peering20.exception.UnknownParameterIdException;
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
    protected int statusCode = -1;
    protected int statusCodeInternal = -1;

    public CommandSubmitAck() {
        this(Pdu.VERSION_2_0);
    }

    public CommandSubmitAck(int version) {
        if (header == null) {
            header = new PduHeader(version);
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
                case FIELD_STATUS_INTERNAL:
                    statusCodeInternal = buffer.removeBytesAsInt(length);
                    break;
                case FIELD_TRANSACTIONID:
                    transactionId = buffer.removeString(length);
                    break;
                case FIELD_CUSTMSGID:
                    custMsgId = buffer.removeString(length);
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
        if (buffer == null || header == null) {
            return null;
        }
        GmmsMessage msg = buffer.remove(msgId);
        if (msg != null) {
            if (statusCode != -1) {
                msg.setStatus(SMPPRespStatus.getGmmsStatus(statusCode));
                msg.setAttachment(statusCode);
            }
            else if (statusCodeInternal != -1) {
                msg.setStatus(GmmsStatus.getStatus(statusCodeInternal));
                msg.setAttachment(SMPPRespStatus.getSmppStatus(
                    statusCodeInternal));
            }
            msg.setMessageType(GmmsMessage.MSG_TYPE_SUBMIT_RESP);
        }
        else {
            log.warn("can't find message with MsgId:{}", msgId);
        }

        return msg;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        TcpByteBuffer buffer = new TcpByteBuffer();

        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_CUSTMSGID, custMsgId);
        appendParameterToBuffer(buffer, FIELD_TRANSACTIONID, transactionId);
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
        appendParameterToBuffer(buffer, FIELD_STATUS_INTERNAL, statusCodeInternal);

        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_SUBMIT_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("statusCodeInternal:").append(statusCodeInternal)
            .toString();
    }

    private void appendSubmitAck4MMVD(GmmsMessage msg) {
    	
        if(msg.getStatusCode() != -1){
            statusCode = SMPPRespStatus.getSmppStatus(msg.getStatusCode());
        }else{
            statusCode = 0x00000000;
        }
    	
//        MessageMode mode = msg.getMessageMode();
//        switch(mode){
//            case PROXY:{
//                if(msg.getAttachment() != null){
//                    statusCode =(Integer) msg.getAttachment();
//                }else{
//                    statusCode = SMPPRespStatus.getSmppStatus(msg.getStatusCode());
//                }
//                break;
//            }
//            case STORE_FORWARD:{
//                if(msg.getStatusCode() != -1){
//                    statusCode = SMPPRespStatus.getSmppStatus(msg.getStatusCode());
//                }else{
//                    statusCode = 0x00000000;
//                }
//                break;
//            }
//        }
    }


    private void appendSubmitAck4GMD(GmmsMessage msg) {
    	
        if (msg.getStatusCode() != -1) {
            statusCodeInternal = msg.getStatusCode();
        }
        else {
            statusCodeInternal = GmmsStatus.SUCCESS.getCode();
        }

//        MessageMode mode = msg.getMessageMode();
//
//        switch (mode) {
//            case PROXY: {
//                statusCodeInternal = msg.getStatusCode();
//                break;
//            }
//            case STORE_FORWARD: {
//                if (msg.getStatusCode() != -1) {
//                    statusCodeInternal = msg.getStatusCode();
//                }
//                else {
//                    statusCodeInternal = GmmsStatus.SUCCESS.getCode();
//                }
//                break;
//            }
//        }
    }


    public void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        msgId = msg.getMsgID();
        custMsgId = msg.getInMsgID();
        transactionId = msg.getOutTransID();
        if (msg.getOA2P() != msg.getCurrentA2P() 
        		&& (ctm.isA2P(msg.getOA2P()) || 
        			ctm.isPartition(msg.getOA2P()))){

            appendSubmitAck4GMD(msg);
//            statusCodeInternal = msg.getStatusCode();
//            if (GmmsStatus.SENDER_ADDR_ERROR.getCode() ==
//                msg.getStatusCode() ||
//                GmmsStatus.RECIPIENT_ADDR_ERROR.getCode() ==
//                msg.getStatusCode() ||
//                GmmsStatus.BinaryFilter.getCode() ==
//                msg.getStatusCode()) {
//                statusCodeInternal = msg.getStatusCode();
//            }
//            else {
//                statusCodeInternal = GmmsStatus.SUCCESS.getCode();
//            }

        }
        else {
            appendSubmitAck4MMVD(msg);
//            if (msg.getAttachment() != null) {
//                statusCode =(Integer) msg.getAttachment();
//            }
//            else {
//                statusCode = SMPPRespStatus.getSmppStatus(msg.getStatusCode());
//            }

//            if (GmmsStatus.SENDER_ADDR_ERROR.getCode() ==
//                msg.getStatusCode()) {
//                statusCode = 0x0000000A;
//            }
//            else if (GmmsStatus.RECIPIENT_ADDR_ERROR.getCode() ==
//                     msg.getStatusCode()) {
//                statusCode = 0x0000000B;
//            }
//            else {
//                statusCode = 0x00000000;
//            }

        }
        //log.info("convertFromMsg:" + this);
    }
}
