package com.king.gmms.protocol.tcp.peering20;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.peering20.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CommandDeliveryReportAck
    extends Respond {
    private static SystemLogger log = SystemLogger.getSystemLogger(
        CommandDeliveryReportAck.class);
    private String msgId;
    private int statusCode = -1;

    public CommandDeliveryReportAck() {
        this(Pdu.VERSION_2_0);
    }

    public CommandDeliveryReportAck(int version) {
        if (header == null) {
            header = new PduHeader(version);
        }
        header.setCommandId(this.COMMAND_DELIVERY_REPORT_ACK);
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
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
        if (buffer == null || header == null) {
            return null;
        }
        GmmsMessage msg = buffer.remove(msgId);
        if (msg != null) {
            msg.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT_RESP);
            if (msg.getStatusCode() < 10000) {
                switch (msg.getStatusCode()) {
                    case 0:
                        msg.setStatus(GmmsStatus.DELIVERED);
                        break;
                    default:
                        msg.setStatus(GmmsStatus.UNDELIVERABLE);
                }
            }
            if (statusCode != 0) {
                msg.setStatusCode(GmmsStatus.
                                  FAIL_SENDOUT_DELIVERYREPORT.
                                  getCode());
                msg.setAttachment(com.king.gmms.protocol.smpp.util.Data.ESME_RUNKNOWNERR);
            }else{
                msg.setAttachment(com.king.gmms.protocol.smpp.util.Data.ESME_ROK);
            }
        }
        else {
            log.warn("Can't found the message by inMsgId:{}" , msgId);
        }
        return msg;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {

        TcpByteBuffer buffer = new TcpByteBuffer();

        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);

        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_DELIVERY_REPORT_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("statusCode:").append(statusCode)
            .toString();
    }

    protected Respond createResponse() {
        return null;
    }

    public void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        if (msg == null) {
            return;
        }
        setMsgId(msg.getOutMsgID());
        setStatusCode(0);
    }
}
