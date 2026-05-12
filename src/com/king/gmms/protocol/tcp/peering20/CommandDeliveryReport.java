package com.king.gmms.protocol.tcp.peering20;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.*;
import com.king.gmms.protocol.tcp.peering20.*;
import com.king.gmms.protocol.tcp.peering20.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

public class CommandDeliveryReport
    extends Request {
    private static SystemLogger log = SystemLogger.getSystemLogger(
        CommandDeliveryReport.class);
    A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
    private String msgId;
    private int statusCode = -1;
    private int statusCodeInternal = -1;

    public CommandDeliveryReport() {
        this(Pdu.VERSION_2_0);
    }

    public CommandDeliveryReport(int version) {
        if (header == null) {
            header = new PduHeader(version);
        }
        header.setCommandId(this.COMMAND_DELIVERY_REPORT);
    }

    public String getMsgId() {
        return msgId;
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
        GmmsMessage message = new GmmsMessage();
        message.setMessageType(GmmsMessage.MSG_TYPE_DELIVERY_REPORT);
        if (statusCode != -1) {
            switch (statusCode) {
                case 0:
                    message.setStatusCode(GmmsStatus.DELIVERED.getCode());
                    break; //10000
                case 1:
                    message.setStatusCode(GmmsStatus.EXPIRED.getCode());
                    break; //10200
                case 2:
                    message.setStatusCode(GmmsStatus.EXPIRED.getCode());
                    break; //10200
                case 3:
                    message.setStatusCode(GmmsStatus.UNDELIVERABLE.
                                          getCode());
                    break; //10400
                case 4:
                    message.setStatusCode(GmmsStatus.ENROUTE.getCode());
                    break; //10105
                case 5:
                    message.setStatusCode(GmmsStatus.UNKNOWN.getCode());
                    break; //10900
                case 6:
                    message.setStatusCode(GmmsStatus.REJECTED.getCode());
                    break; //10500
                default:
                    message.setStatusCode(GmmsStatus.UNKNOWN.getCode());
                    break; //10900
            }
        } else if (statusCodeInternal != -1) {
            message.setStatusCode(statusCodeInternal);
        }
        message.setOutMsgID(msgId);
        if(log.isInfoEnabled()){
			log.info(message, "Received DR,outmsgid={},status code={}" , msgId , message.getStatusCode());
        }
        return message;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {

        TcpByteBuffer buffer = new TcpByteBuffer();

        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
        appendParameterToBuffer(buffer, FIELD_STATUS_INTERNAL, statusCodeInternal);

        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_DELIVERY_REPORT:")
            .append("msgId:").append(msgId).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("statusCodeInternal:").append(statusCodeInternal)
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
        msgId = msg.getInMsgID();
        if (msg.getOA2P() != msg.getCurrentA2P() 
        	&& (ctm.isA2P(msg.getOA2P()) || 
        	   ctm.isPartition(msg.getOA2P()))) {
            statusCodeInternal = msg.getStatusCode();
        }
        else {
            statusCode = getStatus(msg.getStatusCode());
        }
    }

    private int getStatus(int statusCode) {
        int status = -1;
        switch (statusCode) {
            case 0:
            case 10000:
            case 10105:
                status = 0;
                break;
                //   case 10105:
            case 12000:
            case 13000:
                status = 4;
                break;
            case 2100:
            case 2110:
            case 2120:
            case 10400:
            case 11005:
                status = 3;
                break;
            case 10200:
                status = 1;
                break;
            case 10300:
                status = 2;
                break;
            case 10500:
                status = 6;
                break;
            case 10700:
                status = 6;
                break;
            case 9000:
            case 10900:
            default:
                status = 5;
        }
        return status;
    }
}
