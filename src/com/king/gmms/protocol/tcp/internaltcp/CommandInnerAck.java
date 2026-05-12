package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.*;
import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;

/**
 * @deprecated V4.4 废弃。Fast Accept 架构下，Server 在 produceSubmitMessage 后
 *             立即响应客户端，Core 通过 MTStreamConsumer/ResultStreamConsumer 独立完成 CDR 闭环，
 *             不再有任何跨模块 InnerAck TCP 通知。本类不再被 InternalAgentSession 实例化。
 */
@Deprecated
public class CommandInnerAck
    extends Respond {
    private static SystemLogger log = SystemLogger.getSystemLogger(CommandInnerAck.class);
    A2PCustomerManager ctm = gmmsUtility.getCustomerManager();
    protected String msgId;
    protected int statusCode = -1;
    private String  statusText;

    public CommandInnerAck() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(this.COMMAND_INNER_ACK);
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
                case FIELD_STATUSTEXT:
                	statusText = buffer.removeString(length);
                	break;
                default:
                    log.warn("Cant find field with tag: {},len:{}"
                             ,tag,length);
                    buffer.removeBytes(length);
                    break;
            }
        }
    }
    /**
     * convertToMsg
     */
    public GmmsMessage convertToMsg(BufferMonitor buffer) {
        GmmsMessage msg = null;
        if(buffer != null && msgId != null){
        	msg = buffer.remove(msgId);
        }
        
        if (msg == null) {
			msg = buffer.remove(msgId+"_"+statusText);
		}
        if(msg != null ){
        	if(GmmsMessage.MSG_TYPE_SUBMIT.equalsIgnoreCase(msg.getMessageType())
        			&& msg.getStatusCode() < 0 && statusCode > 0){
        		msg.setStatusCode(GmmsStatus.INSUBMIT_RESP_FAILED.getCode());       		
        	}
        	else if(GmmsMessage.MSG_TYPE_DELIVERY_REPORT.equalsIgnoreCase(msg.getMessageType())
        			){
        		if (statusCode == GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode()) {
        			msg.setStatusCode(GmmsStatus.FAIL_SENDOUT_DELIVERYREPORT.getCode());            		
				}
        		msg.setStatusText(statusText);
        	}
        }
        return msg;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        TcpByteBuffer buffer = new TcpByteBuffer();

        appendParameterToBuffer(buffer, FIELD_MSGID, msgId);
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);
        appendParameterToBuffer(buffer, FIELD_STATUSTEXT, statusText);

        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_INNER_ACK:")
            .append("msgId:").append(msgId).append(",")
            .append("statusCode:").append(statusCode).append(",")
            .append("statusText:").append(statusText)
            .toString();
    }


    public void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    	if(msg.getOutMsgID() != null){
    		msgId = msg.getOutMsgID();
    	}else{
    		msgId = msg.getMsgID();
    	}
        statusCode = msg.getStatusCode();
        statusText = msg.getStatusText();
    }
    
}
