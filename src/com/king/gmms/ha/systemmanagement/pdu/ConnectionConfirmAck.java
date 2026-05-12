package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class ConnectionConfirmAck extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionConfirmAck.class);
    /**
     * 0: OK </br>
     * 1: fail, should retry </br>
     * 2: cm.cfg inconsistent, should retry
     */
    private int responseCode = -1;
    public ConnectionConfirmAck(SystemPdu req) {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(COMMAND_CONNECTION_CONFIRM_ACK);
        header.setSequenceNumber(req.getSequenceNumber());
    }
	public ConnectionConfirmAck() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(COMMAND_CONNECTION_CONFIRM_ACK);
	 }
	public boolean isRequest(){
		return false;
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
	            case FIELD_UUID:
	                uuid = buffer.removeString(length);
	                break;
	            case FIELD_TIMESTAMP:
	            	timestamp = buffer.removeString(length);
	                break;
	            case FIELD_RESPONSECODE:
	            	responseCode = buffer.removeBytesAsInt(length);
	                break;
	            default:
	                log.warn("Cant find field with tag: {},len:{}"
	                         ,tag,length);
	                buffer.removeBytes(length);
	                break;
		        }
	    }
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
	    NotEnoughDataInByteBufferException, UnsupportedEncodingException {
	
	    TcpByteBuffer buffer = new TcpByteBuffer();
	
	    appendParameterToBuffer(buffer, FIELD_UUID, uuid);
	    appendParameterToBuffer(buffer, FIELD_TIMESTAMP, timestamp);
	    appendParameterToBuffer(buffer, FIELD_RESPONSECODE, responseCode);
	    return buffer;
	}
	public int getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}
	public String toString() {
        return new StringBuffer("CONNECTION_CONFIRM_ACK ").append(super.toString())
            .append(",responseCode:").append(responseCode)
            .toString();
    }
}
