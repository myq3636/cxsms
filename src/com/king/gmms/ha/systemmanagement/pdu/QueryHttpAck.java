package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class QueryHttpAck extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(QueryHttpAck.class);
    private String value = null;
    
    public QueryHttpAck(SystemPdu req) {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(this.COMMAND_QUERY_HTTP_ACK);
        header.setSequenceNumber(req.getSequenceNumber());
    }
	public QueryHttpAck() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(this.COMMAND_QUERY_HTTP_ACK);
	        assignSequenceNumber();
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
	            case FIELD_VALUE:
	            	value = buffer.removeString(length);
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
	    appendParameterToBuffer(buffer, FIELD_VALUE, value);
	    return buffer;
	}	
    
    
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String toString() {
        return new StringBuffer("QUERY_HttP_ACK uuid:").append(uuid).append(",")
            .append("timestamp:").append(timestamp)
            .append(",value:").append(value)
            .toString();
    }
}
