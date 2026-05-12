package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class QueryHttpRequest extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(QueryHttpRequest.class);
    private String queryMethod = null;
    private int ssid = 0;
    
	public QueryHttpRequest() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(this.COMMAND_QUERY_HTTP_REQUEST);
	        assignSequenceNumber();
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
	            case FIELD_QUERYFLAG:
	            	queryMethod = buffer.removeString(length);
	                break;
	            case FIELD_CUSTOMER_SSID:
	            	ssid = buffer.removeBytesAsInt(length);
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
	    appendParameterToBuffer(buffer, FIELD_QUERYFLAG, queryMethod);
	    appendParameterToBuffer(buffer, FIELD_CUSTOMER_SSID, ssid);
	    return buffer;
	}
    
    
	public int getSsid() {
		return ssid;
	}
	public void setSsid(int ssid) {
		this.ssid = ssid;
	}
	public String getQueryMethod() {
		return queryMethod;
	}
	public void setQueryMethod(String queryMethod) {
		this.queryMethod = queryMethod;
	}
	public String toString() {
        return new StringBuffer("QUERY_HTTP_REQUEST uuid:").append(uuid).append(",")
            .append("timestamp:").append(timestamp)
            .append(",queryMethod:").append(queryMethod)
            .append(",ssid:").append(ssid)
            .toString();
    }
}
