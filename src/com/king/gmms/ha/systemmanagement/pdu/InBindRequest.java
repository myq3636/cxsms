package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

@Deprecated
public class InBindRequest extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(InBindRequest.class);
    private int ssid = -1;
	public InBindRequest() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(this.COMMAND_IN_BIND_REQUEST);
	 }
	public InBindRequest(ConnectionInfo connectionInfo,TransactionURI uri) {
		if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(this.COMMAND_IN_BIND_REQUEST);
		ssid = connectionInfo.getSsid();
        String connectionName = connectionInfo.getConnectionName();
        if(uri==null){
        	uri = new TransactionURI(connectionName);
        }
        uuid = uri.toString();
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
	            case FIELD_SSID:
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
	    appendParameterToBuffer(buffer, FIELD_SSID, ssid);
	    return buffer;
	}
	public int getSsid() {
		return ssid;
	}
	public void setSsid(int ssid) {
		this.ssid = ssid;
	}
	public String toString() {
        return new StringBuffer("IN_BIND_REQUEST uuid:").append(uuid).append(",")
            .append("timestamp:").append(timestamp)
            .append(",ssid:").append(ssid)
            .toString();
    }
}
