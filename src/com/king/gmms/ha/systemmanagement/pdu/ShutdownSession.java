package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class ShutdownSession extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(ShutdownSession.class);
    private int ssid = -1;
    private String connectionName = null;
    private int sessionNum = 0;
	public ShutdownSession() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(this.COMMAND_SHUTDOWN_SESSION);
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
	            case FIELD_SSID:
	            	ssid = buffer.removeBytesAsInt(length);
	                break; 
	            case FIELD_CONNECTIONNAME:
	            	connectionName = buffer.removeString(length);
	                break;     
	            case FIELD_SESSIONNUM:
	            	sessionNum = buffer.removeBytesAsInt(length);
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
	    appendParameterToBuffer(buffer, FIELD_CONNECTIONNAME, connectionName);
	    appendParameterToBuffer(buffer, FIELD_SESSIONNUM, sessionNum);
	    return buffer;
	}
	public String toString() {
        return new StringBuffer("SHUTDOWN_SESSION uuid:").append(uuid).append(",")
            .append("timestamp:").append(timestamp)
            .append(",sessionNum:").append(sessionNum)
            .append(",connectionName:").append(connectionName)
            .append(",ssid:").append(ssid)
            .toString();
    }
	public int getSsid() {
		return ssid;
	}
	public void setSsid(int ssid) {
		this.ssid = ssid;
	}
	public int getSessionNum() {
		return sessionNum;
	}
	public void setSessionNum(int sessionNum) {
		this.sessionNum = sessionNum;
	}
	public String getConnectionName() {
		return connectionName;
	}
	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}
}
