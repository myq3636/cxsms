package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;
import com.king.gmms.util.SystemConstants;

public class ConnectionConfirm extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionConfirm.class);
    private int responseCode = -1;
    private String bindType = null;
    private int ssid = -1;

	public ConnectionConfirm() {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(this.COMMAND_CONNECTION_CONFIRM);
        assignSequenceNumber();
	}
	public ConnectionConfirm(boolean isServer,int ssid, int responseCode) {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(this.COMMAND_CONNECTION_CONFIRM);
        assignSequenceNumber();
        this.responseCode = responseCode;
        if(isServer){
        	bindType = SystemConstants.IN_BIND_TYPE;
        }else{
        	bindType = SystemConstants.OUT_BIND_TYPE;
        }
        this.ssid = ssid;
	}
	public ConnectionConfirm(SystemPdu message,int responseCode) {
		if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(this.COMMAND_CONNECTION_CONFIRM);
        assignSequenceNumber();
        this.uuid = message.getUuid();
        this.responseCode = responseCode;
        if(message.getCommandId()==SystemPdu.COMMAND_IN_BIND_ACK){
        	bindType = SystemConstants.IN_BIND_TYPE;
        }else if(message.getCommandId()==SystemPdu.COMMAND_OUT_BIND_ACK){
        	bindType = SystemConstants.OUT_BIND_TYPE;
        }
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
	            case FIELD_BINDTYPE:
	            	bindType = buffer.removeString(length);
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
	    appendParameterToBuffer(buffer, FIELD_RESPONSECODE, responseCode);
	    appendParameterToBuffer(buffer, FIELD_BINDTYPE, bindType);
	    appendParameterToBuffer(buffer, FIELD_SSID, ssid);
	    return buffer;
	}
	public int getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}
	public String toString() {
        return new StringBuffer("CONNECTION_CONFIRM ").append(super.toString())
            .append(",responseCode:").append(responseCode)
            .append(",bindType:").append(bindType)
            .append(",ssid:").append(ssid)
            .toString();
    }
	public String getBindType() {
		return bindType;
	}
	public void setBindType(String bindType) {
		this.bindType = bindType;
	}
	public int getSsid() {
		return ssid;
	}
	public void setSsid(int ssid) {
		this.ssid = ssid;
	}
}
