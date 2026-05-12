package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class ConnectionStatusNotification extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionStatusNotification.class);
    private int ssid = -1;
    private String bindType = null;
    private String action = null;
    
	public ConnectionStatusNotification() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(COMMAND_CONNECTION_STATUS_NOTIFICATION);
	        assignSequenceNumber();
	}
	
	public ConnectionStatusNotification(int ssid,TransactionURI uri) {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(COMMAND_CONNECTION_STATUS_NOTIFICATION);
        assignSequenceNumber();
        uuid = uri.toString();
        this.ssid = ssid;
	}
	
	public ConnectionStatusNotification(ConnectionInfo connectionInfo,TransactionURI uri) {
		if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(COMMAND_CONNECTION_STATUS_NOTIFICATION);
        assignSequenceNumber();
		ssid = connectionInfo.getSsid();
        String connectionName = connectionInfo.getConnectionName();
        if(uri==null){
        	uri = new TransactionURI(connectionName);
        }
        uuid = uri.toString();
	}
	
	public ConnectionStatusNotification(ConnectionStatusNotification notification){
		super(notification);
        if (header == null) {
            header = new SystemPduHeader();
            header.setCommandId(COMMAND_CONNECTION_STATUS_NOTIFICATION);
            assignSequenceNumber();
        }
		this.ssid = notification.getSsid();
		this.bindType = notification.getBindType();
		this.action = notification.getAction();
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
	            case FIELD_BINDTYPE:
	            	bindType = buffer.removeString(length);
	                break;
	            case FIELD_ACTION:
	            	action = buffer.removeString(length);
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
	    appendParameterToBuffer(buffer, FIELD_BINDTYPE, bindType);
	    appendParameterToBuffer(buffer, FIELD_ACTION, action);
	    return buffer;
	}
	public int getSsid() {
		return ssid;
	}
	public void setSsid(int ssid) {
		this.ssid = ssid;
	}
	public String getBindType() {
		return bindType;
	}
	public void setBindType(String bindType) {
		this.bindType = bindType;
	}
	public String getAction() {
		return action;
	}
	public void setAction(String action) {
		this.action = action;
	}
	public String toString() {
        return new StringBuffer("CONNECTION_STATUS_NOTIFICATION ").append(super.toString())
            .append(",ssid:").append(ssid)
            .append(",bindType:").append(bindType)
            .append(",action:").append(action)
            .toString();
    }
}
