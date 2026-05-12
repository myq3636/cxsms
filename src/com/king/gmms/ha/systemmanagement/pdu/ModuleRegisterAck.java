package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class ModuleRegisterAck extends SystemPdu {
    private static SystemLogger log = SystemLogger.getSystemLogger(ModuleRegisterAck.class);
    private int responseCode = -1;
    private String dbStatus = null;
    private String redisStatus = null;
    
    public ModuleRegisterAck(SystemPdu req) {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(this.COMMAND_MODULE_REGISTER_ACK);
        header.setSequenceNumber(req.getSequenceNumber());
    }
    
	public ModuleRegisterAck() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(this.COMMAND_MODULE_REGISTER_ACK);
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
	            case FIELD_DB_STATUS:
	            	dbStatus = buffer.removeString(length);
	                break;
	            case FIELD_REDIS_STATUS:
	            	redisStatus = buffer.removeString(length);
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
	    appendParameterToBuffer(buffer, FIELD_DB_STATUS, dbStatus);
	    appendParameterToBuffer(buffer, FIELD_REDIS_STATUS, redisStatus);
	    return buffer;
	}
	public int getResponseCode() {
		return responseCode;
	}
	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}
	public String toString() {
        return new StringBuffer("MODULE_REGISTER_ACK uuid:").append(uuid)
            .append(",timestamp:").append(timestamp)
            .append(",responseCode:").append(responseCode)
            .append(",dbStatus:").append(dbStatus)
            .append(",redisStatus:").append(redisStatus)
            .toString();
    }
	public String getDbStatus() {
		return dbStatus;
	}
	public void setDbStatus(String dbStatus) {
		this.dbStatus = dbStatus;
	}
	public String getRedisStatus() {
		return redisStatus;
	}
	public void setRedisStatus(String redisStatus) {
		this.redisStatus = redisStatus;
	}
}
