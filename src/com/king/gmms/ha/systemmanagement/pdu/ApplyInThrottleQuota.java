/**
 * Copyright 2000-2013 King Inc. All rights reserved.
 */
package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class ApplyInThrottleQuota extends SystemPdu {
	
	private static SystemLogger log = SystemLogger.getSystemLogger(ApplyInThrottleQuota.class);

	private int ssid = -1;
	
	public ApplyInThrottleQuota() {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(SystemPdu.COMMAND_APPLY_IN_THROTTLE_QUOTA);
    }

	@Override
	public String toString() {
		return new StringBuffer("APPLY_IN_THROTTLE_QUOTA uuid:").append(uuid).append(",")
        .append("timestamp:").append(timestamp)
        .append(",ssid:").append(ssid)
        .toString();
	}

	@Override
	public void parsePduCommand(TcpByteBuffer buffer) throws NotEnoughDataInByteBufferException, UnsupportedEncodingException, UnknownParameterIdException {
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

	@Override
	public TcpByteBuffer pduCommandToByteBuffer() throws NotEnoughDataInByteBufferException, UnsupportedEncodingException {
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

}
