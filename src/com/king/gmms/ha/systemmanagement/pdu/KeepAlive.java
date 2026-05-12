package com.king.gmms.ha.systemmanagement.pdu;

public class KeepAlive extends SystemPdu {
	 
	public KeepAlive() {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(this.COMMAND_KEEP_ALIVE);
	        assignSequenceNumber();
	 }
	public String toString() {
        return new StringBuffer("KEEP_ALIVE uuid:").append(uuid).append(",")
            .append("timestamp:").append(timestamp)
            .toString();
    }
}
