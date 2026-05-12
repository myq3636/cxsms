package com.king.gmms.ha.systemmanagement.pdu;

public class KeepAliveAck extends SystemPdu {
	 
	public KeepAliveAck(SystemPdu req) {
	        if (header == null) {
	            header = new SystemPduHeader();
	        }
	        header.setCommandId(this.COMMAND_ACK);
	        header.setSequenceNumber(req.getSequenceNumber());
	 }
	public KeepAliveAck() {
        if (header == null) {
            header = new SystemPduHeader();
        }
        header.setCommandId(this.COMMAND_ACK);
	}
	public boolean isRequest(){
		return false;
	}
	public String toString() {
        return new StringBuffer("ACK uuid:").append(uuid).append(",")
            .append("timestamp:").append(timestamp)
            .toString();
    }
}
