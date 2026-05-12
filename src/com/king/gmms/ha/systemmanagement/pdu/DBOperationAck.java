package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class DBOperationAck extends SystemPdu {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(DBOperationAck.class);
	private int responseCode = -1;
	
	public DBOperationAck(SystemPdu req) {
		if (header == null) {
			header = new SystemPduHeader();
		}
		header.setCommandId(this.COMMAND_DB_OPERATION_ACK);
        header.setSequenceNumber(req.getSequenceNumber());
	}
	
	public DBOperationAck() {
		if (header == null) {
			header = new SystemPduHeader();
		}
		header.setCommandId(this.COMMAND_DB_OPERATION_ACK);
	}

	public void parsePduCommand(TcpByteBuffer buffer)
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException, UnknownParameterIdException {
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
			default:
				log.warn("Cant find field with tag: {},len:{}", tag, length);
				buffer.removeBytes(length);
				break;
			}
		}
	}
	public boolean isRequest(){
		return false;
	}
	public TcpByteBuffer pduCommandToByteBuffer()
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {

		TcpByteBuffer buffer = new TcpByteBuffer();

		appendParameterToBuffer(buffer, FIELD_UUID, uuid);
		appendParameterToBuffer(buffer, FIELD_TIMESTAMP, timestamp);
		appendParameterToBuffer(buffer, FIELD_RESPONSECODE, responseCode);
		return buffer;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public String toString() {
		return new StringBuffer("DB_OPERATION_ACK uuid:").append(uuid)
				.append(",").append("timestamp:").append(timestamp).append(
						",responseCode:").append(responseCode).toString();
	}
}
