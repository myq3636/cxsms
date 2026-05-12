package com.king.gmms.ha.systemmanagement.pdu;

import java.io.UnsupportedEncodingException;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.TcpByteBuffer;
import com.king.gmms.protocol.tcp.internaltcp.exception.NotEnoughDataInByteBufferException;
import com.king.gmms.protocol.tcp.internaltcp.exception.UnknownParameterIdException;

public class ChangeDB extends SystemPdu {
	private static SystemLogger log = SystemLogger.getSystemLogger(ChangeDB.class);
	private String action = null;

	public ChangeDB() {
		super();
		if (header == null) {
			header = new SystemPduHeader();
		}
		header.setCommandId(this.COMMAND_CHANGEDB);
		assignSequenceNumber();
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
			case FIELD_ACTION:
				action = buffer.removeString(length);
				break;
			default:
				log.warn("Cant find field with tag: {},len:{}", tag, length);
				buffer.removeBytes(length);
				break;
			}
		}
	}

	public TcpByteBuffer pduCommandToByteBuffer()
			throws NotEnoughDataInByteBufferException,
			UnsupportedEncodingException {

		TcpByteBuffer buffer = new TcpByteBuffer();

		appendParameterToBuffer(buffer, FIELD_UUID, uuid);
		appendParameterToBuffer(buffer, FIELD_TIMESTAMP, timestamp);
		appendParameterToBuffer(buffer, FIELD_ACTION, action);
		return buffer;
	}

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public String toString() {
		return new StringBuffer("CHANGEDB uuid:").append(uuid).append(
				",").append("timestamp:").append(timestamp).append(",action:")
				.append(action).toString();
	}
}
