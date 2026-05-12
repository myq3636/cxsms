package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;

import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;

public class CommandBindAck
    extends Respond {
    private int version = Pdu.VERSION_2_0;
    private int statusCode = -1;


    public CommandBindAck() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(this.COMMAND_BIND_ACK);
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
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
                case FIELD_VERSION:
                    version = buffer.removeBytesAsInt(length);
                    if (version == Pdu.VERSION_1_0) {
                        PDU_HEADER_SIZE = 3;
                    }
                    else {
                        PDU_HEADER_SIZE = 5;
                    }
                    break;
                case FIELD_STATUS:
                    statusCode = buffer.removeBytesAsInt(length);
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

        appendParameterToBuffer(buffer, FIELD_VERSION, version);
        appendParameterToBuffer(buffer, FIELD_STATUS, statusCode);

        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_BIND_ACK:")
            .append("version:").append(version).append(",")
            .append("statusCode:").append(statusCode)
            .toString();
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
        return null;
    }

    public void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    }

}
