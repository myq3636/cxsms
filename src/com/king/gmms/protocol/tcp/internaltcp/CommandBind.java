package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;

import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;

public class CommandBind
    extends Request {
    private int version = Pdu.VERSION_2_0;
    private String userName;
    private String password;
    private String timestamp;
    
    public CommandBind() {
        if (header == null) {
            header = new PduHeader();
        }
        header.setCommandId(this.COMMAND_BIND);
    }

    public int getVersion() {
        return version;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
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
                case FIELD_USERNAME:
                    userName = buffer.removeString(length);
                    break;
                case FIELD_PASSWORD:
                    password = buffer.removeString(length);
                    break;
                case FIELD_TIMESTAMP:
                    timestamp = buffer.removeString(length);
                    break;
                default:
                    log.warn("Cant find field with tag: {},len:{}" , tag ,length);
                    buffer.removeBytes(length);
                    break;
            }
        }
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {

        TcpByteBuffer buffer = new TcpByteBuffer();

        appendParameterToBuffer(buffer, FIELD_VERSION, version);
        appendParameterToBuffer(buffer, FIELD_USERNAME, userName);
        appendParameterToBuffer(buffer, FIELD_PASSWORD, password);
        appendParameterToBuffer(buffer, FIELD_TIMESTAMP, timestamp);
        
        if(log.isDebugEnabled()){
			log.debug("Send:{}", this);
        }
        return buffer;
    }

    public String toString() {
        return new StringBuffer("COMMAND_BIND:")
            .append("version:").append(version).append(",")
            .append("userName:").append(userName)
            .toString();
    }

    protected Respond createResponse() {
        return null;
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
        return null;
    }

    public void convertFromMsg(GmmsMessage msg) throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException {
    }

}
