package com.king.gmms.protocol.tcp.internaltcp;

import java.io.UnsupportedEncodingException;

import com.king.gmms.protocol.tcp.internaltcp.*;
import com.king.gmms.protocol.tcp.internaltcp.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;

public class CommandKeepAliveAck extends Respond{

    public CommandKeepAliveAck() {
        if(header == null)
            header = new PduHeader();
        header.setCommandId(this.COMMAND_ALIVE_ACK);
    }

    public void parsePduCommand(TcpByteBuffer buffer) throws
            NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException {
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
        return null;
    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
            NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        return null;
    }

    public String toString() {
        return "COMMAND_ALIVE_ACK";
    }

    public void convertFromMsg(GmmsMessage msg)  throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException{
    }
}
