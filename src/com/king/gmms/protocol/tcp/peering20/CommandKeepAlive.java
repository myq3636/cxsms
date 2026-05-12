package com.king.gmms.protocol.tcp.peering20;

import java.io.UnsupportedEncodingException;

import com.king.gmms.protocol.tcp.peering20.*;
import com.king.gmms.protocol.tcp.peering20.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;

public class CommandKeepAlive extends Request{

    public CommandKeepAlive() {
        this(Pdu.VERSION_2_0);
    }

    public CommandKeepAlive(int version) {
        if(header == null)
            header = new PduHeader(version);
        header.setCommandId(this.COMMAND_ALIVE);
    }

    public void parsePduCommand(TcpByteBuffer buffer) throws
            NotEnoughDataInByteBufferException, UnsupportedEncodingException,
        UnknownParameterIdException {

    }

    public TcpByteBuffer pduCommandToByteBuffer() throws
            NotEnoughDataInByteBufferException, UnsupportedEncodingException {
        TcpByteBuffer buffer = new TcpByteBuffer();

        return buffer;
    }

    public String toString() {
        return "COMMAND_ALIVE";
    }

    protected Respond createResponse() {
        return null;
    }

    public GmmsMessage convertToMsg(BufferMonitor buffer) {
        return null;
    }

    public void convertFromMsg(GmmsMessage msg)  throws
        NotEnoughDataInByteBufferException, UnsupportedEncodingException{
    }
}
