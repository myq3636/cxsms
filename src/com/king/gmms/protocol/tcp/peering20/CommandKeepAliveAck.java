package com.king.gmms.protocol.tcp.peering20;

import java.io.UnsupportedEncodingException;

import com.king.gmms.protocol.tcp.peering20.*;
import com.king.gmms.protocol.tcp.peering20.exception.*;
import com.king.gmms.util.BufferMonitor;
import com.king.message.gmms.GmmsMessage;

public class CommandKeepAliveAck extends Respond{

    public CommandKeepAliveAck() {
        this(Pdu.VERSION_2_0);
    }

    public CommandKeepAliveAck(int version) {
        if(header == null)
            header = new PduHeader(version);
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
