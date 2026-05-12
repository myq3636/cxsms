package com.king.gmms.protocol.tcp.internaltcp;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.tcp.internaltcp.exception.*;

public class PduHeader {
    private static SystemLogger log = SystemLogger.getSystemLogger(PduHeader.class);

    protected long totalLength = 0;
    protected int commandId = 0;

    private int version = Pdu.VERSION_2_0;

    public PduHeader() {
    }

    public PduHeader(int commandId){
    	setCommandId(commandId);
    }

    public int getCommandId() {
        return commandId;
    }

    public long getTotalLength() {
        return totalLength;
    }

    public static PduHeader parseHeader(TcpByteBuffer buffer) throws
            NotEnoughDataInByteBufferException, UnknownCommandIdException {
        if(buffer == null) {
            log.error("TcpByteBuffer is null in parseHeader method.");
            throw new NotEnoughDataInByteBufferException("TcpByteBuffer is null in parseHeader method.");
        }

        PduHeader theHeader = new PduHeader();

        theHeader.totalLength = buffer.remove4BytesAsLong();
        theHeader.commandId = buffer.remove1ByteAsInt();

        boolean found = false;

        for (int i = 0; i < Pdu.commandIdList.length; i++) {
            if (theHeader.commandId == Pdu.commandIdList[i]) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new UnknownCommandIdException(theHeader.commandId);
        }

        return theHeader;

    }


    public void setCommandId(int newCommandId) {
        commandId = newCommandId;
    }


    public void setTotalLength(long newTotalLength) {
        totalLength = newTotalLength;
    }

    public TcpByteBuffer toByteBuffer() {

        TcpByteBuffer buffer = new TcpByteBuffer();
        try {
            buffer.appendLongAs4Bytes(totalLength);
            buffer.appendIntAs1Byte(commandId);

        } catch (Exception e) {
            log.error(e, e);
        }
        return buffer;
    }
}
