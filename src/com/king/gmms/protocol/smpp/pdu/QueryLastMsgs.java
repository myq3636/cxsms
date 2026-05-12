package com.king.gmms.protocol.smpp.pdu;

/**
 * <p>Title: QueryLastMsgs</p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 * @author: Jesse Duan
 * @version 1.0
 */

import com.king.gmms.protocol.smpp.util.*;

public class QueryLastMsgs extends Request {

    private Address sourceAddr = new Address();
    private byte numberMsgs = Data.DFLT_MSG_LEN;

    public QueryLastMsgs() {
        super(Data.QUERY_LAST_MSGS);
    }

    protected Response createResponse() {
        return new QueryLastMsgsResp();
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           PDUException {
        setNumberMsgs(buffer.removeByte());
        sourceAddr.setData(buffer); // ?
    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendByte(numberMsgs);
        buffer.appendBuffer(getSourceAddr().getData());
        return buffer;
    }

    public void setNumberMsgs(byte value) throws WrongLengthOfStringException {
        // checkByte(value, Data.SM_MSGID_LEN);
        numberMsgs = value;
    }

    public void setSourceAddr(Address value) {
        sourceAddr = value;
    }

    public void setSourceAddr(String address) throws
                                              WrongLengthOfStringException {
        setSourceAddr(new Address(address));
    }

    public void setSourceAddr(byte ton, byte npi, String address) throws
                                                                  WrongLengthOfStringException {
        setSourceAddr(new Address(ton, npi, address));
    }

    public byte getNumberMsgs() {
        return numberMsgs;
    }

    public Address getSourceAddr() {
        return sourceAddr;
    }

    public String debugString() {
        String dbgs = "(queryLastMsgs: ";
        dbgs += super.debugString();
        dbgs += getNumberMsgs();
        dbgs += " ";
        dbgs += getSourceAddr().debugString();
        dbgs += ") ";
        return dbgs;
    }
}
