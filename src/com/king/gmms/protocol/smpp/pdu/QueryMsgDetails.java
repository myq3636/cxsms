package com.king.gmms.protocol.smpp.pdu;

import com.king.gmms.protocol.smpp.util.*;

/**
 * <p>Title: QueryMsgDetails</p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 *
 * @version 1.0
 * @author: Jesse Duan
 */

public class QueryMsgDetails extends Request {
    private String messageId = Data.DFLT_MSGID;
    private short smLength = Data.DFLT_MSG_LEN;
    private Address sourceAddr = new Address();

    public QueryMsgDetails() {
        super(Data.QUERY_MSG_DETAILS);
    }

    protected Response createResponse() {
        return new QueryMsgDetailsResp();
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           PDUException {
        setMessageId(buffer.removeCString());
        sourceAddr.setData(buffer);
        setSmLength(buffer.removeShort());
    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(messageId);
        buffer.appendBuffer(getSourceAddr().getData());
        buffer.appendShort(smLength);
        return buffer;
    }

    public void setMessageId(String value) throws WrongLengthOfStringException {
        if(!version.validateMessageId(value)) { //lenth>9
            if(value.length() > 11) {
                value = value.substring(5, 10) +
                        value.substring(value.length() - (version.lengthMsgId - 5));
            }
            else {
                value = value.substring(0, 8 < value.length() ? 8 : value.length());
            }
        }
        // checkString(value, Data.SM_MSGID_LEN);
        messageId = value;
    }

    public void setSmLength(short value) throws WrongLengthOfStringException {
        smLength = value;
    }

    public Address getSourceAddr() {
        return sourceAddr;
    }

    public short getSMLength() {
        return smLength;
    }

    public String getMessageId() {
        return messageId;
    }

    public String debugString() {
        String dbgs = "(queryMsgDetails: ";
        dbgs += super.debugString();
        dbgs += getMessageId();
        dbgs += getSMLength();
        dbgs += " ";
        dbgs += getSourceAddr().debugString();
        dbgs += ") ";
        return dbgs;
    }

}
