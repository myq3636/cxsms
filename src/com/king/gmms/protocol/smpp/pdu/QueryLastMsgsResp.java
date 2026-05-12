package com.king.gmms.protocol.smpp.pdu;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 * @author: Jesse Duan
 * @version 1.0
 */

import com.king.gmms.protocol.smpp.util.*;

public class QueryLastMsgsResp extends Response {
    private String messageId = Data.DFLT_MSGID;
    private String messageDetails = Data.DFLT_DATE;
    private byte numberMsgs = Data.DFLT_MSG_LEN;

    public QueryLastMsgsResp() {
        super(Data.QUERY_LAST_MSGS_RESP);
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           PDUException {
        setNumberMsgs(buffer.removeByte());
        setMessageDetails(buffer.removeCString());
        setMessageId(buffer.removeCString());

    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendByte(getNumberMsgs());
        buffer.appendCString(getMessageDetails());
        buffer.appendCString(getMessageId());

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
            int temp = Integer.parseInt(value);
            value = Integer.toHexString(temp);
        }
        // checkString(value, Data.SM_MSGID_LEN);
        messageId = value;
    }

    public void setMessageDetails(String value) throws WrongDateFormatException {
        checkDate(value);
        messageDetails = value;
    }

    public void setNumberMsgs(byte value) {
        numberMsgs = value;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getMessageDetails() {
        return messageDetails;
    }

    public byte getNumberMsgs() {
        return numberMsgs;
    }

    public String debugString() {
        String dbgs = "(query_resp: ";
        dbgs += super.debugString();
        dbgs += getNumberMsgs();
        dbgs += " ";
        dbgs += getMessageDetails();
        dbgs += " ";
        dbgs += getMessageId();
        dbgs += " ";
        dbgs += ") ";
        return dbgs;
    }
}
