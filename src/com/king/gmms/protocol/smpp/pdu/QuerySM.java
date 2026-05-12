/*
 * Copyright (c) 1996-2001
 * Logica Mobile Networks Limited
 * All rights reserved.
 *
 * This software is distributed under Logica Open Source License Version 1.0
 * ("Licence Agreement"). You shall use it and distribute only in accordance
 * with the terms of the License Agreement.
 *
 */
package com.king.gmms.protocol.smpp.pdu;

import com.king.gmms.protocol.smpp.util.*;

/**
 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.0, 11 Jun 2001
 */

public class QuerySM extends Request {
    private String messageId = Data.DFLT_MSGID;
    private Address sourceAddr = new Address();

    public QuerySM() {
        super(Data.QUERY_SM);
    }

    protected Response createResponse() {
        return new QuerySMResp();
    }

    public void setBody(SmppByteBuffer buffer)
            throws NotEnoughDataInByteBufferException,
                   TerminatingZeroNotFoundException,
                   PDUException {
        setMessageId(buffer.removeCString());
        sourceAddr.setData(buffer); // ?
    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(messageId);
        buffer.appendBuffer(getSourceAddr().getData());
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

    public void setSourceAddr(Address value) {
        sourceAddr = value;
    }

    public void setSourceAddr(String address)
            throws WrongLengthOfStringException {
        setSourceAddr(new Address(address));
    }

    public void setSourceAddr(byte ton, byte npi, String address)
            throws WrongLengthOfStringException {
        setSourceAddr(new Address(ton, npi, address));
    }

    public String getMessageId() {
        return messageId;
    }

    public Address getSourceAddr() {
        return sourceAddr;
    }

    public String debugString() {
        String dbgs = "(query: ";
        dbgs += super.debugString();
        dbgs += getMessageId();
        dbgs += " ";
        dbgs += getSourceAddr().debugString();
        dbgs += " ";
        dbgs += debugStringOptional();
        dbgs += ") ";
        return dbgs;
    }
}
