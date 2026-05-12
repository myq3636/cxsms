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

public class QuerySMResp extends Response {
    private String messageId = Data.DFLT_MSGID;
    private String finalDate = Data.DFLT_DATE;//1 or 17
    private byte messageState = Data.DFLT_MSG_STATE;
    private byte errorCode = Data.DFLT_ERR;

    public QuerySMResp() {
        super(Data.QUERY_SM_RESP);
    }

    public void setBody(SmppByteBuffer buffer)
            throws NotEnoughDataInByteBufferException,
                   TerminatingZeroNotFoundException,
                   PDUException {
        setMessageId(buffer.removeCString());
        setFinalDate(buffer.removeCString());
        setMessageState(buffer.removeByte());
        setErrorCode(buffer.removeByte());
    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(getMessageId());
        buffer.appendCString(getFinalDate());
        buffer.appendByte(getMessageState());
        buffer.appendByte(getErrorCode());
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

    public void setFinalDate(String value)
            throws WrongDateFormatException {
        checkDate(value);
        finalDate = value;
    }

    public void setMessageState(byte value) {
        messageState = value;
    }

    public void setErrorCode(byte value) {
        errorCode = value;
    }


    public String getMessageId() {
        return messageId;
    }

    public String getFinalDate() {
        return finalDate;
    }

    public byte getMessageState() {
        return messageState;
    }

    public byte getErrorCode() {
        return errorCode;
    }

    public String debugString() {
        String dbgs = "(query_resp: ";
        dbgs += super.debugString();
        dbgs += getMessageId();
        dbgs += " ";
        dbgs += getFinalDate();
        dbgs += " ";
        dbgs += getMessageState();
        dbgs += " ";
        dbgs += getErrorCode();
        dbgs += " ";
        dbgs += debugStringOptional();
        dbgs += ") ";
        return dbgs;
    }
}
