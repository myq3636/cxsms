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

public class SubmitSMResp
        extends Response {

    private String messageId = Data.DFLT_MSGID;

    public SubmitSMResp() {
        super(Data.SUBMIT_SM_RESP);
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           WrongLengthOfStringException {
        if (buffer!=null&&buffer.length()>0)
            setMessageId(buffer.removeCString());
    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(messageId);
        return buffer;
    }

    /**
     * for version3.3: ymmdd+4 of ccurentsystime.
     *
     * @param value String
     * @throws WrongLengthOfStringException
     */
    public void setMessageId(String value) throws WrongLengthOfStringException {
        if(!version.validateMessageId(value)) { //lenth>9
            if(value.length() > 11) {
                value = value.substring(5, 10) +
                        value.substring(value.length() - (version.lengthMsgId - 5));
            }
            else {
                value = value.substring(0,
                                        8 < value.length() ? 8 : value.length());
            }
            int temp = Integer.parseInt(value);
            value = Integer.toHexString(temp);
        }
        // checkString(value, Data.SM_MSGID_LEN);
        messageId = value;
    }

    public String getMessageId() {
        return messageId;
    }

    public String debugString() {
        String dbgs = "(submit_resp: ";
        dbgs += super.debugString();
        dbgs += getMessageId();
        dbgs += " ";
        dbgs += debugStringOptional();
        dbgs += ") ";
        return dbgs;
    }

}
