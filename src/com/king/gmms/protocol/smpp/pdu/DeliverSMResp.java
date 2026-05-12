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

public class DeliverSMResp extends Response {
    private String messageId = Data.DFLT_MSGID;

    public DeliverSMResp() {
        super(Data.DELIVER_SM_RESP);
    }

    public void setBody(SmppByteBuffer buffer)
            throws NotEnoughDataInByteBufferException,
                   TerminatingZeroNotFoundException,
                   WrongLengthOfStringException {
        setMessageId(buffer.removeCString());
    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(messageId);
        return buffer;
    }

    public void setMessageId(String value) throws WrongLengthOfStringException {
        if(!version.validateMessageId(value)) {
            value = value.substring(0, version.lengthMsgId - 1);
        }
        //  checkString(value, Data.SM_MSGID_LEN);
        messageId = value;

    }

    public String getMessageId() {
        return messageId;
    }

    public String debugString() {
        String dbgs = "(deliver_resp: ";
        dbgs += super.debugString();
        dbgs += getMessageId();
        dbgs += " ";
        dbgs += debugStringOptional();
        dbgs += ") ";
        return dbgs;
    }

}
