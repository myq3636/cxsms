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

import com.king.gmms.protocol.smpp.pdu.tlv.*;
import com.king.gmms.protocol.smpp.util.*;

/**
 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.0, 11 Jun 2001
 */

public class DataSMResp
        extends Response {

    // mandatory parameters
    private String messageId = Data.DFLT_MSGID;

    // optional parameters
    private TLVByte deliveryFailureReason = new TLVByte(Data.
            OPT_PAR_DEL_FAIL_RSN);
    private TLVOctets networkErrorCode = new TLVOctets(Data.OPT_PAR_NW_ERR_CODE,
                                                       Data.OPT_PAR_NW_ERR_CODE_MIN, Data.OPT_PAR_NW_ERR_CODE_MAX); // exactly 3
    private TLVString additionalStatusInfoText = new TLVString(Data.
            OPT_PAR_ADD_STAT_INFO, Data.OPT_PAR_ADD_STAT_INFO_MIN,
                                   Data.OPT_PAR_ADD_STAT_INFO_MAX);
    private TLVByte dpfResult = new TLVByte(Data.OPT_PAR_DPF_RES);

    public DataSMResp() {
        super(Data.DATA_SM_RESP);

        registerOptional(deliveryFailureReason);
        registerOptional(networkErrorCode);
        registerOptional(additionalStatusInfoText);
        registerOptional(dpfResult);
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           PDUException {
        setMessageId(buffer.removeCString());
    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(messageId);
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

    public String getMessageId() {
        return messageId;
    }

    public boolean hasDeliveryFailureReason() {
        return deliveryFailureReason.hasValue();
    }

    public boolean hasNetworkErrorCode() {
        return networkErrorCode.hasValue();
    }

    public boolean hasAdditionalStatusInfoText() {
        return additionalStatusInfoText.hasValue();
    }

    public boolean hasDpfResult() {
        return dpfResult.hasValue();
    }

    public void setDeliveryFailureReason(byte value) {
        deliveryFailureReason.setValue(value);
    }

    public void setNetworkErrorCode(SmppByteBuffer value) {
        networkErrorCode.setValue(value);
    }

    public void setAdditionalStatusInfoText(String value) throws
                                                          WrongLengthException {
        additionalStatusInfoText.setValue(value);
    }

    public void setDpfResult(byte value) {
        dpfResult.setValue(value);
    }

    public byte getDeliveryFailureReason() throws ValueNotSetException {
        return deliveryFailureReason.getValue();
    }

    public SmppByteBuffer getNetworkErrorCode() throws ValueNotSetException {
        return networkErrorCode.getValue();
    }

    public String getAdditionalStatusInfoText() throws ValueNotSetException {
        return additionalStatusInfoText.getValue();
    }

    public byte getDpfResult() throws ValueNotSetException {
        return dpfResult.getValue();
    }

    public String debugString() {
        String dbgs = "(data_resp: ";
        dbgs += super.debugString();
        dbgs += getMessageId();
        dbgs += " ";
        dbgs += debugStringOptional();
        dbgs += ") ";
        return dbgs;
    }

}
