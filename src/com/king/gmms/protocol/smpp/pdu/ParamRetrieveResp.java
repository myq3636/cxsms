package com.king.gmms.protocol.smpp.pdu;

import com.king.gmms.protocol.smpp.util.*;


/**
 * <p>Title: ParamRetrieveResp</p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 *
 * @version 1.0
 * @author: Jesse Duan
 */

public class ParamRetrieveResp extends Response {
    private String paramValue = "";

    public ParamRetrieveResp() {
        super(Data.PARAM_RETRIEVE_RESP);
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           PDUException {
        setParamValue(buffer.removeCString());

    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(getParamValue());
        return buffer;
    }

    public void setParamValue(String value) throws WrongLengthOfStringException {
        checkString(value, Data.SM_PARAM_VALUE_LEN);
        paramValue = value;
    }

    public String getParamValue() {
        return (paramValue);
    }

    public String debugString() {
        String dbgs = "(param_retrieve_resp: ";
        dbgs += super.debugString();
        dbgs += getParamValue();
        dbgs += ") ";
        return dbgs;
    }

}
