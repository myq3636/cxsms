package com.king.gmms.protocol.smpp.pdu;

import com.king.gmms.protocol.smpp.util.*;

/**
 * <p>Title: ParamRetrieve</p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 *
 * @version 1.0
 * @author: Jesse Duan
 */

public class ParamRetrieve
        extends Request {
    private String paramName = "";

    public ParamRetrieve() {
        super(Data.PARAM_RETRIEVE);
    }

    protected Response createResponse() {
        return new ParamRetrieveResp();
    }

    public void setBody(SmppByteBuffer buffer) throws
                                           NotEnoughDataInByteBufferException,
                                           TerminatingZeroNotFoundException,
                                           PDUException {
        setParamName(buffer.removeCString());

    }

    public SmppByteBuffer getBody() {
        SmppByteBuffer buffer = new SmppByteBuffer();
        buffer.appendCString(getParamName());
        return buffer;
    }

    public void setParamName(String value) throws
                                           WrongLengthOfStringException {
        checkString(value, Data.SM_PARAM_NAME_LEN);
        paramName = value;
    }

    public String getParamName() {
        return (paramName);
    }

    public String debugString() {
        String dbgs = "(param_retrieve: ";
        dbgs += super.debugString();
        dbgs += getParamName();
        dbgs += ") ";
        return dbgs;
    }

}
