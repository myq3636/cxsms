package com.king.gmms.protocol.smpp.version;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: King</p>
 * @author: Jesse Duan
 * @version 1.0
 */

import com.king.gmms.protocol.smpp.pdu.SmppException;

public class VersionException extends SmppException {
    public VersionException() {
    }

    public VersionException(String msg) {
        super(msg);
    }


}
