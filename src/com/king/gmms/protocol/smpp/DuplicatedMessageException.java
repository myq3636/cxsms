/*
 * DuplicatedMessageException.java
 *
 * Created on February 17, 2003, 5:22 PM
 */

package com.king.gmms.protocol.smpp;

import com.king.message.gmms.GmmsMessage;

/**
 * @author mike
 */
public class DuplicatedMessageException extends java.lang.Exception {
    private GmmsMessage msg = null;

    /**
     * Creates a new instance of <code>DuplicatedMessageException</code> without detail message.
     */
    public DuplicatedMessageException() {
    }

    public DuplicatedMessageException(Throwable cause) {
        super(cause);
    }

    public DuplicatedMessageException(GmmsMessage msg) {
        this.msg = msg;
    }

    public DuplicatedMessageException(String msg) {
        super(msg);
    }

    public GmmsMessage getTheMessage() {
        return msg;
    }

}
