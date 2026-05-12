package com.king.gmms.protocol.tcp.peering20.exception;

public class ExceedMaxWaitNumberException extends java.lang.Exception {

    /**
     * Creates a new instance of <code>ExceedMaxWaitNumber</code> without detail message.
     */
    public ExceedMaxWaitNumberException() {
    }


    /**
     * Constructs an instance of <code>ExceedMaxWaitNumber</code> with the specified detail message.
     * @param msg the detail message.
     */
    public ExceedMaxWaitNumberException(String msg) {
        super(msg);
    }
}
