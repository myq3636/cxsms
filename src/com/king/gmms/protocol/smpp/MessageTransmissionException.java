/*
* MessageTransmittionException.java
*
* Created on February 14, 2003, 10:33 AM
*/

package com.king.gmms.protocol.smpp;

/**
 * @author mike
 */
public class MessageTransmissionException extends java.lang.Exception {

    /**
     * Creates a new instance of <code>MessageTransmittionException</code> without detail message.
     */
    public MessageTransmissionException() {
    }

    public MessageTransmissionException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs an instance of <code>MessageTransmittionException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public MessageTransmissionException(String msg) {
        super(msg);
    }
}
