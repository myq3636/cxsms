/*
 * MessageNoFoundException.java
 *
 * Created on February 13, 2003, 5:20 PM
 */

package com.king.gmms.protocol.smpp;

/**
 * @author mike
 */
public class MessageNoFoundException extends java.lang.Exception {

    /**
     * Creates a new instance of <code>MessageNoFoundException</code> without detail message.
     */
    public MessageNoFoundException() {
    }


    /**
     * Constructs an instance of <code>MessageNoFoundException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public MessageNoFoundException(String msg) {
        super(msg);
    }
}
