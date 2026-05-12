/*
 * InvalidSmppTimeFormatException.java
 *
 * Created on January 2, 2003, 11:39 AM
 */

package com.king.gmms.protocol.smpp;

/**
 * @author mike
 */
public class InvalidSmppTimeFormatException extends java.lang.Exception {

    /**
     * Creates a new instance of <code>InvalidSmppTimeFormatException</code> without detail message.
     */
    public InvalidSmppTimeFormatException() {
    }


    /**
     * Constructs an instance of <code>InvalidSmppTimeFormatException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public InvalidSmppTimeFormatException(String msg) {
        super(msg);
    }
}
