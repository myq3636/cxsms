/*
 * InvalidDeliveryReceiptFormatException.java
 *
 * Created on February 13, 2003, 4:21 PM
 */

package com.king.gmms.protocol.smpp;

/**
 * @author mike
 */
public class InvalidDeliveryReceiptException extends
                                             com.king.message.gmms.InvalidMessageException {
    /**
     * Creates a new instance of <code>InvalidDeliveryReceiptFormatException</code> without detail message.
     */
    public InvalidDeliveryReceiptException() {
    }

    /**
     * Constructs an instance of <code>InvalidDeliveryReceiptFormatException</code> with the specified detail message.
     *
     * @param msg the detail message.
     */
    public InvalidDeliveryReceiptException(String msg) {
        super(msg);
    }

    public InvalidDeliveryReceiptException(Throwable cause) {
        super(cause);
    }
}
