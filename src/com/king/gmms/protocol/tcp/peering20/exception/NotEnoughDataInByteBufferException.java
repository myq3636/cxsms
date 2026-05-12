package com.king.gmms.protocol.tcp.peering20.exception;


public class NotEnoughDataInByteBufferException extends Exception {
    protected int available;
    protected int expected;
    /**
     * NotEnoughDataInByteBufferException constructor comment.
     */
    public NotEnoughDataInByteBufferException(int p_available, int p_expected) {
        super(
                "Not enough data in byte buffer. "
                + "Expected "
                + p_expected
                + ", available: "
                + p_available
                + ".");
        available = p_available;
        expected = p_expected;
    }

    /**
     * NotEnoughDataInByteBufferException constructor comment.
     * @param s java.lang.String
     */
    public NotEnoughDataInByteBufferException(String s) {
        super(s);
    }
}
