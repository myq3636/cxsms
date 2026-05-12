package com.king.mgt.util;
/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class NotEnoughDataInByteBufferException extends Exception {
    private int available;
    private int expected;

    public NotEnoughDataInByteBufferException(int p_available, int p_expected) {
        super("Not enough data in byte buffer. " +
              "Expected " + p_expected +
              ", available: " + p_available + ".");
        available = p_available;
        expected = p_expected;
    }

    public NotEnoughDataInByteBufferException(String s) {
        super(s);
        available = 0;
        expected = 0;
    }

    public int getAvailable() {
        return available;
    }

    public int getExpected() {
        return expected;
    }

}
