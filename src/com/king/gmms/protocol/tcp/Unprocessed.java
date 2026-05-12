package com.king.gmms.protocol.tcp;

import com.king.gmms.protocol.tcp.ByteBuffer;
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
public class Unprocessed {
    public Unprocessed() {
    }

    /**
     * Buffer for data which aren't complete PDU yet. Each time new data
     * are received, they are appended to this buffer and PDU is created
     * from this buffer.
     *
     * @see #hasUnprocessed
     * @see com.logica.smpp.ReceiverBase#receivePDUFromConnection(Connection,Unprocessed)
     */
    private ByteBuffer unprocessed = new ByteBuffer();

    /**
     * Indicates that after creating PDU from <code>unprocessed</code> buffer
     * there were still some data left in the <code>unprocessed</code> buffer.
     * In the next receive even if no new data will be received an attempt
     * to create PDU from <code>unprocessed</code> buffer will be performed.
     *
     * @see #unprocessed
     * @see com.logica.smpp.ReceiverBase#receivePDUFromConnection(Connection,Unprocessed)
     */
    private boolean hasUnprocessed = false;

    /**
     * Contains the time when some data were received from connection.
     * If it is currently longer from <code>lastTimeReceived</code>
     * than specified by <code>receiveTimeout</code>,
     * <code>TimeoutException</code> is thrown.
     *
     * @see com.logica.smpp.ReceiverBase#receiveTimeout
     * @see TimeoutException
     * @see com.logica.smpp.ReceiverBase#receivePDUFromConnection(Connection,Unprocessed)
     */
    private long lastTimeReceived = 0;

    /**
     * Resets flag <code>hasUnprocessed</code>, removes all bytes
     * from <code>unprocessed</code> buffer and sets <code>expected</code>
     * to zero.
     *
     * @see #hasUnprocessed
     * @see #unprocessed
     * @see #expected
     */
    public void reset() {
        hasUnprocessed = false;
        unprocessed.setBuffer(null);
    }

    /**
     * Sets flag <code>hasUnprocessed</code> if there are any
     * unprocessed bytes in <code>unprocessed</code> buffer.
     *
     * @see #hasUnprocessed
     * @see #unprocessed
     */
    public void check() {
        hasUnprocessed = unprocessed.length() > 0;
    }

    public void setHasUnprocessed(boolean value) {
        hasUnprocessed = value;
    }

    public void setLastTimeReceived(long value) {
        lastTimeReceived = value;
    }

    public void setLastTimeReceived() {
        lastTimeReceived = System.currentTimeMillis();
    }

    public ByteBuffer getUnprocessed() {
        return unprocessed;
    }

    public boolean getHasUnprocessed() {
        return hasUnprocessed;
    }

    public long getLastTimeReceived() {
        return lastTimeReceived;
    }

}
