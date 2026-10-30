package com.king.gmms.smpp.pending;

import com.king.message.gmms.GmmsMessage;

public interface PendingDrTimeoutHandler {
    boolean timeoutPendingDr(Object key, GmmsMessage msg);

    /** Write a DR's result (retried after a failed write); the session also acks its DR stream entry. */
    default boolean produceDrResult(Object key, GmmsMessage msg) {
        return com.king.gmms.messagequeue.StreamQueueManager.getInstance().produceResult(msg);
    }
}
