package com.king.gmms.smpp.pending;

import com.king.message.gmms.GmmsMessage;

public interface PendingDrTimeoutHandler {
    boolean timeoutPendingDr(Object key, GmmsMessage msg);
}
