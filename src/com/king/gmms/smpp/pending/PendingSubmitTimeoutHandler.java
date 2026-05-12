package com.king.gmms.smpp.pending;

import com.king.message.gmms.GmmsMessage;

public interface PendingSubmitTimeoutHandler {
    boolean timeoutPendingSubmit(Object key, GmmsMessage msg);
}
