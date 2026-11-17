package com.king.gmms.connectionpool.session;

public enum SmppDrSubmitResult {
    SENT,
    WINDOW_FULL,
    /** DRStreamConsumer: the session's DR window is full before the send, nothing sent */
    WINDOW_FULL_PRECHECK,
    FAILED
}
