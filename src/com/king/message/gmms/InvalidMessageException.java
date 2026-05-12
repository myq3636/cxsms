package com.king.message.gmms;

public class InvalidMessageException
        extends Exception {
    private GmmsStatus status = null;

    public InvalidMessageException() {
    }

    public InvalidMessageException(GmmsStatus status, String message) {
        super(message);
        this.status = status;
    }

    public InvalidMessageException(GmmsStatus status) {
        super(status.getText());
        this.status = status;
    }

    public InvalidMessageException(String msg) {
        super(msg);
    }

    public InvalidMessageException(Throwable cause) {
        super(cause);
    }

    public GmmsStatus getError() {
        return this.status;
    }

}
