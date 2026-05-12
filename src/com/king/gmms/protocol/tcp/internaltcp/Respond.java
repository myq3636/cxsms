package com.king.gmms.protocol.tcp.internaltcp;

public abstract class Respond extends Pdu {

    public Respond() {

    }

    public boolean canRespond() { return false; }

    public boolean isRequest() {
        return false;
    }

}
