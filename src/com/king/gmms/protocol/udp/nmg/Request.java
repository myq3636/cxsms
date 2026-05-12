/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public abstract class Request extends Pdu {
    /**
     * Request constructor comment.
     * @param version int
     */
    public Request() {
    }

    public boolean canRespond() {
        return true;
    }

    protected abstract Respond createResponse();

    public Respond getRespond() {
        Respond response = createResponse();
        return response;
    }

    public boolean isRequest() {
        return true;
    }

}
