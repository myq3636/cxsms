/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.protocol.udp.nmg;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public abstract class Respond extends Pdu {

    public Respond() {
    }

    public boolean canRespond() { 
    	return false; 
    }

    public boolean isRequest() {
        return false;
    }

}
