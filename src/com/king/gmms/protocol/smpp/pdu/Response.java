/*
 * Copyright (c) 1996-2001
 * Logica Mobile Networks Limited
 * All rights reserved.
 *
 * This software is distributed under Logica Open Source License Version 1.0
 * ("Licence Agreement"). You shall use it and distribute only in accordance
 * with the terms of the License Agreement.
 *
 */
package com.king.gmms.protocol.smpp.pdu;

/**
 * Represents a PDU response. All classes which are used as SMPP response are
 * derived from this class.
 *
 * @author Logica Mobile Networks SMPP Open Source Team
 * @version 1.1, 9 Oct 2001
 */

/*
  02-10-01 ticp@logica.com comments added, indentation changed -> spaces
  09-10-01 ticp@logica.com added registration of original request if the response
                           was created from Request by getResponse
*/

public abstract class Response extends PDU {
    /**
     * The original request which this response relates to.
     *
     * @see Request#getResponse()
     */
    private Request originalRequest = null;

    /**
     * Create a request PDU with default parameters.
     */
    public Response() {
    }

    /**
     * Create response PDU with given command id. Derived classes usually uses
     * <code>super(THE_COMMAND_ID)</code> where the <code>THE_COMMAND_ID</code>
     * is the command id of the PDU the derived class represents.
     *
     * @param commandId int
     */
    public Response(int commandId) {
        super(commandId);
    }

    /**
     * Returns false as there can be no response to a response.
     *
     * @return boolean
     * @see PDU#canResponse()
     */
    public boolean canResponse() {
        return false;
    }

    /**
     * Returns false.
     *
     * @return boolean
     * @see PDU#isRequest()
     */
    public boolean isRequest() {
        return false;
    }

    /**
     * Returns true.
     *
     * @return boolean
     * @see PDU#isResponse()
     */
    public boolean isResponse() {
        return true;
    }

    /**
     * Sets the original <code>Request</code> which this <code>Response</code>
     * was created from.
     *
     * @param originalRequest Request
     * @see Request#getResponse()
     */
    public void setOriginalRequest(Request originalRequest) {
        this.originalRequest = originalRequest;
    }

    /**
     * Returns the original <code>Request</code> which this
     * <code>Response</code> was created from.
     *
     * @return Request
     * @see Request#getResponse()
     */
    public Request getOriginalRequest() {
        return originalRequest;
    }

}
