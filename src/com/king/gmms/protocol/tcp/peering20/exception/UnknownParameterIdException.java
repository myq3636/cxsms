package com.king.gmms.protocol.tcp.peering20.exception;


public class UnknownParameterIdException extends Exception {
    int parameterId = 0;

    public UnknownParameterIdException() {
        super();
    }


    public UnknownParameterIdException(int parameterId) {
        this.parameterId = parameterId;
    }

}
