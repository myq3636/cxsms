package com.king.gmms.protocol.tcp.internaltcp.exception;


public class UnknownParameterIdException extends Exception {
    int parameterId = 0;

    public UnknownParameterIdException() {
        super();
    }


    public UnknownParameterIdException(int parameterId) {
        this.parameterId = parameterId;
    }

}
