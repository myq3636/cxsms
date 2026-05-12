package com.king.gmms.protocol.tcp.peering20.exception;

import com.king.gmms.protocol.tcp.peering20.*;

public class UnsupportedCommandIdException extends Exception {
        PduHeader header=null;
/**
 * UnsupportedCommandIdException constructor comment.
 */
public UnsupportedCommandIdException() {
        super();
}
/**
 * UnsupportedCommandIdException constructor comment.
 * @param s java.lang.String
 */
public UnsupportedCommandIdException(PduHeader newPduHeader) {
        header=newPduHeader;
}
/**
 * Insert the method's description here.
 * Creation date: (1/6/2003 4:49:52 PM)
 * @return com.king.gmms.protocol.cmpp.pdu.PduHeader
 */
public PduHeader getHeader() {
        return header;
}
/**
 * Insert the method's description here.
 * Creation date: (1/6/2003 4:49:52 PM)
 * @param newHeader com.king.gmms.protocol.cmpp.pdu.PduHeader
 */
public void setHeader(PduHeader newHeader) {
        header = newHeader;
}
}
