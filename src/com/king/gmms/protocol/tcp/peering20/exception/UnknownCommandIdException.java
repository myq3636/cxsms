package com.king.gmms.protocol.tcp.peering20.exception;

/**
 * Insert the type's description here.
 * Creation date: (1/6/2003 4:18:29 PM)
 * @author: Administrator
 */
public class UnknownCommandIdException extends Exception {
        long commandId=0L;
/**
 * UnknownCommandIdException constructor comment.
 */
public UnknownCommandIdException() {
        super();
}
/**
 * UnknownCommandIdException constructor comment.
 * @param s java.lang.String
 */
public UnknownCommandIdException(long newCommandId) {
        commandId=newCommandId;
}
}
