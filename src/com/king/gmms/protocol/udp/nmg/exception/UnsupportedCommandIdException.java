package com.king.gmms.protocol.udp.nmg.exception;

import com.king.gmms.protocol.udp.nmg.PduHeader;

public class UnsupportedCommandIdException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	PduHeader header = null;

	/**
	 * UnsupportedCommandIdException constructor comment.
	 */
	public UnsupportedCommandIdException() {
		super();
	}

	/**
	 * UnsupportedCommandIdException constructor comment.
	 * 
	 * @param s java.lang.String
	 */
	public UnsupportedCommandIdException(PduHeader newPduHeader) {
		header = newPduHeader;
	}

	/**
	 * Insert the method's description here. Creation date: (1/6/2003 4:49:52 PM)
	 * 
	 * @return com.king.gmms.protocol.cmpp.pdu.PduHeader
	 */
	public PduHeader getHeader() {
		return header;
	}

	/**
	 * Insert the method's description here. Creation date: (1/6/2003 4:49:52 PM)
	 * 
	 * @param newHeader com.king.gmms.protocol.cmpp.pdu.PduHeader
	 */
	public void setHeader(PduHeader newHeader) {
		header = newHeader;
	}
}
