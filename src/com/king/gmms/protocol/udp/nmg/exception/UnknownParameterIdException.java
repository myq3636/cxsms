package com.king.gmms.protocol.udp.nmg.exception;

public class UnknownParameterIdException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int parameterId = 0;

	public UnknownParameterIdException() {
		super();
	}

	public UnknownParameterIdException(int parameterId) {
		this.parameterId = parameterId;
	}

}
