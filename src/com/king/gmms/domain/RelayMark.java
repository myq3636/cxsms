/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.domain;

/**
 * RelayMark for routing info
 * @author bensonchen
 * @version 1.0.0
 */
public class RelayMark implements Comparable<RelayMark> {
	private int relay = 0;
	private String senderAddr;
	private String contentTplID;
	public int getRelay() {
		return relay;
	}
	public void setRelay(int relay) {
		this.relay = relay;
	}
	public String getSenderAddr() {
		return senderAddr;
	}
	public void setSenderAddr(String senderAddr) {
		this.senderAddr = senderAddr;
	}
	public String getContentTplID() {
		return contentTplID;
	}
	public void setContentTplID(String contentTpl) {
		this.contentTplID = contentTpl;
	}
	/** 
	 * for reverse sort
	 * @param o
	 * @return
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(RelayMark other) {
		// reverse sort
		if (this.senderAddr.compareTo(other.getSenderAddr())>0) {
			return -1;
		}
		if (this.senderAddr.compareTo(other.getSenderAddr())<0) {
			return 1;
		}
		return 0;
	}
}
		
