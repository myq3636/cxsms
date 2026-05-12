/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.gmms.domain;

/**
 * Info of RentAddrCondition
 * @author bensonchen
 * @version 1.0.0
 */
public class RentAddrCondition implements Comparable<RentAddrCondition> {
	private String rAddrPrefix = null;
	/**
	 * partner short name
	 */
	private String partner = null;
	
	public String getrAddrPrefix() {
		return rAddrPrefix;
	}
	public void setrAddrPrefix(String rAddrPrefix) {
		this.rAddrPrefix = rAddrPrefix;
	}
	public void setPartner(String partner) {
		this.partner = partner;
	}
	public String getPartner() {
		return partner;
	}
	
	/** 
	 * for reverse sort
	 * @param o
	 * @return
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(RentAddrCondition other) {
		// reverse sort
		if (this.rAddrPrefix.compareTo(other.getrAddrPrefix())>0) {
			return -1;
		}
		if (this.rAddrPrefix.compareTo(other.getrAddrPrefix())<0) {
			return 1;
		}
		return 0;
	}
	
	public String toString() {
		return new StringBuffer().append("{RAddrPrefix:").append(this.rAddrPrefix)
		                        .append(",").append("Partner:").append(this.partner+"}")
		                        .toString();		
	}

}
