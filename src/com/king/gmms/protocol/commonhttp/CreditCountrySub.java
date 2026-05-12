package com.king.gmms.protocol.commonhttp;

import java.util.List;


/**
 * 微信返回的订单实体
 * @author hxy
 *
 */

 
public class CreditCountrySub {
	
	private int status;
	private long credit;
	private String cc;	
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public long getCredit() {
		return credit;
	}
	public void setCredit(long credit) {
		this.credit = credit;
	}
	public String getCc() {
		return cc;
	}
	public void setCc(String cc) {
		this.cc = cc;
	}	
	
	

}
