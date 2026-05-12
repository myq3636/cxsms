package com.king.gmms.protocol.commonhttp;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * 微信返回的订单实体
 * @author hxy
 *
 */

 
public class CreditSyncRequest {
	
	private int ssid;
	//客户状态 0：不可用，1：可用
	private int status;
	//客户剩余条数
	private long credit;
	private long timemark;
	private String tranId;
	public int getSsid() {
		return ssid;
	}
	public void setSsid(int ssid) {
		this.ssid = ssid;
	}
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
	public long getTimemark() {
		return timemark;
	}
	public void setTimemark(long timemark) {
		this.timemark = timemark;
	}
	public String getTranId() {
		return tranId;
	}
	public void setTranId(String tranId) {
		this.tranId = tranId;
	}	
	
	

}
