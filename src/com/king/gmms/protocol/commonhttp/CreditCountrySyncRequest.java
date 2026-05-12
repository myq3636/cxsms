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

 
public class CreditCountrySyncRequest {
	
	private int ssid;
	private long timemark;
	private List<CreditCountrySub> countryCredit;
	private String tranId;
	public int getSsid() {
		return ssid;
	}
	public void setSsid(int ssid) {
		this.ssid = ssid;
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
	public List<CreditCountrySub> getCountryCredit() {
		return countryCredit;
	}
	public void setCountryCredit(List<CreditCountrySub> countryCredit) {
		this.countryCredit = countryCredit;
	}
	
	
	
	

}
