package com.king.gmms.domain;

public class EnumberDTO {

   private String country;
   private String operator;
   private String city;
   private String queryMethod;
   private String queryTime;
   private String number;
   private String type;
   private String mccmnc;
   private int prefix;
   private int opId;
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getOperator() {
		return operator;
	}
	public void setOperator(String operator) {
		this.operator = operator;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getQueryMethod() {
		return queryMethod;
	}
	public void setQueryMethod(String queryMethod) {
		this.queryMethod = queryMethod;
	}
	public String getQueryTime() {
		return queryTime;
	}
	public void setQueryTime(String queryTime) {
		this.queryTime = queryTime;
	}
	public String getNumber() {
		return number;
	}
	public void setNumber(String number) {
		this.number = number;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getMccmnc() {
		return mccmnc;
	}
	public void setMccmnc(String mccmnc) {
		this.mccmnc = mccmnc;
	}
	public int getOpId() {
		return opId;
	}
	public void setOpId(int opId) {
		this.opId = opId;
	}
	
	public int getPrefix() {
		return prefix;
	}
	public void setPrefix(int prefix) {
		this.prefix = prefix;
	}
	@Override
	public String toString() {
		return "EnumberDTO [country=" + country + ", operator=" + operator + ", city=" + city + ", queryMethod="
				+ queryMethod + ", queryTime=" + queryTime + ", number=" + number + ", type=" + type + ", mccmnc="
				+ mccmnc + ", prefix=" + prefix + ", opId=" + opId + "]";
	}
	
   
   
}
