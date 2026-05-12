package com.king.rest.util;

import java.util.Date;

public class SmsRequest {
	
	private String username;
	private String password;
	private String recipient;
	private String sender;
	private String charset;
	private String content;
	private Date expiredDate;
	private int deliveryReport;
	private Date timestamp;
	private byte[] udh;
	
	private byte[]binaryContent;
	private int serviceTypeID;
	private Date scheduleDeliveryTime;
		
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getRecipient() {
		return recipient;
	}
	public void setRecipient(String recipient) {
		this.recipient = recipient;
	}
	public String getSender() {
		return sender;
	}
	public void setSender(String sender) {
		this.sender = sender;
	}
	public String getCharset() {
		return charset;
	}
	public void setCharset(String charset) {
		this.charset = charset;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public Date getExpiredDate() {
		return expiredDate;
	}
	public void setExpiredDate(Date expiredDate) {
		this.expiredDate = expiredDate;
	}
	public int getDeliveryReport() {
		return deliveryReport;
	}
	public void setDeliveryReport(int deliveryReport) {
		this.deliveryReport = deliveryReport;
	}
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}
	public byte[] getUdh() {
		return udh;
	}
	public void setUdh(byte[] udh) {
		this.udh = udh;
	}
	public byte[] getBinaryContent() {
		return binaryContent;
	}
	public void setBinaryContent(byte[] binaryContent) {
		this.binaryContent = binaryContent;
	}
	public int getServiceTypeID() {
		return serviceTypeID;
	}
	public void setServiceTypeID(int serviceTypeID) {
		this.serviceTypeID = serviceTypeID;
	}
	public Date getScheduleDeliveryTime() {
		return scheduleDeliveryTime;
	}
	public void setScheduleDeliveryTime(Date scheduleDeliveryTime) {
		this.scheduleDeliveryTime = scheduleDeliveryTime;
	}

	
	
	


}
