package com.king.gmms.protocol.commonhttp;

import java.lang.reflect.Method;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.king.message.gmms.GmmsMessage;

/**
 * 微信返回的订单实体
 * @author hxy
 *
 */

@XmlRootElement(name="MtMessage")  
@XmlAccessorType(XmlAccessType.FIELD)  
public class YueFanMessage {

	@XmlElement(required = true)
	private String phoneNumber;
	@XmlElement
	private String content;
	@XmlElement
	private String sendTime;
	@XmlElement
	private String smsId;
	@XmlElement
	private String subCode;
	
	
	
	public String getPhoneNumber() {
		return phoneNumber;
	}



	public void setPhoneNumber(String phoneNumber) {
		this.phoneNumber = phoneNumber;
	}



	public String getContent() {
		return content;
	}



	public void setContent(String content) {
		this.content = content;
	}



	public String getSendTime() {
		return sendTime;
	}



	public void setSendTime(String sendTime) {
		this.sendTime = sendTime;
	}



	public String getSmsId() {
		return smsId;
	}



	public void setSmsId(String smsId) {
		this.smsId = smsId;
	}



	public String getSubCode() {
		return subCode;
	}



	public void setSubCode(String subCode) {
		this.subCode = subCode;
	}



	public boolean setProperty(String propertyName, Object value) {
		boolean bret = false;
		Method[] methods = YueFanMessage.class.getMethods();
		for (Method f : methods) {
			if (f.getName().equalsIgnoreCase("set" + propertyName)) {
				try {
					f.invoke(this, value);
					bret = true;
					break;
				} catch (Exception e) {
					System.out.println("set properyt " + propertyName + " error:" + e.getMessage());
					continue;
				}
				
			}
		}
		return bret;
	}
	
	

}
