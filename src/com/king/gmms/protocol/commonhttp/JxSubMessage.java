package com.king.gmms.protocol.commonhttp;

import java.lang.reflect.Method;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.alibaba.fastjson.annotation.JSONField;
import com.king.message.gmms.GmmsMessage;

/**
 * 微信返回的订单实体
 * @author hxy
 *
 */

@XmlRootElement(name="MSG")  
@XmlAccessorType(XmlAccessType.FIELD)  
public class JxSubMessage {
	
	@XmlElement(required = true)
	@JSONField(name = "FROM")
	private String FROM;
	@XmlElement(required = true)
	@JSONField(name = "TO")
	private String TO;
	@XmlElement
	@JSONField(name = "BODY")
	private String BODY;
	@XmlElement
	@JSONField(name = "REFERENCE")
	private String REFERENCE;
	@XmlElement
	@JSONField(name = "DCS")
	private String DCS;
	@XmlElement
	@JSONField(name = "MINIMUMNUMBEROFMESSAGEPARTS")
	private String MINIMUMNUMBEROFMESSAGEPARTS;
	@XmlElement
	@JSONField(name = "MAXIMUMNUMBEROFMESSAGEPARTS")
	private String MAXIMUMNUMBEROFMESSAGEPARTS;
	@XmlElement
	@JSONField(name = "VALIDITY")
	private String VALIDITY;
	@XmlElement
	@JSONField(name = "RSSID")
	private String RSSID;
	
	public String getFROM() {
		return FROM;
	}
	public void setFROM(String fROM) {
		FROM = fROM;
	}
	public String getTO() {
		return TO;
	}
	public void setTO(String tO) {
		TO = tO;
	}
	public String getBODY() {
		return BODY;
	}
	public void setBODY(String bODY) {
		BODY = bODY;
	}
	public String getREFERENCE() {
		return REFERENCE;
	}
	public void setREFERENCE(String rEFERENCE) {
		REFERENCE = rEFERENCE;
	}
	public String getDCS() {
		return DCS;
	}
	public void setDCS(String dCS) {
		DCS = dCS;
	}
	public String getMINIMUMNUMBEROFMESSAGEPARTS() {
		return MINIMUMNUMBEROFMESSAGEPARTS;
	}
	public void setMINIMUMNUMBEROFMESSAGEPARTS(String mINIMUMNUMBEROFMESSAGEPARTS) {
		MINIMUMNUMBEROFMESSAGEPARTS = mINIMUMNUMBEROFMESSAGEPARTS;
	}
	public String getMAXIMUMNUMBEROFMESSAGEPARTS() {
		return MAXIMUMNUMBEROFMESSAGEPARTS;
	}
	public void setMAXIMUMNUMBEROFMESSAGEPARTS(String mAXIMUMNUMBEROFMESSAGEPARTS) {
		MAXIMUMNUMBEROFMESSAGEPARTS = mAXIMUMNUMBEROFMESSAGEPARTS;
	}
	public String getVALIDITY() {
		return VALIDITY;
	}
	public void setVALIDITY(String vALIDITY) {
		VALIDITY = vALIDITY;
	}
	
	
	
	public String getRSSID() {
		return RSSID;
	}
	public void setRSSID(String rSSID) {
		RSSID = rSSID;
	}
	public boolean setProperty(String propertyName, Object value) {
		boolean bret = false;
		Method[] methods = JxSubMessage.class.getMethods();
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
