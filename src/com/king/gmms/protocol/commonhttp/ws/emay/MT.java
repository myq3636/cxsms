package com.king.gmms.protocol.commonhttp.ws.emay;

import java.lang.reflect.Method;

import com.king.framework.SystemLogger;

public class MT {
	private static SystemLogger log = SystemLogger.getSystemLogger(MT.class);
	String softwareSerialNo = "";
	String key = "";
	String[] mobiles;
	String smsContent = "";
	long smsID ;
	String sendTime = "";
	String srcCharset = "GBK";
	String addSerial = "";
	int smsPriority = 5;
	
	public String getSoftwareSerialNo() {
		return softwareSerialNo;
	}
	
	public void setSoftwareSerialNo(String softwareSerialNo) {
		this.softwareSerialNo = softwareSerialNo;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String[] getMobiles() {
		return mobiles;
	}
	public void setMobiles(String[] mobiles) {
		this.mobiles = mobiles;
	}
	public String getSmsContent() {
		return smsContent;
	}
	public void setSmsContent(String smsContent) {
		this.smsContent = smsContent;
	}
	public String getAddSerial() {
		return addSerial;
	}
	public void setAddSerial(String addSerial) {
		this.addSerial = addSerial;
	}
	public long getSmsID() {
		return smsID;
	}
	public void setSmsID(long smsID) {
		this.smsID = smsID;
	}
	public String getSendTime() {
		return sendTime;
	}
	public void setSendTime(String sendTime) {
		this.sendTime = sendTime;
	}
	public String getSrcCharset() {
		return srcCharset;
	}
	public void setSrcCharset(String srcCharset) {
		this.srcCharset = srcCharset;
	}
	public int getSmsPriority() {
		return smsPriority;
	}
	public void setSmsPriority(int smsPriority) {
		this.smsPriority = smsPriority;
	}
	
	@SuppressWarnings("unchecked")
	public void setProperty(Object instance,String propertyName,Object value){
    	Class clazz = instance.getClass();
		Method[] methods = clazz.getMethods();
		
		try {
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("set" + propertyName)) {
					f.invoke(instance,value);
				}
			}
		} catch (Exception e) {
			log.warn("set properyt " + propertyName + " error!");
		}
	}
	
}
