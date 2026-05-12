package com.king.gmms.protocol.commonhttp.ws.vivas;

import java.lang.reflect.Method;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for msgObjectRequest complex type.
 * 
 * <p>
 * The following schema fragment specifies the expected content contained within
 * this class.
 * 
 * <pre>
 * &lt;complexType name="msgObjectRequest">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="checksum" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="msgid" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *         &lt;element name="msisdn" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "msgObjectRequest", propOrder = { "checksum", "msgid", "msisdn" })
public class MsgObjectRequest {

	protected String checksum;
	protected String msgid;
	protected String msisdn;

	/**
	 * Gets the value of the checksum property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getChecksum() {
		return checksum;
	}

	/**
	 * Sets the value of the checksum property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setChecksum(String value) {
		this.checksum = value;
	}

	/**
	 * Gets the value of the msgid property.
	 * 
	 */
	public String getMsgid() {
		return msgid;
	}

	/**
	 * Sets the value of the msgid property.
	 * 
	 */
	public void setMsgid(String value) {
		this.msgid = value;
	}

	/**
	 * Gets the value of the msisdn property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getMsisdn() {
		return msisdn;
	}

	/**
	 * Sets the value of the msisdn property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setMsisdn(String value) {
		this.msisdn = value;
	}
	/**
     * 
     * @param clazz
     * @param instance
     * @param propertyName
     * @return
     */
    public Object getProperty(Object instance,String propertyName){
    	Class clazz = instance.getClass();
		Method[] methods = clazz.getMethods();
		Object obj = null;
		try {
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("get" + propertyName)) {
					obj =f.invoke(instance);
				}
			}
		} catch (Exception e) {
			System.out.println("get properyt " + propertyName + " error!");
		}
		return obj;
	}
    /**
     * 
     * @param clazz
     * @param instance
     * @param propertyName
     * @param value
     */
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
			System.out.println("set properyt " + propertyName + " error!");
		}
	}
}
