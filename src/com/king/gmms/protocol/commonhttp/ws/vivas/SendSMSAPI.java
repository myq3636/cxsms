package com.king.gmms.protocol.commonhttp.ws.vivas;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for sendSMSAPI complex type.
 * 
 * <p>
 * The following schema fragment specifies the expected content contained within
 * this class.
 * 
 * <pre>
 * &lt;complexType name="sendSMSAPI">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="arg0" type="{http://www.w3.org/2001/XMLSchema}long"/>
 *         &lt;element name="arg1" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="arg2" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="arg3" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="arg4" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="arg5" type="{http://www.w3.org/2001/XMLSchema}string" minOccurs="0"/>
 *         &lt;element name="arg6" type="{http://www.w3.org/2001/XMLSchema}byte" minOccurs="0"/>
 *         &lt;element name="arg7" type="{http://ws.smsb.vivas.vn/}msgObjectRequest" maxOccurs="unbounded" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "sendSMSAPI", propOrder = { "arg0", "arg1", "arg2", "arg3",
		"arg4", "arg5", "arg6", "arg7" })
public class SendSMSAPI {

	protected String arg0;
	protected String arg1;
	protected String arg2;
	protected String arg3;
	protected String arg4;
	protected String arg5;
	protected String arg6;
	protected MsgObjectRequest arg7 = new MsgObjectRequest();

	/**
	 * Gets the value of the arg0 property.
	 * 
	 */
	public String getArg0() {
		return arg0;
	}

	/**
	 * Sets the value of the arg0 property.
	 * 
	 */
	public void setArg0(String value) {
		this.arg0 = value;
	}

	/**
	 * Gets the value of the arg1 property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getArg1() {
		return arg1;
	}

	/**
	 * Sets the value of the arg1 property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setArg1(String value) {
		this.arg1 = value;
	}

	/**
	 * Gets the value of the arg2 property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getArg2() {
		return arg2;
	}

	/**
	 * Sets the value of the arg2 property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setArg2(String value) {
		this.arg2 = value;
	}

	/**
	 * Gets the value of the arg3 property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getArg3() {
		return arg3;
	}

	/**
	 * Sets the value of the arg3 property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setArg3(String value) {
		this.arg3 = value;
	}

	/**
	 * Gets the value of the arg4 property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getArg4() {
		return arg4;
	}

	/**
	 * Sets the value of the arg4 property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setArg4(String value) {
		this.arg4 = value;
	}

	/**
	 * Gets the value of the arg5 property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getArg5() {
		return arg5;
	}

	/**
	 * Sets the value of the arg5 property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setArg5(String value) {
		this.arg5 = value;
	}

	/**
	 * Gets the value of the arg6 property.
	 * 
	 * @return possible object is {@link Byte }
	 * 
	 */
	public String getArg6() {
		return arg6;
	}

	/**
	 * Sets the value of the arg6 property.
	 * 
	 * @param value
	 *            allowed object is {@link Byte }
	 * 
	 */
	public void setArg6(String value) {
		this.arg6 = value;
	}

	/**
	 * Gets the value of the arg7 property.
	 * 
	 * <p>
	 * This accessor method returns a reference to the live list, not a
	 * snapshot. Therefore any modification you make to the returned list will
	 * be present inside the JAXB object. This is why there is not a
	 * <CODE>set</CODE> method for the arg7 property.
	 * 
	 * <p>
	 * For example, to add a new item, do as follows:
	 * 
	 * <pre>
	 * getArg7().add(newItem);
	 * </pre>
	 * 
	 * 
	 * <p>
	 * Objects of the following type(s) are allowed in the list
	 * {@link MsgObjectRequest }
	 * 
	 * 
	 */
	public MsgObjectRequest getArg7() {
		if (arg7 == null) {
			arg7 = new MsgObjectRequest();
		}
		return this.arg7;
	}
	/**
	 * Gets the value of the checksum property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getChecksum() {
		return arg7.getChecksum();
	}

	/**
	 * Sets the value of the checksum property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setChecksum(String value) {
		arg7.setChecksum(value);
	}

	/**
	 * Gets the value of the msgid property.
	 * 
	 */
	public String getMsgid() {
		return arg7.getMsgid();
	}

	/**
	 * Sets the value of the msgid property.
	 * 
	 */
	public void setMsgid(String value) {
		arg7.setMsgid(value);
	}

	/**
	 * Gets the value of the msisdn property.
	 * 
	 * @return possible object is {@link String }
	 * 
	 */
	public String getMsisdn() {
		return arg7.getMsisdn();
	}

	/**
	 * Sets the value of the msisdn property.
	 * 
	 * @param value
	 *            allowed object is {@link String }
	 * 
	 */
	public void setMsisdn(String value) {
		arg7.setMsisdn(value);
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
