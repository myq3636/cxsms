package com.king.gmms.protocol.commonhttp.ws.vivas;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for sendSMSAPIResponse complex type.
 * 
 * <p>
 * The following schema fragment specifies the expected content contained within
 * this class.
 * 
 * <pre>
 * &lt;complexType name="sendSMSAPIResponse">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="return" type="{http://ws.smsb.vivas.vn/}responeObject" minOccurs="0"/>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "sendSMSAPIResponse", propOrder = { "_return" })
public class SendSMSAPIResponse {

	@XmlElement(name = "return")
	protected ResponeObject _return;

	/**
	 * Gets the value of the return property.
	 * 
	 * @return possible object is {@link ResponeObject }
	 * 
	 */
	public ResponeObject getReturn() {
		return _return;
	}

	/**
	 * Sets the value of the return property.
	 * 
	 * @param value
	 *            allowed object is {@link ResponeObject }
	 * 
	 */
	public void setReturn(ResponeObject value) {
		this._return = value;
	}

}
