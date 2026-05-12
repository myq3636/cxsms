package com.king.gmms.protocol.commonhttp;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * 微信返回的订单实体
 * @author hxy
 *
 */

@XmlRootElement(name="AUTHENTICATION")  
@XmlAccessorType(XmlAccessType.FIELD)  
public class JxToken {
	@JSONField(name = "PRODUCTTOKEN")
	@XmlElement(required = true)
	private String PRODUCTTOKEN;

	public String getPRODUCTTOKEN() {
		return PRODUCTTOKEN;
	}

	public void setPRODUCTTOKEN(String pRODUCTTOKEN) {
		PRODUCTTOKEN = pRODUCTTOKEN;
	}
	

}
