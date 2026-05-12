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

@XmlRootElement(name="MESSAGES")  
@XmlAccessorType(XmlAccessType.FIELD)  
public class JxMessage {
	@JSONField(name = "AUTHENTICATION")
	@XmlElement(required = true)
	private JxToken AUTHENTICATION;
	@XmlElement(required = true)
	@JSONField(name = "MSG")
	private List<JxSubMessage> MSG;
	public JxToken getAUTHENTICATION() {
		return AUTHENTICATION;
	}
	public void setAUTHENTICATION(JxToken aUTHENTICATION) {
		AUTHENTICATION = aUTHENTICATION;
	}
	public List<JxSubMessage> getMSG() {
		return MSG;
	}
	public void setMSG(List<JxSubMessage> mSG) {
		MSG = mSG;
	}
	
	

}
