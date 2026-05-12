package com.king.gmms.protocol.commonhttp.ws.vivas;

import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;

import com.king.framework.SystemLogger;
import com.king.gmms.protocol.commonhttp.ws.vivas.MsgObjectRequest;
import com.king.gmms.protocol.commonhttp.ws.vivas.ResponeObject;
import com.king.gmms.protocol.commonhttp.ws.vivas.SendSMSDelegate;
import com.king.gmms.protocol.commonhttp.ws.vivas.SendSMSService;
import com.sun.xml.internal.ws.client.BindingProviderProperties;

public class VivasClient {
    private static SystemLogger log = SystemLogger.getSystemLogger(VivasClient.class);
	private URL wsurl = null;
	private QName serviceName = null;
	public VivasClient(String chlUrl){
		serviceName = new QName("http://ws.smsb.vivas.vn/", "SendSMSService");
		try {
			URL baseUrl;
			baseUrl = com.king.gmms.protocol.commonhttp.ws.vivas.SendSMSService.class.getResource(".");
			if(chlUrl==null||chlUrl.isEmpty()){
				wsurl = new URL(baseUrl,
				"http://123.30.23.179:8080/SMS_BN_Core_Server/SendSMSPort?wsdl");
			}else{
				wsurl = new URL(baseUrl,chlUrl);
			}
		} catch (MalformedURLException e) {
			log.warn("Failed to create URL for the wsdl Location: "+chlUrl+", retrying as a local file",e);
		}
	}
	public ResponeObject verifySMSAPI(String requestId, String userName, String password){
		ResponeObject  resObj = new ResponeObject();
		SendSMSService sms = new SendSMSService(wsurl,serviceName);
		SendSMSDelegate smsDelegate = sms.getSendSMSPort();
		
		resObj = smsDelegate.verifySMSAPI(requestId, userName, password);
		return resObj;
	}
	public ResponeObject sendSMSAPI(String requestId, String userName, String password, String brandName, 
			String textMsg,String sendTime, String type, MsgObjectRequest msgObj){
		ResponeObject  resObj = new ResponeObject();
		SendSMSService sms = new SendSMSService(wsurl,serviceName);
		SendSMSDelegate smsDelegate = sms.getSendSMSPort();
		
		resObj = smsDelegate.sendSMSAPI(requestId,userName,password,brandName,textMsg,sendTime,type,msgObj);
		return resObj;
		
	}
}
