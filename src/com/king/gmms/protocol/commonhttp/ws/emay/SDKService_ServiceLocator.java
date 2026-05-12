/**
 * SDKService_ServiceLocator.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.king.gmms.protocol.commonhttp.ws.emay;

import javax.xml.rpc.ServiceException;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;

public class SDKService_ServiceLocator extends org.apache.axis.client.Service implements com.king.gmms.protocol.commonhttp.ws.emay.SDKService_Service {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static SystemLogger log = SystemLogger.getSystemLogger(SDKService_ServiceLocator.class);
	public SDKService_ServiceLocator() {
		
    }


    public SDKService_ServiceLocator(org.apache.axis.EngineConfiguration config) {
        super(config);
    }

    public SDKService_ServiceLocator(java.lang.String wsdlLoc, javax.xml.namespace.QName sName) throws javax.xml.rpc.ServiceException {
        super(wsdlLoc, sName);
    }

    // Use to get a proxy class for SDKService
//    private java.lang.String SDKService_address = "http://116.58.219.223:8080/sdk/SDKService";
    private java.lang.String SDKService_address = "http://sdk4report.eucp.b2m.cn:8080/sdk/SDKService";

    public java.lang.String getSDKServiceAddress() {
        return SDKService_address;
    }

    // The WSDD service name defaults to the port name.
    private java.lang.String SDKServiceWSDDServiceName = "SDKService";

    public java.lang.String getSDKServiceWSDDServiceName() {
        return SDKServiceWSDDServiceName;
    }

    public void setSDKServiceWSDDServiceName(java.lang.String name) {
        SDKServiceWSDDServiceName = name;
    }

    public com.king.gmms.protocol.commonhttp.ws.emay.SDKClient getSDKService() throws javax.xml.rpc.ServiceException {
       java.net.URL endpoint;
        try {
        	if(SDKService_address == null || "".equals(SDKService_address)){
        		SDKService_address = "http://sdk4report.eucp.b2m.cn:8080/sdk/SDKService";
        	}
            endpoint = new java.net.URL(SDKService_address);
        }
        catch (java.net.MalformedURLException e) {
            throw new javax.xml.rpc.ServiceException(e);
        }
        return getSDKService(endpoint);
    }

    public SDKServiceBindingStub initSDKClient(A2PCustomerInfo cst) throws javax.xml.rpc.ServiceException{
    	SDKServiceBindingStub sdkClient = null;
    	A2PSingleConnectionInfo singleInfo = (A2PSingleConnectionInfo)cst;
			this.setSDKServiceEndpointAddress(singleInfo.getChlURL()[0]);
			try {
				sdkClient = (SDKServiceBindingStub)this.getSDKService();
			} catch (ServiceException e) {
				log.error("construct SDKServiceBindingStub instance failed, the exception is:"+e);
				 throw new javax.xml.rpc.ServiceException(e);
			}
			return sdkClient;
	  }
    
    public com.king.gmms.protocol.commonhttp.ws.emay.SDKClient getSDKService(java.net.URL portAddress) throws javax.xml.rpc.ServiceException {
        try {
            com.king.gmms.protocol.commonhttp.ws.emay.SDKServiceBindingStub _stub = new com.king.gmms.protocol.commonhttp.ws.emay.SDKServiceBindingStub(portAddress, this);
            _stub.setPortName(getSDKServiceWSDDServiceName());
            return _stub;
        }
        catch (org.apache.axis.AxisFault e) {
            return null;
        }
    }

    public void setSDKServiceEndpointAddress(java.lang.String address) {
        SDKService_address = address;
    }

   
    public javax.xml.namespace.QName getServiceName() {
        return new javax.xml.namespace.QName("http://sdkhttp.eucp.b2m.cn/", "SDKService");
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(java.lang.String portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        
if ("SDKService".equals(portName)) {
            setSDKServiceEndpointAddress(address);
        }
        else 
{ // Unknown Port Name
            throw new javax.xml.rpc.ServiceException(" Cannot set Endpoint Address for Unknown Port" + portName);
        }
    }

    /**
    * Set the endpoint address for the specified port name.
    */
    public void setEndpointAddress(javax.xml.namespace.QName portName, java.lang.String address) throws javax.xml.rpc.ServiceException {
        setEndpointAddress(portName.getLocalPart(), address);
    }

}
