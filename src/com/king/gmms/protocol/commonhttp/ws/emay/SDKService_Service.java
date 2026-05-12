/**
 * SDKService_Service.java
 *
 * This file was auto-generated from WSDL
 * by the Apache Axis 1.4 Apr 22, 2006 (06:55:48 PDT) WSDL2Java emitter.
 */

package com.king.gmms.protocol.commonhttp.ws.emay;

public interface SDKService_Service extends javax.xml.rpc.Service {
    public java.lang.String getSDKServiceAddress();

    public com.king.gmms.protocol.commonhttp.ws.emay.SDKClient getSDKService() throws javax.xml.rpc.ServiceException;

    public com.king.gmms.protocol.commonhttp.ws.emay.SDKClient getSDKService(java.net.URL portAddress) throws javax.xml.rpc.ServiceException;
}
