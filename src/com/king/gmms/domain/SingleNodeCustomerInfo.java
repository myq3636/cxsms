package com.king.gmms.domain;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class SingleNodeCustomerInfo extends A2PMultiConnectionInfo{

    private Map<String, ConnectionInfo> incomingConnectionMap = Collections.
        synchronizedMap(new HashMap<String, ConnectionInfo> ());
    private Map<String, ConnectionInfo> outgoingConnectionMap = Collections.
        synchronizedMap(new HashMap<String, ConnectionInfo> ());

    public SingleNodeCustomerInfo() {
    }

    public SingleNodeCustomerInfo(A2PSingleConnectionInfo sc){
        super(sc);
    }

    public Map<String, ConnectionInfo> getConnectionMap(boolean isServer) {
        if (isServer) {
            return incomingConnectionMap;
        }
        else {
            return outgoingConnectionMap;
        }
    }

    public void addIncomingConnection(String connName, ConnectionInfo connInfo) {
        incomingConnectionMap.put(connName, connInfo);
    }

    public void addOutgoingConnection(String connName, ConnectionInfo connInfo) {
        outgoingConnectionMap.put(connName, connInfo);
    }

    public void addConnection(Map<String, ConnectionInfo> map, boolean isServer){
        if(isServer){
            incomingConnectionMap.putAll(map);
        }else{
            outgoingConnectionMap.putAll(map);
        }
    }

    public ConnectionInfo getIncomingConnection(String connName) {
        if (incomingConnectionMap!=null &&
            incomingConnectionMap.containsKey(connName)) {
            return incomingConnectionMap.get(connName);
        }
        else {
            return null;
        }
    }

    public ConnectionInfo getOutgoingConnection(String connName) {
        if (outgoingConnectionMap!=null &&
            outgoingConnectionMap.containsKey(connName)) {
            return outgoingConnectionMap.get(connName);
        }
        else {
            return null;
        }
    }

    public void assignValue(A2PCustomerConfig cfg, Map<String, Integer> shorNameSsidMap) throws
        CustomerConfigurationException {
        super.assignValue(cfg, shorNameSsidMap);

        A2PCustomerMultiValue incomingConns = cfg.parseMultiValue("IncomingConnection");
        if(incomingConns != null){
            for(Group g : incomingConns.getAllGroups()){
                String conn_ID = g.getAttr("ConnName").getStringValue();

                ConnectionInfo connInfo = assignConnectionInfo(g);
                if(connInfo != null){
                    connInfo.setIsServer(true);
                    connInfo.setSsid(cfg.getSSID());
                    connInfo.setConnectionName(conn_ID);
                    addIncomingConnection(conn_ID, connInfo);
                }
            }
        }

        A2PCustomerMultiValue outgoingConns = cfg.parseMultiValue("OutgoingConnection");
        if(outgoingConns != null){
            for(Group g : outgoingConns.getAllGroups()){
                String conn_ID = g.getAttr("ConnName").getStringValue();

                ConnectionInfo connInfo = assignConnectionInfo(g);
                if(connInfo != null){
                    connInfo.setIsServer(false);
                    connInfo.setSsid(cfg.getSSID());
                    connInfo.setConnectionName(conn_ID);
                    if (connInfo.IsInit()) {
                        this.setInit(true);
                    }

                    addOutgoingConnection(conn_ID, connInfo);
                }
            }
        }
    }
    
    public String toString(){
		String prefix = "\r\n";
		return new StringBuffer().append(super.toString())				
		.append(prefix).append("Incoming connection:").append(this.incomingConnectionMap.values())			
		.append(prefix).append("outgoing connection:").append(this.outgoingConnectionMap.values())			
		.toString();
	}
}
