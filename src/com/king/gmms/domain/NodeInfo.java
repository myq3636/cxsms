package com.king.gmms.domain;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.Iterator;

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
public class NodeInfo {
    public NodeInfo() {
    }

    private String nodeName;
    private String nodeType = "TR";
    private Map<String, ConnectionInfo> incomingConnectionMap = Collections.synchronizedMap(new HashMap<String,ConnectionInfo>());
    private Map<String, ConnectionInfo> outgoingConnectionMap = Collections.synchronizedMap(new HashMap<String,ConnectionInfo>());

    public String getNodeName() {
        return nodeName;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeType() {
        return nodeType;
    }

    public void setNodeType(String nodeType) {
        this.nodeType = nodeType;
    }

    public Map<String, ConnectionInfo> getConnectionMap(boolean isServer) {
        if (isServer) {
            return incomingConnectionMap;
        }else {
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
        if(incomingConnectionMap.containsKey(connName)) {
            return incomingConnectionMap.get(connName);
        }else {
            return null;
        }
    }

    public ConnectionInfo getOutgoingConnection(String connName) {
        if(outgoingConnectionMap.containsKey(connName)) {
            return outgoingConnectionMap.get(connName);
        }else {
            return null;
        }
    }

    public void print(){
        System.out.println("NodeName:" + this.nodeName + ",NodeType:" + this.nodeType);
        System.out.println("IncomingConnectionMap:");
        Iterator it = incomingConnectionMap.entrySet().iterator();
        Map.Entry entry = null;
        ConnectionInfo connInfo = null;
        while(it.hasNext()){
            entry = (Map.Entry)it.next();
            connInfo = (ConnectionInfo)entry.getValue();
            connInfo.print();
        }

        System.out.println("OutgoingConnectionMap:");
        it = outgoingConnectionMap.entrySet().iterator();
        while(it.hasNext()){
            entry = (Map.Entry)it.next();
            connInfo = (ConnectionInfo)entry.getValue();
            connInfo.print();
        }

    }
    
    public String toString(){
    	String prefix = "\r\n";
		return new StringBuffer().append("NodeName:").append(this.nodeName)
		.append(prefix).append("NodeType:").append(this.nodeType)
		.append(prefix).append("IncomingConnectionMap:").append(this.incomingConnectionMap.values())
		.append(prefix).append("OutgoingConnectionMap:").append(this.incomingConnectionMap.values())
		.toString();
    }
}

