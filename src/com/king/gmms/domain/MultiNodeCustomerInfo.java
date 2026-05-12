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
public class MultiNodeCustomerInfo extends A2PMultiConnectionInfo {

    private Map<String,
                NodeInfo> nodeMap = Collections.synchronizedMap(new
        HashMap<String, NodeInfo> ());

    private String submitNodePolicy = "random";
    private String drNodePolicy = "originalway";
    private String primarySubmitNode;
    private String primaryDRNode;

    private int nodeSwitchTime = 0;
    private int nodeRecoveryTime = 900 * 1000;

    public MultiNodeCustomerInfo() {
    }
    
    public MultiNodeCustomerInfo(A2PSingleConnectionInfo sc) {
        super(sc);
    }


    public int getNodeNumber() {
        return nodeMap.size();
    }

    public void setPrimarySubmitNode(String primarySubmitNode) {
        this.primarySubmitNode = primarySubmitNode;
    }

    public void setSubmitNodePolicy(String submitNodePolicy) {
        this.submitNodePolicy = submitNodePolicy;
    }

    public void setDrNodePolicy(String drNodePolicy) {
        this.drNodePolicy = drNodePolicy;
    }

    public void setNodeSwitchTime(int nodeSwitchTime) {
        this.nodeSwitchTime = nodeSwitchTime;
    }

    public void setNodeRecoveryTime(int nodeRecoveryTime) {
        this.nodeRecoveryTime = nodeRecoveryTime;
    }

    public void setPrimaryDRNode(String primaryDRNode) {
        this.primaryDRNode = primaryDRNode;
    }

    public Map getNodeMap() {
        return nodeMap;
    }

    public String getPrimarySubmitNode() {
        return primarySubmitNode;
    }

    public String getSubmitNodePolicy() {
        return submitNodePolicy;
    }

    public String getDrNodePolicy() {
        return drNodePolicy;
    }

    public int getNodeSwitchTime() {
        return nodeSwitchTime;
    }

    public int getNodeRecoveryTime() {
        return nodeRecoveryTime;
    }

    public String getPrimaryDRNode() {
        return primaryDRNode;
    }

    public NodeInfo getNodeInfo(String nodename) {
        if (nodeMap!=null && nodeMap.containsKey(nodename)) {
            return nodeMap.get(nodename);
        }
        else {
            return null;
        }
    }

    public void putNodeInfo(String nodename, NodeInfo nodeInfo) {
        nodeMap.put(nodename, nodeInfo);
    }

    public void setNodeMap(Map nodeMap) {
        this.nodeMap = nodeMap;
    }

    public void assignValue(A2PCustomerConfig cfg, Map<String, Integer> shorNameSsidMap) throws
        CustomerConfigurationException {
        super.assignValue(cfg, shorNameSsidMap);

        A2PCustomerMultiValue submit_node_policy = cfg.parseMultiValue("SubmitPolicy_Node");
        if (submit_node_policy != null) {
            this.submitNodePolicy = submit_node_policy.getAttr("Policy").getStringValue("random");
            if("primary".equalsIgnoreCase(submitNodePolicy)){
                this.primarySubmitNode =
                    submit_node_policy.getAttr("Primary").getStringValue();
            }
        }

        A2PCustomerMultiValue dr_node_policy = cfg.parseMultiValue("DRPolicy_Node");
        if (dr_node_policy != null) {
            this.drNodePolicy = dr_node_policy.getAttr("Policy").getStringValue("originalway");
            if("primary".equalsIgnoreCase(drNodePolicy)){
                this.primaryDRNode =
                    dr_node_policy.getAttr("Primary").getStringValue();
            }
        }

        nodeSwitchTime = cfg.getInt("nodeSwitchTime", 0);
        nodeRecoveryTime = cfg.getInt("nodeRecoveryTime", 900)*1000;

        A2PCustomerMultiValue incomingConns = cfg.parseMultiValue("IncomingConnection");
        if(incomingConns != null){
            for(Group g : incomingConns.getAllGroups()){
                String nodeID = g.getAttr("NodeName").getStringValue();
                String conn_ID = g.getAttr("ConnName").getStringValue();
                NodeInfo nodeInfo = getNodeInfo(nodeID);
                if(nodeInfo == null){
                    nodeInfo = new NodeInfo();
                    nodeInfo.setNodeName(nodeID);
                    putNodeInfo(nodeID, nodeInfo);
                }

                ConnectionInfo connInfo = assignConnectionInfo(g);
                if(connInfo != null){
                    connInfo.setIsServer(true);
                    connInfo.setSsid(cfg.getSSID());
                    connInfo.setConnectionName(conn_ID);
                    nodeInfo.addIncomingConnection(conn_ID, connInfo);
                }
            }
        }

        A2PCustomerMultiValue outgoingConns = cfg.parseMultiValue("OutgoingConnection");
        if(outgoingConns != null){
            for(Group g : outgoingConns.getAllGroups()){
                String nodeID = g.getAttr("NodeName").getStringValue();
                String conn_ID = g.getAttr("ConnName").getStringValue();
                NodeInfo nodeInfo = getNodeInfo(nodeID);
                if(nodeInfo == null){
                    nodeInfo = new NodeInfo();
                    nodeInfo.setNodeName(nodeID);
                    putNodeInfo(nodeID, nodeInfo);
                }

                ConnectionInfo connInfo = assignConnectionInfo(g);
                if(connInfo != null){
                    connInfo.setIsServer(false);
                    connInfo.setSsid(cfg.getSSID());
                    connInfo.setConnectionName(conn_ID);
                    if (connInfo.IsInit()) {
                        this.setInit(true);
                    }

                    nodeInfo.addOutgoingConnection(conn_ID, connInfo);
                }
            }
        }

        A2PCustomerMultiValue nodeTypes = cfg.parseMultiValue("NodeType");
        if(nodeTypes != null){
            for(Group g : nodeTypes.getAllGroups()){
                String nodeID = g.getAttr("NodeName").getStringValue();
                String nodeType = g.getAttr("Type").getStringValue("TR");
                NodeInfo nodeInfo = getNodeInfo(nodeID);
                if(nodeInfo == null){
                    nodeInfo = new NodeInfo();
                    nodeInfo.setNodeName(nodeID);
                    putNodeInfo(nodeID, nodeInfo);
                }
                nodeInfo.setNodeType(nodeType);
            }
        }
    }
    
    public String toString(){
		String prefix = "\r\n";
		return new StringBuffer().append(super.toString())				
		.append(prefix).append("node info:").append(this.nodeMap.values())			
		.append(prefix).append("SubmitPolicy_Node:").append(this.submitNodePolicy)	
		.append(prefix).append("DRPolicy_Node:").append(this.drNodePolicy)			
		.append(prefix).append("nodeSwitchTime:").append(this.nodeSwitchTime)	
		.append(prefix).append("nodeRecoveryTime:").append(this.nodeRecoveryTime)				     
		.toString();
	}
}
