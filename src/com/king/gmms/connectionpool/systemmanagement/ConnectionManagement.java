package com.king.gmms.connectionpool.systemmanagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;





import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.BindMode;
import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.connection.ConnectionManagerImplWithBindMode;
import com.king.gmms.connectionpool.connection.MultiClientConnectionImpl;
import com.king.gmms.connectionpool.connection.MultiServerConnectionImpl;
import com.king.gmms.connectionpool.connection.NodeClientConnection;
import com.king.gmms.connectionpool.connection.NodeConnectionManagerWithBindMode;
import com.king.gmms.connectionpool.connection.NodeServerConnection;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.strategy.CustomerNodeStrategy;
import com.king.gmms.connectionpool.systemmanagement.strategy.CustomerStrategy;
import com.king.gmms.connectionpool.systemmanagement.strategy.SessionStrategyInterface;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.domain.MultiNodeCustomerInfo;
import com.king.gmms.domain.NodeInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.SystemSessionFactory;
import com.king.gmms.messagequeue.NodeManager;
import com.king.gmms.strategy.IndexBalanceStrategy;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.GmmsMessage;

public class ConnectionManagement {
	protected GmmsUtility gmmsUtility  = null;
	protected ArrayList<String> persistentConnProtocol = null;
	protected Map<String, ConnectionManager> ssidConnManager = null;
	protected Map<String, NodeManager> ssidNodeManager = null;
	protected Map<String,Map<Integer,ArrayList<Session>>> moduleSession = null;
	protected Map<String,SessionStrategyInterface> ssidSessionStrategy = null;
	protected A2PCustomerManager custManager = null;
	protected ModuleManager moduleManager = null;
	protected SystemSessionFactory sysSessionFactory = null;
	private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionManagement.class);
	protected String selfModule = null;
	protected ReentrantLock sessionLock = null;
	
	protected final String incoming_tag = "_IncomingConnction";
	protected final String outgoing_tag = "_OutgoingConnction";
	
	public ConnectionManagement(){
		gmmsUtility = GmmsUtility.getInstance();
		selfModule = System.getProperty("module");
		String  persistentConnProtocols = gmmsUtility.getCommonProperty("LongConnectionProtocols", "SMPP");
		persistentConnProtocol = new ArrayList<String>();
		String[] protocols = persistentConnProtocols.split(",");
		for(String protocolName: protocols){
			if(protocolName != null){
				persistentConnProtocol.add(protocolName.trim().toLowerCase());
			}
		}
		ssidConnManager = new ConcurrentHashMap<String,ConnectionManager>();
		ssidNodeManager = new ConcurrentHashMap<String,NodeManager>();
		moduleSession = new ConcurrentHashMap<String,Map<Integer,ArrayList<Session>>>();
		ssidSessionStrategy = new ConcurrentHashMap<String,SessionStrategyInterface>();
		custManager = gmmsUtility.getCustomerManager();
		moduleManager = ModuleManager.getInstance();
		sysSessionFactory = SystemSessionFactory.getInstance();
		sessionLock = new ReentrantLock();
		initConnectionManagement();
	}
	public void initConnectionManagement(){
		for(String protocolName : persistentConnProtocol){
			initConnection(protocolName);
		}
	}
	
	private boolean initConnection(String protocolName){
	   ArrayList<Integer> alSsid = custManager.getSsidByProtocol(protocolName);
	   if(alSsid==null||alSsid.isEmpty()){
		   log.warn("No Ssid to get by protocol {}!",protocolName);
		   return false;
	   }
	   initConnection(alSsid);
	   return true;
	}
	/**
	 * initConnection by ssid
	 */
	public void initConnection(ArrayList<Integer> ssidList){
		A2PCustomerInfo ci = null;
		for(int ssid:ssidList){
			 try{
				   ci = custManager.getCustomerBySSID(ssid);
			 	   if (custManager.inCurrentA2P(custManager.getConnectedRelay(ssid,GmmsMessage.AIC_MSG_TYPE_TEXT))) {
			     	   switch(ci.getConnectionType()){
			        	   case 1:
			        		   initType1OutgoingConnction((A2PSingleConnectionInfo)ci);
			        		   initType1IncomingConnction((A2PSingleConnectionInfo)ci);
			        		   break;
			        	   case 2:
			        		   initType2OutgoingConnction((SingleNodeCustomerInfo)ci);
			        		   initType2IncomingConnction((SingleNodeCustomerInfo)ci);
			        		   break;
			        	   case 3:
			        		   initType3OutgoingConnction((MultiNodeCustomerInfo) ci);
			        		   initType3IncomingConnction((MultiNodeCustomerInfo) ci);
			        		   break;
			        	   default:
			        		   break;
			     	   }
			        }
	           }catch(Exception e){
	        	   log.warn("Ignored failed connection initialization for ssid "+ssid,e);
	        	   continue;
	           }
		}
	}
	/**
     * initSingleConnectionInfo
     * @param ssid
     * @return
     */
    private boolean initType1IncomingConnction(A2PSingleConnectionInfo gmmscust){
	    	String tcpConnectionName = SystemConstants.SINGLE_CONNECTION_NAME;
		    SingleNodeCustomerInfo ci = new SingleNodeCustomerInfo(gmmscust);
		
		    ConnectionInfo connInfo = new ConnectionInfo();
		    connInfo.setConnectionName(tcpConnectionName);
		    connInfo.setSsid(gmmscust.getSSID());
		    connInfo.setIsServer(true);
		
		    String isp = (gmmscust.isPersistent() == true ? "yes" : "no");
		    connInfo.setIsPersistent(isp);
//		    int receiverNum = gmmscust.getMinReceiverNumber();
		    connInfo.setMinReceiverNum(gmmscust.getMinReceiverNumber());
		    connInfo.setMaxReceiverNum(gmmscust.getMaxReceiverNumber());
		    connInfo.setMinSenderNum(gmmscust.getMinSenderNumber());
		    connInfo.setMaxSenderNum(gmmscust.getMaxSenderNumber());
		    
		
		    int ver = gmmscust.getSMPPVersion().getVersionID();
		    String tem = Integer.toHexString(ver);
		    connInfo.setVersion(Integer.parseInt(tem));
		
	        int sessionnum = gmmscust.getSessionNumber();
	        connInfo.setSessionNum( (sessionnum == 0 ? -1 : sessionnum));
	        connInfo.setUserName(gmmscust.getSpID());
	        connInfo.setPassword(gmmscust.getAuthKey());
	
	        if (!this.checkConnectionInfo(connInfo,true)){
	        	   return false;
	        }
	        
	        ci.addIncomingConnection(tcpConnectionName, connInfo);

	        ConnectionManagerImplWithBindMode connManager = new ConnectionManagerImplWithBindMode();
	        Connection conn = new MultiServerConnectionImpl(new IndexBalanceStrategy(),true);
	        conn.initialize(connInfo);
	        connManager.insertConnection(conn);
	        ssidConnManager.put(gmmscust.getSSID() + "_" + connInfo.getConnectionName(),(ConnectionManager)connManager);
	        Map<String, ConnectionInfo> connectionMap = ci.getConnectionMap(true);
	        CustomerStrategy sessionStrategy = new CustomerStrategy(ci, connectionMap, connManager, true);
			String queue = ci.getChlQueue();
			if(queue!=null && queue.endsWith(SystemConstants.SERVER_MODULE_TYPE )){
				String sedString = ci.getSSID()+ "_" + queue;
				if(!ssidSessionStrategy.containsKey(sedString)){
					ssidSessionStrategy.put(sedString, sessionStrategy);
				}
			}
			
			queue = ci.getReceiverQueue();
			if(queue!=null && queue.endsWith(SystemConstants.SERVER_MODULE_TYPE )){
				String sedString = ci.getSSID()+ "_" + queue;
				if(!ssidSessionStrategy.containsKey(sedString)){				
					ssidSessionStrategy.put(sedString, sessionStrategy);
				}
			}
	    	return true;
    }
    /**
     * initSingleConnectionInfo
     * @param ssid
     * @return
     */
    private boolean initType1OutgoingConnction(A2PSingleConnectionInfo gmmscust){
	    	String tcpConnectionName = SystemConstants.SINGLE_CONNECTION_NAME;
		    SingleNodeCustomerInfo ci = new SingleNodeCustomerInfo(gmmscust);
		
		    ConnectionInfo connInfo = new ConnectionInfo();
		    connInfo.setConnectionName(tcpConnectionName);
		    connInfo.setSsid(gmmscust.getSSID());
		    connInfo.setIsServer(true);
		
		    String isp = (gmmscust.isPersistent() == true ? "yes" : "no");
		    connInfo.setIsPersistent(isp);
		    connInfo.setMinReceiverNum(gmmscust.getMinReceiverNumber());
		    connInfo.setMaxReceiverNum(gmmscust.getMaxReceiverNumber());
		    connInfo.setMinSenderNum(gmmscust.getMinSenderNumber());
		    connInfo.setMaxSenderNum(gmmscust.getMaxSenderNumber());
		
		    int ver = gmmscust.getSMPPVersion().getVersionID();
		    String tem = Integer.toHexString(ver);
		    connInfo.setVersion(Integer.parseInt(tem));
		
	        ci.setInit(gmmscust.isChlInit());
	        if (gmmscust.getClientSessionNumber() >= 1) {
	            connInfo.setSessionNum(gmmscust.getClientSessionNumber());
	        }
	        connInfo.setBindMode(gmmscust.getBindMode());
	        String[] chlUrl = gmmscust.getChlURL();
	        if(chlUrl==null){
	        	return false;
	        }
	        int n = 0;
	        for(String url : chlUrl){
	            String temp = tcpConnectionName + n;
	            n++;
	            connInfo.setConnectionName(temp);
	            connInfo.setURL(url);
	            if(!initConnectionInfo(connInfo,gmmscust,ci)){
	            	return false;
	            }
	        }

	        ConnectionManagerImplWithBindMode connManager = new ConnectionManagerImplWithBindMode();
	        Connection conn = new MultiClientConnectionImpl(new IndexBalanceStrategy(),true);
	        conn.initialize(connInfo);
	        connManager.insertConnection(conn);
	        ssidConnManager.put(gmmscust.getSSID() + "_" + connInfo.getConnectionName(),(ConnectionManager)connManager);
	        Map<String, ConnectionInfo> connectionMap = ci.getConnectionMap(false);
	        CustomerStrategy sessionStrategy = new CustomerStrategy(ci, connectionMap, connManager, false);
			String queue = ci.getChlQueue();
			if(queue!=null && queue.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
				String sedString = ci.getSSID()+ "_" + queue;
				if(!ssidSessionStrategy.containsKey(sedString)){
					ssidSessionStrategy.put(sedString, sessionStrategy);
				}
			}
			
			queue = ci.getReceiverQueue();
			if(queue!=null && queue.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
				String sedString = ci.getSSID()+ "_" + queue;
				if(!ssidSessionStrategy.containsKey(sedString)){				
					ssidSessionStrategy.put(sedString, sessionStrategy);
				}
			}
	    	return true;
    }
    /**
     * 
     * @param source
     * @param sc
     * @param sn
     * @return
     */
	public boolean initConnectionInfo(ConnectionInfo source,
			A2PSingleConnectionInfo sc, SingleNodeCustomerInfo sn) {
		ConnectionInfo receiver = null;
		String port = sc.getPort();
		if(port==null||"".equals(port.trim())){
			return false;
		}
		source.setPort(Integer.parseInt(port));
		source.setUserName(sc.getChlAcctName());
		source.setPassword(sc.getChlPassword());

		if (!this.checkConnectionInfo(source,false)){
			return false;
		}

		BindMode bindmode = source.getBindMode();

		switch (bindmode) {
			case Transceiver:
				break;
			case Receiver:
				if (sc.getPort2() != null) {
					source.setPort(Integer.parseInt(sc.getPort2()));
				}
	
				if (sc.getChlAcctNamer() != null) {
					source.setUserName(sc.getChlAcctNamer());
					source.setPassword(sc.getChlPasswordr());
				}
				break;
			case Transmitter:
				receiver = new ConnectionInfo(source);
				receiver.setBindMode(BindMode.Receiver);
				String name = source.getConnectionName();
				receiver.setConnectionName(name + "_R");
				if (sc.getPort2() != null) {
					receiver.setPort(Integer.parseInt(sc.getPort2()));
				}
	
				if (sc.getChlAcctNamer() != null) {
					receiver.setUserName(sc.getChlAcctNamer());
					receiver.setPassword(sc.getChlPasswordr());
				}
				break;
			default:
				break;
		}
		sn.addOutgoingConnection(source.getConnectionName(), source);
		if (receiver != null) {
			sn.addOutgoingConnection(receiver.getConnectionName(),
					receiver);
		}
		return true;
	}
    /**
     * 
     * @param info
     * @return
     */
    private boolean checkConnectionInfo(ConnectionInfo info,boolean isServer) {
        boolean result = true;
        if (isServer) {
            if (info.getUserName() == null || info.getPassword() == null) {
                result = false;
            }
        }
        else {
            if (info.getUserName() == null || info.getPassword() == null ||
                info.getPort() == -1 || info.getURL() == null) {
                return false;
            }

        }
        return result;
    }
    /**
     * initType2IncomingConnction
     * @param ci
     * @return
     */
	private boolean initType2IncomingConnction(SingleNodeCustomerInfo ci){
        Map<String, ConnectionInfo> connectionMap = ci.getConnectionMap(true);
        Iterator itConnection = connectionMap.values().iterator();
        ConnectionInfo connInfo;
        ConnectionManagerImplWithBindMode connManager = new ConnectionManagerImplWithBindMode();
        Map<String, ConnectionInfo> newConnInfoMap = new HashMap<String, ConnectionInfo>();
	    while (itConnection.hasNext()) { //while has connection
	        connInfo = (ConnectionInfo) itConnection.next();
	        Connection conn = new MultiServerConnectionImpl(new IndexBalanceStrategy(),true);
	        conn.initialize(connInfo);
	        connManager.insertConnection(conn);
	        ssidConnManager.put(ci.getSSID() + "_" + connInfo.getConnectionName(),(ConnectionManager)connManager);
	        if (connInfo.getBindMode() == BindMode.Transmitter
					&& connInfo.isCreateReviver() == true) {
				ConnectionInfo newInfo = new ConnectionInfo(connInfo);
				newInfo.setBindMode(BindMode.Receiver);
				String name = newInfo.getConnectionName();
				newInfo.setConnectionName(name + "_R");
				Connection newconn = new MultiServerConnectionImpl(new IndexBalanceStrategy(),true);
				newconn.initialize(newInfo);
		        connManager.insertConnection(newconn);
		        if (newInfo != null) {
		        	newConnInfoMap.put(newInfo.getConnectionName(), newInfo);
				}
		        ssidConnManager.put(ci.getSSID() + "_" + newInfo.getConnectionName(),(ConnectionManager)connManager);
			}
	    } 
	    if(!newConnInfoMap.isEmpty()){
	    	ci.addConnection(newConnInfoMap,true);
	    }
    	
		CustomerStrategy sessionStrategy = new CustomerStrategy(ci, connectionMap, connManager, true);
		String queue = ci.getChlQueue();
		if(queue!=null && queue.endsWith(SystemConstants.SERVER_MODULE_TYPE )){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
		
		queue = ci.getReceiverQueue();
		if(queue!=null && queue.endsWith(SystemConstants.SERVER_MODULE_TYPE )){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){				
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
	    return true;
	}
	/**
	 * initType2OutgoingConnction
	 * @param ci
	 * @return
	 */
	private boolean initType2OutgoingConnction(SingleNodeCustomerInfo ci){
		Map<String, ConnectionInfo> connectionMap = ci.getConnectionMap(false);
		Iterator itConnection = connectionMap.values().iterator();
		ConnectionInfo connInfo;
		ConnectionManagerImplWithBindMode connManager = new ConnectionManagerImplWithBindMode();
        Map<String, ConnectionInfo> newConnInfoMap = new HashMap<String, ConnectionInfo>();
		while (itConnection.hasNext()) { //while has connection
			connInfo = (ConnectionInfo) itConnection.next();
			Connection conn = new MultiClientConnectionImpl(new IndexBalanceStrategy(),true);
			conn.initialize(connInfo);
			connManager.insertConnection(conn);
			ssidConnManager.put(ci.getSSID() + "_" + connInfo.getConnectionName(),(ConnectionManager)connManager);
			if (connInfo.getBindMode() == BindMode.Transmitter
					&& connInfo.isCreateReviver() == true) {
				ConnectionInfo newConnInfo = new ConnectionInfo(connInfo);
				newConnInfo.setBindMode(BindMode.Receiver);
				String name = newConnInfo.getConnectionName();
				newConnInfo.setConnectionName(name + "_R");
				Connection newconn = new MultiClientConnectionImpl(new IndexBalanceStrategy(),true);
				newconn.initialize(newConnInfo);
		        connManager.insertConnection(newconn);
		        if (newConnInfo != null) {
		        	newConnInfoMap.put(newConnInfo.getConnectionName(), newConnInfo);
				}
		        ssidConnManager.put(ci.getSSID() + "_" + newConnInfo.getConnectionName(),(ConnectionManager)connManager);
			}
		} 
		if(!newConnInfoMap.isEmpty()){
		    	ci.addConnection(newConnInfoMap,false);
		}
		CustomerStrategy sessionStrategy = new CustomerStrategy(ci, connectionMap, connManager, false);
		String queue = ci.getChlQueue();
		if(queue!=null && queue.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
		
		queue = ci.getReceiverQueue();
		if(queue!=null && queue.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){				
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
		return true;
	}
	/**
	 * initType3IncomingConnction
	 * @param ci
	 * @return
	 */
	private boolean initType3IncomingConnction(MultiNodeCustomerInfo ci){
		NodeManager nm = new NodeManager(ci,0,0,true);
	    nm.init();
		   
		
		Map nodeMap = ci.getNodeMap();
        Iterator itNode = nodeMap.values().iterator();
        NodeInfo ni;
        Map connectionMap;
        while (itNode.hasNext()) { //while has node
            ni = (NodeInfo) itNode.next();
            //set the node into NodeConnectionManager
            Node node = nm.getNode(ni.getNodeName());
            //one node has one NodeConnectionManager
            NodeConnectionManagerWithBindMode ncm = new NodeConnectionManagerWithBindMode();
            ncm.setNode(node);
            ncm.addObserver(nm);
            
            node.init(ci, ncm);
            connectionMap = ni.getConnectionMap(true);
            Iterator itConnection = connectionMap.values().iterator();
            ConnectionInfo connInfo;
            Map<String, ConnectionInfo> newConnInfoMap = new HashMap<String, ConnectionInfo>();
            while (itConnection.hasNext()) { //while has connection
                connInfo = (ConnectionInfo) itConnection.next();
                Connection conn = new NodeServerConnection(true);
                conn.initialize(connInfo);
                //add the connection into NodeConnectionManager
                ncm.insertConnection(conn);
                ssidConnManager.put(ci.getSSID() + "_" + connInfo.getConnectionName(),(ConnectionManager)ncm);
                if (connInfo.getBindMode() == BindMode.Transmitter
    					&& connInfo.isCreateReviver() == true) {
    				ConnectionInfo newConnectionInfo = new ConnectionInfo(connInfo);
    				newConnectionInfo.setBindMode(BindMode.Receiver);
    				String name = newConnectionInfo.getConnectionName();
    				newConnectionInfo.setConnectionName(name + "_R");
    				Connection newconn = new NodeServerConnection(new IndexBalanceStrategy(),true);
    				newconn.initialize(newConnectionInfo);
    				ncm.insertConnection(newconn);
    				if (newConnectionInfo != null) {
    					newConnInfoMap.put(newConnectionInfo.getConnectionName(), newConnectionInfo);
    				}
                    ssidConnManager.put(ci.getSSID() + "_" + newConnectionInfo.getConnectionName(),(ConnectionManager)ncm);
    			}
            }
            ni.addConnection(newConnInfoMap, true);
        } //end has node
        ssidNodeManager.put(ci.getSSID()+incoming_tag, nm);
        
		CustomerNodeStrategy sessionStrategy = new CustomerNodeStrategy(ci, nm);
		String queue = ci.getChlQueue();
		if(queue!=null && queue.endsWith(SystemConstants.SERVER_MODULE_TYPE )){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
		
		queue = ci.getReceiverQueue();
		if(queue!=null && queue.endsWith(SystemConstants.SERVER_MODULE_TYPE )){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){				
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
		return true;
	}
	/**
	 * 	initType3OutgoingConnction
	 * @param ci
	 * @return
	 */
	private boolean initType3OutgoingConnction(MultiNodeCustomerInfo ci){
	   NodeManager nm = new NodeManager(ci,0,0,false);
	   nm.init();
	   
	   Map nodeMap = ci.getNodeMap();
       Iterator itNode = nodeMap.values().iterator();
       NodeInfo ni;
       Map connectionMap;
       while (itNode.hasNext()) { //while has node
           ni = (NodeInfo) itNode.next();
           //set the node into NodeConnectionManager
           Node node = nm.getNode(ni.getNodeName());
           //one node has one NodeConnectionManager
           NodeConnectionManagerWithBindMode ncm = new NodeConnectionManagerWithBindMode();
           ncm.setNode(node);
           ncm.addObserver(nm);

           node.init(ci, ncm);
           
           connectionMap = ni.getConnectionMap(false);
           Iterator itConnection = connectionMap.values().iterator();
           ConnectionInfo connInfo;
           Map<String, ConnectionInfo> newConnInfoMap = new HashMap<String, ConnectionInfo>();
           while (itConnection.hasNext()) { //while has connection
               connInfo = (ConnectionInfo) itConnection.next();
               Connection conn = new NodeClientConnection(true);
               conn.initialize(connInfo);
               //add the connection into NodeConnectionManager
               ncm.insertConnection(conn);
               ssidConnManager.put(ci.getSSID() + "_" + connInfo.getConnectionName(),(ConnectionManager)ncm);
	           if (connInfo.getBindMode() == BindMode.Transmitter
	   					&& connInfo.isCreateReviver() == true) {
	   				ConnectionInfo newConnectionInfo = new ConnectionInfo(connInfo);
	   				newConnectionInfo.setBindMode(BindMode.Receiver);
	   				String name = newConnectionInfo.getConnectionName();
	   				newConnectionInfo.setConnectionName(name + "_R");
	   				Connection newconn = new NodeServerConnection(new IndexBalanceStrategy(),true);
	   				newconn.initialize(newConnectionInfo);
	   				ncm.insertConnection(newconn);
	   				if (newConnectionInfo != null) {
	   					newConnInfoMap.put(newConnectionInfo.getConnectionName(), newConnectionInfo);
					}
	                ssidConnManager.put(ci.getSSID() + "_" + newConnectionInfo.getConnectionName(),(ConnectionManager)ncm);
	   			}
           }
           ni.addConnection(newConnInfoMap, false);
		} 
        ssidNodeManager.put(ci.getSSID() + outgoing_tag, nm);
        
		CustomerNodeStrategy sessionStrategy = new CustomerNodeStrategy(ci, nm);
		String queue = ci.getChlQueue();
		if(queue!=null && queue.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
		
		queue = ci.getReceiverQueue();
		if(queue!=null && queue.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
			String sedString = ci.getSSID()+ "_" + queue;
			if(!ssidSessionStrategy.containsKey(sedString)){				
				ssidSessionStrategy.put(sedString, sessionStrategy);
			}
		}
		
		return true;
	}
	
	public void clearConnection(ArrayList<Integer> ssidList){
		String prefix = null;
		for(int ssid:ssidList){
			prefix = ssid+"_";
			if(!ssidConnManager.isEmpty()){
				for(String key:ssidConnManager.keySet()){
					if(key.startsWith(prefix)){
						ssidConnManager.remove(key);
					}
				}
			}
			if(!ssidSessionStrategy.isEmpty()){
				for(String key:ssidSessionStrategy.keySet()){
					if(key.startsWith(prefix)){
						ssidSessionStrategy.remove(key);
					}
				}
				
			}
			if(!ssidNodeManager.isEmpty()){
				for(String key:ssidNodeManager.keySet()){
					if(key.startsWith(prefix)){
						ssidNodeManager.remove(key);
					}
				}
				
			}
			//clear session map
			Map<Integer,ArrayList<Session>> sessionMap = null;
			for(Map.Entry<String,Map<Integer,ArrayList<Session>>> entry: moduleSession.entrySet()){
				sessionMap=entry.getValue();
				if(sessionMap!=null && sessionMap.containsKey(ssid)){
					sessionMap.remove(ssid);
				}
			}
		}
	}

	/**
	 * stopSession
	 * @param custInfo
	 * @param transaction
	 * @param isServer
	 * @return
	 */
	public boolean stopSession(A2PCustomerInfo custInfo, TransactionURI transaction, boolean isServer){
		String connectionName = transaction.getConnectionName();
		String module = transaction.getModule().getModule();
		String sedString = custInfo.getSSID() + "_" + connectionName;
		ConnectionManager connManager = ssidConnManager.get(sedString);
		
		Session session = connManager.getSession(transaction);
		session.stop();
		connManager.deleteSession(connectionName, session);
		
		Map<Integer,ArrayList<Session>> sessionMap = moduleSession.get(module);
		ArrayList<Session> sessionList = null;
		if(sessionMap != null){
			sessionList = sessionMap.get(custInfo.getSSID());
			if(sessionList != null){
				sessionList.remove(session);
			}
		}
		return true;
	}
	/**
	 * moduleStop
	 * @param module
	 * @return
	 */
	public boolean moduleStop(String module){
		if(module == null){
			return false;
		}
		Map<Integer,ArrayList<Session>> sessionMap = moduleSession.get(module);
		
		Set<Map.Entry<Integer, ArrayList<Session>>> sessionMapValue = sessionMap.entrySet();
		for(Map.Entry<Integer, ArrayList<Session>> sessionEntry: sessionMapValue){
			int ssid = ((Map.Entry<Integer, ArrayList<Session>>)sessionEntry).getKey();
			ArrayList<Session> sessionList = ((Map.Entry<Integer, ArrayList<Session>>)sessionEntry).getValue();
			for(Session session : sessionList){
				if(session != null){
					TransactionURI transaction = session.getTransactionURI();
					String connectionName = transaction.getConnectionName();
					ConnectionManager connManager = ssidConnManager.get(ssid+"_"+connectionName);
					if(connManager!= null){
						session.stop();
						connManager.deleteSession(connectionName, session);
					}
				}
			}
		}
		sessionMap.clear();
		return true;
	}
	/**
	 * getSession
	 * @param msg
	 * @param ssid
	 * @param queue
	 * @return
	 */
	public Session getSession(GmmsMessage msg,int ssid, String queue){
		Session session = null;
		if(msg == null){
			return session;
		}
		String sedString = ssid+ "_" +queue;
		SessionStrategyInterface sessionStrategy = ssidSessionStrategy.get(sedString);
		if(sessionStrategy != null){
			session = sessionStrategy.getSession(msg);
		}
		return session;
	}
	
	public ArrayList<String> getPersistentConnProtocol() {
		return persistentConnProtocol;
	}
	public Map<String, ConnectionManager> getSsidConnManager() {
		return ssidConnManager;
	}
	
	/**
	 * print sessions in modules for troubleshooting
	 */
	protected void printModuleSessions() {
		try {
			if (moduleSession.size() > 0 && log.isDebugEnabled()) {
				Set<String> modules = moduleSession.keySet();
				for (String mod : modules) {
					log.debug("exist sessions in module:{}",
							mod);
					Map<Integer, ArrayList<Session>> ssidSessionMap4prt = moduleSession
							.get(mod);
					for (Map.Entry<Integer, ArrayList<Session>> ssidSessionEntry : ssidSessionMap4prt
							.entrySet()) {
						int ssid = ssidSessionEntry.getKey();
						ArrayList<Session> sessions = ssidSessionEntry
								.getValue();
						for (Session session : sessions) {
							log.debug("Session ssid:{};session:{}", ssid,
									session.getTransactionURI());
						}
					}
				}
			}
		} catch (Exception e) {
			log.info("printModuleSessions exception", e);
		}
		
	}
	
}
