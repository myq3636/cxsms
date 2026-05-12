package com.king.gmms.customerconnectionfactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.BindMode;
import com.king.gmms.connectionpool.connection.AbstractMultiConnection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForFunction;
import com.king.gmms.domain.*;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.messagequeue.OperatorMessageQueue;
import com.king.gmms.util.SystemConstants;


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
public abstract class AbstractConnectionFactory implements CustomerConnectionFactory {

    protected GmmsUtility gmmsUtility = null;
    protected ConcurrentHashMap<Integer, OperatorMessageQueue> ssid2messageQueues;
    protected A2PCustomerManager cim;
    protected boolean isServer;
    protected ConcurrentHashMap<String, ConnectionManager> ssid2connectionManagers;
    protected String moduleName;
    private static SystemLogger log = SystemLogger.getSystemLogger(AbstractConnectionFactory.class);
    protected ConnectionManagementForFunction systemManager = null;
    protected boolean isEnableMgt = false;
    
    public AbstractConnectionFactory() {
        gmmsUtility = GmmsUtility.getInstance();
        ssid2messageQueues = new ConcurrentHashMap<Integer, OperatorMessageQueue> ();
        ssid2connectionManagers = new ConcurrentHashMap<String, ConnectionManager> ();
        cim = gmmsUtility.getCustomerManager();
        moduleName = System.getProperty("module");
        isEnableMgt = gmmsUtility.isSystemManageEnable();
        if(isEnableMgt){
	        systemManager = ConnectionManagementForFunction.getInstance();
	        systemManager.setConnectionFactory(this);
        }
    }
    
    public void putConnectionManager(int ssid, String name,
            ConnectionManager ncm) {
    	ssid2connectionManagers.put(ssid + "_" + name, ncm);
	}
    public void clearConnectionManager(int ssid, String name) {
    	ssid2connectionManagers.remove(ssid + "_" + name);
	}
	/**
	 * getConnectionManager for server
	 * @param ssid
	 * @param name
	 * @return
	 */
	public ConnectionManager getConnectionManager(int ssid, String name) {
		ConnectionManager manager = null;
		if (ssid2connectionManagers.containsKey(ssid + "_" + name)) {
			manager = ssid2connectionManagers.get(ssid + "_" + name);
		}else{
			Set<String> cnameSet = ssid2connectionManagers.keySet();
    		if(log.isDebugEnabled()){
    			log.debug("ssid2connectionManagers with ssid:{}",ssid);
    		}	
			
			/*
			 * for(String cname:cnameSet){ if(log.isDebugEnabled()){
			 * log.debug("ssid2connectionManagers key:{}",cname); }
			 * 
			 * }
			 */
		}
		return manager;
	}
	/**
	 * getConnectionManager for client
	 * @param ssid
	 * @return
	 */
	public ConnectionManager getConnectionManagerBySSID(int ssid) {
		ConnectionManager manager = null;
		String prefix = ssid+"_";
		Set<String> keySet = ssid2connectionManagers.keySet();
		for(String key:keySet){
			if(key.startsWith(prefix)){
				manager = ssid2connectionManagers.get(key);
				break;
			}
    		if(log.isDebugEnabled()){
    			log.debug("getConnectionManagerBySSID key:{}, prefix={}",key,prefix);
    		}	
			
		}
		return manager;
	}
	/**
	 * 
	 * @param ssid
	 * @param queue
	 */
    protected void setOperatorMessageQueue(int ssid,OperatorMessageQueue queue){
        removeOperatorMessageQueue(ssid);
        ssid2messageQueues.put(ssid,queue);
    }
   /**
    * 
    * @param ssid
    */
    protected void removeOperatorMessageQueue(int ssid){
        if (ssid2messageQueues.containsKey(ssid)) {
        	OperatorMessageQueue messageQueue = ssid2messageQueues.remove(ssid);
        	messageQueue.stopMessageQueue();
        }
    }
    /**
     * 
     * @param ssid
     * @return
     */
    public OperatorMessageQueue getOperatorMessageQueue(int ssid){
    	if(ssid2messageQueues.containsKey(ssid)){
    		return ssid2messageQueues.get(ssid);
    	}
    	return null;
    }

    
    /**
     * 
     * @param info
     * @return
     */
    protected boolean checkConnectionInfo(ConnectionInfo info) {
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
     * 
     * @param source
     * @param sc
     * @param sn
     * @return
     */
	public boolean initConnectionInfo(ConnectionInfo source,
			A2PSingleConnectionInfo sc, SingleNodeCustomerInfo sn) {
		ConnectionInfo transmitter = null;
		source.setPort(Integer.parseInt(sc.getPort()));
		source.setUserName(sc.getChlAcctName());
		source.setPassword(sc.getChlPassword());

		if (!this.checkConnectionInfo(source)){
			if (log.isInfoEnabled()) {
        		log.info("checkConnectionInfo failed, " + source);
        	}
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
				transmitter = new ConnectionInfo(source);
				transmitter.setBindMode(BindMode.Transmitter);
				String name = source.getConnectionName();
				transmitter.setConnectionName(name);
				source.setConnectionName(name + "_R");
				source.setBindMode(BindMode.Receiver);
				if (sc.getPort2() != null) {
					source.setPort(Integer.parseInt(sc.getPort2()));
				}
	
				if (sc.getChlAcctNamer() != null) {
					source.setUserName(sc.getChlAcctNamer());
					source.setPassword(sc.getChlPasswordr());
				}
				break;
			default:
				break;
		}
		sn.addOutgoingConnection(source.getConnectionName(), source);
		if (transmitter != null) {
			sn.addOutgoingConnection(transmitter.getConnectionName(),
					transmitter);
		}
		return true;
	}
	/**
	 * deleteConnection
	 * @param ssid
	 * @param connectionName
	 */
	protected void deleteConnection(A2PCustomerInfo cust,ConnectionInfo connInfo){
		int ssid = cust.getSSID();
		String connectionName = connInfo.getConnectionName();
		ConnectionManager connMng = this.getConnectionManager(ssid, connectionName);
		if(connMng==null){
			log.warn("No ConnectionManager for ssid {},connectionName {}",ssid,connectionName);
			return;
		}
		destroySession(connMng,connectionName);
		if (connInfo.getBindMode() == BindMode.Transmitter
				&& connInfo.isCreateReviver() == true) {
			destroySession(connMng,connectionName+"_R");
		}
        this.clearConnectionManager(ssid, connectionName);
	}
	/**
	 * destroySession
	 * @param connMng
	 * @param connectionName
	 */
	private void destroySession(ConnectionManager connMng,String connectionName){
		AbstractMultiConnection connection = (AbstractMultiConnection)connMng.getConnection(connectionName);
		if(connection!=null){
			Map<TransactionURI, Session> sessionMap = connection.getTransactionURI2Connections();
			if(sessionMap==null || sessionMap.isEmpty()){
				log.warn("No sessions of connectionName {}",connectionName);
				return;
			}
			ArrayList<Session> sessionList = new ArrayList<Session>(sessionMap.values());//avoid ModifiedException
			for(Session session:sessionList){
				session.destroy();
			}
		}
	}
	/**
	 * deleteConnection
	 * @param ssid
	 * @param nodeName
	 */
	protected void deleteConnection4Node(int ssid,String nodeName,ConnectionInfo connInfo){
		String connectionName = connInfo.getConnectionName();
		ConnectionManager connMng = this.getConnectionManager(ssid, nodeName);
		if(connMng==null){
			log.warn("No ConnectionManager for ssid {},nodeName {}",ssid,nodeName);
			return;
		}
		destroySession(connMng,connectionName);
		if (connInfo.getBindMode() == BindMode.Transmitter
				&& connInfo.isCreateReviver() == true) {
			destroySession(connMng,connectionName+"_R");
		}
	}
	/**
     * initSingleConnectionInfo
     * @param ssid
     * @return
     */
    public boolean initSingleConnectionInfo(int ssid){
	    	boolean result = false;
	    	String tcpConnectionName = SystemConstants.SINGLE_CONNECTION_NAME;
	    	A2PSingleConnectionInfo gmmscust = (A2PSingleConnectionInfo)
	        gmmsUtility.
	        getCustomerManager().getCustomerBySSID(ssid);
		    SingleNodeCustomerInfo tempCustomerInfo = new
		        SingleNodeCustomerInfo(gmmscust);
		
		    ConnectionInfo connInfo = new ConnectionInfo();
		    connInfo.setConnectionName(tcpConnectionName);
		    connInfo.setSsid(ssid);
		    connInfo.setIsServer(isServer);
		
		    String isp = (gmmscust.isPersistent() == true ? "yes" : "no");
		    connInfo.setIsPersistent(isp);
		    connInfo.setMinReceiverNum(gmmscust.getMinReceiverNumber());
		    connInfo.setMaxReceiverNum(gmmscust.getMaxReceiverNumber());
		    connInfo.setMinSenderNum(gmmscust.getMinSenderNumber());
		    connInfo.setMaxSenderNum(gmmscust.getMaxSenderNumber());
		
		    int ver = gmmscust.getSMPPVersion().getVersionID();
		    String tem = Integer.toHexString(ver);
		    connInfo.setVersion(Integer.parseInt(tem));
		
		    if (isServer) {
		        int sessionnum = gmmscust.getSessionNumber();
		        connInfo.setSessionNum( (sessionnum == 0 ? -1 : sessionnum));
		        connInfo.setUserName(gmmscust.getSpID());
		        connInfo.setPassword(gmmscust.getAuthKey());
		
		        if (!this.checkConnectionInfo(connInfo)) {
		        	if (log.isInfoEnabled()) {
		        		log.info("checkConnectionInfo failed, " + connInfo);
		        	}
		            return false;
		        }
		
		        tempCustomerInfo.addIncomingConnection(tcpConnectionName, connInfo);
		        gmmsUtility.getCustomerManager().addServerInfoMap(connInfo);
		        result = true;
		    }
		    else {
		        tempCustomerInfo.setInit(gmmscust.isChlInit());
		        if (gmmscust.getClientSessionNumber() >= 1) {
		            connInfo.setSessionNum(gmmscust.getClientSessionNumber());
		        }
		        connInfo.setBindMode(gmmscust.getBindMode());
		        String[] chlUrl = gmmscust.getChlURL();
		        if(chlUrl==null){
		        	if (log.isInfoEnabled()) {
		        		log.info("{} chlUrl is null", connInfo.getSsid());
		        	}
		        	return false;
		        }
		        int n = 0;
		        for(String url : chlUrl){
		            String temp = tcpConnectionName + n;
		            n++;
		            connInfo.setConnectionName(temp);
		            connInfo.setURL(url);
		            if(initConnectionInfo(connInfo,gmmscust,tempCustomerInfo)){
		            	result = true;
		            }
		                
		        }
		        if (!result){
		        	return result;
		        }
		    }
		    gmmsUtility.getCustomerManager().addCustomerInfoBySsid(ssid,tempCustomerInfo);
	    	return result;
	    }
    /**
     * initSingleConnectionInfo
     * @param ssid
     * @return
     */
    public boolean clearSingleConnectionInfo(A2PCustomerInfo cust){
    		int ssid = cust.getSSID();
    		cust = gmmsUtility.getCustomerManager().getCustomerBySSID(ssid);//SingleNodeCustomerInfo
    		boolean result = false;
    		if(cust instanceof A2PSingleConnectionInfo){
    			SingleNodeCustomerInfo tempCustomerInfo = new SingleNodeCustomerInfo((A2PSingleConnectionInfo)cust);
    			result = clearMultiConnectionInfo(tempCustomerInfo);
    		}else{
    			result = clearMultiConnectionInfo(cust);
    		}
		    gmmsUtility.getCustomerManager().clearCustomerInfoBySsid(ssid);
	    	return result;
    }
    /**
     * initMultiConnectionInfo
     * @param ssid
     * @return
     */
	public boolean initMultiConnectionInfo(int ssid) {
		try {
			A2PCustomerInfo mCustomerInfo = (SingleNodeCustomerInfo) this.gmmsUtility
					.getCustomerManager().getCustomerBySSID(ssid);

			Map<String, ConnectionInfo> connectionMap = ((SingleNodeCustomerInfo) mCustomerInfo)
					.getConnectionMap(isServer);
			Iterator itConnection = connectionMap.values().iterator();
			Map<String, ConnectionInfo> newConnInfo = null;
			ConnectionInfo connInfo;
			while (itConnection.hasNext()) { // while has connection
				connInfo = (ConnectionInfo) itConnection.next();
				if (connInfo.getBindMode() == BindMode.Transmitter
						&& connInfo.isCreateReviver() == true) {
					if (newConnInfo == null) {
						newConnInfo = new HashMap<String, ConnectionInfo>();
					}

					ConnectionInfo newConn = new ConnectionInfo(connInfo);
					newConn.setBindMode(BindMode.Receiver);
					String name = newConn.getConnectionName();
					newConn.setConnectionName(name + "_R");
					newConnInfo.put(newConn.getConnectionName(), newConn);
					if (isServer) {
						gmmsUtility.getCustomerManager().addServerInfoMap(
								newConn);
					}
				}
			}

			if (newConnInfo != null) {
				((SingleNodeCustomerInfo) mCustomerInfo).addConnection(
						newConnInfo, this.isServer);
			}
		} catch (Exception ex) {
			log.warn(ex, ex);

		}
		return true;
	}
	 /**
     * initMultiConnectionInfo
     * @param ssid
     * @return
     */
	public boolean clearMultiConnectionInfo(A2PCustomerInfo cust) {
		try {
			int ssid = cust.getSSID();
			A2PCustomerInfo mCustomerInfo = (SingleNodeCustomerInfo) cust;

			Map<String, ConnectionInfo> connectionMap = ((SingleNodeCustomerInfo) mCustomerInfo)
					.getConnectionMap(isServer);
			Iterator itConnection = connectionMap.values().iterator();
			ArrayList<String> name2remove = new ArrayList<String>();
			ConnectionInfo connInfo;
			while (itConnection.hasNext()) { // while has connection
				connInfo = (ConnectionInfo) itConnection.next();
				String connectionName = connInfo.getConnectionName();
				if (connInfo.getBindMode() != BindMode.Receiver) {
					this.deleteConnection(mCustomerInfo, connInfo);
				}
				name2remove.add(connectionName);
				if (isServer) {
					gmmsUtility.getCustomerManager().clearServerInfoMap(connInfo);
				}
			}
			for(String name:name2remove){
				connectionMap.remove(name);
			}
			this.removeOperatorMessageQueue(ssid);
		} catch (Exception ex) {
			log.warn(ex, ex);

		}
		return true;
	}
	/**
	 * initMultiNodeConnection
	 * 
	 * @param ssid
	 */
	protected void clearMultiNodeConnection(A2PCustomerInfo cust) {
		int ssid = cust.getSSID();
		MultiNodeCustomerInfo ci = (MultiNodeCustomerInfo)cust;
		Map nodeMap = ci.getNodeMap();
		Iterator itNode = nodeMap.values().iterator();
		NodeInfo ni;
		Map connectionMap;
		ArrayList<String> name2remove = null;
		while (itNode.hasNext()) { // while has node
			ni = (NodeInfo) itNode.next();
			connectionMap = ni.getConnectionMap(isServer);
			name2remove = new ArrayList<String>();
			Iterator itConnection = connectionMap.values().iterator();
			ConnectionInfo connInfo;
			while (itConnection.hasNext()) { // while has connection
				connInfo = (ConnectionInfo) itConnection.next();
				name2remove.add(connInfo.getConnectionName());
				if (connInfo.getBindMode() != BindMode.Receiver) {
					deleteConnection4Node(ssid,ni.getNodeName(),connInfo);
				}
				gmmsUtility.getCustomerManager().clearConnNodeMapping(connInfo.getConnectionName());
				gmmsUtility.getCustomerManager().clearConnIPMapping(connInfo.getConnectionName());
				if (isServer) {
					gmmsUtility.getCustomerManager().clearServerInfoMap(connInfo);
				}
			} // end has connection
			clearConnectionManager(ssid, ni.getNodeName());
			for(String name:name2remove){
				connectionMap.remove(name);
			}
		} // end has node
		this.removeOperatorMessageQueue(ssid);
	}
	public abstract void initConnectionFactory(int ssid, int type);

	public ConnectionManagementForFunction getSystemManager() {
		return systemManager;
	}

}
