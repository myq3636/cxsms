package com.king.gmms.connectionpool.systemmanagement;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.king.db.DatabaseStatus;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.session.CustomerSession;
import com.king.gmms.connectionpool.systemmanagement.session.NodeCustomerSession;
import com.king.gmms.customerconnectionfactory.CustomerConnectionFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PCustomerManager;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.pdu.ChangeDB;
import com.king.gmms.ha.systemmanagement.pdu.ChangeDBAck;
import com.king.gmms.ha.systemmanagement.pdu.ChangeRedis;
import com.king.gmms.ha.systemmanagement.pdu.ChangeRedisAck;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotification;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotificationAck;
import com.king.gmms.ha.systemmanagement.pdu.ShutdownSession;
import com.king.gmms.ha.systemmanagement.pdu.ShutdownSessionAck;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.gmms.util.SystemConstants;

public class ConnectionManagementForFunction implements SystemManagementInterface {
	private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionManagementForFunction.class);
	protected GmmsUtility gmmsUtility  = null;
	private CustomerConnectionFactory connectionFactory = null;
	private static ConnectionManagementForFunction instance = null;
	protected Map<String,Map<Integer,ArrayList<Session>>> moduleSession = null;
	protected ModuleManager moduleManager = null;
	protected A2PCustomerManager custManager = null;
	protected String selfModule = null;
	protected ReentrantLock sessionLock = null;

	public ConnectionManagementForFunction(){
		selfModule = System.getProperty("module");
		gmmsUtility = GmmsUtility.getInstance();
		moduleSession = new ConcurrentHashMap<String,Map<Integer,ArrayList<Session>>>();
		moduleManager = ModuleManager.getInstance();
		custManager = gmmsUtility.getCustomerManager();
		sessionLock = new ReentrantLock();
	}
	
	/**
	 * singleton model
	 * @return
	 */
	public static synchronized ConnectionManagementForFunction getInstance(){
		if(null == instance){
			instance = new ConnectionManagementForFunction();
		}
		return instance;
	}
	
	public boolean stopSession(A2PCustomerInfo custInfo, TransactionURI transaction, boolean isServer){
		String connectionName = transaction.getConnectionName();
		String module = transaction.getModule().getModule();
		ConnectionManager connManager = getConnectionManager(custInfo, connectionName);
		if(connManager == null){
			log.warn("Can't get connManager when ssid={},transaction={}",custInfo.getSSID(),transaction);
			return false;
		}
		Session session = connManager.getSession(transaction);
		if(session == null){
			log.warn("Can't get session when ssid={},transaction={}",custInfo.getSSID(),transaction);
			return true;
		}else{
			session.stop();
			connManager.deleteSession(connectionName, session);
		}
		
		sessionLock.lock();
		try{
			Map<Integer,ArrayList<Session>> sessionMap = moduleSession.get(module);
			ArrayList<Session> sessionList = null;
			if(sessionMap != null){
				sessionList = sessionMap.get(custInfo.getSSID());
				if(sessionList != null){
					sessionList.remove(session);
				}
			}
		} finally{
			sessionLock.unlock();
		}
		
		return true;
	}
	
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
					A2PCustomerInfo custInfo = custManager.getCustomerBySSID(ssid);
					ConnectionManager connManager = getConnectionManager(custInfo, connectionName);
					if(connManager!= null){
						session.stop();
						connManager.deleteSession(connectionName, session);
					}else{
						log.warn("Can't get connManager when ssid={},transaction={}",custInfo.getSSID(),transaction);
					}
				}
			}
		}
		sessionMap.clear();
		return true;
	}
	
	public boolean insertSession(A2PCustomerInfo custInfo, TransactionURI transaction, boolean isServer){
		boolean result = false;
		ConnectionManager connManager = null;
		String connectionName = transaction.getConnectionName();
		sessionLock.lock();
		
		try {
			switch(custInfo.getConnectionType()){
			case 1:
			case 2:
				connManager = connectionFactory.getConnectionManager(custInfo.getSSID(),connectionName);
				if(connManager==null){
					log.warn("Can't get connManager when ssid={},transaction={}",custInfo.getSSID(),transaction);
					return false;
				}
				CustomerSession custSession = new CustomerSession(transaction, isServer);
				custSession.setConnectionManager(connManager);
				result = connManager.insertSession(connectionName, custSession);
				if(result){
					custSession.connect();
					String module = transaction.getModule().getModule();
					Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession.get(module);
					ArrayList<Session> sessionList = null; 
					if(ssidSessionMap != null){
						sessionList = ssidSessionMap.get(custInfo.getSSID());
						if(sessionList == null){
							sessionList = new ArrayList<Session>();
							sessionList.add(custSession);
							ssidSessionMap.put(custInfo.getSSID(), sessionList);
						}else{
							sessionList.add(custSession);
						}
					}else{
						ssidSessionMap = new ConcurrentHashMap<Integer,ArrayList<Session>>();
						sessionList = new ArrayList<Session>();
						sessionList.add(custSession);
						ssidSessionMap.put(custInfo.getSSID(), sessionList);
						moduleSession.put(module, ssidSessionMap);
					}
				}
				break;
			case 3:
	         	String nodeName = custManager.getNodeIDByConnectionID(connectionName);
				connManager = connectionFactory.getConnectionManager(custInfo.getSSID(),nodeName);
				if(connManager==null){
					log.warn("Can't get connManager when ssid={},transaction={}",custInfo.getSSID(),transaction);
					return false;
				}
				NodeCustomerSession nodeCustSession = new NodeCustomerSession(custInfo,transaction,isServer);
				nodeCustSession.setConnectionManager(connManager);
				result = connManager.insertSession(connectionName, nodeCustSession);
				if(result){
					nodeCustSession.connect();
					String module = transaction.getModule().getModule();
					Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession.get(module);
					ArrayList<Session> sessionList = null; 
					if(ssidSessionMap != null){
						sessionList = ssidSessionMap.get(custInfo.getSSID());
						if(sessionList == null){
							sessionList = new ArrayList<Session>();
							sessionList.add(nodeCustSession);
							ssidSessionMap.put(custInfo.getSSID(), sessionList);
						}else{
							sessionList.add(nodeCustSession);
						}
					}else{
						ssidSessionMap = new ConcurrentHashMap<Integer,ArrayList<Session>>();
						sessionList = new ArrayList<Session>();
						sessionList.add(nodeCustSession);
						ssidSessionMap.put(custInfo.getSSID(), sessionList);
						moduleSession.put(module, ssidSessionMap);
					}
				}
				break;
			default:
				break;
		}
		} finally{
			sessionLock.unlock();
		}
		
		return result;
	}

	
	/**
	 * handle status notification
	 * syn with another system management module
	 * @param uuid
	 * @param ssid
	 * @param user
	 * @param passwd
	 * @param action
	 * @param moduleType
	 * @return
	 */
	public SystemPdu handleStatusNotification(SystemPdu message) {
		String version = this.custManager.getConfFileVersion();
		boolean verFlag = message.getConfFileVersion().equalsIgnoreCase(version);
		ConnectionStatusNotificationAck response = new ConnectionStatusNotificationAck(message);
		response.setUuid(message.getUuid());
		if(verFlag){
			boolean flag = updateConnectionInfo(message);						
			if (flag) {
				response.setResponseCode(0);
			} else {
				response.setResponseCode(1);
			}
		}else{
			response.setResponseCode(2);
			log.warn("cm.cfg didn't consistent with Function manager's,reject {}!", message);
		}
		
		return response;
	}	
	/**
	 * updateCache
	 * @param uuid
	 * @param ssid
	 * @param user
	 * @param passwd
	 * @param action
	 * @param moduleType
	 * @return
	 */
	private boolean updateConnectionInfo(SystemPdu message) {
		ConnectionStatusNotification notifyMsg = (ConnectionStatusNotification)message;
		String uuid = notifyMsg.getUuid();
		String action = notifyMsg.getAction();
		if(uuid==null){
			return false;
		}
		
		TransactionURI tranaction = TransactionURI.fromString(uuid);
		String moduleName = tranaction.getModule().getModule();
		if(tranaction.getConnectionName()==null||"".equals(tranaction.getConnectionName())){//module
			if(SystemConstants.DISCONNECTED_ACTION.equalsIgnoreCase(action)){//clear connection info of module
				this.clearConnnectionByModule(message,moduleName);
				return true;
			}else{
					log.warn("Invalid tranaction when handle StatusNotification:{}",message);
				return false;
			}
		}
		
		int ssid = notifyMsg.getSsid();
		if(ssid <= 0){
			log.warn("Invalid ssid {} when handle StatusNotification:{}", ssid,	message);
			return false;
		}
		
		//update connection info of connection
		if(SystemConstants.CONNECTED_ACTION.equalsIgnoreCase(action)){
			return this.addSession(message);
		}else if(SystemConstants.DISCONNECTED_ACTION.equalsIgnoreCase(action)){
			A2PCustomerInfo custInfo = this.custManager.getCustomerBySSID(ssid);
			boolean isServer = false;
			String bindType = notifyMsg.getBindType();
			if(SystemConstants.IN_BIND_TYPE.equalsIgnoreCase(bindType)){
				isServer = true;
			}else if(SystemConstants.OUT_BIND_TYPE.equalsIgnoreCase(bindType)){
				isServer = false;
			}
			return this.stopSession(custInfo,tranaction,isServer);
		}else{
				log.warn("unkown action when handle StatusNotification:{}",message);
		}
		return false;
	}
	/**
	 * clearConnnectionByModule
	 */
	private void clearConnnectionByModule(SystemPdu message,String moduleName){
			Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession.remove(moduleName);
			if(ssidSessionMap==null){
				return;
			}
			boolean isServer = false;
			if(SystemPdu.COMMAND_IN_BIND_REQUEST==message.getCommandId()){
				isServer = true;
			}else if(SystemPdu.COMMAND_OUT_BIND_REQUEST==message.getCommandId()){
				isServer = false;
			}
			for(int ssid:ssidSessionMap.keySet()){
				ArrayList<Session> sessions = ssidSessionMap.get(ssid);
				for(Session session:sessions){
					TransactionURI transaction = session.getTransactionURI();
					A2PCustomerInfo custInfo = this.custManager.getCustomerBySSID(ssid);
					this.stopSession(custInfo, transaction, isServer);
				}
			}
			ssidSessionMap.clear();
			ssidSessionMap = null;
	}
	/**
	 * addSession
	 * @param message
	 * @return
	 */
	private boolean addSession(SystemPdu message){
		int msgType = message.getCommandId();
		int ssid = -1;
		boolean isServer = false;
		if(SystemPdu.COMMAND_IN_BIND_REQUEST==msgType){
			isServer = true;
		}else if(SystemPdu.COMMAND_OUT_BIND_REQUEST==msgType){
			isServer = false;
		}
		ssid = ((ConnectionStatusNotification)message).getSsid();
		A2PCustomerInfo custInfo = custManager.getCustomerBySSID(ssid);
		if(custInfo==null){
			log.warn("Get null A2PCustomerInfo with ssid={}",ssid);
			return false;
		}
		TransactionURI transURI = TransactionURI.fromString(message.getUuid());
		return this.insertSession(custInfo, transURI, isServer);
	}
	
	
	
	public SystemPdu handleInBindRequest(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}
	public SystemPdu handleOutBindRequest(SystemPdu message) {
		return null;
	}
	
	public SystemPdu handleConnectionConfirm(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public SystemPdu handleDBChanged(SystemPdu message) {
		String moduleType = moduleManager.getFullModuleType(selfModule);
		if(!SystemConstants.MQM_MODULE_TYPE.equalsIgnoreCase(moduleType)){
			return null;
		}
		ChangeDB pdu = (ChangeDB)message;
		DatabaseStatus dbStatus = DatabaseStatus.get(pdu.getAction());
		this.gmmsUtility.setHandover(dbStatus);
		ChangeDBAck ack = new ChangeDBAck(message);
		ack.setUuid(pdu.getUuid());
		ack.setResponseCode(0);
		return ack;
	}

	
	public SystemPdu handleDBRequest(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public SystemPdu handleModuleRegister(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	
	public SystemPdu handleModuleStop(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	public CustomerConnectionFactory getConnectionFactory() {
		return connectionFactory;
	}

	public void setConnectionFactory(CustomerConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@Override
	public SystemPdu handleRedisChanged(SystemPdu message) {
		ChangeRedis pdu = (ChangeRedis)message;
		String redisStatus = pdu.getAction();
		gmmsUtility.setRedisClientFlag(redisStatus);
		ChangeRedisAck ack = new ChangeRedisAck(message);
		ack.setUuid(pdu.getUuid());
		ack.setResponseCode(0);
		return ack;
	}

	@Override
	public SystemPdu handleShutdownSession(SystemPdu message) {
		ShutdownSession pdu = (ShutdownSession)message;
		int ssid = pdu.getSsid();
		String connectionName = pdu.getConnectionName();
		ShutdownSessionAck pduAck = new  ShutdownSessionAck(pdu);
		pduAck.setUuid(pdu.getUuid());
		A2PCustomerInfo custInfo = custManager.getCustomerBySSID(ssid);
		ConnectionManager connectionManager = getConnectionManager(custInfo, connectionName);
		if(connectionManager==null){
			pduAck.setResponseCode(1);
		}else{
			int totalSessionNum = connectionManager.getSessionNum();
			int sessionSize = pdu.getSessionNum();
			if(sessionSize<=totalSessionNum){
				for(int i=0; i < sessionSize/2; i++){
					Session s = connectionManager.getSession();
					if(s!=null){
						s.stop();
					}
				}
				pduAck.setResponseCode(0);
			}else{
				pduAck.setResponseCode(1);
			}
		}
		return pduAck;
	}

	@Override
	public SystemPdu handleShutdownSessionAck(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
	public ConnectionManager getConnectionManager(A2PCustomerInfo custInfo, String  connectionName){
		if(connectionName == null || custInfo == null){
			return null;
		}
		ConnectionManager connManager = null;
		switch(custInfo.getConnectionType()){
			case 1:
			case 2:
				connManager = connectionFactory.getConnectionManager(custInfo.getSSID(),connectionName);
				break;
			case 3:
				String nodeName = custManager.getNodeIDByConnectionID(connectionName);
				connManager = connectionFactory.getConnectionManager(custInfo.getSSID(),nodeName);
				break;
		}
		return connManager;
	}

	/** 
	 * @param message
	 * @return
	 * @see com.king.gmms.connectionpool.systemmanagement.SystemManagementInterface#handleReportInMsgCount(com.king.gmms.ha.systemmanagement.pdu.SystemPdu)
	 */
	@Override
	public SystemPdu handleReportInMsgCount(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	/** 
	 * @param message
	 * @return
	 * @see com.king.gmms.connectionpool.systemmanagement.SystemManagementInterface#handleApplyInThrottleQuota(com.king.gmms.ha.systemmanagement.pdu.SystemPdu)
	 */
	@Override
	public SystemPdu handleApplyInThrottleQuota(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	/** 
	 * @param message
	 * @see com.king.gmms.connectionpool.systemmanagement.SystemManagementInterface#handleApplyInThrottleQuotaAck(com.king.gmms.ha.systemmanagement.pdu.SystemPdu)
	 */
	@Override
	public void handleApplyInThrottleQuotaAck(SystemPdu message) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public SystemPdu handleQueryHttpRequest(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SystemPdu handleQueryHttpAck(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void handleConnectionHttpConfirm(SystemPdu message) {
		// TODO Auto-generated method stub
		
	}
}
