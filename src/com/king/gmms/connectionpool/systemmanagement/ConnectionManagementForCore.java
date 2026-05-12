package com.king.gmms.connectionpool.systemmanagement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.king.db.DatabaseStatus;
import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.session.CustomerSession;
import com.king.gmms.connectionpool.systemmanagement.session.NodeCustomerSession;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.pdu.ChangeDB;
import com.king.gmms.ha.systemmanagement.pdu.ChangeDBAck;
import com.king.gmms.ha.systemmanagement.pdu.ChangeRedis;
import com.king.gmms.ha.systemmanagement.pdu.ChangeRedisAck;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotification;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotificationAck;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.gmms.util.SystemConstants;

public class ConnectionManagementForCore extends ConnectionManagement implements SystemManagementInterface{
	private static ConnectionManagementForCore instance = null;
	private static SystemLogger log = SystemLogger.getSystemLogger(ConnectionManagementForCore.class);
    
	private ConnectionManagementForCore(){
		super();
		moduleManager = ModuleManager.getInstance();
		// V4.1 Stateless: Clear maps that relied on legacy TCP sync
		moduleSession.clear();
		ssidConnManager.clear();
	}
	/**
	 * singleton model
	 * @return
	 */
	public static synchronized ConnectionManagementForCore getInstance(){
		if(null == instance){
			instance = new ConnectionManagementForCore();
		}
		return instance;
	}
	/**
	 * addSession
	 * @param message
	 * @return
	 */
	private boolean addSession(SystemPdu message){
		boolean isServer = false;
		ConnectionStatusNotification notifyMsg = (ConnectionStatusNotification)message;
		String bindType = notifyMsg.getBindType();
		int ssid = notifyMsg.getSsid();
		if(SystemConstants.IN_BIND_TYPE.equalsIgnoreCase(bindType)){
			isServer = true;
		}else if(SystemConstants.OUT_BIND_TYPE.equalsIgnoreCase(bindType)){
			isServer = false;
		}
		A2PCustomerInfo custInfo = super.custManager.getCustomerBySSID(ssid);
		if(custInfo==null){
			log.warn("Get null A2PCustomerInfo with ssid={}",ssid);
			return false;
		}
		TransactionURI transURI = TransactionURI.fromString(message.getUuid());
		return this.insertSession(custInfo, transURI, isServer);
	}
	/**
	 * clearSession
	 * @param message
	 * @return
	 */
	private boolean clearSession(TransactionURI transaction,int ssid){
		boolean result = false;
		A2PCustomerInfo custInfo = super.custManager.getCustomerBySSID(ssid);
		String connectionName = transaction.getConnectionName();
		String ssid_conName = custInfo.getSSID() + "_" + connectionName;
		//clear ssidConnManager
		ConnectionManager connectionManager = super.ssidConnManager.get(ssid_conName);
		Session session = connectionManager.getSession(transaction);
		if(session==null){
			return true;
		}
		connectionManager.deleteSession(connectionName, session);
		//clear moduleSession
		sessionLock.lock();
		try{
			String module = transaction.getModule().getModule();
			Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession.get(module);
			ArrayList<Session> sessionList = ssidSessionMap.get(ssid);
			result = sessionList.remove(session);
		}finally{
			sessionLock.unlock();
		}
		return result;
	}
	/**
	 * handleConnectionConfirm
	 * @param message
	 * @return
	 */
	public SystemPdu handleConnectionConfirm(SystemPdu message){
		return null;
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
		// V4.1 Async Transformation: No longer processing session status via TCP
		ConnectionStatusNotificationAck response = new ConnectionStatusNotificationAck(message);
		response.setUuid(message.getUuid());
		response.setResponseCode(0); // Always success to appease the sender
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
		// V4.1 Stateless: Bypassing internal connection map updates
		return true;
	}
	/**
	 * clearConnnectionByModule
	 */
	private void clearConnnectionByModule(String moduleName){
			Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession.remove(moduleName);
			if(ssidSessionMap==null){
				return;
			}
			int sessionSize = 0;
			for(int ssid:ssidSessionMap.keySet()){
				ArrayList<Session> sessions = ssidSessionMap.get(ssid);
				sessionSize += sessions.size();
				for(Session session:sessions){
					TransactionURI transaction = session.getTransactionURI();
					String connectionName = transaction.getConnectionName();
					String ssid_conName = ssid + "_" + connectionName;
					ConnectionManager connManager = ssidConnManager.get(ssid_conName);
					connManager.deleteSession(connectionName, session);
				}
			}
			ssidSessionMap.clear();
			ssidSessionMap = null;
			if(log.isInfoEnabled()){
				log.info("cleared ConnnectionInfo size {} by module:{}",sessionSize,moduleName);
			}
	}
	/**
	 * handleBindRequest
	 * @param uuid
	 * @param content
	 * @return
	 */
	public SystemPdu handleInBindRequest(SystemPdu message) {
		return null;
	}
	public SystemPdu handleOutBindRequest(SystemPdu message) {
		return null;
	}
	/**
	 * handleModuleRegister
	 * @param message
	 * @return
	 */
	public SystemPdu handleModuleRegister(SystemPdu message) {
		return null;
	}
	/**
	 * handleModuleStop
	 * @param message
	 * @return
	 */
	public SystemPdu handleModuleStop(SystemPdu message) {
		return null;
	}
	/**
	 * moduleStatus changed, need notify to router modules
	 */
	public void moduleStatusNotify(SystemPdu response){
		List<SystemSession> systemSessions = sysSessionFactory.getAliveSessionsByModuleType(SystemConstants.ROUTER_MODULE_TYPE);
        ConnectionStatusNotification notifyMsg = new ConnectionStatusNotification();
        notifyMsg.setUuid(response.getUuid());
    	if(SystemPdu.COMMAND_MODULE_STOP_ACK==response.getCommandId()){
        	notifyMsg.setAction(SystemConstants.DISCONNECTED_ACTION);
    	}
    	if(systemSessions==null){
			log.debug("SystemSession List is null for router!");
    		return;
    	}
    	for(SystemSession session:systemSessions){
			try{
	    		session.start();
				Thread.sleep(10);//TODO:configure sleep time
			}catch(InterruptedException e){
				e.printStackTrace();
				continue;
			}
    		session.putMessage(notifyMsg);
    	}
	}
	/**
	 * 
	 * @param uuid
	 * @param moduleType
	 * @return
	 */
	public boolean moduleRegister(String uuid) {
		String[] uuidArr = uuid.split(SystemConstants.COLON_SEPARATOR);
		if(uuidArr.length < 2){
			return false;
		}
		String modName = uuidArr[1];
		clearConnnectionByModule(modName);
		moduleManager.updateModuleStatus2Up(modName);
		return true;
	}
	/**
	 * synConnectionInfo to router/slave MGT
	 */
	public void notifyConnectionedInfo2Module(String moduleName){
		List<SystemPdu> messageList = makeMessagesInCache();
		SystemSession moduleSysClient = sysSessionFactory.getSessionByModuleName(moduleName);
		if(moduleSysClient!=null){
			for(SystemPdu message:messageList){
				moduleSysClient.putMessage(message);
			}
		}
	}
	/**
	 * generate system message for notification
	 * @return
	 */
	private List<SystemPdu> makeMessagesInCache(){
		List<SystemPdu> messageList = new ArrayList<SystemPdu>();
		for(String moduleName:moduleSession.keySet()){
			Map<Integer, ArrayList<Session>>  ssidSessionMap = moduleSession.get(moduleName);
			for(int ssid:ssidSessionMap.keySet()){
				ArrayList<Session> sessionList = ssidSessionMap.get(ssid);
				for(Session session:sessionList){
					ConnectionStatusNotification message = new ConnectionStatusNotification();
					TransactionURI transactionURI = session.getTransactionURI();
					String UUID = transactionURI.toString();
					message.setUuid(UUID);
					message.setAction(SystemConstants.CONNECTED_ACTION);
					message.setSsid(ssid);
					messageList.add(message);
				}
			}
		}
		return messageList;
	}
	
	/**
	 * handleDBRequest
	 * @param uuid
	 */
	public SystemPdu handleDBRequest(SystemPdu message) {
		// TODO Auto-generated method stub
		return message;
	}
	/**
	 * 
	 * @param uuid
	 */
	public SystemPdu handleDBChanged(SystemPdu message) {
		ChangeDB pdu = (ChangeDB)message;
		DatabaseStatus dbStatus = DatabaseStatus.get(pdu.getAction());
		gmmsUtility.setHandover(dbStatus);
		ChangeDBAck ack = new ChangeDBAck(message);
		ack.setUuid(pdu.getUuid());
		ack.setResponseCode(0);
		return ack;
	}
	
	public boolean insertSession(A2PCustomerInfo custInfo, TransactionURI transaction, boolean isServer){
		boolean result = false;
		String connectionName = transaction.getConnectionName();
		String sedString = custInfo.getSSID() + "_" + connectionName;
		ConnectionManager connManager = ssidConnManager.get(sedString);
		if(connManager==null){
			log.warn("Can't find ConnectionManager by {}",sedString);
			return false;
		}
		Session oldSession = connManager.getSession(transaction);
		if(oldSession!=null){
			if(log.isInfoEnabled()){
				log.info("The session {} already exist!",transaction);
			}
			return true;
		}
		sessionLock.lock();
		try{
			switch(custInfo.getConnectionType()){
				case 1:
				case 2:
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
			
		}finally{
			sessionLock.unlock();
		}
		//remove log 2020.12.28
		//printModuleSessions();
		
		return result;
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
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public SystemPdu handleShutdownSessionAck(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
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
