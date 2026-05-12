package com.king.gmms.connectionpool.systemmanagement;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.king.db.DBHAConstants;
import com.king.db.DataControl;
import com.king.db.DataControlException;
import com.king.db.DatabaseStatus;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.MailSender;
import com.king.gmms.connectionpool.connection.Connection;
import com.king.gmms.connectionpool.connection.ConnectionManager;
import com.king.gmms.connectionpool.node.Node;
import com.king.gmms.connectionpool.session.Session;
import com.king.gmms.connectionpool.systemmanagement.session.CustomerSession;
import com.king.gmms.connectionpool.systemmanagement.session.NodeCustomerSession;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.SingleNodeCustomerInfo;
import com.king.gmms.ha.HATools;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.ha.TransactionURI;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.ha.systemmanagement.pdu.ApplyInThrottleQuota;
import com.king.gmms.ha.systemmanagement.pdu.ApplyInThrottleQuotaAck;
import com.king.gmms.ha.systemmanagement.pdu.ChangeDB;
import com.king.gmms.ha.systemmanagement.pdu.ChangeDBAck;
import com.king.gmms.ha.systemmanagement.pdu.ChangeRedis;
import com.king.gmms.ha.systemmanagement.pdu.ChangeRedisAck;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionConfirm;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionConfirmAck;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionHttpConfirm;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotification;
import com.king.gmms.ha.systemmanagement.pdu.ConnectionStatusNotificationAck;
import com.king.gmms.ha.systemmanagement.pdu.DBOperationAck;
import com.king.gmms.ha.systemmanagement.pdu.InBindAck;
import com.king.gmms.ha.systemmanagement.pdu.InBindRequest;
import com.king.gmms.ha.systemmanagement.pdu.ModuleRegisterAck;
import com.king.gmms.ha.systemmanagement.pdu.ModuleStopAck;
import com.king.gmms.ha.systemmanagement.pdu.OutBindAck;
import com.king.gmms.ha.systemmanagement.pdu.OutBindRequest;
import com.king.gmms.ha.systemmanagement.pdu.QueryHttpAck;
import com.king.gmms.ha.systemmanagement.pdu.QueryHttpRequest;
import com.king.gmms.ha.systemmanagement.pdu.ReportInMsgCount;
import com.king.gmms.ha.systemmanagement.pdu.ShutdownSession;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.gmms.messagequeue.NodeManager;
import com.king.gmms.util.SystemConstants;
import com.king.redis.RedisMonitor;

public class ConnectionManagementForMGT extends ConnectionManagement implements
		SystemManagementInterface {
	private Map<String, Set<TransactionURI>> waitingConfirm = null;
	private static ConnectionManagementForMGT instance = null;
	private Map<String, SystemPdu> bufferMap = null;
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ConnectionManagementForMGT.class);
	private boolean keepRunning = false;// for confirmation thread
	private Map<String, TransactionURI> dbSession = null;
	private ReentrantLock confirmLock = null;
	private ReentrantLock dbLock = null;
	private Properties redisProp = null;
	private Object mutex = new Object();
	/**
	 * </p> key: module name
	 * </p> value: module incoming msg count
	 */
	private ConcurrentMap<String, Integer> moduleInMsgCountMap = null;
	
	private ConnectionManagementForMGT() {
		super();
		confirmLock = new ReentrantLock();
		dbLock = new ReentrantLock();
		bufferMap = new ConcurrentHashMap<String, SystemPdu>();// moduleName,uuid,connectionInfo
		moduleInMsgCountMap = new ConcurrentHashMap<String, Integer>();
		waitingConfirm = new ConcurrentHashMap<String, Set<TransactionURI>>();
		dbSession = new ConcurrentHashMap<String, TransactionURI>();
		String filePath = gmmsUtility.getRedisFile();
		redisProp = new Properties();
		FileInputStream fileInputStream;
		try {
			fileInputStream = new FileInputStream(filePath);
			redisProp.load(fileInputStream);			
		} catch (Exception e) {
			log.error("load redis status error!");
		}
				
	}

	/**
	 * singleton model
	 * 
	 * @return
	 */
	public static synchronized ConnectionManagementForMGT getInstance() {
		if (null == instance) {
			instance = new ConnectionManagementForMGT();
		}
		return instance;
	}

	/**
	 * handleBindRequest
	 * 
	 * @param uuid
	 * @param content
	 * @return
	 */
	public SystemPdu handleInBindRequest(SystemPdu message) {
		boolean flag = this.addSession(message);
		InBindAck response = new InBindAck();
		response.setUuid(message.getUuid());
		if (flag) {
			response.setResponseCode(0);
		} else {
			response.setResponseCode(1);
		}
		bufferMap.put(message.getUuid(), message);
		if(log.isDebugEnabled()){
			log.debug("bufferMap put with :{}", message);
		}
		return response;
	}

	/**
	 * handleBindRequest
	 * 
	 * @param uuid
	 * @param content
	 * @return
	 */
	public SystemPdu handleOutBindRequest(SystemPdu message) {
		String confFileVersion = custManager.getConfFileVersion();
		boolean isConsistent = message.getConfFileVersion().equals(confFileVersion);
		OutBindAck response = new OutBindAck(message);
		response.setUuid(message.getUuid());
		if(isConsistent){
			boolean flag = this.addSession(message);
			if (flag) {
				response.setResponseCode(0);
			} else {
				response.setResponseCode(1);
			}
		}else{
			response.setResponseCode(1);
			log.warn("cm.cfg didn't consistent with system manager's,reject {}", message);
		}
		bufferMap.put(message.getUuid(), message);
		log.debug("bufferMap put with :{}", message);
		return response;
	}

	/**
	 * addSession
	 * 
	 * @param message
	 * @return
	 */
	private boolean addSession(SystemPdu message) {
		int msgType = message.getCommandId();
		int ssid = -1;
		boolean isServer = false;
		if (SystemPdu.COMMAND_IN_BIND_REQUEST == msgType) {
			isServer = true;
			ssid = ((InBindRequest) message).getSsid();
		} else if (SystemPdu.COMMAND_OUT_BIND_REQUEST == msgType) {
			isServer = false;
			ssid = ((OutBindRequest) message).getSsid();
		}
		A2PCustomerInfo custInfo = super.custManager.getCustomerBySSID(ssid);
		if (custInfo == null) {
			log.warn("Get null A2PCustomerInfo with ssid={}", ssid);
			return false;
		}
		TransactionURI transURI = TransactionURI.fromString(message.getUuid());
		return this.isAllow(custInfo, transURI, isServer);
	}

	/**
	 * clearSession
	 * 
	 * @param message
	 * @return
	 */
	private boolean clearSession4Confirm(TransactionURI transaction, int ssid) {
		boolean result = false;
		A2PCustomerInfo custInfo = super.custManager.getCustomerBySSID(ssid);
		if (custInfo == null) {
			log.info("Null A2PCustomerInfo when clear session with ssid={}",
					ssid);
			return false;
		}
		String connectionName = transaction.getConnectionName();
		String ssid_conName = custInfo.getSSID() + "_" + connectionName;
		if (!waitingConfirm.containsKey(ssid_conName)) {
			return true;
		}
		Set<TransactionURI> transURISet = waitingConfirm.get(ssid_conName);
		if (transURISet == null) {
			waitingConfirm.remove(ssid_conName);
			return result;
		}
		if (transURISet.contains(transaction)) {
			transURISet.remove(transaction);
		}
		if (transURISet.isEmpty()) {
			waitingConfirm.remove(ssid_conName);
		}
		return true;
	}

	/**
	 * clearSession
	 * 
	 * @param message
	 * @return 0 need retry
	 *         <br> 1 session is null, don't retry, just notify to function module with same type
	 *         <br> 2 session removed, notify to mgt, core, func
	 */
	private int clearSession4Notification(TransactionURI transaction,
			int ssid, SystemPdu message) {
		int result = 0;		
		// clear ssidConnManager
		sessionLock.lock();
		try {
			A2PCustomerInfo custInfo = super.custManager.getCustomerBySSID(ssid);
			String connectionName = transaction.getConnectionName();
			if(custInfo == null){
				return 0;
			}
			String ssid_conName = custInfo.getSSID() + "_" + connectionName;
			ConnectionManager connectionManager = super.ssidConnManager.get(ssid_conName);
			if(connectionManager==null){
				log.info("No connectionManager for {}",ssid_conName);
				return 0;
			}
			Session session = connectionManager.getSession(transaction);
			if (session == null) {
				log.info("session is null when clearSession4Notification with ssid_conName={},transaction={}",
								ssid_conName,transaction);
				// when excute customer -a, mgt and coreengine connectionManager will be update and session is null, 
				// but still need to notify function module
				// ref A2PCustomerManager.clearConnectionManagement
				this.notify2Other(message);
				
				return 1;
			}
			connectionManager.deleteSession(connectionName, session);
			// clear moduleSession
			String module = transaction.getModule().getModule();
			Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession
					.get(module);
			ArrayList<Session> sessionList = ssidSessionMap.get(ssid);
			if(sessionList.remove(session)){
				result = 2;
			}
		} finally {
			sessionLock.unlock();
		}
		return result;
	}

	/**
	 * handleConnectionConfirm
	 * 
	 * @param message
	 * @return
	 */
	public synchronized SystemPdu handleConnectionConfirm(SystemPdu message) {
		String confFileVersion = custManager.getConfFileVersion();
		if(log.isTraceEnabled()){
			log.trace("cm.cfg conf version md5 is:{},and message version is:{}", confFileVersion, message.getConfFileVersion());
		}
		boolean verFlag = message.getConfFileVersion().equals(confFileVersion);
		
		ConnectionConfirmAck response = new ConnectionConfirmAck(message);
		response.setUuid(message.getUuid());
		// 0: ok; 1: failed-should retry; 2: version inconsistent-should retry
		response.setResponseCode(0);
		
		if (verFlag) {
			ConnectionConfirm confirmPdu = ((ConnectionConfirm) message);
			int statusCode = confirmPdu.getResponseCode();
			String uuid = confirmPdu.getUuid();
			TransactionURI transURI = TransactionURI.fromString(uuid);
			String bindType = confirmPdu.getBindType();
			boolean isServer = false;
			int ssid = -1;
			if (SystemConstants.IN_BIND_TYPE.equalsIgnoreCase(bindType)) {
				ssid = confirmPdu.getSsid();
				isServer = true;
			} else if (SystemConstants.OUT_BIND_TYPE.equalsIgnoreCase(bindType)) {
				SystemPdu originalMessage = bufferMap.remove(uuid);
				if (originalMessage == null) {
	        		if(log.isDebugEnabled()){
	        			log.debug("bufferMap remove null message when uuid={}", uuid);
	        		}
					return response;
				}
	    		if(log.isDebugEnabled()){
	    			log.debug("bufferMap remove with:{}", originalMessage);
	    		}
				ssid = ((OutBindRequest) originalMessage).getSsid();
				isServer = false;
			}
			if (ssid <= 0) {
				log.info("Invalid ssid {} for {}", ssid, message);
				return response;
			}
			if (statusCode == 1) {
				this.clearSession4Confirm(transURI, ssid);
				log.info("clearSession4Confirm for {}", message);
				return response;
			} else if (statusCode == 0) {
				A2PCustomerInfo custInfo = custManager.getCustomerBySSID(ssid);
				boolean flag = this.insertSession4Confirm(custInfo, transURI, isServer);
				if (!flag) {
					log.info("insertSession failed uri:{}", transURI);
					response.setResponseCode(1);
					return response;
				}
				ConnectionStatusNotification notifyPdu = new ConnectionStatusNotification(ssid,transURI);
				notifyPdu.setAction(SystemConstants.CONNECTED_ACTION);
				notifyPdu.setBindType(bindType);
				this.notify2Core(notifyPdu);
				this.notify2Other(notifyPdu);
				return response;
			}
		} else {
			// cm.cfg version inconsistent, should retry
			response.setResponseCode(2);
			return response;
		}
		return response;
	}

	/**
	 * handle status notification syn with another system management module
	 * 
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
		if(log.isTraceEnabled()){
			log.trace("cm.cfg conf version md5 is:{},and message version is:{}",version,message.getConfFileVersion());
		}
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
		}		
		return response;
	}

	/**
	 * updateCache
	 * 
	 * @param uuid
	 * @param ssid
	 * @param user
	 * @param passwd
	 * @param action
	 * @param moduleType
	 * @return
	 */
	private boolean updateConnectionInfo(SystemPdu message) {
		ConnectionStatusNotification notifyMsg = (ConnectionStatusNotification) message;
		String uuid = notifyMsg.getUuid();
		String action = notifyMsg.getAction();
		if (uuid == null) {
			return false;
		}
		TransactionURI transaction = TransactionURI.fromString(uuid);
		String moduleName = transaction.getModule().getModule();
		if(message.getModuleType().equals(SystemConstants.MQM_MODULE_TYPE)){
			if (SystemConstants.CONNECTED_ACTION.equalsIgnoreCase(action)) {
				if (dbSession.isEmpty()) {
					dbSession.put(moduleName, transaction);
					if(log.isInfoEnabled()){
						log.info("Update dbSession:{}", transaction);
					}
				} 
			}else if (SystemConstants.DISCONNECTED_ACTION.equalsIgnoreCase(action)) {
				dbSession.clear();
			}else{
				return false;
			}
			return true;
		}
		if (transaction.getConnectionName() == null
				|| "".equals(transaction.getConnectionName())) {// module
			if (SystemConstants.DISCONNECTED_ACTION.equalsIgnoreCase(action)) {// clear connection info of module
				TransactionURI uri = clearConnnectionByModule(moduleName);
				if(uri!=null){
					moduleStatusNotify2Core(message);
					moduleStatusNotify2Other(message);
				}
				return true;
			} else {
				log.warn(
						"Invalid tranaction when handle StatusNotification:{}",
						message);
				return false;
			}
		}
		int ssid = notifyMsg.getSsid();
		if (ssid <= 0) {
			log.warn("Invalid ssid {} when handle StatusNotification:{}", ssid,
					message);
			return false;
		}
		// update connection info of connection
		if (SystemConstants.CONNECTED_ACTION.equalsIgnoreCase(action)) {
			A2PCustomerInfo custInfo = super.custManager
					.getCustomerBySSID(ssid);
			if(custInfo==null){
				return false;
			}
			boolean isServer = false;
			String bindType = notifyMsg.getBindType();
			if (SystemConstants.IN_BIND_TYPE.equalsIgnoreCase(bindType)) {
				isServer = true;
			} else if (SystemConstants.OUT_BIND_TYPE.equalsIgnoreCase(bindType)) {
				isServer = false;
			}
			return this.insertSession4Notify(custInfo, transaction, isServer);
		} else if (SystemConstants.DISCONNECTED_ACTION.equalsIgnoreCase(action)) {
			int result = this.clearSession4Notification(transaction, ssid, message);
			if(result == 2){ // session removed in MGT success, notify others
				// notify core and mgt
				this.notify2Core(message);
				// notify func
				this.notify2Other(message);
			}
			return (result > 0);
		} else {
			log.warn("unkown action when handle StatusNotification:{}",
							message);
		}
		return false;
	}

	/**
	 * clearConnnectionByModule
	 */
	private TransactionURI clearConnnectionByModule(String moduleName) {
		String moduleType = this.moduleManager.getModuleType(moduleName);
		if (SystemConstants.MQM_MODULE_TYPE.equalsIgnoreCase(moduleType)) {
			if (!dbSession.isEmpty() && dbSession.containsKey(moduleName)) {
				if(log.isInfoEnabled()){
					log.info("Clear dbSession:{}", moduleName);
				}
				dbSession.remove(moduleName);
			}
			return null;
		}
		String lastStatus = moduleManager.getModuleStatusMap().get(moduleName)
				.get(1);
		if (SystemConstants.INITIAL_MODULE_STATUS.equalsIgnoreCase(lastStatus)
				|| SystemConstants.ROUTER_MODULE_TYPE
						.equalsIgnoreCase(moduleType)) {
			return null;
		}
		Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession
				.remove(moduleName);
		TransactionURI transaction = null;
		if (ssidSessionMap == null) {
			return transaction;
		}
		int sessionSize = 0;
		for (int ssid : ssidSessionMap.keySet()) {
			ArrayList<Session> sessions = ssidSessionMap.get(ssid);
			sessionSize += sessions.size();
			for (Session session : sessions) {
				transaction = session.getTransactionURI();
				String connectionName = transaction.getConnectionName();
				String ssid_conName = ssid + "_" + connectionName;
				ConnectionManager connManager = ssidConnManager
						.get(ssid_conName);
				connManager.deleteSession(connectionName, session);
			}
		}
		ssidSessionMap.clear();
		ssidSessionMap = null;
		if(log.isInfoEnabled()){
			log.info("cleared ConnnectionInfo size {} by module:{}", sessionSize,
				moduleName);
		}
		return transaction;
	}

	/**
	 * handleModuleRegister
	 * 
	 * @param message
	 * @return
	 */
	public SystemPdu handleModuleRegister(SystemPdu message) {
		String moduleType = message.getModuleType();
		String moduleName = message.getModuleName();
		String fullModuleType = moduleManager.getFullModuleType(moduleName);
		String lastStatus = moduleManager.getModuleStatusMap().get(moduleName)
				.get(0);
		ModuleRegisterAck response = new ModuleRegisterAck();
		response.setUuid(message.getUuid());
		boolean flag = updateModuleStart(moduleName);
		if (flag) {
			response.setResponseCode(0);
		} else {
			response.setResponseCode(1);
		}
		if (SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(moduleType)
				||SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)
				||SystemConstants.SERVER_MODULE_TYPE.equalsIgnoreCase(moduleType)
				||SystemConstants.CLIENT_MODULE_TYPE.equalsIgnoreCase(moduleType)
				|| SystemConstants.MQM_MODULE_TYPE.equalsIgnoreCase(moduleType)) {
			String dbStatus = loadDBstatus();
			response.setDbStatus(dbStatus);
			String redisStatus = loadRedisStatus();
			response.setRedisStatus(redisStatus);
			this.gmmsUtility.setRedisClientFlag(redisStatus);
		}
		try{
			// notify status to other modules
			if (flag) {// restart
				if (SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(moduleType)) {// router restart
					notifyConnectionInfo2Module(moduleName);// if router restart,MGT need syn connectioninfo to it
				}else if (SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)) {//slave MGT startup
					notifyConnectionInfo2Module(moduleName);// if slave restart,MGT need syn connectioninfo to it
					if(!dbSession.isEmpty()){
						Object[] temp = dbSession.values().toArray();
						TransactionURI uri = (TransactionURI)temp[0];
						this.notifyDBSession2MGT(uri);
					}
				} else if ((SystemConstants.SERVER_MODULE_TYPE
						.equalsIgnoreCase(moduleType) || SystemConstants.CLIENT_MODULE_TYPE
						.equalsIgnoreCase(moduleType))) {
					if(!SystemConstants.INITIAL_MODULE_STATUS.equalsIgnoreCase(lastStatus)){// protocol module restart
						moduleStatusNotify2Core(response);// MGT need notify router to clear old info
						moduleStatusNotify2Other(response);// notify other protocol module
					}
					syncSessionsOfOther(moduleName);
				}
				if((SystemConstants.MULTISMPPSERVER_MODULE_TYPE.equalsIgnoreCase(fullModuleType)
						||SystemConstants.SSLSMPPSERVER_MODULE_TYPE.equalsIgnoreCase(fullModuleType))
						&& !SystemConstants.INITIAL_MODULE_STATUS.equalsIgnoreCase(lastStatus)){//decide whether to shutdown session
					long durationTime = Integer.parseInt(gmmsUtility.getCommonProperty("SystemManager.ShutdownSessionDelayTime", "300").trim())*1000;
					//if durationTime<0, ignore
					if(durationTime>=0){
						log.trace("Execute ShutdownSessionTask after time:{} ms.",durationTime);
						List<String> modules = moduleManager.getAliveModulesWithSameType(moduleName);
						if(modules.size()>0){//other module is running 
							new Timer().schedule(new ShutdownSessionTask(moduleName),durationTime);//delay for minutes
						}
					}
				}
				moduleManager.updateModuleStatus2Up(moduleName);
			}
		}catch(Exception ex){
			log.error(ex,ex);
		}
		return response;
	}
	/**
	 * loadDBstatus
	 */
	private String loadDBstatus() {
		Properties dbprop = DatabaseStatus.readDBStatus();
		if(dbprop.isEmpty()){
			log.warn("loadDBstatus with empty db properties.");
			return DatabaseStatus.MASTER_USED.name();
		}
		String dbStatus4Used = dbprop.getProperty(DBHAConstants.USED_KEY)+ "_used";
		return dbStatus4Used;
	}
	
	private String loadRedisStatus() {
		String redisStatus4Used = redisProp.getProperty("Used");
		if(redisStatus4Used==null){
			redisStatus4Used = "M";
		}
		return redisStatus4Used;
	}
	/**
	 * handleModuleStop
	 * 
	 * @param message
	 * @return
	 */
	public SystemPdu handleModuleStop(SystemPdu message) {
		String uuid = message.getUuid();
		boolean flag = updateModuleStop(uuid);
		ModuleStopAck response = new ModuleStopAck(message);
		response.setUuid(uuid);
		if (flag) {
			response.setResponseCode(0);
		} else {
			response.setResponseCode(1);
		}
		// notify status to other modules
		if (flag) {
			String moduleType = message.getModuleType();
			String moduleName = message.getModuleName();
			if (SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)) {// mgt stop
				ModuleConnectionInfo connectionInfo4Master = moduleManager
						.getConnectionInfoOfMasterMGT();
				if (moduleName.equals(connectionInfo4Master.getModuleName())) {// master down
					moduleManager.updateModuleStatus2Down(moduleName);
					MailSender.getInstance().sendAlertMail(
							"A2P alert mail from "
									+ ModuleURI.self().getAddress()
									+ " for master SystemManager Down!",
							"Master SystemManager Down!", null);
				} else {// slave down
					// send alert mail
					MailSender.getInstance().sendAlertMail(
							"A2P alert mail from "
									+ ModuleURI.self().getAddress()
									+ " for slave SystemManager Down!",
							"Slave SystemManager Down!", null);
				}
			} else if (SystemConstants.ROUTER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {// router stop
			} else if (SystemConstants.SERVER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)
					|| SystemConstants.CLIENT_MODULE_TYPE
							.equalsIgnoreCase(moduleType)) {// protocol module is down
				moduleStatusNotify2Core(response);// if protocol module stop,
													// MGT need notify router to clear old info
				moduleStatusNotify2Other(response);// notify other protocl module
			} else if (SystemConstants.MQM_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				moduleStatusNotify2Other(response);// notify other protocol module
			}
			moduleManager.updateModuleStatus2Down(moduleName);
		}
		return response;
	}

	/**
	 * didn't received keep alive response
	 */
	public void moduleDown(String downModule) {
		String downModuleType = this.moduleManager.getModuleType(downModule);
		sessionLock.lock();
		try {
			if (SystemConstants.ROUTER_MODULE_TYPE
					.equalsIgnoreCase(downModuleType)) {
				this.moduleManager.updateModuleStatus2Down(downModule);
			} else if (SystemConstants.MGT_MODULE_TYPE
					.equalsIgnoreCase(downModuleType)) {
				this.moduleManager.updateModuleStatus2Down(downModule);
				MailSender.getInstance().sendAlertMail(
						"A2P alert mail from " + ModuleURI.self().getAddress()
								+ " for SystemManager Down!",
						"SystemManager Down!", null);
			} else {// protocol module down
				String lastStatus = moduleManager.getModuleStatusMap().get(
						downModule).get(0);
				if (SystemConstants.UP_MODULE_STATUS
						.equalsIgnoreCase(lastStatus)) {// restart
					TransactionURI transaction = this
							.clearConnnectionByModule(downModule);
					if (transaction == null) {
						return;
					}
					transaction.setConnectionName("");// indicate it is module down
					ConnectionStatusNotification notifyMsg = new ConnectionStatusNotification();
					notifyMsg.setUuid(transaction.toString());
					if(log.isInfoEnabled()){
						log.info("Detect {} is down, notify to core:{}",
							downModule, notifyMsg.getUuid());
					}
					moduleStatusNotify2Core(notifyMsg);// if protocol module down, MGT need notify
														// router to clear old info
					moduleStatusNotify2Other(notifyMsg);// if protocol module down, MGT need notify
														// other protocol modules to clear old info
				}
				this.moduleManager.updateModuleStatus2Down(downModule);
			}
		} finally {
			sessionLock.unlock();
		}
	}

	/**
	 * notify to core engine/slave mgt
	 * 
	 * @param msg
	 */
	private void notify2Core(SystemPdu msg) {
		List<SystemSession> systemSessions = new ArrayList<SystemSession>();
		List<SystemSession> routerSessions = sysSessionFactory
				.getSessionsByModuleType(SystemConstants.ROUTER_MODULE_TYPE);
		systemSessions.addAll(routerSessions);
		List<SystemSession> mgtSessions = sysSessionFactory.getSessionsByModuleType(SystemConstants.MGT_MODULE_TYPE);
		if(mgtSessions.size()>0){
				for(SystemSession sysSession:mgtSessions){
					if(!this.selfModule.equals(sysSession.getName())){
						systemSessions.add(sysSession);
					}
				}
		}
		if (systemSessions.isEmpty()) {
			log.info("SystemSession List is empty for router!");
			return;
		}
		for (SystemSession session : systemSessions) {
			if (!session.isKeepRunning()) {
				try {
					session.start();
					Thread.sleep(200);// TODO:configure sleep time
				} catch (InterruptedException e) {
					continue;
				}
			}
			boolean sendSuccess = false;
			SystemPdu pdu = new ConnectionStatusNotification((ConnectionStatusNotification)msg);
			try {
				sendSuccess = session.send(pdu);
			} catch (Exception e) {
				log.error("notify2Core exception:", e);
			}
			if (!sendSuccess) {
				session.putMessage(pdu);
			}
			else{
				session.getMessageBuffer().put(pdu.getSequenceNumber(), pdu);
			}
			if(log.isInfoEnabled()){
				log.info("notify to {} with notifyMsg:{};sendSuccess={}", session
					.getModuleName(), pdu, sendSuccess);
			}
		}
	}

	/**
	 * moduleStatus changed, need notify to router modules
	 */
	private void moduleStatusNotify2Core(SystemPdu response) {
		ConnectionStatusNotification notifyMsg = new ConnectionStatusNotification();
		notifyMsg.setUuid(response.getUuid());
		notifyMsg.setAction(SystemConstants.DISCONNECTED_ACTION);// to clear old info
		notify2Core(notifyMsg);
	}

	/**
	 * moduleStatus changed, need notify to router modules
	 */
	private void notify2Other(SystemPdu msg) {
		List<SystemSession> systemSessions = new ArrayList<SystemSession>();
		String moduleName = msg.getModuleName();
		List<String> protocolModules = moduleManager
				.getAliveModulesWithSameType(moduleName);
		for (String module : protocolModules) {
			if (!moduleName.equals(module)) {
				SystemSession client = sysSessionFactory
						.getSessionByModuleName(module);
				systemSessions.add(client);
			}
		}
		if (systemSessions == null) {
			log.debug("SystemSession List is null for other!");
			return;
		}
		for (SystemSession session : systemSessions) {
			if (!session.isKeepRunning()) {
				try {
					session.start();
					Thread.sleep(200);// TODO:configure sleep time
				} catch (InterruptedException e) {
					continue;
				}
			}
			boolean sendSuccess = false;
			SystemPdu pdu = new ConnectionStatusNotification((ConnectionStatusNotification)msg);
			try {
				sendSuccess = session.send(pdu);
				if(log.isInfoEnabled()){
					log.info("notify2Other to {} with notifyMsg:{}", session
						.getModuleName(), pdu);
				}
			} catch (Exception e) {
				log.error(e, e);
			}
			if (!sendSuccess) {
				session.putMessage(pdu);
				if(log.isInfoEnabled()){
					log.info("notify2Other failed to {} with notifyMsg:{}", session
						.getModuleName(), pdu);
				}
			}else{
				session.getMessageBuffer().put(pdu.getSequenceNumber(), pdu);
			}
		}
	}

	/**
	 * moduleStatus changed, need notify to other protocol modules
	 */
	private void moduleStatusNotify2Other(SystemPdu response) {
		ConnectionStatusNotification notifyMsg = new ConnectionStatusNotification();
		notifyMsg.setUuid(response.getUuid());
		notifyMsg.setAction(SystemConstants.DISCONNECTED_ACTION);// to clear old info
		notify2Other(notifyMsg);
	}

	/**
	 * 
	 * @param uuid
	 * @param moduleType
	 * @return
	 */
	private boolean updateModuleStart(String modName) {
		clearConnnectionByModule(modName);
		moduleManager.updateModuleStatus2Up(modName);
		return true;
	}

	/**
	 * syn ConnectionInfo to router/slave MGT
	 */
	public void notifyConnectionInfo2Module(String moduleName) {
		List<SystemPdu> messageList = makeMessagesInCache();
		if (messageList == null || messageList.isEmpty()) {
			log
					.debug("No connection info in cache, so needn't notify CoreEgine.");
			return;
		}
		SystemSession moduleSysSession = sysSessionFactory
				.getSessionByModuleName(moduleName);
		if (moduleSysSession != null) {
			if (!moduleSysSession.isKeepRunning()) {
				try {
					moduleSysSession.start();
					Thread.sleep(50);// TODO:configure sleep time
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			for (SystemPdu message : messageList) {
				boolean sendSuccess = false;
				try {
					sendSuccess = moduleSysSession.send(message);
				} catch (Exception e) {
					log.error(e, e);
				}
				if (!sendSuccess) {
					moduleSysSession.putMessage(message);
				}else{
					moduleSysSession.getMessageBuffer().put(message.getSequenceNumber(), message);
				}
			}
		}
	}

	/**
	 * generate system message for notification
	 * 
	 * @return
	 */
	private List<SystemPdu> makeMessagesInCache() {
		if (moduleSession.isEmpty()) {
			log.info("No cached session to make!");
		}
		List<SystemPdu> messageList = new ArrayList<SystemPdu>();
		Map<Integer, ArrayList<Session>> ssidSessionMap = null;
		for (String moduleName : moduleSession.keySet()) {
			ssidSessionMap = moduleSession.get(moduleName);
			if(ssidSessionMap==null || ssidSessionMap.isEmpty()){
				continue;
			}
			ArrayList<Session> sessionList = null;
			for (int ssid : ssidSessionMap.keySet()) {
				sessionList = ssidSessionMap.get(ssid);
				if(sessionList==null || sessionList.isEmpty()){
					continue;
				}
				for (Session session : sessionList) {
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
	
	
	public Session getSession(int ssid, TransactionURI transaction){
		
		if(ssid <= 0 || transaction == null){
			return null;
		}
		
		A2PCustomerInfo custInfo = super.custManager.getCustomerBySSID(ssid);
		
		if(custInfo==null){
			return null;
		}
		
		String connectionName = transaction.getConnectionName();
		
		String sedString = custInfo.getSSID() + "_" + connectionName;
		ConnectionManager connManager = ssidConnManager.get(sedString);
		if(connManager ==null){
			return null;
		}
		return connManager.getSession(transaction);
	}

	
	/**
	 * sync customer sessions to other modules
	 * 
	 * @return
	 */
	private void syncSessionsOfOther(String module) {
		if (moduleSession.isEmpty()) {
			log.info("No session to sync!");
			return;
		}
		List<SystemPdu> messageList = new ArrayList<SystemPdu>();
		List<String>  modules = moduleManager.getAliveModulesWithSameType(module);
		Map<Integer, ArrayList<Session>> ssidSessionMap = null;
		for (String moduleName : modules) {
			if(module.equals(moduleName)){
				continue;
			}
			ssidSessionMap = moduleSession.get(moduleName);
			if(ssidSessionMap==null || ssidSessionMap.isEmpty()){
				continue;
			}
			ArrayList<Session> sessionList = null;
			for (int ssid : ssidSessionMap.keySet()) {
				sessionList = ssidSessionMap.get(ssid);
				if(sessionList==null || sessionList.isEmpty()){
					continue;
				}
				for (Session session : sessionList) {
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
		if(messageList.isEmpty()){
			return;
		}
		SystemSession moduleSysSession = sysSessionFactory.getSessionByModuleName(module);
		if (moduleSysSession != null) {
			if (!moduleSysSession.isKeepRunning()) {
				try {
					moduleSysSession.start();
					Thread.sleep(50);// TODO:configure sleep time
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			for (SystemPdu message : messageList) {
				boolean sendSuccess = false;
				try {
					sendSuccess = moduleSysSession.send(message);
				} catch (Exception e) {
					log.error(e, e);
				}
				if (!sendSuccess) {
					moduleSysSession.putMessage(message);
				}else{
					moduleSysSession.getMessageBuffer().put(message.getSequenceNumber(), message);
				}
			}
		}
	}
	/**
	 * handleModuleStop
	 * 
	 * @param uuid
	 * @param moduleType
	 * @return
	 */
	private boolean updateModuleStop(String uuid) {
		String[] uuidArr = uuid.split(SystemConstants.COLON_SEPARATOR);
		if (uuidArr.length < 2) {
			return false;
		}
		String modName = uuidArr[1];
		clearConnnectionByModule(modName);
		moduleManager.updateModuleStatus2Down(modName);
		return true;
	}

	/**
	 * handleDBRequest
	 * 
	 * @param uuid
	 */
	public SystemPdu handleDBRequest(SystemPdu message) {
		String uriString = message.getUuid();
		TransactionURI uri = TransactionURI.fromString(uriString);
		int responseCode = 1;
		dbLock.lock();
		try {
			if (dbSession.isEmpty()) {
				String moduleName = uri.getModule().getModule();
				dbSession.put(moduleName, uri);
				if(log.isInfoEnabled()){
					log.info("Add dbSession:{}", uri);
				}
				responseCode = 0;
				notifyDBSession2MGT(uri);
			}
		}catch(Exception e){
			log.error("handleDBRequest error:",e);
		} finally {
			dbLock.unlock();
		}
		DBOperationAck response = new DBOperationAck();
		response.setUuid(message.getUuid());
		response.setResponseCode(responseCode);
		return response;
	}
	/**
	 * notifyDBSession2MGT
	 */
	private void notifyDBSession2MGT(TransactionURI uri){
		SystemSession systemSession = sysSessionFactory.getSystemSessionOfMGT();
		if (systemSession != null) {
			ConnectionStatusNotification notifyPdu = new ConnectionStatusNotification(-1,uri);
			notifyPdu.setAction(SystemConstants.CONNECTED_ACTION);
			systemSession.getMessageBuffer().put(notifyPdu.getSequenceNumber(), notifyPdu);
			try{
				systemSession.send(notifyPdu);		
				log.info("Sync dbSession to MGT.");
			}catch(Exception e){
				log.error("Send notifyPdu failed!",e);
			}
		}		
	}
	/**
	 * 
	 * @param uuid
	 */
	public SystemPdu handleDBChanged(SystemPdu message) {
		ChangeDB pdu = (ChangeDB)message;
		DatabaseStatus dbStatus = DatabaseStatus.get(pdu.getAction());
		DataControl dbControl = DataControl.getInstance();
		DatabaseStatus usedDbStatus = dbControl.getUsedDatabaseStatus();
		if(dbStatus != usedDbStatus){
			dbControl.setUsedDatabaseStatus(dbStatus);
			HATools.updateDBStatus2File(dbStatus);
		}
		ChangeDBAck ack = new ChangeDBAck(message);
		ack.setUuid(pdu.getUuid());
		ack.setResponseCode(0);
		return ack;	
	}

	public boolean isAllow(A2PCustomerInfo custInfo,
			TransactionURI transaction, boolean isServer) {
		boolean result = false;
		if (custInfo == null || transaction == null) {
			return result;
		}
		String connectionName = transaction.getConnectionName();

		int maxSessionNum = 0;
		switch (custInfo.getConnectionType()) {
		case 1:
			A2PSingleConnectionInfo singleConnInfo = ((A2PSingleConnectionInfo) custInfo);
			if (singleConnInfo == null) {
				log.debug("connInfo is null when connectionName:{}",
						connectionName);
				return false;
			}
			if (isServer) {
				maxSessionNum = singleConnInfo.getSessionNumber() == 0 ? -1
						: singleConnInfo.getSessionNumber();
			} else {
				maxSessionNum = singleConnInfo.getClientSessionNumber();
			}
			break;
		case 2:
			if (isServer) {
				ConnectionInfo connInfo = ((SingleNodeCustomerInfo) custInfo)
						.getIncomingConnection(connectionName);
				if (connInfo == null) {
					log.debug("connInfo is null when connectionName:{}",
							connectionName);
					return false;
				}
				maxSessionNum = connInfo.getSessionNum();
			} else {
				ConnectionInfo connInfo = ((SingleNodeCustomerInfo) custInfo)
						.getOutgoingConnection(connectionName);
				maxSessionNum = connInfo.getSessionNum();
			}
			break;
		case 3:
			String tag = null;
			if (isServer) {
				tag = super.incoming_tag;
			} else {
				tag = super.outgoing_tag;
			}
			NodeManager nm = ssidNodeManager.get(custInfo.getSSID() + tag);
			Node node = nm.getConn2Node(connectionName);
			if (node != null) {
				Map connectionMap = node.getConnectionMap();
				ConnectionInfo connInfo = (ConnectionInfo) connectionMap
						.get(connectionName);
				if (connInfo == null) {
					log.debug("connInfo is null when connectionName:{}",
							connectionName);
					return false;
				}
				maxSessionNum = connInfo.getSessionNum();
			}
			break;
		default:
			break;
		}

		String ssid_connName = custInfo.getSSID() + "_" + connectionName;
		ConnectionManager connManager = ssidConnManager.get(ssid_connName);
		int sessionNum = 0;
		if (connManager != null) {
			Connection connection = connManager.getConnection(connectionName);
			if (connection != null) {
				sessionNum = connection.getSessionNum();
			}
		}

		confirmLock.lock();
		try {
			Set<TransactionURI> transSet = waitingConfirm.get(ssid_connName);
			if (transSet == null) {
				if (maxSessionNum < 0 || sessionNum < maxSessionNum) {
					transSet = new HashSet<TransactionURI>();
					transSet.add(transaction);
					waitingConfirm.put(ssid_connName, transSet);
					result = true;
				}
			} else {
				if (maxSessionNum < 0
						|| sessionNum + transSet.size() < maxSessionNum) {
					transSet.add(transaction);
					result = true;
				}
			}
		} finally {
			confirmLock.unlock();
		}
		return result;
	}

	/**
	 * insert session
	 */
	public boolean insertSession(A2PCustomerInfo custInfo,
			TransactionURI transaction, boolean isServer) {
		return true;
	}

	/**
	 * insert session for master mgt
	 * 
	 * @param custInfo
	 * @param transaction
	 * @param isServer
	 * @return true: success or no need retry </br>
	 *         false: failed(need retry).
	 */
	public boolean insertSession4Confirm(A2PCustomerInfo custInfo,
			TransactionURI transaction, boolean isServer) {
		boolean result = true;
		String connectionName = transaction.getConnectionName();
		String ssid_conName = custInfo.getSSID() + "_" + connectionName;
		String module = transaction.getModule().getModule();
		Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession
				.get(module);
		ArrayList<Session> sessionList = null;
		sessionLock.lock();
		try {
			if(!isServer){//out bind
				Set<TransactionURI> transURISet = waitingConfirm.get(ssid_conName);
				if (transURISet == null) {
					log.trace("insertSession4Master transURISet with null!{}",
							ssid_conName);
					return true;
				}
				if(log.isInfoEnabled()){
					log.info("transURISet size:{}", transURISet.size());
				}
				if (transURISet.contains(transaction)) {
					transURISet.remove(transaction);
				}else {
					log.trace(
							"insertSession4Master transURISet didn't contains transaction :{}",
							transaction);
					return true;
				}	
			}
			ConnectionManager connManager = ssidConnManager.get(ssid_conName);
			if (connManager == null) {
				log.trace("insertSession4Master ssidConnManager with null!{}",
								ssid_conName);
				// should retry later
				return false;
			}
			
			log.debug("insertSession with transaction:{}", transaction);
			
			switch (custInfo.getConnectionType()) {
				case 1:
				case 2:
					CustomerSession custSession = new CustomerSession(
							transaction, isServer);
					custSession.setConnectionManager(connManager);
					result = connManager.insertSession(connectionName,
							custSession);
					if (result) {
						custSession.connect();
						if (ssidSessionMap != null) {
							sessionList = ssidSessionMap
									.get(custInfo.getSSID());
							if (sessionList == null) {
								sessionList = new ArrayList<Session>();
								sessionList.add(custSession);
								ssidSessionMap.put(custInfo.getSSID(),
										sessionList);
							} else {
								sessionList.add(custSession);
							}
						} else {
							ssidSessionMap = new ConcurrentHashMap<Integer, ArrayList<Session>>();
							sessionList = new ArrayList<Session>();
							sessionList.add(custSession);
							ssidSessionMap.put(custInfo.getSSID(), sessionList);
							moduleSession.put(module, ssidSessionMap);
						}
					}else{
						log.info("insertSession failed:{}", transaction);
					}
					break;
				case 3:
					NodeCustomerSession nodeCustSession = new NodeCustomerSession(
							custInfo, transaction, isServer);
					nodeCustSession.setConnectionManager(connManager);
					result = connManager.insertSession(connectionName,
							nodeCustSession);
					if (result) {
						nodeCustSession.connect();
						if (ssidSessionMap != null) {
							sessionList = ssidSessionMap
									.get(custInfo.getSSID());
							if (sessionList == null) {
								sessionList = new ArrayList<Session>();
								sessionList.add(nodeCustSession);
								ssidSessionMap.put(custInfo.getSSID(),
										sessionList);
							} else {
								sessionList.add(nodeCustSession);
							}
						} else {
							ssidSessionMap = new ConcurrentHashMap<Integer, ArrayList<Session>>();
							sessionList = new ArrayList<Session>();
							sessionList.add(nodeCustSession);
							ssidSessionMap.put(custInfo.getSSID(), sessionList);
							moduleSession.put(module, ssidSessionMap);
						}
					}else{
						log.info("insertSession failed:{}", transaction);
					}
					break;
				default:
					break;
			}
			
			
			
		} finally {
			sessionLock.unlock();
		}
		
		//remove log 2020.12.28
		//printModuleSessions();
		
		return result;
	}

	/**
	 * insert session for slave mgt
	 */
	public boolean insertSession4Notify(A2PCustomerInfo custInfo,
			TransactionURI transaction, boolean isServer) {
		boolean result = false;
		sessionLock.lock();
		try {
			String connectionName = transaction.getConnectionName();
			if(custInfo==null){
				return false;
			}
			String sedString = custInfo.getSSID() + "_" + connectionName;
			ConnectionManager connManager = ssidConnManager.get(sedString);
			if(connManager ==null){
				return false;
			}
			Session oldSession = connManager.getSession(transaction);
			if (oldSession != null) {
				if(log.isInfoEnabled()){
					log.info("The session {} already exist!", transaction);
				}
				return true;
			}
			
			switch (custInfo.getConnectionType()) {
			case 1:
			case 2:
				CustomerSession custSession = new CustomerSession(transaction,
						isServer);
				custSession.setConnectionManager(connManager);
				result = connManager.insertSession(connectionName, custSession);
				if (result) {
					custSession.connect();
					String module = transaction.getModule().getModule();
					Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession
							.get(module);
					ArrayList<Session> sessionList = null;
					if (ssidSessionMap != null) {
						sessionList = ssidSessionMap.get(custInfo.getSSID());
						if (sessionList == null) {
							sessionList = new ArrayList<Session>();
							sessionList.add(custSession);
							ssidSessionMap.put(custInfo.getSSID(), sessionList);
						} else {
							sessionList.add(custSession);
						}
					} else {
						ssidSessionMap = new ConcurrentHashMap<Integer, ArrayList<Session>>();
						sessionList = new ArrayList<Session>();
						sessionList.add(custSession);
						ssidSessionMap.put(custInfo.getSSID(), sessionList);
						moduleSession.put(module, ssidSessionMap);
					}
				}
				break;
			case 3:
				NodeCustomerSession nodeCustSession = new NodeCustomerSession(
						custInfo, transaction, isServer);
				nodeCustSession.setConnectionManager(connManager);
				result = connManager.insertSession(connectionName,
						nodeCustSession);
				if (result) {
					nodeCustSession.connect();
					String module = transaction.getModule().getModule();
					Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession
							.get(module);
					ArrayList<Session> sessionList = null;
					if (ssidSessionMap != null) {
						sessionList = ssidSessionMap.get(custInfo.getSSID());
						if (sessionList == null) {
							sessionList = new ArrayList<Session>();
							sessionList.add(nodeCustSession);
							ssidSessionMap.put(custInfo.getSSID(), sessionList);
						} else {
							sessionList.add(nodeCustSession);
						}
					} else {
						ssidSessionMap = new ConcurrentHashMap<Integer, ArrayList<Session>>();
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
			
		} finally {
			sessionLock.unlock();
		}
		
		//remove log 2020.12.28
		//printModuleSessions();
		
		return result;
	}
	/**
     * ShutdownSessionTask Timer
     * @author Jianming Yang
     *
     */
    private class ShutdownSessionTask extends TimerTask{
    	private String moduleName;
    	ShutdownSessionTask(String moduleName){
    		this.moduleName = moduleName;
    	}
    	public void run(){
    		log.debug("Start to shutdown session.");
    		shutdownSession(moduleName);
    	}
    }
	private void shutdownSession(String moduleName){
			String fullModuleType = moduleManager.getFullModuleType(moduleName);
			if(fullModuleType == null || "".equalsIgnoreCase(fullModuleType)){
				return;
			}
		    List<String> modules =  moduleManager.getModulesWithFullType(fullModuleType);
		    int modSize = modules.size();
		    if(modSize < 2){
		    	return;
		    }
		    Map<String,Map<String,Integer>> sessionNumMap = new HashMap<String,Map<String,Integer>>();
		    //ssid_connname,module,sessionNum
		    for(String module:modules){
		    	if(module.equals(moduleName)){//ignore the register module
		    		continue;
		    	}
		    	Map<Integer, ArrayList<Session>> ssidSessionMap = moduleSession.get(module);
		    	if (ssidSessionMap == null) {
					log.debug("No session to shutdown for {}",module);
					continue;
				}
				for (Map.Entry<Integer, ArrayList<Session>> ssidSessionEntry : ssidSessionMap
						.entrySet()) {
					int ssid = ssidSessionEntry.getKey();
					ArrayList<Session> sessions = ssidSessionEntry.getValue();
					Map<String,Integer> connectionSessionMap = getSessionNumMap(sessions);
					for(String connName : connectionSessionMap.keySet()){
						Map<String,Integer> moduleSessionMap = (Map<String,Integer>)sessionNumMap.get(ssid+","+connName);
						if(moduleSessionMap==null){
							moduleSessionMap = new HashMap<String,Integer>();
							sessionNumMap.put(ssid+","+connName, moduleSessionMap);
						}
						moduleSessionMap.put(module,connectionSessionMap.get(connName));
					}
				}
		    }
		    for (Map.Entry<String,Map<String,Integer>> ssidSessionEntry : sessionNumMap
					.entrySet()) {
				String ssid_cname = ssidSessionEntry.getKey();
				int maxSessionNum = 0;
				int minSessionNum = 0;
				String moduleNameWithMax = null;
				Map<String,Integer> modSessionNumMap = ssidSessionEntry.getValue();
				for(Map.Entry<String,Integer> modNumEntry:modSessionNumMap.entrySet()){
					String module = modNumEntry.getKey();
					int modSessionNum = modNumEntry.getValue();
					if(modSessionNum > maxSessionNum){
						maxSessionNum = modSessionNum;
						moduleNameWithMax = module;
					}else if(modSessionNum < minSessionNum){
						minSessionNum = modSessionNum;
					}
				}
				if(maxSessionNum > modSize && maxSessionNum > modSize*minSessionNum){
			    	sendShutdownSession(ssid_cname,maxSessionNum,moduleNameWithMax);
				}
		    }
	}
	/**
	 * getSessionNumMap by connection name
	 * @param sessionList
	 * @return
	 */
	private Map<String,Integer> getSessionNumMap(ArrayList<Session> sessionList){
		Map<String,Integer> connectionSessionMap = new HashMap<String,Integer>();
		int sessionSize = sessionList.size();
		if(sessionSize==0){
			return connectionSessionMap;
		}
		for(Session s:sessionList){
			String connectionName = s.getTransactionURI().getConnectionName();
//			BindMode bindMode = s.getConnectionInfo().getBindMode();
			if(connectionName.endsWith("_R")){
				continue;
			}
			Integer sessionNum = connectionSessionMap.get(connectionName);
			if(sessionNum==null){
				connectionSessionMap.put(connectionName, 1);
			}else{
				connectionSessionMap.put(connectionName, sessionNum+1);
			}
		}
		return connectionSessionMap;
	}
	/**
	 * send shutdown session pdu
	 * @param ssid_cname
	 * @param sessionNum
	 * @param module
	 */
	private void  sendShutdownSession(String ssid_cname,int sessionNum,String module){
		ShutdownSession pdu = new ShutdownSession();
		try{
			String[] arr = ssid_cname.split(",");
			int ssid = Integer.valueOf(arr[0]);
			pdu.setSsid(ssid);
			pdu.setConnectionName(arr[1]);
			pdu.setSessionNum(sessionNum);
	    	SystemSession client = sysSessionFactory.getSessionByModuleName(module);
	    	if(client!=null){
	    		client.send(pdu);
	    	}
		}catch(Exception e){
			log.error(e.getMessage());
		}	
	}
	public void startConfirmScanThread() {
		new ConfirmScanThread().start();
	}

	public void shutdownConfirmScanThread() {
		keepRunning = false;
	}
	@Override
	public SystemPdu handleRedisChanged(SystemPdu message) {
		ChangeRedis pdu = (ChangeRedis)message;
		String redisStatus = pdu.getAction();
		String fileRedisStatus = redisProp.getProperty("Used");
		String filePath = gmmsUtility.getRedisFile();
		redisProp.setProperty(DBHAConstants.USED_KEY, redisStatus);
		try {
			File file = new File(filePath);
			if(!file.exists()){
				file.createNewFile();
			}	
			if(!redisStatus.equalsIgnoreCase(fileRedisStatus)){
				OutputStream fos = new FileOutputStream(filePath);				
				redisProp.store(fos, "Update "+DBHAConstants.USED_KEY+" to file.");
				RedisMonitor.getInstance().setMasterOrSlave(redisStatus);
			}
		} catch (Exception e) {
			log.error("load redis status error!");
		}
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
		return null;
	} 
	/**
	 * ConfirmScanThread
	 * 
	 * @author jianmingyang
	 * 
	 */
	class ConfirmScanThread extends Thread {
		private final static long confirmmationTimeout = 1000 * 180;// 3 minutes

		public ConfirmScanThread() {
			keepRunning = true;
		}

		public void start() {
			Thread thread = new Thread(A2PThreadGroup.getInstance(), this,
					"scanTrd ");
			thread.start();
			log.info("Confirmation scan Thread start...");
		}

		public void run() {
			while (keepRunning) {
				try {
					Thread.sleep(confirmmationTimeout);
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (bufferMap.isEmpty()) {
					continue;
				}
				List<String> msg2Remove = new ArrayList<String>();
				for (Map.Entry<String, SystemPdu> msgEntry : bufferMap
						.entrySet()) {
					String uriString = msgEntry.getKey();
					SystemPdu bindMsg = msgEntry.getValue();
					TransactionURI uri = TransactionURI.fromString(uriString);
					if (System.currentTimeMillis()
							- Long.valueOf(bindMsg.getTimestamp()) > confirmmationTimeout) {
						msg2Remove.add(uriString);
						// remove for waitingConfirm first
						int ssid = -1;
						int commandId = bindMsg.getCommandId();
						if (SystemPdu.COMMAND_IN_BIND_REQUEST == commandId) {
							ssid = ((InBindRequest) bindMsg).getSsid();
						} else if (SystemPdu.COMMAND_OUT_BIND_REQUEST == commandId) {
							ssid = ((OutBindRequest) bindMsg).getSsid();
						}
						String connectionName = uri.getConnectionName();
						String ssid_key = ssid + "_" + connectionName;
						Set<TransactionURI> transSet = waitingConfirm
								.get(ssid_key);
						if(transSet!=null){
							transSet.remove(uri);
						}
						
					}
				}
				for (String uriStr : msg2Remove) {
					bufferMap.remove(uriStr);
					log.info("Remove timeout TransactionURI:{}", uriStr);
				}
			}
			log.info("Receiver Thread stop for session.");
		}
	}
	/** 
	 * Handle protocol module reported incoming msg count
	 * @param message
	 * @return
	 * @see com.king.gmms.connectionpool.systemmanagement.SystemManagementInterface#handleReportInMsgCount(com.king.gmms.ha.systemmanagement.pdu.SystemPdu)
	 */
	@Override
	public SystemPdu handleReportInMsgCount(SystemPdu message) {
		try {
			ReportInMsgCount pdu = (ReportInMsgCount)message;
			String moduleName = pdu.getModuleName();
			moduleInMsgCountMap.put(moduleName, pdu.getModuleIncomingMsgCount());
		} catch (Exception e) {
			log.warn(e, e);
		}
		// the pdu not need ack
		return null;
	}

	/** 
	 * Handle apply incoming quota request from protocol module
	 * @param message
	 * @return
	 * @see com.king.gmms.connectionpool.systemmanagement.SystemManagementInterface#handleApplyInThrottleQuota(com.king.gmms.ha.systemmanagement.pdu.SystemPdu)
	 */
	@Override
	public SystemPdu handleApplyInThrottleQuota(SystemPdu message) { 
		try {
			int totalCount = 0;
			for (String moduleName : moduleInMsgCountMap.keySet() ) {
				totalCount += moduleInMsgCountMap.get(moduleName);
			}
			int sysThreshold = gmmsUtility.getSystemIncomingThreshold();
			long reportInterval = TimeUnit.MILLISECONDS.toSeconds(gmmsUtility.getReportModuleIncomingMsgCountInterval());
			
			int sysLoad = (int) ((float)totalCount/reportInterval/sysThreshold*100);
			if (log.isDebugEnabled()) {
				log.debug("totalCount={}, calculated system load:{}%", totalCount, sysLoad);
			}
			
			ApplyInThrottleQuota pdu = (ApplyInThrottleQuota)message;
			
			ApplyInThrottleQuotaAck ack = new ApplyInThrottleQuotaAck(message);
			ack.setUuid(pdu.getUuid());
			ack.setSsid(pdu.getSsid());
			ack.setSysLoadPercent(sysLoad>100? 100: sysLoad);
			return ack;
		} catch (Exception e) {
			log.warn(e, e);
			return null;
		}
	}
	
	public Properties getRedisProp() {
		return redisProp;
	}

	public void updateRedisPropParameter(String redisFlag) {
		redisProp.setProperty(DBHAConstants.USED_KEY, redisFlag);
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
		QueryHttpRequest pdu = (QueryHttpRequest) message;		
		int ssid = pdu.getSsid();
		A2PCustomerInfo cm = this.custManager.getCustomerBySSID(ssid);
		String queryMethod = pdu.getQueryMethod(); // judge query thread is MO or MTDR 
		String value = null;
		if (cm != null) {
			value = readMinIDbyRedis(cm,queryMethod);
		}
		QueryHttpAck ack = new QueryHttpAck(message);
		ack.setUuid(pdu.getUuid());
		ack.setValue(value);
		return ack;
	}

	@Override
	public SystemPdu handleQueryHttpAck(SystemPdu message) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void handleConnectionHttpConfirm(SystemPdu message) {
		ConnectionHttpConfirm pdu = (ConnectionHttpConfirm) message;
		int ssid = pdu.getSsid();
		String shortName = null;
		if(this.custManager.getCustomerBySSID(ssid)!=null){
			shortName = this.custManager.getCustomerBySSID(ssid).getShortName();
		}
		String queryMethod = pdu.getQueryMethod();
		shortName = shortName+"_"+queryMethod;
		String value = pdu.getValue();
		if(value!=null&&value.split(":").length==2){
			String minID = value.split(":")[1];
			if(minID ==null){
				minID="0";
			}
			Map<String, String> map = new HashMap<String, String>();
			map.put("QUERY_FLAG", "true");
			map.put("MIN_ID", minID);
			map.put("ETIME", System.currentTimeMillis() + "");
			if (shortName != null) {
				try {
					this.writeRedis(shortName, map);
				} catch (DataControlException e) {
					log.warn("get mysql lock error when Client thread set Min_ID to redis!");
				} catch (Throwable e) {
					log.warn(e.toString());
				}
			}
		}			
		if (log.isTraceEnabled()) {
			log.trace("set redis MinId value is:{}", value);
		}
	}

	private String readMinIDbyRedis(A2PCustomerInfo cm,String queryMethod) {
		String minId = null;
		boolean flag = false;
		String shortName = cm.getShortName();
		shortName = shortName+"_"+queryMethod; //create new key for redis 
		synchronized(mutex){
			try {
				if (this.gmmsUtility.getDataConnection().getLock()) {
					String rs[] = new String[] { "QUERY_FLAG", "MIN_ID", "ETIME" };
					List<String> list = this.gmmsUtility.getRedisClient()
							.getHashMap(shortName, rs);
					if (list != null && list.size() == 3&&list.get(0)!=null) {
						if ("false".equalsIgnoreCase(list.get(0))) {
							int time = (int) ((System.currentTimeMillis() - Long
									.parseLong(list.get(2))) / 1000);
							if (time >= cm.getMsgQueryInterval()/1000) {
								if (time >= this.gmmsUtility.getMin_ID_expireTime()) {
									minId = list.get(1);
									flag = true;
								}
							}
	
						} else {
							int time = (int) ((System.currentTimeMillis() - Long
									.parseLong(list.get(2))) / 1000);
							if(time< cm.getMsgQueryInterval()/1000){
								flag = false;
							}else{
								flag = true;
							}
							minId = list.get(1);						
						}
						if(flag){
							Map<String, String> map = new HashMap<String, String>();
							map.put("QUERY_FLAG", "false");
							map.put("MIN_ID", list.get(1));
							map.put("ETIME", System.currentTimeMillis() + "");
							this.gmmsUtility.getRedisClient().setHash(shortName, map);
						}					
					} else {
						flag = true;
						minId = this.readFile(shortName);
						if(minId==null){
							minId = "0";
						}
						Map<String, String> map = new HashMap<String, String>();
						map.put("QUERY_FLAG", "false");
						map.put("MIN_ID", minId);
						map.put("ETIME", System.currentTimeMillis() + "");
						this.gmmsUtility.getRedisClient().setHash(shortName, map);
					}
					
				}
			} catch (Exception e) {			
				log.warn("get lock error when client thread get MinID from redis");
			}finally{
				try {
					this.gmmsUtility.getDataConnection().relLock();
				} catch (Exception e2) {
					log.warn("get lock error!");
				}
				
			}
		}
		return flag + ":" + minId;
	}

	private boolean writeRedis(String shortName, Map map)
			throws DataControlException, Throwable {
		boolean flag = false;
		int count = 0;
		if (shortName == null) {
			return false;
		}
		synchronized(mutex){
			while (!flag && count < 50) {
				try {
					if (this.gmmsUtility.getDataConnection().getLock()) {
						count = 0;
						flag = this.gmmsUtility.getRedisClient().setHash(shortName,
								map);					
					} else {
						count++;
						Thread.sleep(510);
						log.info("System wait time to get DBlock when set redis value!");
					}
	
				} catch (Exception e) {
					log.warn("writeRedis value error!");
				}finally{
					try {
						this.gmmsUtility.getDataConnection().relLock();
					} catch (Exception e2) {
						log.warn("get lock error!");
					}
				}
			}
		}
		return flag;
	}

	
	private String readFile(String path) {
		String interffaceConfig = System.getProperty("a2p_home") + "ha/" + path;
		File file = new File(interffaceConfig);
		String minId = "0";
		BufferedReader bw = null;
		try {
			bw = new BufferedReader(new FileReader(file));
			minId = bw.readLine();
		} catch (Exception e) {
			log.error("file is not exist and Exception is {}", e.getMessage());
			return "0";
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (Exception e) {

				}
			}
		}
		return minId;
	}
	/*
	private long getGMTTime(){
		long now = new Date().getTime();
		TimeZone local = TimeZone.getDefault();
		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}
		long gmtNow = now - diff;
		return gmtNow;
	}*/
}
