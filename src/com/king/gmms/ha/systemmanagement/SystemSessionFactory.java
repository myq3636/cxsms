package com.king.gmms.ha.systemmanagement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForMGT;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.systemmanagement.pdu.ModuleRegisterAck;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.gmms.util.SystemConstants;

public class SystemSessionFactory {
	protected Map<String,List<SystemSession>> sessionMapWithType = null;//moduleType,List
	protected Map<String,SystemSession> sessionMapWithName = null;//moduleName,List
	protected ModuleManager moduleManager = null;
	protected String selfModule = null;
	protected GmmsUtility gmmsUtility  = null;
	private static SystemSessionFactory instance = null;
	private static SystemLogger log = SystemLogger.getSystemLogger(SystemSessionFactory.class);
    private boolean isRegister = false;
    protected Map<String,LinkedBlockingQueue<SystemPdu>> messageQueueMap;


	private SystemSessionFactory(){
		gmmsUtility = GmmsUtility.getInstance();
		selfModule = System.getProperty("module");
		moduleManager = ModuleManager.getInstance();
		sessionMapWithType = new ConcurrentHashMap<String,List<SystemSession>>();
		sessionMapWithName = new ConcurrentHashMap<String,SystemSession>();
		messageQueueMap = new ConcurrentHashMap<String,LinkedBlockingQueue<SystemPdu>>();
	}
	
	public synchronized static SystemSessionFactory getInstance(){
		if(null == instance){
			instance = new SystemSessionFactory();
			instance.initSession();
		}
		return instance;
	}
	/**
     * init system client for gateway modules
     */
	private void initSession(){
    	String selfModuleType = moduleManager.getModuleType(selfModule);
    	if(SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(selfModuleType)){
    		List<ModuleConnectionInfo>  routerConInfos = moduleManager.getConnectionInfo4Router();
    		initSessionMap(SystemConstants.ROUTER_MODULE_TYPE,routerConInfos);//init for router
    		List<ModuleConnectionInfo>  serverConInfos = moduleManager.getConnectionInfo4Server();
    		initSessionMap(SystemConstants.SERVER_MODULE_TYPE,serverConInfos);//init for server
    		List<ModuleConnectionInfo>  clientConInfos = moduleManager.getConnectionInfo4Client();
    		initSessionMap(SystemConstants.CLIENT_MODULE_TYPE,clientConInfos);//init for client
    		List<ModuleConnectionInfo>  mqmConInfos = moduleManager.getConnectionInfo4MQM();
    		initSessionMap(SystemConstants.MQM_MODULE_TYPE,mqmConInfos);//init for client
    		ModuleConnectionInfo masterGMT = moduleManager.getConnectionInfoOfMasterMGT();
    		ModuleConnectionInfo slaveGMT = moduleManager.getConnectionInfoOfSlaveMGT();
    		if(slaveGMT!=null){
    			List<ModuleConnectionInfo>  mgtConInfos = new ArrayList<ModuleConnectionInfo>();
    			if(slaveGMT.getModuleName().equalsIgnoreCase(selfModule)){//this is slave MGT
    				mgtConInfos.add(masterGMT);
    			}else{
    				mgtConInfos.add(slaveGMT);
    			}
	    		initSessionMap(SystemConstants.MGT_MODULE_TYPE,mgtConInfos);//init for mgt
    		}
    	}else if(SystemConstants.SERVER_MODULE_TYPE.equalsIgnoreCase(selfModuleType)
    			||SystemConstants.CLIENT_MODULE_TYPE.equalsIgnoreCase(selfModuleType)
    			||SystemConstants.MQM_MODULE_TYPE.equalsIgnoreCase(selfModuleType)
    			||SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(selfModuleType)){
    		List<ModuleConnectionInfo>  mgtConInfos = moduleManager.getConnectionInfo4MGT();
    		initSessionMap4Function(SystemConstants.MGT_MODULE_TYPE,mgtConInfos);
    	}
    }
    /**
     * initClientMap
     * @param moduleType
     * @param conInfos
     */
    private void initSessionMap(String moduleType, List<ModuleConnectionInfo>  conInfos){
    	List<SystemSession> sessionList = new ArrayList<SystemSession>();
    	for(ModuleConnectionInfo cinfo:conInfos){
    		try{
	    		 if(!messageQueueMap.containsKey(cinfo.getModuleName())){
	             	LinkedBlockingQueue<SystemPdu> msgQueue = new LinkedBlockingQueue<SystemPdu>();
	             	messageQueueMap.put(cinfo.getModuleName(), msgQueue);
	             }
            	SystemSession systemSession = new SystemSession(cinfo);
                sessionMapWithName.put(cinfo.getModuleName(), systemSession);
            	sessionList.add(systemSession);
            	systemSession.start();
            	Thread.sleep(100);
            }catch(Exception e){
            	log.warn("Start client failed:",e);
            	continue;
            }
    	}
    	if(!sessionList.isEmpty()){
    		sessionMapWithType.put(moduleType, sessionList);//moduleType,systemClient
    	}
    }
    /**
     * initClientMap
     * @param moduleType
     * @param conInfos
     */
    private void initSessionMap4Function(String moduleType, List<ModuleConnectionInfo>  conInfos){
    	List<SystemSession> sessionList = new ArrayList<SystemSession>();
		ModuleConnectionInfo masterGMT = moduleManager.getConnectionInfoOfMasterMGT();
    	for(ModuleConnectionInfo cinfo:conInfos){
    		try{
	    		 if(!messageQueueMap.containsKey(cinfo.getModuleName())){
	             	LinkedBlockingQueue<SystemPdu> msgQueue = new LinkedBlockingQueue<SystemPdu>();
	             	messageQueueMap.put(cinfo.getModuleName(), msgQueue);
	             }
            	SystemSession systemSession = new SystemSession(cinfo);
                sessionMapWithName.put(cinfo.getModuleName(), systemSession);
            	sessionList.add(systemSession);
            	if(masterGMT.getModuleName().equals(cinfo.getModuleName())){
            		systemSession.start();
                	Thread.sleep(100);
            	}
            }catch(Exception e){
            	log.warn("Start client failed:",e);
            	continue;
            }
    	}
    	if(!sessionList.isEmpty()){
    		sessionMapWithType.put(moduleType, sessionList);//moduleType,systemClient
    	}
    }
	public Map<String, List<SystemSession>> getSessionMapWithType() {
		return sessionMapWithType;
	}

	public Map<String, SystemSession> getSessionMapWithName() {
		return sessionMapWithName;
	}
	/**
	 * getSessionsByModuleType
	 * @param moduleType
	 * @return
	 */
	public List<SystemSession> getAliveSessionsByModuleType(String moduleType){
		List<SystemSession> sessions  = sessionMapWithType.get(moduleType);
		List<SystemSession> rtSessions = new ArrayList<SystemSession>();
		if(sessions!=null){
			for(SystemSession session:sessions){
				String moduleName = session.getModuleName();
				if(moduleManager.isAliveModule(moduleName)){
					rtSessions.add(session);
				}
			}
		}
		return rtSessions;
	}
	/**
	 * getSessionsByModuleType
	 * @param moduleType
	 * @return
	 */
	public List<SystemSession> getSessionsByModuleType(String moduleType){
		List<SystemSession> sessions  = sessionMapWithType.get(moduleType);
		List<SystemSession> rtSessions = new ArrayList<SystemSession>();
		if(sessions!=null){
			for(SystemSession session:sessions){
					rtSessions.add(session);
			}
		}
		return rtSessions;
	}
	/**
	 * getSessionsByModuleType
	 * @param moduleType
	 * @return
	 */
	public List<SystemSession> getSessionsForModuleWithDB(){
		List<SystemSession> coreSessions  = getSessionsByModuleType(SystemConstants.ROUTER_MODULE_TYPE);
		List<SystemSession> mqmSessions = getSessionsByModuleType(SystemConstants.MQM_MODULE_TYPE);
		List<SystemSession> mgtSessions = getSessionsByModuleType(SystemConstants.MGT_MODULE_TYPE);
		List<SystemSession> commonServerSessions = getSessionsByModuleType(SystemConstants.SERVER_MODULE_TYPE);
		List<SystemSession> MulitClientSessions = getSessionsByModuleType(SystemConstants.CLIENT_MODULE_TYPE);
		coreSessions.addAll(mqmSessions);
		coreSessions.addAll(mgtSessions);
		coreSessions.addAll(commonServerSessions);
		coreSessions.addAll(MulitClientSessions);
		return coreSessions;
	}
	
	/**
	 * getSessionByModuleName
	 * @param moduleName
	 * @return
	 */
	public SystemSession getSessionByModuleName(String moduleName) {
		if(moduleManager.isAliveModule(moduleName)){
			return sessionMapWithName.get(moduleName);
		}else{
			return null;
		}
	}

	public SystemSession getSystemSessionForFunction(){
		ModuleConnectionInfo cinfo = moduleManager.getConnectionInfoOfMasterMGT();
		if(cinfo==null){
			 cinfo = moduleManager.getConnectionInfoOfSlaveMGT();
			 if(cinfo==null){
				 return null;
			 }
		}
		return sessionMapWithName.get(cinfo.getModuleName());
	}
	/**
	 * slave connect to master
	 * @return
	 */
	public SystemSession getSystemSessionOfMGT(){
		List<SystemSession> sessions =  sessionMapWithType.get(SystemConstants.MGT_MODULE_TYPE);
		if(sessions==null||sessions.isEmpty()){
			return null;
		}
		return sessions.get(0);
	}
	
	public void startConfirmScanThread() {
		String selfModuleType = moduleManager.getModuleType(selfModule);
    	if(SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(selfModuleType)){
    		ConnectionManagementForMGT.getInstance().startConfirmScanThread();
    	}
	}
	public void shutdownConfirmScanThread(){
		String selfModuleType = moduleManager.getModuleType(selfModule);
    	if(SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(selfModuleType)){
    		ConnectionManagementForMGT.getInstance().shutdownConfirmScanThread();
    	}
	}
	/**
	 * is module register successful
	 * @return
	 */
	public boolean isRegister() {
        String moduleType = moduleManager.getModuleType(selfModule);
		if(SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)){
			return true;
		}
		return isRegister;
	}
	/**
	 * is module register successful
	 */
	public void setRegister(boolean isRegister) {
		this.isRegister = isRegister;
	}

	/**
	 * moduleRegister returned ack when module startup
	 */
	public ModuleRegisterAck moduleRegisterInDetail() {
		ModuleRegisterAck ack = null;
		List<SystemSession> sessions =  sessionMapWithType.get(SystemConstants.MGT_MODULE_TYPE);
		if(sessions==null||sessions.isEmpty()){
			return null;
		}else if(sessions.size()==1){
			return sessions.get(0).moduleRegisterInDetail();
		}else{
			int index = 0 ;
			while(ack==null){
				SystemSession session = sessions.get(index);
				if(!session.isKeepRunning()){
					session.start();
				}
				ack = session.moduleRegisterInLimit();
				index = 1-index;
			}
		}
		return ack;
	}
	/**
	 * moduleRegister when module startup
	 */
	public boolean moduleRegister() {
		ModuleRegisterAck ack = moduleRegisterInDetail();
		if(ack!=null){
			return ack.getResponseCode() == 0;
		}else{
			return false;
		}
	}

	public LinkedBlockingQueue<SystemPdu> getMessageQueue(String moduleName) {
		return messageQueueMap.get(moduleName);
	}
}
