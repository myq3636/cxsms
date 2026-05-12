package com.king.gmms.ha.systemmanagement;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Properties;

import com.king.db.DBHAConstants;
import com.king.db.DBHeartBeat;
import com.king.db.DataControl;
import com.king.db.DatabaseStatus;
import com.king.db.MasterHeartBeat;
import com.king.db.SlaveHeartBeat;
import com.king.framework.A2PService;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForMGT;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.HATools;
import com.king.gmms.ha.systemmanagement.pdu.ChangeDB;
import com.king.gmms.ha.systemmanagement.pdu.ChangeRedis;
import com.king.gmms.ha.systemmanagement.pdu.ModuleRegisterAck;
import com.king.gmms.ha.systemmanagement.pdu.SystemPdu;
import com.king.redis.RedisMonitor;

public class SystemManager implements A2PService, Runnable, Observer,LifecycleListener {
	protected GmmsUtility gmmsUtility;
	protected volatile boolean running;
	protected String module;
	protected SystemListener systemListener = null;
	private Thread serverThread;
	private SystemSessionFactory sysFactory = null;
	protected SystemSession systemSession = null; // system client
	protected int port; // system listener port
	protected boolean canHandover = false;
	private MasterHeartBeat masterHeartBeat = null;
	private SlaveHeartBeat slaveHeartBeat = null;
	private RedisMonitor redisMonitor = null;
	private volatile DatabaseStatus dbStatus4Master = DatabaseStatus.MASTER_UP;
	private volatile DatabaseStatus dbStatus4Slave = DatabaseStatus.SLAVE_UP;
	private volatile DatabaseStatus dbStatus4Used = DatabaseStatus.MASTER_USED;
    private static SystemLogger log = SystemLogger.getSystemLogger(SystemManager.class);
    private LifecycleSupport lifecycle;
    private boolean redisHandoverFlag =false;
	
    public SystemManager() {
		gmmsUtility = GmmsUtility.getInstance();
		running = true;
		systemListener = SystemListener.getInstance();
		module = System.getProperty("module");
		sysFactory = SystemSessionFactory.getInstance();
		redisMonitor = RedisMonitor.getInstance();
		initLifecycle();
	}

	/**
	 * start the thread
	 */
	public void run() {
		systemListener.start();
	}
	
	private void initLifecycle(){
		lifecycle = gmmsUtility.getLifecycleSupport();
		if(lifecycle!=null){
    		lifecycle.addListener(Event.TYPE_SWITCHDB, this, 1);
    		lifecycle.addListener(Event.TYPE_SWITCHREDIS, this, 1);
    	}else{
    		log.error("lifecycle is null!");
    	}
	}
	public int OnEvent(Event event) {
		log.trace("Event Received. Type: {}", event.getEventType());
		if (canHandover && (event.getEventType() == Event.TYPE_SWITCHDB)) {
			if(event.getEventSubType()==1){//master
				switch2Master();
			}else if(event.getEventSubType()==2){//slave
				switch2Slave();
			}
		} else if (redisHandoverFlag && (event.getEventType() == Event.TYPE_SWITCHREDIS)) {
			if(event.getEventSubType()==1){//master
				switchRedisStatus("M");
			}else if(event.getEventSubType()==2){//slave
				switchRedisStatus("S");
			}
		}
		return 0;
	}
	
	public void startHeartBeat() {
		loadDBstatus();
		gmmsUtility.initDBManager(dbStatus4Used);
		DataControl dataControl = DataControl.getInstance();
		canHandover = dataControl.getCanHandover();
		if (canHandover) {
			masterHeartBeat = MasterHeartBeat.getInstance();
			masterHeartBeat.addObserver(this);
			masterHeartBeat.start();
			slaveHeartBeat = SlaveHeartBeat.getInstance();
			slaveHeartBeat.addObserver(this);
			slaveHeartBeat.start();
		}
	}

	public void stopHeartBeat() {
		if (canHandover) {
			masterHeartBeat.stop();
			slaveHeartBeat.stop();
		}
	}

	private void startRedisMonitor(){
		redisHandoverFlag = Boolean.valueOf(gmmsUtility.getCommonProperty("Redis_Handover_flag", "false"));
		if(redisHandoverFlag){
			redisMonitor.addObserver(this);
			//log.debug("redisMonitor start!");
			redisMonitor.startMonitor();
			//log.debug("redisMonitor end!");			
		}
		gmmsUtility.initRedisClient(redisMonitor.getMasterOrSlave());
	}
	private void stopRedisMonitor(){
		if(redisHandoverFlag){
			redisMonitor.stopMonitor();
		}
		
	}
	/**
	 * startService
	 */
	public boolean startService() {
		boolean isRegister = false;
		try {
			ModuleManager moduleManager = gmmsUtility.getModuleManager();
			ModuleConnectionInfo moduleInfo = moduleManager
					.getConnectionInfoOfSlaveMGT();
			if (moduleInfo != null) {
				if (module.equalsIgnoreCase(moduleInfo.getModuleName())) {// slave
					try {
						systemSession = sysFactory
								.getSystemSessionOfMGT();
						if (systemSession != null) {
							ModuleRegisterAck ack = systemSession.moduleRegisterInLimit();
							if(ack!=null && ack.getResponseCode()==0){
								isRegister=true;
							}
							if (isRegister) {
								syncDBStatus(ack);
								ModuleConnectionInfo masterMGT = moduleManager
										.getConnectionInfoOfMasterMGT();
								if (masterMGT != null) {// can register indicate master alive
									moduleManager
											.updateModuleStatus2Up(masterMGT
													.getModuleName());
								}
							} 
						}
					} catch (Exception e) {
						log.error(e,e);
					}
				} else {
					systemSession = sysFactory.getSystemSessionOfMGT();
					if (systemSession != null) {
						ModuleRegisterAck ack = systemSession.moduleRegisterInLimit();
						if(ack!=null && ack.getResponseCode()==0){
							isRegister=true;
						}
						if(isRegister){
							syncDBStatus(ack);
						}
					}
				}
			}
			if(!isRegister){
				log.warn("module register failed!");
			}
			serverThread = new Thread(A2PThreadGroup.getInstance(), this,
					module);
			serverThread.start();
			sysFactory.startConfirmScanThread();
			startHeartBeat();
			startRedisMonitor();
			log.info("{} starting...",module);
			return true;
		} catch (Exception ex) {
			log.fatal("SystemManager initialize fail!", ex);
			System.exit(-1);
			return false;
		}
	}

	/**
	 * stopService
	 */
	public boolean stopService() {
		running = false;
		try {
			ModuleConnectionInfo moduleInfo = gmmsUtility.getModuleManager()
					.getConnectionInfoOfSlaveMGT();
			if (moduleInfo != null) {// has slave
				systemSession.moduleStop();
			}
			serverThread.join();
			systemListener.stop();
			sysFactory.shutdownConfirmScanThread();
			stopRedisMonitor();
			stopHeartBeat();
		} catch (Exception e) {
			log.warn(e, e);
		}
			log.info("{} stopped!",module);
		return true;
	}

	/**
	 * update db status
	 */
	public void update(Observable o, Object arg) {
		if (o instanceof DBHeartBeat && arg instanceof DatabaseStatus) {
			DatabaseStatus st = (DatabaseStatus) arg;
			DBHeartBeat heartbeat = (DBHeartBeat) o;
			updateDatabaseStatus(heartbeat, st);
		} else if(o instanceof RedisMonitor&&arg instanceof String){
			String flag = (String)arg;
			//log.debug("update redis status: {}",flag);
			announceRedisHandleOver(flag);
		}else {
			log.warn("Error arguements");
		}
	}

	private void loadDBstatus() {
		Properties dbprop = DatabaseStatus.readDBStatus();
		if(dbprop.isEmpty()){
			log.warn("loadDBstatus with empty db properties.");
			return;
		}
		dbStatus4Master = DatabaseStatus.get("Master_"
				+ dbprop.getProperty(DBHAConstants.MASTER_KEY));
		dbStatus4Slave = DatabaseStatus.get("Slave_"
				+ dbprop.getProperty(DBHAConstants.SLAVE_KEY));
		dbStatus4Used = DatabaseStatus.get(dbprop
				.getProperty(DBHAConstants.USED_KEY)
				+ "_used");
		log.info("loadDBstatus-->Master={};Slave={};Used={}" 
				,dbStatus4Master,dbStatus4Slave,dbStatus4Used);
	}

	private void updateDatabaseStatus(DBHeartBeat heartbeat,
			DatabaseStatus status) {
		if ((DBHAConstants.MASTER_KEY.equalsIgnoreCase(heartbeat.getDbName()) && (status == dbStatus4Master))
				|| (DBHAConstants.SLAVE_KEY.equalsIgnoreCase(heartbeat.getDbName()) && (status == dbStatus4Slave))) {
			log.debug("Already updated!heartbeat.dbName={};status={};dbStatus4Slave={}" ,
							heartbeat.getDbName(),status ,dbStatus4Slave);
			return; // new status is same to current db status
		}
		switch (status) {
			case MASTER_UP:
				if (this.dbStatus4Slave.equals(DatabaseStatus.SLAVE_DOWN)) {
					this.switch2Master();// if slave is down,switch to master now
				}
				this.dbStatus4Master = status;
				HATools.updateDBStatus2File(status);
				break;
			case MASTER_DOWN:// find master is down,so switch 2 slave
				this.dbStatus4Master = status;
				if (DatabaseStatus.MASTER_USED.equals(this.dbStatus4Used)) {
					switch2Slave();
				}
				HATools.updateDBStatus2File(status);
				break;
			case SLAVE_UP:
				// if master is down in the same time,switch to slave
				if (DatabaseStatus.MASTER_DOWN.equals(this.dbStatus4Master)) {
					this.switch2Slave();
				}
				this.dbStatus4Slave = status;
				HATools.updateDBStatus2File(status);
				break;
			case SLAVE_DOWN:
				// if slave is used before it is down,switch to master
				if (DatabaseStatus.SLAVE_USED.equals(this.dbStatus4Used)) {
					this.switch2Master();
				}
				this.dbStatus4Slave = status;
				HATools.updateDBStatus2File(status);
				MailSender.getInstance().sendAlertMail("A2P alert mail from SMG",
						"The Slave DB is DOWN!", null);
				break;
			case MASTER_USED: // switch DB by event from UI
				this.switch2Master();
				break;
			case SLAVE_USED:// switch DB by event from UI
				this.switch2Slave();
				break;
			default:
				break;
		}
	}

	/**
	 * switch to db status
	 * 
	 * @return
	 */
	private boolean syncDBStatus(ModuleRegisterAck ack) {
		String dbstatusStr = ack.getDbStatus();
		DatabaseStatus dbStatus = DatabaseStatus.get(dbstatusStr);
		String redisStatus = ack.getRedisStatus();
		Boolean result = false;
		if (!dbStatus.equals(dbStatus4Used)) {
			this.dbStatus4Used = dbStatus;
			DataControl.getInstance().setUsedDatabaseStatus(dbStatus4Used);
			HATools.updateDBStatus2File(dbStatus4Used);
			result = true;
			log.info("Update {} to {}" ,dbStatus,dbStatus4Used);
		} else {
			log.info("Abort,sync DBStatus from {} to {}." ,dbStatus4Used, dbStatus);
		}
		result = setRStatus2File(redisStatus);	
		if(result){
			redisMonitor.setMasterOrSlave(redisStatus);
		}
		gmmsUtility.setRedisClientFlag(redisStatus);
		return result;
	}
	/**
	 * switch to master db
	 * 
	 * @return
	 */
	private boolean switch2Master() {
		log.info("Start to Switch2Master....");
		if (DatabaseStatus.MASTER_UP.equals(dbStatus4Master)
				&& !DatabaseStatus.MASTER_USED.equals(dbStatus4Used)) {
			announceDatabaseHandleOver(DBHAConstants.SWITCH2MASTER);
			this.dbStatus4Used = DatabaseStatus.MASTER_USED;
			DataControl.getInstance().setUsedDatabaseStatus(dbStatus4Used);
			HATools.updateDBStatus2File(dbStatus4Used);
			return true;
		} else {
			log.info("Abort!Master DB is {},Used= {} when switch to master!" ,
					dbStatus4Master,dbStatus4Used);
		}
		return false;
	}

	/**
	 * switch to slave db
	 * 
	 * @return
	 */
	private boolean switch2Slave() {
		log.info("Start to Switch2Slave....");
		if (DatabaseStatus.SLAVE_UP.equals(dbStatus4Slave)
				&& !DatabaseStatus.SLAVE_USED.equals(dbStatus4Used)) {
			announceDatabaseHandleOver(DBHAConstants.SWITCH2SLAVE);
			this.dbStatus4Used = DatabaseStatus.SLAVE_USED;
			DataControl.getInstance().setUsedDatabaseStatus(dbStatus4Used);
			HATools.updateDBStatus2File(dbStatus4Used);
			return true;
		} else {
			log.info("Abort! Slave DB is {},Used={} when switch to slave!" , 
					dbStatus4Slave,dbStatus4Used);
		}
		return false;
	}
	/**
	 * announceDatabaseHandleOver
	 * @param change2DB
	 */
	private void announceDatabaseHandleOver(String change2DB){
		List<SystemSession> sessions  = sysFactory.getSessionsForModuleWithDB();
		if(sessions==null||sessions.isEmpty()){
			return;
		}
		ChangeDB pdu = (ChangeDB)SystemPdu.createPdu(SystemPdu.COMMAND_CHANGEDB);
		pdu.setAction(change2DB);
		for(SystemSession session : sessions){
			try{
				session.send(pdu);
			}catch(Exception e){
				log.error(e.getMessage());
			}
		}
	}
	private void announceRedisHandleOver(String change2Redis){
		ConnectionManagementForMGT.getInstance().updateRedisPropParameter(change2Redis);
		gmmsUtility.setRedisClientFlag(change2Redis);
		List<SystemSession> sessions  = sysFactory.getSessionsForModuleWithDB();
		if(sessions==null||sessions.isEmpty()){
			return;
		}
		ChangeRedis pdu = (ChangeRedis)SystemPdu.createPdu(SystemPdu.COMMAND_CHANGEREDIS);
		pdu.setAction(change2Redis);
		for(SystemSession session : sessions){
			try{
				session.send(pdu);
			}catch(Exception e){
				log.error(e.getMessage());
			}
		}
	}
	public boolean isRunning() {
		return running;
	}
			
	private boolean switchRedisStatus(String s){
		log.info("Switch Redis to  {}....",s);	
		/*boolean result = false;
		redisMonitor.setMasterOrSlave(s);
		result = setRStatus2File(s);
		announceRedisHandleOver(s);	*/					
		return redisMonitor.switchRedis(s);				
	}
	
	private boolean setRStatus2File(String redisStatus){
        boolean result = false;
		String filePath = gmmsUtility.getRedisFile();
		Properties redisProp = new Properties();
		FileInputStream fileInputStream;
		try {
			File file = new File(filePath);
			if(!file.exists()){
				file.createNewFile();
			}			
			fileInputStream = new FileInputStream(filePath);
			redisProp.load(fileInputStream);			
		} catch (Exception e) {
			log.error("load redis status error!");
		}
		String fileRedisStatus = redisProp.getProperty("Used");		
		try {			
			if(!redisStatus.equalsIgnoreCase(fileRedisStatus)){
				OutputStream fos = new FileOutputStream(filePath);
				redisProp.setProperty(DBHAConstants.USED_KEY, redisStatus);
				redisProp.store(fos, "Update "+DBHAConstants.USED_KEY+" to file.");
			}
			result = true;
		} catch (Exception e) {
			log.error("system sync redis status error!");
		}
		return result;
	}
}
