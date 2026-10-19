package com.king.db;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Observable;
import java.util.Properties;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;

/**
 * 
 * @author Jianming Yang
 * @version 2010-10-12
 */
public abstract class DBHeartBeat extends Observable implements Runnable {
	protected static SystemLogger log = SystemLogger
			.getSystemLogger(DBHeartBeat.class);
	protected boolean initialed = false;
	private volatile boolean running = false;
	protected volatile DatabaseStatus dbStatus = DatabaseStatus.MASTER_UP;// TODO: need read db_status file
	private static int retryLimit = 3;
	private volatile int retryCount = 0;
	private long interval = 5000L; // 5 seconds
	private Object mutex = new Object();
	protected String dbName = null;
	private static Properties dbprop = new Properties();
	private static Properties prop = new Properties();
	protected DataControl dataControl = DataControl.getInstance();
	protected Connection connection = null;
	protected 		Statement stmt = null;

	/**
	 * init properties and create db connection
	 * 
	 * @param dbName
	 */
	protected void init(String dbName) {
		log.trace("initialed:{} with dbName {}", initialed , dbName);
		if (initialed) {
			return;
		}
		this.dbName = dbName;
		String a2phome = System.getProperty("a2p_home", "/usr/local/a2p/");
		if (a2phome == null) {
			System.out.println("No a2p_home definited!");
			System.exit(-1);
		} else if (!a2phome.endsWith("/")) {
			a2phome = a2phome + "/";
			System.setProperty("a2p_home", a2phome);
		}
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
		gmmsUtility
				.initUtility(a2phome + DBHAConstants.GMMS_FILE_RELATIVE_PATH);
		try {
			prop.load(new FileInputStream(a2phome
					+ "Gmms/GmmsConfig.properties"));
			dbprop = DatabaseStatus.readDBStatus();
			dbStatus = DatabaseStatus.get(dbName + "_"
					+ dbprop.getProperty(dbName));
			log.debug("{} dbStatus:{}",dbName, dbStatus);
			String limitStr = gmmsUtility.getCommonProperty("DataControl_FailureLimit", "2");
			retryLimit = Integer.parseInt(limitStr);
			String intervalInSeconds = gmmsUtility.getCommonProperty("DataControl_MonitorInterval", "10");
			interval = Long.parseLong(intervalInSeconds)*1000L;
		} catch (Exception e) {
			log.debug(e, e);
			System.exit(-1);
		}
		initialed = true;
	}


	protected abstract void getConnection() throws Exception;

	public void run() {
		while (running) {
			try {
				Thread.sleep(interval); // M-12: 用静态 Thread.sleep，去掉废弃的实例调用写法
				// H-11: 每轮重验缓存连接(isValid 带超时，可快速发现半开 socket)，失效则关闭重建。
				if (connection == null || !connection.isValid(2)) {
					this.closeSession();
					this.getConnection();
				}
				if (connection != null && stmt!=null) {
					boolean exeStatus = stmt.execute("select 1=1");
					if (this.dbStatus.toString().endsWith("_DOWN") && exeStatus) { // find DB recovered
						changeStatus();
					} else if(exeStatus) {
						synchronized (mutex) {
							retryCount=0;
						} 
					}
					log.trace("try when dbStatus:{}", this.dbStatus);
				}else{
					throw new DataControlException("Cannot open connection");
				}
			} catch (Exception e) {
				log.trace("closeSession when dbStatus:{}",this.dbStatus);
				try{
					this.closeSession();
					this.closeSessionFactory();
					if (this.dbStatus.toString().endsWith("_UP")) {
						exceptionHandler(e);
					} else {
						log.trace("db connect failed when dbStatus:{}", dbStatus);
					}
				}catch(Exception ex){
					log.error(ex.getMessage());
				}
			}
		}
		this.closeSession();
		this.closeSessionFactory();
	}

	public void start() {
		log.info("start DBHeartBeat thread for {}!", this.dbName);
		synchronized (mutex) {
			if (running) {
				return;
			}
			running = true;
			Thread thread = new Thread(this, dbName + "HeartBeat");
			thread.start();
		}
	}

	public void stop() {
		synchronized (mutex) {
			running = false;
		}
	}

	/**
	 * Close current session
	 */
	public void closeSession() {
		log.trace("start closeSession for {}...", dbName);
		try {
			if (stmt != null) {
				this.stmt.close();
				this.stmt = null;
			}
			if (connection != null) {
				DataControl.releaseConnection(resolveHeartbeatDsName(), this.connection);
				this.connection = null;
			}
		} catch (Exception ex) {
			log.error("Close session exception:", ex);
		}
	}

	/**
	 * Close current session factory
	 */
	public void closeSessionFactory() {
		log.trace("skip closeSessionFactory for {}, JDBC pool is managed by DataControl.", dbName);
	}

	private String resolveHeartbeatDsName() {
		if (DBHAConstants.SLAVE_KEY.equals(dbName)) {
			return "backupgmms";
		}
		return "gmms";
	}

	public void notifyDBStatus(DatabaseStatus status) {
		synchronized (mutex) {
			setChanged();
			notifyObservers(status);
		}
	}

	/**
	 * handle DB exception
	 * 
	 * @param e
	 */
	public void exceptionHandler(Exception e) {
		String message = e.getMessage();
		log.debug("exception:{}.retryCount:{}", message, retryCount);
		if ((null == message)
				|| message.contains("Communications link failure")
				|| message.contains("Connection reset")
				|| message.contains("connection abort")
				|| message.contains("Cannot open connection")
				|| message.contains("Connection refused")
				|| message
						.contains("Software caused connection abort: recv failed")) {
			changeStatus();
		}
	}
	
	private void changeStatus() {
		// H-12: 自增、阈值判定、状态翻转放在同一临界区，避免阈值判定在锁外导致的双翻转/漏触发；
		// 观察者通知放到锁外，避免持锁期间执行 SystemManager failover 回调。
		DatabaseStatus newStatus = null;
		synchronized (mutex) {
			if (++retryCount < this.retryLimit) {
				return;
			}
			retryCount = 0;
			DatabaseStatus oldstat = this.dbStatus;
			int ordinal = this.dbStatus.ordinal();// up<-->down
			ordinal += (ordinal % 2 == 0) ? 1 : -1;
			this.dbStatus = DatabaseStatus.get(Integer.toString(ordinal));
			newStatus = this.dbStatus;
			log.trace("changeStatus:oldstatus={};dbStatus={}", oldstat, this.dbStatus);
		}
		notifyDBStatus(newStatus);
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
}
