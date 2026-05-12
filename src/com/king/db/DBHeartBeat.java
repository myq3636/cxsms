package com.king.db;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Observable;
import java.util.Properties;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

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
	private boolean running = false;
	protected DatabaseStatus dbStatus = DatabaseStatus.MASTER_UP;// TODO: need read db_status file
	private static int retryLimit = 3;
	private int retryCount = 0;
	private long interval = 5000L; // 5 seconds
	private Object mutex = new Object();
	protected String dbName = null;
	private static Properties dbprop = new Properties();
	private static Properties prop = new Properties();
	protected DataControl dataControl = DataControl.getInstance();
	protected SessionFactory factory = null;
	protected Connection connection = null;
	protected 		Statement stmt = null;
	protected ThreadLocal session = new ThreadLocal();

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
				Thread.currentThread().sleep(interval);
				if (null == connection) {
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
					throw new HibernateException("Cannot open connection");
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
				this.connection.close();
				this.connection = null;
			}
			Session s = (Session) session.get();
			session.set(null);
			if (s != null && s.isOpen()) {
				s.close();
			}
		} catch (Exception ex) {
			log.error("Close session exception:", ex);
		}
	}

	/**
	 * Close current session factory
	 */
	public void closeSessionFactory() {
		log.trace("start closeSessionFactory for {}...", dbName);
		try {
			if (factory != null && !factory.isClosed()) {
				factory.close();
				factory = null;
			}
		} catch (Exception ex) {
			log.error("Close session exception:", ex);
		}
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
		synchronized (mutex) {
			retryCount++;
		}
		if (retryCount >= this.retryLimit) {//get property
			log.trace("enter changeStatus...");
			DatabaseStatus oldstat = this.dbStatus;
			synchronized (mutex) {
				retryCount = 0;
				int ordinal = this.dbStatus.ordinal();// up<-->down
				if (ordinal % 2 == 0) {
					ordinal++;
				} else {
					ordinal--;
				}
				this.dbStatus = DatabaseStatus.get(Integer.toString(ordinal));
				log.trace("changeStatus:oldstatus={};dbStatus={}",
						oldstat, this.dbStatus);
			}
			notifyDBStatus(this.dbStatus);
		}
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
}
