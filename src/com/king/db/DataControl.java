package com.king.db;

/**
 * <p>Title: </p>
 * DataControl
 * <p>Description: </p>
 * Data Control
 * <p>Copyright: Copyright (c) 2005</p>
 *
 * <p>Company: </p>
 * King Inc.
 * @version 2.0
 */
import java.io.FileInputStream;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.tool.hbm2ddl.SchemaExport;

import com.king.framework.SystemLogger;

public class DataControl {
	private static SystemLogger log = SystemLogger.getSystemLogger(DataControl.class);
	private static boolean initialized = false;
	private static Hashtable dsConfigs;
	private static Properties mprop;
	private static Hashtable dataManagers = new Hashtable();
	private static DataControl instance = new DataControl();
	// private static DBMonitor dbMonitor;
	private static DatabaseStatus dbStatus4Used = DatabaseStatus.MASTER_USED;
	private static boolean canHandover = false;

	// private static int failureLimit = 5;

	private DataControl() {
	}

	public static DataControl getInstance() {
		return instance;
	}

	/**
	 * Initiate DataControl.
	 * 
	 * @param configFile
	 * @throws
	 */
	public static void init(String configFile) throws DataControlException {
		Properties prop;
		if (!initialized) {
			try {
				prop = new Properties();
				prop.load(new FileInputStream(configFile));
			} catch (Exception e) {
				throw new DataControlException(
						"Fail to initialize DataControl. Error reading property file.",
						e);
			}
			init(prop,DatabaseStatus.MASTER_USED);
		}
	}

	/**
	 * Initiate DataControl.
	 * 
	 * @param prop
	 * @throws DataControlException
	 */
	public static void init(Properties prop,DatabaseStatus dbstatus) throws DataControlException {
		log.trace("init DataControl with initialized={}", initialized);
		if (!initialized) {
			mprop = prop;

			canHandover = Boolean.parseBoolean(prop.getProperty(
					"DataControl_HandOver", "false"));
			try {
				dsConfigs = readDsNames(prop); // Database names
			} catch (ConfigurationException ex) {
				throw new DataControlException(ex);
			}
			// load db status and set UsedDatabaseStatus to dataControl
			dbStatus4Used = dbstatus;
			createSessionFactories(dsConfigs);
			initialized = true;
		}
	}

	private static void createSessionFactories(Hashtable dsConfigs)
			throws DataControlException {
		String dsName;
		DbObject dsConfig;
		DataManager[] dms;
		SessionFactory[] factories = null;
		Enumeration dsNames = dsConfigs.keys();
		while (dsNames.hasMoreElements()) {
			dsName = (String) dsNames.nextElement();
			if (dsName.startsWith("backup")) {
				continue;
			}
			factories = new SessionFactory[2];
			dsConfig = (DbObject) dsConfigs.get(dsName);
			dms = dsConfig.getDataManagers();
			if (dms == null) {
				throw new DataControlException(
						"No DataManager is configured to be associated with Data Source: "
								+ dsName);
			}
			log.trace("start init master db.dsName={}", dsName);
			if (DatabaseStatus.MASTER_USED.equals(dbStatus4Used)) {
				factories[0] = getSessionFactory(dsConfig);
			}
			if (canHandover && DatabaseStatus.SLAVE_USED.equals(dbStatus4Used)) {
				dsConfig = (DbObject) dsConfigs.get("backup" + dsName);
				if (dsConfig != null) {
					factories[1] = getSessionFactory(dsConfig);
				}
			}
			log.trace("dms.length={}", dms.length);
			for (int i = 0; i < dms.length; i++) {
				// associate datamanager with the corresponding session factory
				log.trace("setSessionFactory for dataManager:{}", i);
				dms[i].setSessionFactory(factories);
				dms[i].setDsName(dsName);
				// put data manager in a hashtable for relater retreval
				dataManagers.put(dms[i].getClass().getName(), dms[i]);
			}
			log.trace("dataManagers size={}", dataManagers.size());
		}
	}

	public static SessionFactory getMasterSessionFactory(String dsName)
			throws DataControlException {
		Enumeration dsNames = dsConfigs.keys();
		DbObject dsConfig = (DbObject) dsConfigs.get(dsName);
		DataManager[] dms = dsConfig.getDataManagers();
		if (dms == null) {
			throw new DataControlException(
					"No DataManager is configured to be associated with Data Source: "
							+ dsName);
		}
		return getSessionFactory(dsConfig);
	}

	public static SessionFactory getSlaveSessionFactory(String dsName)
			throws DataControlException {
		DbObject dsConfig = (DbObject) dsConfigs.get("backup" + dsName);
		return getSessionFactory(dsConfig);
	}

	public static SessionFactory getSessionFactory(DbObject dsConfig) {
		SessionFactory factory = null;
		try {
			Configuration config = (new Configuration()).setProperty(
					"hibernate.connection.driver_class", dsConfig.getDriver())
					.setProperty("hibernate.connection.url", dsConfig.getUrl())
					.setProperty("hibernate.connection.username",
							dsConfig.getUsername()).setProperty(
							"hibernate.connection.password",
							dsConfig.getPassword()).setProperty(
							"hibernate.connection.provider_class", "org.hibernate.connection.C3P0ConnectionProvider"
							).setProperty(
							"hibernate.c3p0.min_size",
							String.valueOf(dsConfig.getMinPoolSize()))
					.setProperty("hibernate.c3p0.max_size",
							String.valueOf(dsConfig.getMaxPoolSize()))
					.setProperty("hibernate.c3p0.timeout",
							String.valueOf(dsConfig.getTimeout())).setProperty(
							"hibernate.c3p0.max_statements",
							String.valueOf(dsConfig.getMaxStatement()))
					.setProperty("hibernate.c3p0.idle_test_period",
							String.valueOf(3000)).setProperty(
							"hibernate.c3p0.unreturnedConnectionTimeout",
							"180").setProperty(
							"hibernate.c3p0.debugUnreturnedConnectionStackTraces",
							"true").setProperty(
							"hibernate.connection.autocommit", "false")
					.setProperty("hibernate.dialect",
							dsConfig.getHibernateDialect());
			SchemaExport schemaExport = new SchemaExport(config);
			schemaExport.create(false, true);
			List l = schemaExport.getExceptions();
			if (l.size() > 0) {
				for (Object object : l) {
					log.info("failed reason:{}", object);
				}
				log.info("buildSessionFactory failed!");
				return null;
			}
			factory = config.buildSessionFactory();
		} catch (Throwable t) {
			log.info(t.getMessage());
		}
		return factory;
	}

	public static DataManager getDataManager(String dmName)
			throws DataControlException {

		DataManager dm = (DataManager) dataManagers.get(dmName);
		if (dm == null)
			throw new DataControlException("Fail to get " + dmName
					+ " from DataControl.");

		// Call the init method of dmName class
		try {
			dm.initDataManager();
		} catch (Exception ex) {
			throw new DataControlException(
					"Throw exceptions when called the initDataManager() method in "
							+ dmName + "class: " + ex.getMessage());
		}
		return dm;
	}

	private static Hashtable readDsNames(Properties prop)
			throws ConfigurationException {
		dsConfigs = new Hashtable();
		String strDsNames = prop.getProperty("DataControl_DS_Names", null);
		if (strDsNames == null) {
			throw new ConfigurationException(
					"No data source configured to be connected.");
		} else {
			strDsNames = strDsNames.trim();
		}
		StringTokenizer dbNames = new StringTokenizer(strDsNames, ",");
		String dsName = null;
		String prefix = null;
		while (dbNames.hasMoreTokens()) {
			dsName = dbNames.nextToken().trim();
			// Let DSConfig further parse the properties file
			dsConfigs.put(dsName, new DbObject(prop, dsName));
			prefix = "backup" + dsName;
			if (canHandover
					&& prop.getProperty("DS_backup" + dsName + "_Driver") != null) {
				dsConfigs.put(prefix, new DbObject(prop, prefix, true));
			}
		}
		return dsConfigs;
	}

	public static boolean isInitialized() {
		return initialized;
	}

	public static Properties getProp() {
		return mprop;
	}

	public synchronized void setHandover(DatabaseStatus dbStatus) {
		if(dbStatus==dbStatus4Used){
			return;
		}
		log.warn("DataControl start handover,set DB status to {}", dbStatus);
		this.dbStatus4Used = dbStatus;
		Iterator iter = dataManagers.values().iterator();
		while (iter.hasNext()) {
			DataManager dm = (DataManager) (iter.next());
			log.trace("dataManagers start closeIdelFactory!");
			dm.closeIdelFactory(dbStatus);
		}
	}

	public DatabaseStatus getUsedDatabaseStatus() {
		return this.dbStatus4Used;
	}

	public void setUsedDatabaseStatus(DatabaseStatus dbstatus) {
		this.dbStatus4Used = dbstatus;
	}

	public boolean getCanHandover() {
		return canHandover;
	}
}
