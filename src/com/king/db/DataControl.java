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
import java.sql.Connection;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import com.king.framework.SystemLogger;
import com.king.gmms.metrics.MetricsCollector;
import com.king.gmms.metrics.MetricsNames;

public class DataControl {
	private static SystemLogger log = SystemLogger.getSystemLogger(DataControl.class);
	private static boolean initialized = false;
	private static Hashtable dsConfigs;
	private static Properties mprop;
	private static Hashtable dataManagers = new Hashtable();
	private static Hashtable dataSources = new Hashtable();
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
			log.info("DB access mode: {}", prop.getProperty("DB.AccessMode", "hikari").trim());
			createDataManagers(dsConfigs);
			String startupCheckValue = prop.getProperty("DB.StartupCheck", "false");
			String startupCheckSql = prop.getProperty("DB.StartupCheckSql", "SELECT 1").trim();
			boolean startupCheckEnabled = Boolean.parseBoolean(startupCheckValue.trim());
			log.info("DB startup check config. enabled={}, rawValue={}, sql={}",
					startupCheckEnabled, startupCheckValue, startupCheckSql);
			if (startupCheckEnabled) {
				startupCheck(startupCheckSql);
			}
			initialized = true;
		}
	}

	private static void createDataManagers(Hashtable dsConfigs)
			throws DataControlException {
		String dsName;
		DbObject dsConfig;
		DataManager[] dms;
		Enumeration dsNames = dsConfigs.keys();
		while (dsNames.hasMoreElements()) {
			dsName = (String) dsNames.nextElement();
			if (dsName.startsWith("backup")) {
				continue;
			}
			dsConfig = (DbObject) dsConfigs.get(dsName);
			dms = dsConfig.getDataManagers();
			if (dms == null) {
				throw new DataControlException(
						"No DataManager is configured to be associated with Data Source: "
								+ dsName);
			}
			log.trace("register db dataManager.dsName={}", dsName);
			log.trace("dms.length={}", dms.length);
			for (int i = 0; i < dms.length; i++) {
				dms[i].setDsName(dsName);
				// put data manager in a hashtable for relater retreval
				dataManagers.put(dms[i].getClass().getName(), dms[i]);
			}
			log.trace("dataManagers size={}", dataManagers.size());
		}
	}

	public static Connection getMasterConnection(String dsName)
			throws DataControlException {
		return getConnection(dsName);
	}

	public static Connection getSlaveConnection(String dsName)
			throws DataControlException {
		return getConnection("backup" + dsName);
	}

	public static Connection getConnection(String dsName) throws DataControlException {
		try {
			if (dsConfigs == null) {
				throw new DataControlException("DataControl is not initialized.");
			}
			DbObject dsConfig = (DbObject) dsConfigs.get(dsName);
			if (dsConfig == null) {
				throw new DataControlException("No DbObject found for " + dsName);
			}
			HikariDataSource dataSource = getDataSource(dsName, dsConfig);
			return dataSource.getConnection();
		} catch (DataControlException e) {
			throw e;
		} catch (Exception e) {
			throw new DataControlException("Cannot get JDBC connection for " + dsName + ": " + e.getMessage(), e);
		}
	}

	public static void releaseConnection(String dsName, Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			log.warn("Close unmanaged JDBC connection failed. dsName={}", dsName, e);
		}
	}

	private static synchronized HikariDataSource getDataSource(String dsName, DbObject dsConfig)
			throws DataControlException {
		HikariDataSource dataSource = (HikariDataSource) dataSources.get(dsName);
		if (dataSource == null || dataSource.isClosed()) {
			dataSource = new HikariDataSource(buildHikariConfig(dsName, dsConfig));
			dataSources.put(dsName, dataSource);
			log.info("HikariCP pool initialized. dsName={}, driver={}, url={}, maxPoolSize={}, minIdle={}, connectionTimeoutMs={}",
					dsName, dsConfig.getDriver(), dsConfig.getUrl(), dsConfig.getMaxPoolSize(),
					dsConfig.getMinPoolSize(), getConnectionTimeoutMs(dsConfig));
		}
		return dataSource;
	}

	private static HikariConfig buildHikariConfig(String dsName, DbObject dsConfig) {
		HikariConfig config = new HikariConfig();
		config.setPoolName("A2P-" + dsName);
		config.setDriverClassName(dsConfig.getDriver());
		config.setJdbcUrl(dsConfig.getUrl());
		config.setUsername(dsConfig.getUsername());
		config.setPassword(dsConfig.getPassword());
		config.setMaximumPoolSize(dsConfig.getMaxPoolSize());
		config.setMinimumIdle(Math.min(dsConfig.getMinPoolSize(), dsConfig.getMaxPoolSize()));
		config.setConnectionTimeout(getConnectionTimeoutMs(dsConfig));
		config.setIdleTimeout(longProp("DB.Hikari.IdleTimeoutMs", 600000L));
		config.setMaxLifetime(longProp("DB.Hikari.MaxLifetimeMs", 1800000L));
		config.setValidationTimeout(longProp("DB.Hikari.ValidationTimeoutMs", 3000L));
		config.setLeakDetectionThreshold(longProp("DB.Hikari.LeakDetectionThresholdMs", 0L));
		config.setKeepaliveTime(longProp("DB.Hikari.KeepaliveTimeMs", 0L));
		config.setAutoCommit(Boolean.parseBoolean(getProperty("DB.Hikari.AutoCommit", "true")));
		String testQuery = getProperty("DB.Hikari.ConnectionTestQuery", null);
		if (testQuery != null && testQuery.trim().length() > 0) {
			config.setConnectionTestQuery(testQuery.trim());
		}
		return config;
	}

	private static long getConnectionTimeoutMs(DbObject dsConfig) {
		long timeout = longProp("DB.Hikari.ConnectionTimeoutMs", -1L);
		if (timeout > 0) {
			return timeout;
		}
		timeout = dsConfig.getTimeout();
		if (timeout <= 0) {
			return 30000L;
		}
		if (timeout < 1000L) {
			return timeout * 1000L;
		}
		return timeout;
	}

	private static String getProperty(String key, String defaultValue) {
		if (mprop == null) {
			return defaultValue;
		}
		return mprop.getProperty(key, defaultValue);
	}

	private static long longProp(String key, long defaultValue) {
		try {
			return Long.parseLong(getProperty(key, String.valueOf(defaultValue)).trim());
		} catch (Exception e) {
			log.warn("Invalid long property {}, fallback to {}", key, defaultValue);
			return defaultValue;
		}
	}

	private static void startupCheck(String sql) {
		Enumeration dsNames = dsConfigs.keys();
		while (dsNames.hasMoreElements()) {
			String dsName = (String) dsNames.nextElement();
			Connection connection = null;
			Statement statement = null;
			try {
				connection = getConnection(dsName);
				statement = connection.createStatement();
				statement.execute(sql);
				log.info("DB startup check success. dsName={}, sql={}", dsName, sql);
			} catch (Exception e) {
				log.error("DB startup check failed. dsName={}, sql={}", dsName, sql, e);
			} finally {
				try {
					if (statement != null) {
						statement.close();
					}
				} catch (Exception e) {
				}
				releaseConnection(dsName, connection);
			}
		}
	}

	public static void logPoolStats() {
		if (dataSources == null || dataSources.isEmpty()) {
			return;
		}
		Enumeration dsNames = dataSources.keys();
		while (dsNames.hasMoreElements()) {
			String dsName = (String) dsNames.nextElement();
			Object source = dataSources.get(dsName);
			if (!(source instanceof HikariDataSource)) {
				continue;
			}
			HikariDataSource dataSource = (HikariDataSource) source;
			if (dataSource.isClosed()) {
				continue;
			}
			HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();
			if (pool == null) {
				continue;
			}
			MetricsCollector collector = MetricsCollector.getInstance();
			collector.setGauge(MetricsNames.dbGauge(dsName, "active"), pool.getActiveConnections());
			collector.setGauge(MetricsNames.dbGauge(dsName, "idle"), pool.getIdleConnections());
			collector.setGauge(MetricsNames.dbGauge(dsName, "total"), pool.getTotalConnections());
			collector.setGauge(MetricsNames.dbGauge(dsName, "waiting"), pool.getThreadsAwaitingConnection());
			collector.setGauge(MetricsNames.dbGauge(dsName, "max"), dataSource.getMaximumPoolSize());
			collector.setGauge(MetricsNames.dbGauge(dsName, "min_idle"), dataSource.getMinimumIdle());
		}
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
			log.trace("dataManagers start closeIdleConnection!");
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
