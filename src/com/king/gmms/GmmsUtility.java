/**
 *
 * Standard Logged
 */
package com.king.gmms;

/**
 * <p>Title:       GmmsUtility</p>
 * <p>Description: This class provide all the method which will be used in all
 *                 the listener and client</p>
 */

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.*;

import com.king.db.*;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.gmms.connectionpool.ssl.SslConfiguration;
import com.king.gmms.domain.*;
import com.king.gmms.milter.AntiSpamMilter;
import com.king.gmms.threadpool.ExecutorServiceManager;
import com.king.gmms.threadpool.ThreadPoolProfile;
import com.king.gmms.threadpool.impl.A2PThreadPoolExecutor;
import com.king.gmms.threadpool.impl.DefaultExecutorServiceManager;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.*;
import com.king.redis.RedisClient;
import com.king.gmms.metrics.MetricsNames;
import com.king.gmms.metrics.MetricsReporter;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class GmmsUtility {
	private static GmmsUtility instance = new GmmsUtility();
	private static SystemLogger log;
	private LifecycleSupport lifecycleSupport;
	private MessageStoreManager messageStoreManager;
	private CDRManager cdrManager;
	private RedisClient redisClient;
	private DBLockConnection dataConnection;
	private HttpInterfaceManager httpInterfaceManager;
	private A2PCustomerManager customerManager;
	private ModuleManager moduleManager;
	private MessageAddressInterpreter messageAddressInterpreter;
	private boolean initialized;
	private Properties properties;
	private String gmmsConfigFile;
	private String serverIP;
	private String serviceIP;
	private HashSet<String> screenedIPs;
	private int initPhoneLen;
	private int expireTime;
	private int finalExpireTime;
	private static Map<Integer, Random> randomMap = new HashMap<Integer, Random>();
	private static Map<Integer, Random> routingMap = new HashMap<Integer, Random>();
	
	/**
	 * define the min value of ExpireTime from customer
	 * Unit: minute
	 * Default: 1 hour
	 */
	private int minMessageExpireTimeFromCust;
	
	/**
	 * define the max value of ExpireTime from customer, 
	 * also limit the value of scheduleDeliveryTime from customer
	 * Unit: minute
	 * Default 7 days
	 */
	private int maxMessageExpireTimeFromCust;
	
	private int drInterval;
	private int connectionSilentTime;
	private String routerModule;
	private HashSet<String> loopbacks;
	private int alertFreq;
	private int alertCount;
	private int maxSilentTime;
	private int cdrMaxTime;
	private int cdrMaxSize;
	private boolean isSystemManageEnable = false;

	private String blackListFile = null;
	private String whiteListFile = null;
	private String routingFile = null;
	private String antiSpamFile = null;
	private String contentTemplateFile = null;
	private String redisFile = null;
	private String cmTempFile = null;
	private String phonePrefixFile = null;
	private String vendorTemplateFile = null;
	private String recipientRuleFile = null;
	private String senderActionDir;
	private String recipientActionDir;
	private String contentActionDir;
	private String vendorRoutingFile;

	private int cacheMsgTimeout = 180000;
	private int csmIntegrityCacheTimeout = 3600000; // 1 hour
	private int csmIntegrityCacheCapacity = 100000;
	private int enquireLinkResponseTime = 100;	
	private int resendDRThrottle = 0;	

	private TimeZone local;

	private volatile int count = 0;
	private Object mutex = new Object();

	private volatile boolean isRunningStoreDRMode = false;
	private boolean storeDRModeEnable = false;

	private static final String HEXES = "0123456789ABCDEF";
	private static final String HEX_CHARSET = "UTF-16BE";
	
	/**
	 * the blackhole DC ssid
	 * the message to blackhole will not be delivered to next hop
	 */
	private static int blackholeSsid = -1;
	
	/**
	 * Consecutive number of throttling control time window
     * when need to send alert mail 
	 */
	private int conSecThrottleWinNumToAlert;
	
	/**
	 * max number of throttling alert mail to send
	 */
	private int maxThrottleAlertMailNum;
	
	/**
	 *  The interval of protocol module report incoming message count to SYS module.
	 */
	private int reportModuleIncomingMsgCountInterval;
	
	/**
	 *  The expire time of dynamic customer incoming throttle, 
	 *  after that time the dynamic throttle will be reset.
	 */
	private int dynamicCustInThresholdExipreTime;

	// V4.0 Redis Stream Sharding
	private String nodeId = "0";
	private int totalShards = 1;
	private Set<Integer> myShards = new HashSet<>();

	/**
	 *  The system max average incoming threshold (per second).
	 */
	private int systemIncomingThreshold;

	/**
	 * default customer incoming threshold (per second).
	 */
	private int defaultCustIncomingThreshold;
	
	/**
	 *  The max magnification of customer threshold
	 */
    private int maxCustIncomingThresholdMagnification;

    
    private ExecutorServiceManager executorServiceManager;
    
    private ThreadPoolProfile defaultThreadPoolProfile;
    
    private int dnsTimeout = 20;
    
    private SslConfiguration sslConfiguration = null;
    
    private int maxServiceTypeID = 3;
    
    private PhoneNumberGeo phoneNumberGeo;
    
    /**
     * Smpp serviceTypeID tag
     * Hex format
     * Default: 1401
     */
    private short smppServiceTypeIDTag = -1;

    /**
     * MinID expire time in redis for query thread
     */
    private int min_ID_expireTime = 0;
    
    private com.king.framework.lifecycle.RedisControlSubscriber controlSubscriber;
    private Thread controlSubscriberThread;

	private GmmsUtility() {
		initialized = false;
		screenedIPs = new HashSet<String>();
		loopbacks = new HashSet<String>();
		local = TimeZone.getDefault();		
	}

	public static GmmsUtility getInstance() {
		return instance;
	}

	public void close() {
		initialized = false;
		try {
			com.king.gmms.ha.ModuleStatusReporter.stopAll();
			if (controlSubscriber != null) {
				controlSubscriber.stop();
			}
			controlSubscriberThread = null;
			if (executorServiceManager != null) {
				executorServiceManager.shutdownAll();
			}
			
		} catch (Exception e) {
			log.error(e, e);
		}
	}

	public synchronized void startControlSubscriber() {
		if (controlSubscriberThread != null && controlSubscriberThread.isAlive()) {
			return;
		}
		controlSubscriber = new com.king.framework.lifecycle.RedisControlSubscriber();
		controlSubscriberThread = new Thread(controlSubscriber, "RedisControlSubscriberThread");
		controlSubscriberThread.setDaemon(true);
		controlSubscriberThread.start();
		log.info("RedisControlSubscriber thread started.");
	}

	public void stopCDRManager() {
		cdrManager.close();
	}

    public void initDBManager(DatabaseStatus dbstatus){
    	
    	try{
    		log.debug("init DBManager!");
	    	if(!DataControl.isInitialized()) {
	    		if(dbstatus==null){
	    			dbstatus=DatabaseStatus.MASTER_USED;
	    		}
	            DataControl.init(properties,dbstatus);
	        }
	        messageStoreManager = (MessageStoreManager) DataControl.getDataManager(MessageStoreManager.class.getName());
	        dataConnection = (DBLockConnection) DataControl.getDataManager(DBLockConnection.class.getName());
	        messageStoreManager.init();
    	}catch(Exception e){
            log.fatal(e, e);
            System.exit( -1);
    	}
    }   
    
    public void initRedisClient(String flag){
    	try{
    		log.info("initRedisClient start. flag={}", flag);
    		log.info("initRedisClient getting RedisClient instance.");
    		redisClient = RedisClient.getInstance();
    		log.info("initRedisClient got RedisClient instance.");
    		if("M".equalsIgnoreCase(flag)){
    			log.info("initRedisClient setting Redis HA flag to master.");
    			redisClient.setRedisHaFlag(true);
    		}else{
    			log.info("initRedisClient setting Redis HA flag to slave.");
    			redisClient.setRedisHaFlag(false);
    		}
    		startControlSubscriber();
    		log.info("initRedisClient finished. flag={}", flag);
    		
    	}catch(Exception e){
            log.error("GmmsUtility init redisClient error!", e);
            //System.exit( -1);
    	}
    }
    public void setRedisClientFlag(String flag){
        if(redisClient==null){
        	redisClient = RedisClient.getInstance();
        }
    	if("M".equalsIgnoreCase(flag)){
    		redisClient.setRedisHaFlag(true);
		}else{
			redisClient.setRedisHaFlag(false);
		}	
    }
    public void initCDRManager(){
        cdrMaxTime = Integer.parseInt(properties.getProperty("CDRFileSwitchInterval", "300").trim());
        cdrMaxSize = Integer.parseInt(properties.getProperty("CDRFileMaxSize", "100").trim());
        cdrManager = new CDRManager();
    }
    /**
     * handover db 
     * @param dbStatus
     */
    public void setHandover(DatabaseStatus dbStatus){
    	DataControl.getInstance().setHandover(dbStatus);
    }
	public LifecycleSupport getLifecycleSupport() {
		return this.lifecycleSupport;
	}

	public String getBlacklistFilePath() {
		return this.blackListFile;
	}
	
	public String getWhitelistFilePath() {
		return this.whiteListFile;
	}

	public String getRoutingFilePath() {
		return this.routingFile;
	}

	public String getPhonePrefixFile() {
		return this.phonePrefixFile;
	}
	
	public String getAntiSpamFilePath() {
		return this.antiSpamFile;
	}

	public void initUtility(String propFile) {
		try {
			gmmsConfigFile = propFile;
			Properties properties = new Properties();
			FileInputStream fis = new FileInputStream(propFile);
			properties.load(fis);
			fis.close();
			initUtility(properties);
		} catch (IOException e) {
			log.fatal(e, e);
			System.exit(-1);
		}
	}

	public void initProperties(String propFile) {
		try {
			Properties tempProperties = new Properties();
			FileInputStream fis = new FileInputStream(propFile);
			tempProperties.load(fis);
			fis.close();
			properties = tempProperties;
		} catch (IOException e) {
			log.fatal(e, e);
			System.exit(-1);
		}
	}

	/**
	 * when DataControl has not been initialized. listener call.
	 * 
	 * @param props
	 *            Properties
	 */
	public synchronized void initUtility(Properties props) {
		try {
			if (initialized) {
				return;
			}
			properties = props;
			String a2phome = System.getProperty("a2p_home","/usr/local/a2p/");
			if (gmmsConfigFile == null || gmmsConfigFile.trim().length() == 0) {
				gmmsConfigFile = a2phome + "Gmms/GmmsConfig.properties";
			}
			this.blackListFile = a2phome + "conf/blacklist.cfg";
			this.whiteListFile = a2phome + "conf/whitelist.cfg";
			this.routingFile = a2phome + "conf/routing";
			this.senderActionDir = a2phome + "conf/senderactiondir";
			this.recipientActionDir = a2phome + "conf/recipientactiondir";
			this.contentActionDir = a2phome + "conf/contentactiondir";
			this.antiSpamFile = a2phome + "conf/antiSpam.cfg";
			this.contentTemplateFile = a2phome + "conf/";
			this.redisFile = a2phome +"ha/RedisStatus";			
			String cmFile = a2phome + "conf/cm.cfg";		
			this.phonePrefixFile = a2phome + "conf/senderReplacement.cfg";
			this.vendorTemplateFile = a2phome + "conf/vendorTemplateReplacement.cfg";
			this.vendorRoutingFile = a2phome + "conf/vendorRoutingReplacement.cfg";
			this.recipientRuleFile = a2phome + "conf/recipientRuleFile.cfg";
			this.lifecycleSupport = new LifecycleSupport();
			// Log4J initialization must be done after JNDI initialization
			// DOMConfigurator.configure(a2phome + "conf/log4j-config.xml");
			try {
				phoneNumberGeo = new PhoneNumberGeo(a2phome);
			} catch (Exception e) {
				log.error("phone.dat load failed.", e);
			}
            
			System.setProperty("log4j.configurationFile", a2phome
					+ "conf/log4j2.xml");
			log = SystemLogger.getSystemLogger(GmmsUtility.class);
			String moduleName = System.getProperty("module");

			if(log.isInfoEnabled()){
				log.info("A2P launcher initializes GmmsUtility for Service {} from {}",
							moduleName, a2phome);
			}
			lifecycleSupport.addListener(com.king.framework.lifecycle.event.Event.TYPE_GMMS_CONFIG_RELOAD,
					new GmmsConfigReloadListener(), 1);

			this.cmTempFile = a2phome+"/temp/"+moduleName+"_tempCm.cfg."+new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			
			// V4.0 Node ID initialization for distributed uniqueness
			nodeId = props.getProperty("NodeID", "0").trim();
			System.setProperty("NodeID", nodeId);
			com.king.message.gmms.MessageIdGenerator.setNodeId(toNumericNodeId(nodeId));
			com.king.message.gmms.MessageIdGenerator.setIncludeNodeId(
					Boolean.parseBoolean(props.getProperty("MessageIdIncludeNodeId", "false").trim()));
			
			routerModule = props.getProperty("RouterModule", "DeliveryRouter")
					.trim();
			serverIP = props.getProperty("ServerIP").trim();
			serviceIP = props.getProperty("ServiceIP").trim();
			if (props.getProperty("ScreenedIPs") != null) {
				StringTokenizer st = new StringTokenizer(props.getProperty(
						"ScreenedIPs").trim(), ",");
				while (st.hasMoreTokens()) {
					screenedIPs.add(st.nextToken());
				}
			}
			if (properties.getProperty("LoopbackAddresses") != null) {
				StringTokenizer st = new StringTokenizer(properties
						.getProperty("LoopbackAddresses").trim(), ",");
				while (st.hasMoreTokens()) {
					String address = st.nextToken().trim();
					while (address.startsWith("+")) {
						address = address.substring(1);
					}
					loopbacks.add(address);
				}
			}
			alertFreq = Integer.parseInt(props.getProperty(
					"AlertMailFrequence", "100").trim());
			initPhoneLen = Integer.parseInt(props.getProperty("InitPhoneLen",
					"0").trim());

			expireTime = Integer.parseInt(props.getProperty(
					"MessageExpireTime", "1380").trim());
			resendDRThrottle = Integer.parseInt(props.getProperty(
					"ResendDRThrottle", "0").trim());
			min_ID_expireTime = Integer.parseInt(props.getProperty(
					"Redis_MinIDExpireTime", "300").trim());
			finalExpireTime = Integer.parseInt(props.getProperty(
					"MessageFinalExpireTime", "1440").trim());
			drInterval = Integer.parseInt(props.getProperty("DRInterval", "20")
					.trim());
			connectionSilentTime = Integer.parseInt(props.getProperty(
					"ConnectionSilentTime").trim());

			customerManager = new A2PCustomerManager(cmFile);
			httpInterfaceManager = new HttpInterfaceManager();
			// didn't init DataControl & cdrManager for system manager
			moduleManager = ModuleManager.getInstance();
			messageAddressInterpreter = new MessageAddressInterpreter();
			maxSilentTime = Integer.parseInt(props.getProperty("MaxSilentTime",
					"60").trim());
			cacheMsgTimeout = Integer.parseInt(props.getProperty(
					"CacheMessageTimeout", "180000").trim());
			csmIntegrityCacheTimeout = Integer.parseInt(props.getProperty(
					"CsmIntegrityCacheTimeout", "3600000").trim()); // 1 hour
			csmIntegrityCacheCapacity = Integer.parseInt(props.getProperty(
					"CsmIntegrityCacheCapacity", "100000").trim());
			enquireLinkResponseTime = Integer.parseInt(props.getProperty(
					"WaitingEnquireLinkResponseTime", "100"));
			String storeDRMode = props
					.getProperty("StoreDRModeEnable", "False");
			if ("True".equalsIgnoreCase(storeDRMode)) {
				storeDRModeEnable = true;
			}
			
			String isSystemManageEnableString = props.getProperty(
					"SystemManager.enable", "False");
			if (isSystemManageEnableString.equalsIgnoreCase("True")) {
				isSystemManageEnable = true;
			}
			
			blackholeSsid = Integer.parseInt(props.getProperty(
					"BlackholeSsid", "-1").trim());
			
			// V4.0 Sharding Configuration
			totalShards = Integer.parseInt(props.getProperty("TotalShards", "1").trim());
			String myShardsStr = props.getProperty("MyShards", "0").trim();
			myShards = parseShardSet(myShardsStr, "MyShards");
			
			// throttling control conf
			conSecThrottleWinNumToAlert = Integer.parseInt(props.getProperty(
					"ThrottlingControl.ConsecutiveNumToAlert", "3"));
			maxThrottleAlertMailNum = Integer.parseInt(props.getProperty(
			        "ThrottlingControl.MaxAlertMailNum", "1"));
			reportModuleIncomingMsgCountInterval = Integer.parseInt(props.getProperty(
					"ThrottlingControl.ReportModuleIncomingMsgCountInterval", "60"));
			dynamicCustInThresholdExipreTime = Integer.parseInt(props.getProperty(
					"ThrottlingControl.DynamicCustIncomingThresholdExipreTime", "90"));
			systemIncomingThreshold = Integer.parseInt(props.getProperty(
					"ThrottlingControl.SystemIncomingThreshold", "2000"));
			defaultCustIncomingThreshold = Integer.parseInt(props.getProperty(
					"ThrottlingControl.DefaultCustIncomingThreshold", "20"));
			maxCustIncomingThresholdMagnification = Integer.parseInt(props.getProperty(
					"ThrottlingControl.MaxCustIncomingThresholdMagnification", "5"));
			
			sslConfiguration = new SslConfiguration();
			String keyStorePath = props.getProperty("Ssl.KeyStorePath", "/usr/local/a2p/conf/ssl/keystore").trim();
			if(keyStorePath != null && keyStorePath.length()>0){
				sslConfiguration.setKeyStorePath(keyStorePath);
			}
			String keyStrorePwd = props.getProperty("Ssl.KeyStorePassword", "tomcata2p").trim();
			if(keyStrorePwd != null && keyStrorePwd.length()>0){
				sslConfiguration.setKeyStorePassword(keyStrorePwd);
			}
			String KeyManagerPwd = props.getProperty("Ssl.KeyManagerPassword", "tomcata2p").trim();
			if(KeyManagerPwd != null && KeyManagerPwd.length()>0){
				sslConfiguration.setKeyManagerPassword(KeyManagerPwd);
			}
			String keyStoreType = props.getProperty("Ssl.KeyStoreType","JKS").trim();
			if(keyStoreType != null && !"".equalsIgnoreCase(keyStoreType)){
				sslConfiguration.setKeyStoreType(keyStoreType);
			}
			String trustStorePath = props.getProperty("Ssl.TrustStorePath", "/usr/local/a2p/conf/ssl/truststore").trim();
			if(trustStorePath != null && trustStorePath.length()>0){
				sslConfiguration.setTrustStorePath(trustStorePath);
			}
			String trustStrorePwd = props.getProperty("Ssl.TrustStorePassword", "tomcata2p").trim();
			if(trustStrorePwd != null && trustStrorePwd.length()>0){
				sslConfiguration.setTrustStorePassword(trustStrorePwd);
			}
			String trustStoreType = props.getProperty("Ssl.TrustStoreType","JKS").trim();
			if(trustStoreType != null && !"".equalsIgnoreCase(trustStoreType)){
				sslConfiguration.setTrustStoreType(trustStoreType);
			}
			
			//When isClientSendCA is false, client module should not send its CA to remote TLS server
			//so renew SSL configuration to screen the configuration of KeyStore and TrustedStore
			String  isClientTrustAll = props.getProperty("Ssl.IsClientTrustAll","false").trim();
			if(isClientTrustAll != null && "true".equalsIgnoreCase(isClientTrustAll)){
				String moduleType = moduleManager.getFullModuleType(moduleName);
				if(moduleType != null && moduleType.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
					sslConfiguration = new SslConfiguration();
				}
			}
			
			String sslProtocol = props.getProperty("Ssl.SSLProtocol","TLS").trim();
			if(sslProtocol != null && !"".equalsIgnoreCase(sslProtocol)){
				sslConfiguration.setSslProtocol(sslProtocol);
			}
			
			String isWantClientAuth = props.getProperty("Ssl.WantClientAuth","false").trim();
			if(isWantClientAuth != null && "true".equalsIgnoreCase(isWantClientAuth)){
				sslConfiguration.setWantClientAuth(true);
			}
			
			String isNeedClientAuth = props.getProperty("Ssl.NeedClientAuth","false").trim();
			if(isNeedClientAuth != null && "true".equalsIgnoreCase(isNeedClientAuth)){
				sslConfiguration.setNeedClientAuth(true);
			}
			
			// use the CertAlias in keystore as A2P certificate
			String certAlias = props.getProperty("Ssl.CertAlias","").trim();
			if(certAlias != null && certAlias.length()>0){
				sslConfiguration.setCertAlias(certAlias);
			}
			
			// default thread pool conf
			int minPoolSize = Integer.parseInt(props.getProperty("ThreadPool.MinPoolSize", "1"));
			int maxPoolSize = Integer.parseInt(props.getProperty("ThreadPool.MaxPoolSize", "5"));
			// unit: second
			long keepAliveTime = Long.parseLong(props.getProperty("ThreadPool.KeepAliveTime", "60"));
			int maxPoolQueueSize = Integer.parseInt(props.getProperty("ThreadPool.MaxQueueSize", "1000"));
			defaultThreadPoolProfile = new ThreadPoolProfile("defaultThreadPoolProfile");
			defaultThreadPoolProfile.setPoolSize(minPoolSize);
			defaultThreadPoolProfile.setMaxPoolSize(maxPoolSize);
			defaultThreadPoolProfile.setKeepAliveTime(keepAliveTime);
			defaultThreadPoolProfile.setTimeUnit(TimeUnit.SECONDS);
			defaultThreadPoolProfile.setMaxQueueSize(maxPoolQueueSize);
			defaultThreadPoolProfile.setRejectedPolicy(new A2PThreadPoolExecutor.A2PCallerRunsPolicy());
			defaultThreadPoolProfile.setNeedSafeExit(false);
			executorServiceManager = new DefaultExecutorServiceManager(defaultThreadPoolProfile, lifecycleSupport);
			
			dnsTimeout = Integer.parseInt(props.getProperty("DNSTimeOut", "20").trim());
			maxServiceTypeID = Integer.parseInt(props.getProperty("MaxServiceTypeID", "3").trim());
			smppServiceTypeIDTag = Short.parseShort(props.getProperty("Smpp.ServiceTypeIDTag", "1401").trim(), 16);
			
			// default 1 hour
			minMessageExpireTimeFromCust = Integer.parseInt(props.getProperty(
					"MinMessageExpireTimeFromCust", "60").trim()); 
			// default 7 days
			maxMessageExpireTimeFromCust = Integer.parseInt(props.getProperty(
					"MaxMessageExpireTimeFromCust", "10080").trim());
			
			applyMetricsConfig(props);
			applyDbPoolStatsConfig(props);
			
			initialized = true;
		} catch (Exception e) {
			log.fatal("GmmsUtility initUtility failed", e);
			System.exit(-1);
		}
	}

	public synchronized int reloadGmmsConfig(String action) {
		if (action != null && action.trim().length() > 0 && !"-a".equalsIgnoreCase(action.trim())) {
			log.warn("Reload GmmsConfig only supports -a now, action={}", action);
		}
		Properties newProps = new Properties();
		String a2phome = System.getProperty("a2p_home", "/usr/local/a2p/");
		String configFile = (gmmsConfigFile != null && gmmsConfigFile.trim().length() > 0)
				? gmmsConfigFile
				: a2phome + "Gmms/GmmsConfig.properties";
		try {
			FileInputStream fis = new FileInputStream(configFile);
			try {
				newProps.load(fis);
			} finally {
				fis.close();
			}
			Properties oldProps = properties;
			Set<String> changedKeys = diffProperties(oldProps, newProps);
			if (changedKeys.isEmpty()) {
				log.info("GmmsConfig reload completed. file={}, changed=0, applied=0, restartRequired=0", configFile);
				return 0;
			}
			Properties effectiveProps = copyProperties(oldProps);
			for (String key : changedKeys) {
				if (isRuntimeConfigKey(key)) {
					effectiveProps.setProperty(key, newProps.getProperty(key).trim());
				}
			}
			validateRuntimeConfig(effectiveProps, changedKeys);
			List<String> applied = new ArrayList<String>();
			List<String> restartRequired = new ArrayList<String>();
			List<String> unknown = new ArrayList<String>();
			for (String key : changedKeys) {
				if (isRuntimeConfigKey(key)) {
					applied.add(key);
				} else if (isRestartRequiredConfigKey(key)) {
					restartRequired.add(key);
				} else {
					unknown.add(key);
				}
			}
			properties = effectiveProps;
			applyRuntimeConfig(effectiveProps, applied);
			log.info("GmmsConfig reload completed. file={}, changed={}, applied={}, restartRequired={}, unknown={}",
					configFile, changedKeys.size(), applied.size(), restartRequired.size(), unknown.size());
			logConfigChanges("Applied GmmsConfig", oldProps, newProps, applied);
			logConfigChanges("Restart required GmmsConfig", oldProps, newProps, restartRequired);
			logConfigChanges("Unknown GmmsConfig", oldProps, newProps, unknown);
			return 0;
		} catch (Exception e) {
			log.error("GmmsConfig reload failed. file=" + configFile, e);
			return 1;
		}
	}

	private Set<String> diffProperties(Properties oldProps, Properties newProps) {
		TreeSet<String> keys = new TreeSet<String>();
		if (newProps != null) {
			for (Object key : newProps.keySet()) {
				keys.add(String.valueOf(key));
			}
		}
		TreeSet<String> changed = new TreeSet<String>();
		for (String key : keys) {
			String oldValue = normalizePropertyValue(oldProps == null ? null : oldProps.getProperty(key));
			String newValue = normalizePropertyValue(newProps == null ? null : newProps.getProperty(key));
			if (!stringEquals(oldValue, newValue)) {
				changed.add(key);
			}
		}
		return changed;
	}

	private Properties copyProperties(Properties source) {
		Properties target = new Properties();
		if (source != null) {
			for (Object key : source.keySet()) {
				String stringKey = String.valueOf(key);
				String value = source.getProperty(stringKey);
				if (value != null) {
					target.setProperty(stringKey, value);
				}
			}
		}
		return target;
	}

	private String normalizePropertyValue(String value) {
		return value == null ? null : value.trim();
	}

	private boolean stringEquals(String left, String right) {
		if (left == null) {
			return right == null;
		}
		return left.equals(right);
	}

	private void validateRuntimeConfig(Properties props, Set<String> changedKeys) {
		for (String key : changedKeys) {
			if (!isRuntimeConfigKey(key)) {
				continue;
			}
			if (isLongRuntimeKey(key)) {
				Long.parseLong(props.getProperty(key, getDefaultRuntimeValue(key)).trim());
			} else if (isIntegerRuntimeKey(key)) {
				Integer.parseInt(props.getProperty(key, getDefaultRuntimeValue(key)).trim());
			} else if ("Smpp.ServiceTypeIDTag".equals(key)) {
				Short.parseShort(props.getProperty(key, "1401").trim(), 16);
			} else if (isBooleanRuntimeKey(key)) {
				Boolean.parseBoolean(props.getProperty(key, "false").trim());
			} else if ("TotalShards".equals(key)) {
				Integer.parseInt(props.getProperty(key, "1").trim());
			} else if (isShardRuntimeKey(key)) {
				int oldTotalShards = totalShards;
				try {
					totalShards = Integer.parseInt(props.getProperty("TotalShards", String.valueOf(totalShards)).trim());
					parseShardSet(props.getProperty(key, "0"), key);
				} finally {
					totalShards = oldTotalShards;
				}
			}
		}
	}

	private void applyRuntimeConfig(Properties props, List<String> applied) {
		if (applied == null || applied.isEmpty()) {
			return;
		}
		boolean shardChanged = false;
		boolean metricsChanged = false;
		boolean dbPoolStatsChanged = false;
		for (String key : applied) {
			if ("AlertMailFrequence".equals(key)) alertFreq = intProp(props, key, 100);
			else if ("InitPhoneLen".equals(key)) initPhoneLen = intProp(props, key, 0);
			else if ("MessageExpireTime".equals(key)) expireTime = intProp(props, key, 1380);
			else if ("ResendDRThrottle".equals(key)) resendDRThrottle = intProp(props, key, 0);
			else if ("Redis_MinIDExpireTime".equals(key)) min_ID_expireTime = intProp(props, key, 300);
			else if ("MessageFinalExpireTime".equals(key)) finalExpireTime = intProp(props, key, 1440);
			else if ("DRInterval".equals(key)) drInterval = intProp(props, key, 20);
			else if ("ConnectionSilentTime".equals(key)) connectionSilentTime = intProp(props, key, connectionSilentTime);
			else if ("MaxSilentTime".equals(key)) maxSilentTime = intProp(props, key, 60);
			else if ("CacheMessageTimeout".equals(key)) cacheMsgTimeout = intProp(props, key, 180000);
			else if ("CsmIntegrityCacheTimeout".equals(key)) csmIntegrityCacheTimeout = intProp(props, key, 3600000);
			else if ("CsmIntegrityCacheCapacity".equals(key)) csmIntegrityCacheCapacity = intProp(props, key, 100000);
			else if ("WaitingEnquireLinkResponseTime".equals(key)) enquireLinkResponseTime = intProp(props, key, 100);
			else if ("StoreDRModeEnable".equals(key)) storeDRModeEnable = Boolean.parseBoolean(props.getProperty(key, "False").trim());
			else if ("BlackholeSsid".equals(key)) blackholeSsid = intProp(props, key, -1);
			else if ("DNSTimeOut".equals(key)) dnsTimeout = intProp(props, key, 20);
			else if ("MaxServiceTypeID".equals(key)) maxServiceTypeID = intProp(props, key, 3);
			else if ("Smpp.ServiceTypeIDTag".equals(key)) smppServiceTypeIDTag = Short.parseShort(props.getProperty(key, "1401").trim(), 16);
			else if ("MinMessageExpireTimeFromCust".equals(key)) minMessageExpireTimeFromCust = intProp(props, key, 60);
			else if ("MaxMessageExpireTimeFromCust".equals(key)) maxMessageExpireTimeFromCust = intProp(props, key, 10080);
			else if ("CDRFileSwitchInterval".equals(key)) cdrMaxTime = intProp(props, key, 300);
			else if ("CDRFileMaxSize".equals(key)) cdrMaxSize = intProp(props, key, 100);
			else if ("MessageIdIncludeNodeId".equals(key)) com.king.message.gmms.MessageIdGenerator.setIncludeNodeId(
					Boolean.parseBoolean(props.getProperty(key, "false").trim()));
			else if ("ScreenedIPs".equals(key)) screenedIPs = parseStringSet(props.getProperty(key), false);
			else if ("LoopbackAddresses".equals(key)) loopbacks = parseStringSet(props.getProperty(key), true);
			else if ("TotalShards".equals(key)) {
				totalShards = intProp(props, key, 1);
				shardChanged = true;
			} else if (isShardRuntimeKey(key)) {
				shardChanged = true;
			} else if ("ThrottlingControl.ConsecutiveNumToAlert".equals(key)) conSecThrottleWinNumToAlert = intProp(props, key, 3);
			else if ("ThrottlingControl.MaxAlertMailNum".equals(key)) maxThrottleAlertMailNum = intProp(props, key, 1);
			else if ("ThrottlingControl.ReportModuleIncomingMsgCountInterval".equals(key)) reportModuleIncomingMsgCountInterval = intProp(props, key, 60);
			else if ("ThrottlingControl.DynamicCustIncomingThresholdExipreTime".equals(key)) dynamicCustInThresholdExipreTime = intProp(props, key, 90);
			else if ("ThrottlingControl.SystemIncomingThreshold".equals(key)) systemIncomingThreshold = intProp(props, key, 2000);
			else if ("ThrottlingControl.DefaultCustIncomingThreshold".equals(key)) defaultCustIncomingThreshold = intProp(props, key, 20);
			else if ("ThrottlingControl.MaxCustIncomingThresholdMagnification".equals(key)) maxCustIncomingThresholdMagnification = intProp(props, key, 5);
			else if ("Metrics.Enable".equals(key) || "Metrics.ReportIntervalSeconds".equals(key)
					|| "Metrics.LogLevel".equals(key) || "Metrics.PrintZero".equals(key)
					|| "Metrics.PrintGauge".equals(key) || "Metrics.MinTPS".equals(key)
					|| "Metrics.Ssid.Enable".equals(key)) metricsChanged = true;
			else if ("DB.PoolStats.Enable".equals(key) || "DB.PoolStats.IntervalSeconds".equals(key)) dbPoolStatsChanged = true;
		}
		if (shardChanged) {
			reloadLocalShardAssignment();
		}
		if (metricsChanged) {
			applyMetricsConfig(props);
		}
		if (dbPoolStatsChanged) {
			applyDbPoolStatsConfig(props);
		}
	}

	private int intProp(Properties props, String key, int defaultValue) {
		return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)).trim());
	}

	private double doubleProp(Properties props, String key, double defaultValue) {
		return Double.parseDouble(props.getProperty(key, String.valueOf(defaultValue)).trim());
	}

	private void applyMetricsConfig(Properties props) {
		boolean metricsEnable = Boolean.parseBoolean(props.getProperty("Metrics.Enable", "true").trim());
		int metricsInterval = intProp(props, "Metrics.ReportIntervalSeconds", 30);
		String metricsLogLevel = props.getProperty("Metrics.LogLevel", "DEBUG").trim();
		boolean metricsPrintZero = Boolean.parseBoolean(props.getProperty("Metrics.PrintZero", "false").trim());
		boolean metricsPrintGauge = Boolean.parseBoolean(props.getProperty("Metrics.PrintGauge", "false").trim());
		double metricsMinTps = doubleProp(props, "Metrics.MinTPS", 0.01);
		boolean metricsSsidEnable = Boolean.parseBoolean(props.getProperty("Metrics.Ssid.Enable", "false").trim());
		MetricsNames.configureSsidMetrics(metricsSsidEnable);
		MetricsReporter.getInstance().applyConfig(metricsEnable, metricsInterval, metricsLogLevel,
				metricsPrintZero, metricsPrintGauge, metricsMinTps);
	}

	private void applyDbPoolStatsConfig(Properties props) {
		boolean enable = Boolean.parseBoolean(props.getProperty("DB.PoolStats.Enable", "false").trim());
		int interval = intProp(props, "DB.PoolStats.IntervalSeconds", 60);
		if (enable) {
			JdbcPoolStatsReporter.getInstance().restart(interval);
		} else {
			JdbcPoolStatsReporter.getInstance().stop();
			log.info("JdbcPoolStatsReporter disabled by DB.PoolStats.Enable=false");
		}
	}

	private HashSet<String> parseStringSet(String value, boolean stripLeadingPlus) {
		HashSet<String> result = new HashSet<String>();
		if (value == null || value.trim().length() == 0) {
			return result;
		}
		StringTokenizer st = new StringTokenizer(value.trim(), ",");
		while (st.hasMoreTokens()) {
			String item = st.nextToken().trim();
			if (stripLeadingPlus) {
				while (item.startsWith("+")) {
					item = item.substring(1);
				}
			}
			if (item.length() > 0) {
				result.add(item);
			}
		}
		return result;
	}

	private void reloadLocalShardAssignment() {
		String module = System.getProperty("module", "");
		if (module.toLowerCase().contains("core")) {
			initCoreShardAssignment();
		} else if (module.toLowerCase().contains("client")) {
			initClientShardAssignment();
		} else {
			myShards = parseShardSet(properties.getProperty("MyShards", "0"), "property:MyShards");
			log.info("Shard assignment reloaded for module {}. totalShards={}, myShards={}",
					module, totalShards, formatShardSet(myShards));
		}
	}

	private boolean isRuntimeConfigKey(String key) {
		return isDirectRuntimeKey(key) || isCommonRuntimeKey(key) || isModuleRuntimeKey(key)
				|| isShardRuntimeKey(key) || isRedisStreamTuningKey(key) || isMqmRuntimeKey(key);
	}

	private boolean isDirectRuntimeKey(String key) {
		String[] keys = new String[] {
			"AlertMailFrequence", "InitPhoneLen", "MessageExpireTime", "ResendDRThrottle",
			"Redis_MinIDExpireTime", "MessageFinalExpireTime", "DRInterval", "ConnectionSilentTime",
			"MaxSilentTime", "CacheMessageTimeout", "CsmIntegrityCacheTimeout", "CsmIntegrityCacheCapacity",
			"WaitingEnquireLinkResponseTime", "StoreDRModeEnable", "BlackholeSsid", "DNSTimeOut",
			"MaxServiceTypeID", "Smpp.ServiceTypeIDTag", "MinMessageExpireTimeFromCust",
			"MaxMessageExpireTimeFromCust", "CDRFileSwitchInterval", "CDRFileMaxSize",
			"MessageIdIncludeNodeId", "ScreenedIPs", "LoopbackAddresses", "Metrics.Enable", "Metrics.ReportIntervalSeconds",
			"Metrics.LogLevel", "Metrics.PrintZero", "Metrics.PrintGauge", "Metrics.MinTPS",
			"Metrics.Ssid.Enable",
			"AutoInDR.DirectStreamEnable", "AutoInDR.DelayDispatcherEnable", "AutoInDR.DelayBatchSize",
			"AutoInDR.DelayClaimTimeoutMs", "AutoInDR.DelayRetryDelayMs", "AutoInDR.DelayMaxRetry",
			"AutoInDR.FallbackToLegacyDelayDR", "AutoInDR.DelayPayloadTTLSeconds",
			"AutoInDR.DispatcherIdleSleepMs",
			"DB.StartupCheck", "DB.StartupCheckSql", "DB.PoolStats.Enable", "DB.PoolStats.IntervalSeconds",
			"ThrottlingControl.ConsecutiveNumToAlert", "ThrottlingControl.MaxAlertMailNum",
			"ThrottlingControl.ReportModuleIncomingMsgCountInterval",
			"ThrottlingControl.DynamicCustIncomingThresholdExipreTime",
			"ThrottlingControl.SystemIncomingThreshold",
			"ThrottlingControl.DefaultCustIncomingThreshold",
			"ThrottlingControl.MaxCustIncomingThresholdMagnification"
		};
		return contains(keys, key);
	}

	private boolean isCommonRuntimeKey(String key) {
		String[] keys = new String[] {
			"RedisStreamGlobalPELMonitorEnable", "RedisStreamDoorbellScanLimit",
			"RedisStreamAutoClaimIdleMs", "RedisStreamAutoClaimBatchSize",
			"RedisStreamTraceLogEnable",
			"SMPPSubmitPendingRedisScanIntervalMs", "SMPPSubmitPendingRedisScanBatchSize",
			"SMPPSubmitPendingRedisResultRetryMs", "SMPPSubmitPendingRedisRetryTTLSeconds",
			"SMPPDRPendingRedisScanIntervalMs", "SMPPDRPendingRedisScanBatchSize",
			"SMPPDRPendingRedisResultRetryMs", "SMPPDRPendingRedisRetryTTLSeconds",
			"SMSOptionRecipitLenCheck", "mnpqueryurl", "DNSAddress", "DNSPort",
			"DNSMaxFailedLimit", "DNSTestPeriod", "DNSTestNumber", "DNSBufferCapacity",
			"DNSBufferTimeout", "NMGAddress", "NMGPort", "DefaultRetryPolicy",
			"AntiSpam.Characters2Escape", "DescendingTime", "Skytel_Prefix",
			"Indigo_prefix", "APBW_CPID", "APBW_SID", "DefaultSuffix", "LocalSuffix",
			"ReadBuffersize", "enquirelinktime", "SMSQueryDRHttpModule", "MaxCPUUsage",
			"SystemStatusCheckInterval", "RuninOverloadStatusPeriod", "ExpiredMessageQueueSize",
			"ExceptionMsgFileSwitchInterval", "ExceptionMsgFileMaxSize", "PmqFileSwitchInterval",
			"PmqFileMaxSize", "SdqFileSwitchInterval", "SdqFileMaxSize"
		};
		return contains(keys, key);
	}

	private boolean isModuleRuntimeKey(String key) {
		String module = System.getProperty("module");
		if (module == null || module.length() == 0 || !key.startsWith(module + ".")) {
			return false;
		}
		String suffix = key.substring(module.length() + 1);
		String[] keys = new String[] {
			"ConnectTimeout", "ReadTimeout", "ContentLength", "Asynchronous", "ClearTime",
			"ClearProcessedMsgTime", "DRClearTime", "MaxCDRAsynQueueSize",
			"MaxSDQAsynQueueSize", "OptionCell"
		};
		return contains(keys, suffix);
	}

	private boolean isShardRuntimeKey(String key) {
		return "TotalShards".equals(key) || "MyShards".equals(key)
				|| key.startsWith("CoreShard.")
				|| key.startsWith("ClientShard.");
	}

	private boolean isMqmRuntimeKey(String key) {
		return "MQM.ActiveStandbyEnable".equals(key)
				|| "MQM.ActivePriority".equals(key)
				|| "MQM.ActiveLeaseTtlSeconds".equals(key)
				|| "MQM.ActiveRenewSeconds".equals(key)
				|| "MQM.StandbyCheckSeconds".equals(key)
				|| "MQM.ActivePriorityDelayMs".equals(key);
	}

	private boolean isIntegerRuntimeKey(String key) {
		if ("Smpp.ServiceTypeIDTag".equals(key) || isBooleanRuntimeKey(key) || isShardRuntimeKey(key)
				|| isLongRuntimeKey(key)) {
			return false;
		}
		if (key.startsWith("RedisStreamConsumer.") || key.startsWith("RedisStreamMonitor.")) {
			return true;
		}
		if ("MQM.ActiveLeaseTtlSeconds".equals(key) || "MQM.ActiveRenewSeconds".equals(key)
				|| "MQM.StandbyCheckSeconds".equals(key) || "MQM.ActivePriorityDelayMs".equals(key)) {
			return true;
		}
		return isDirectRuntimeKey(key) && !"ScreenedIPs".equals(key) && !"LoopbackAddresses".equals(key);
	}

	private boolean isBooleanRuntimeKey(String key) {
		return "StoreDRModeEnable".equals(key) || "RedisStreamGlobalPELMonitorEnable".equals(key)
				|| "RedisStreamTraceLogEnable".equals(key)
				|| "RedisStreamTrimApproximate".equals(key)
				|| "RedisStreamBackPressureEnable".equals(key)
				|| "RedisStreamMonitor.Enable".equals(key)
				|| "Metrics.Enable".equals(key) || "Metrics.PrintZero".equals(key)
				|| "Metrics.PrintGauge".equals(key) || "Metrics.Ssid.Enable".equals(key)
				|| "AutoInDR.DirectStreamEnable".equals(key)
				|| "AutoInDR.DelayDispatcherEnable".equals(key)
				|| "AutoInDR.FallbackToLegacyDelayDR".equals(key)
				|| "MQM.ActiveStandbyEnable".equals(key);
	}

	private boolean isLongRuntimeKey(String key) {
		return "RedisStreamMaxLen".equals(key)
				|| "AutoInDR.DelayClaimTimeoutMs".equals(key)
				|| "AutoInDR.DelayRetryDelayMs".equals(key)
				|| "AutoInDR.DispatcherIdleSleepMs".equals(key);
	}

	private boolean isRedisStreamTuningKey(String key) {
		return "RedisStreamMaxLen".equals(key)
				|| "RedisStreamTrimApproximate".equals(key)
				|| "RedisStreamBackPressureEnable".equals(key)
				|| key.startsWith("RedisStreamConsumer.")
				|| key.startsWith("RedisStreamMonitor.");
	}

	private String getDefaultRuntimeValue(String key) {
		if ("ConnectionSilentTime".equals(key)) return String.valueOf(connectionSilentTime);
		if ("AlertMailFrequence".equals(key)) return "100";
		if ("InitPhoneLen".equals(key)) return "0";
		if ("MessageExpireTime".equals(key)) return "1380";
		if ("ResendDRThrottle".equals(key)) return "0";
		if ("Redis_MinIDExpireTime".equals(key)) return "300";
		if ("MessageFinalExpireTime".equals(key)) return "1440";
		if ("DRInterval".equals(key)) return "20";
		if ("MaxSilentTime".equals(key)) return "60";
		if ("CacheMessageTimeout".equals(key)) return "180000";
		if ("CsmIntegrityCacheTimeout".equals(key)) return "3600000";
		if ("CsmIntegrityCacheCapacity".equals(key)) return "100000";
		if ("WaitingEnquireLinkResponseTime".equals(key)) return "100";
		if ("BlackholeSsid".equals(key)) return "-1";
		if ("DNSTimeOut".equals(key)) return "20";
		if ("MaxServiceTypeID".equals(key)) return "3";
		if ("MinMessageExpireTimeFromCust".equals(key)) return "60";
		if ("MaxMessageExpireTimeFromCust".equals(key)) return "10080";
		if ("CDRFileSwitchInterval".equals(key)) return "300";
		if ("CDRFileMaxSize".equals(key)) return "100";
		if ("Metrics.ReportIntervalSeconds".equals(key)) return "30";
		if ("Metrics.LogLevel".equals(key)) return "DEBUG";
		if ("Metrics.PrintZero".equals(key)) return "false";
		if ("Metrics.PrintGauge".equals(key)) return "false";
		if ("Metrics.MinTPS".equals(key)) return "0.01";
		if ("Metrics.Ssid.Enable".equals(key)) return "false";
		if ("DB.PoolStats.Enable".equals(key)) return "false";
		if ("DB.PoolStats.IntervalSeconds".equals(key)) return "60";
		if ("RedisStreamMaxLen".equals(key)) return "1000000";
		if ("RedisStreamTrimApproximate".equals(key)) return "true";
		if ("RedisStreamBackPressureEnable".equals(key)) return "false";
		if ("RedisStreamMonitor.Enable".equals(key)) return "false";
		if ("RedisStreamMonitor.IntervalSeconds".equals(key)) return "30";
		if ("RedisStreamMonitor.WarnXLEN".equals(key)) return "100000";
		if ("RedisStreamMonitor.BlockXLEN".equals(key)) return "500000";
		if ("RedisStreamMonitor.WarnPending".equals(key)) return "10000";
		if ("MQM.ActiveStandbyEnable".equals(key)) return "true";
		if ("MQM.ActivePriority".equals(key)) return "MsgQueueMonitor1,MsgQueueMonitor2";
		if ("MQM.ActiveLeaseTtlSeconds".equals(key)) return "30";
		if ("MQM.ActiveRenewSeconds".equals(key)) return "10";
		if ("MQM.StandbyCheckSeconds".equals(key)) return "5";
		if ("MQM.ActivePriorityDelayMs".equals(key)) return "3000";
		if ("AutoInDR.DirectStreamEnable".equals(key)) return "false";
		if ("AutoInDR.DelayDispatcherEnable".equals(key)) return "true";
		if ("AutoInDR.DelayBatchSize".equals(key)) return "200";
		if ("AutoInDR.DelayClaimTimeoutMs".equals(key)) return "60000";
		if ("AutoInDR.DelayRetryDelayMs".equals(key)) return "3000";
		if ("AutoInDR.DelayMaxRetry".equals(key)) return "20";
		if ("AutoInDR.FallbackToLegacyDelayDR".equals(key)) return "true";
		if ("AutoInDR.DelayPayloadTTLSeconds".equals(key)) return "172800";
		if ("AutoInDR.DispatcherIdleSleepMs".equals(key)) return "200";
		return "0";
	}

	private boolean isRestartRequiredConfigKey(String key) {
		String module = System.getProperty("module");
		return "NodeID".equals(key) || "RouterModule".equals(key) || "ServerIP".equals(key)
				|| "ServiceIP".equals(key) || "SystemManager.enable".equals(key)
				|| "DB.AccessMode".equals(key) || key.startsWith("DB.Hikari.")
				|| "DataControl_DS_Names".equals(key) || "DataControl_HandOver".equals(key)
				|| key.startsWith("DS_")
				|| key.startsWith("Redis_") || key.startsWith("Redis.")
				|| key.startsWith("RedisMonitor_")
				|| key.startsWith("Ssl.") || key.startsWith("ThreadPool.")
				|| (module != null && key.startsWith(module + ".") && isModuleRestartRequiredKey(key.substring(module.length() + 1)))
				|| key.contains(".MinMessageProcessorNumber") || key.contains(".MaxMessageProcessorNumber")
				|| key.contains(".ProcessorWorkQueueSize") || key.contains(".RouterProcessorNumber")
				|| key.contains(".RouterProcessorQueueNumber") || key.contains(".BufferWindowsSize")
				|| key.contains(".BufferTimeout");
	}

	private boolean isModuleRestartRequiredKey(String suffix) {
		String[] keys = new String[] {"Port", "SenderNumber", "SenderQueueNumber"};
		return contains(keys, suffix);
	}

	private boolean contains(String[] keys, String key) {
		for (String value : keys) {
			if (value.equals(key)) {
				return true;
			}
		}
		return false;
	}

	private void logConfigChanges(String title, Properties oldProps, Properties newProps, List<String> keys) {
		if (keys == null || keys.isEmpty()) {
			return;
		}
		for (String key : keys) {
			log.info("{}: {} {} -> {}", title, key,
					maskConfigValue(key, oldProps == null ? null : oldProps.getProperty(key)),
					maskConfigValue(key, newProps == null ? null : newProps.getProperty(key)));
		}
	}

	private String maskConfigValue(String key, String value) {
		if (value == null) {
			return null;
		}
		String lowerKey = key == null ? "" : key.toLowerCase();
		if (lowerKey.contains("password") || lowerKey.contains("passwd") || lowerKey.contains("pwd")) {
			return "******";
		}
		return value;
	}

	public int getTotalShards() {
		return totalShards;
	}

	public Set<Integer> getMyShards() {
		return myShards;
	}

	public synchronized void initCoreShardAssignment() {
		String module = System.getProperty("module");
		String shardConfig = null;
		String source = null;
		if (redisClient != null && !isBlank(module)) {
			try {
				shardConfig = redisClient.getString("system:core:shards:" + module);
				source = "redis:system:core:shards:" + module;
			} catch (Exception e) {
				log.warn("Failed to load core shard assignment from redis for module=" + module, e);
			}
		}
		if (isBlank(shardConfig) && !isBlank(module)) {
			shardConfig = properties.getProperty("CoreShard." + module);
			source = "property:CoreShard." + module;
		}
		if (isBlank(shardConfig) && redisClient != null) {
			try {
				shardConfig = redisClient.getString("system:core:shards:" + getNodeId());
				source = "redis:system:core:shards:" + getNodeId();
			} catch (Exception e) {
				log.warn("Failed to load core shard assignment from redis for nodeId=" + getNodeId(), e);
			}
		}
		if (isBlank(shardConfig)) {
			shardConfig = properties.getProperty("CoreShard." + getNodeId());
			source = "property:CoreShard." + getNodeId();
		}
		if (isBlank(shardConfig)) {
			shardConfig = properties.getProperty("MyShards", "0");
			source = "property:MyShards";
		}
		myShards = parseShardSet(shardConfig, source);
		if (log.isInfoEnabled()) {
			log.info("Core shard assignment loaded. nodeId={}, module={}, totalShards={}, myShards={}, source={}, moduleKey=CoreShard.{}, nodeKey=CoreShard.{}",
					getNodeId(), module, totalShards, formatShardSet(myShards), source, module, getNodeId());
		}
	}

	public synchronized void initClientShardAssignment() {
		String module = System.getProperty("module");
		String shardConfig = null;
		String source = null;
		if (redisClient != null && !isBlank(module)) {
			try {
				shardConfig = redisClient.getString("system:client:shards:" + module);
				source = "redis:system:client:shards:" + module;
			} catch (Exception e) {
				log.warn("Failed to load client shard assignment from redis for module=" + module, e);
			}
		}
		if (isBlank(shardConfig) && !isBlank(module)) {
			shardConfig = properties.getProperty("ClientShard." + module);
			source = "property:ClientShard." + module;
		}
		if (isBlank(shardConfig) && redisClient != null) {
			try {
				shardConfig = redisClient.getString("system:client:shards:" + getNodeId());
				source = "redis:system:client:shards:" + getNodeId();
			} catch (Exception e) {
				log.warn("Failed to load client shard assignment from redis for nodeId=" + getNodeId(), e);
			}
		}
		if (isBlank(shardConfig)) {
			shardConfig = properties.getProperty("ClientShard." + getNodeId());
			source = "property:ClientShard." + getNodeId();
		}
		if (isBlank(shardConfig)) {
			shardConfig = properties.getProperty("MyShards", "0");
			source = "property:MyShards";
		}
		myShards = parseShardSet(shardConfig, source);
		if (log.isInfoEnabled()) {
			log.info("Client shard assignment loaded. nodeId={}, module={}, totalShards={}, myShards={}, source={}, moduleKey=ClientShard.{}, nodeKey=ClientShard.{}",
					getNodeId(), module, totalShards, formatShardSet(myShards), source, module, getNodeId());
		}
	}

	private Set<Integer> parseShardSet(String shardConfig, String source) {
		Set<Integer> shards = new HashSet<Integer>();
		try {
			if (shardConfig != null) {
				String[] segments = shardConfig.split(",");
				for (String segment : segments) {
					String value = segment.trim();
					if (value.length() == 0) {
						continue;
					}
					if (value.contains("-")) {
						String[] range = value.split("-", 2);
						int start = Integer.parseInt(range[0].trim());
						int end = Integer.parseInt(range[1].trim());
						if (start > end) {
							throw new IllegalArgumentException("Invalid shard range: " + value);
						}
						for (int i = start; i <= end; i++) {
							addValidShard(shards, i, source);
						}
					} else {
						addValidShard(shards, Integer.parseInt(value), source);
					}
				}
			}
		} catch (Exception e) {
			log.error("Failed to parse shard assignment from " + source + ": " + shardConfig, e);
			shards.clear();
		}
		if (shards.isEmpty()) {
			log.warn("Shard assignment is empty, fallback to shard 0. source={}, value={}", source, shardConfig);
			shards.add(0);
		}
		return shards;
	}

	private void addValidShard(Set<Integer> shards, int shardId, String source) {
		if (shardId < 0 || shardId >= totalShards) {
			log.warn("Ignore invalid shardId {} from {}, totalShards={}", shardId, source, totalShards);
			return;
		}
		shards.add(shardId);
	}

	public String formatShardSet(Set<Integer> shards) {
		if (shards == null || shards.isEmpty()) {
			return "";
		}
		TreeSet<Integer> sorted = new TreeSet<Integer>(shards);
		StringBuilder builder = new StringBuilder();
		for (Integer shard : sorted) {
			if (builder.length() > 0) {
				builder.append(",");
			}
			builder.append(shard);
		}
		return builder.toString();
	}

	private int toNumericNodeId(String value) {
		try {
			return Integer.parseInt(value);
		} catch (Exception e) {
			return Math.abs(value == null ? 0 : value.hashCode()) % 10000;
		}
	}

	private boolean isBlank(String value) {
		return value == null || value.trim().length() == 0;
	}
	
	public boolean isLoopbackAddress(String address) {
		return loopbacks.contains(address);
	}

	public boolean needAlertMail() {
		boolean result = alertCount == 0;
		alertCount = (alertCount + 1) % alertFreq;
		return result;
	}

	/**
	 * checkSubmitMessage Provides the sub-classes to check the message
	 * integrity The function return nothing if the message is ok, otherwise, a
	 * specific exception is raised.
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	public boolean preCheckMessage(GmmsMessage msg) {
		boolean result = true;
		A2PCustomerInfo cust = null;
		try {
			if (msg == null) {
				result = false;
				return result;
			}
			
			// check serviceTypeID
			int serviceTypeID = msg.getServiceTypeID();
			if (serviceTypeID<0 || serviceTypeID > maxServiceTypeID) {
				msg.setStatus(GmmsStatus.INVALID_SERVICETYPEID);
				if(log.isInfoEnabled()){
					log.info(msg, "check ServiceTypeID error, value is {}", serviceTypeID);
				}
				result = false;
				return result;
			}
			
			if (!removePlusSign(msg)) {
				result = false;
				return false;
			}

			cust = customerManager.getCustomerBySSID(msg.getOSsID());
			if (cust == null) {
				result = false;
				return false;
			}

			if (!isNumber(msg.getRecipientAddress())
					|| !customerManager.checkRecPrefixAndLen(msg.getRecipientAddress(),msg.getOSsID())) {
				
				msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
				if(log.isInfoEnabled()){
					log.info(msg,
								"check recipient number digits or number lengths error!");
				}
				result = false;
				return false;
			}

			if (!isGSM7BitCharacter(msg.getSenderAddress())) {
				msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
				result = false;
				return false;
			}

			ArrayList list = cust.getNumberLens();
			if (list == null || list.isEmpty()) {
				if (!checkInitLen(msg.getSenderAddress(), cust
						.getOriMinNumberLen() == -1 ? initPhoneLen : cust
						.getOriMinNumberLen())) {
					msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
					if(log.isInfoEnabled()){
						log.info(msg, "check sender number lengths error!");
					}
					result = false;
					return false;
				}
			}// end if of list == null
			else {
				if (!list.contains(msg.getSenderAddress().length())) {
					msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
					if(log.isInfoEnabled()){
						log.info(msg, "check sender number lengths error!");
					}
					result = false;
					return false;
				}
			}

			// check sender prefix
			if (!this.matchOriPrefixList(cust.getAllowOriPrefixList(), msg
					.getSenderAddress())) {
				msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
				if(log.isInfoEnabled()){
					log.info(msg, "check sender number prefix list error!");
				}
				result = false;
				return false;
			}

			// check black list
			BlackList bl = customerManager.getBlackList();
			if (bl != null && !bl.allowWhenReceived(msg)) {// modified by
															// Jianming in
															// v1.0.1

				if(log.isInfoEnabled()){
					log.info(msg, "check black list error!");
				}
				msg.setStatus(GmmsStatus.POLICY_DENIED);
				result = false;
				return false;
			}

			// check content white list
			/*if (!customerManager.isAllowByContentWhiteList(msg)) {

				if(log.isInfoEnabled()){
					log.info(msg, "msg is blocked by content whitelist!");
				}
				msg.setStatus(GmmsStatus.POLICY_DENIED);
				result = false;
				return false;
			}*/

			// check antiSpam
			if (cust.isSupportIncomingAntiSpam()) {
				if (AntiSpamMilter.getInstance().checkAntiSpam(cust.getSSID(),
						msg, true)) {
					msg.setStatus(GmmsStatus.SPAMED);
					if(log.isInfoEnabled()){
						log.info(msg, "msg is antiSpam!");
					}
					result = false;
					return false;
				}
			}

		} catch (Exception ex) {
			result = false;
			return false;
		} finally {
			if (!result) {
				if(log.isInfoEnabled()){
					log.info(msg, 
						"Message error,statuscode: {}, message type: {}"
						,msg.getStatusCode(),msg.getMessageType());
				}
			}
		}
		return result;
	}
	
	//pre blacklist check for block 
	public boolean preBlackListCheck(GmmsMessage msg) {
		boolean result = true;
		A2PCustomerInfo cust = null;
		try {
			if (msg == null) {
				result = false;
				return result;
			}
			// check black list
			BlackList bl = customerManager.getBlackList();
			if (bl != null && !bl.allowBlackList(msg)) {// modified by
															// Jianming in
															// v1.0.1
				if(log.isInfoEnabled()){
					log.info(msg, "check black list error!");
				}
				msg.setStatus(GmmsStatus.POLICY_DENIED);
				result = false;
				return false;
			}			

		} catch (Exception ex) {
			result = false;
			return false;
		} finally {
			if (!result) {
				if(log.isInfoEnabled()){
					log.info(msg, 
						"Message error,statuscode: {}, message type: {}"
						,msg.getStatusCode(),msg.getMessageType());
				}
			}
		}
		return result;
	}
	
	public boolean afterCheckMessage(GmmsMessage msg) {
		boolean result = true;
		try {
			if (msg == null) {
				result = false;
				return result;
			}
			
			// duplicate check
			if (DuplicateMessageCheck.getInstance().isDuplicateMsg(msg)) {
				msg.setStatus(GmmsStatus.DUPLICATE_MSG);
				if(log.isInfoEnabled()){
					log.info(msg, "msg is duplicate");
				}
				result = false;
				return result;
			}

		} catch (Exception ex) {
			result = false;
			return result;
		} finally {
			if (!result) {
				if(log.isInfoEnabled()){
					log.info(msg, 
						"Message error,statuscode: {}, message type: {}"
						,msg.getStatusCode(),msg.getMessageType());
				}
			}
		}
		return result;
	}
	
	

	/**
	 * remove the "+" in address
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return boolean
	 */
	public boolean removePlusSign(GmmsMessage msg) {
		if (msg.getRecipientAddress() == null) {
			msg.setStatus(GmmsStatus.RECIPIENT_ADDR_ERROR);
			return false;
		}
		if (msg.getSenderAddress() == null) {
			msg.setStatus(GmmsStatus.SENDER_ADDR_ERROR);
			return false;
		}
		msg.setRecipientAddress(msg.getRecipientAddress().trim());
		msg.setSenderAddress(msg.getSenderAddress().trim());
		if (msg.getRecipientAddress().indexOf("+") != -1) {
			msg.setRecipientAddress(msg.getRecipientAddress().substring(1));
		}
		/*
		 * if (msg.getSenderAddress().indexOf("+") != -1) {
		 * msg.setSenderAddress(msg.getSenderAddress().substring(1)); }
		 */
		return true;
	}

	public String getServerIP() {
		return serverIP;
	}

	public String getServiceIP() {
		return serviceIP;
	}

	public boolean isAddressScreened(String addr) {
		return screenedIPs.contains(addr);
	}

	public boolean checkInitLen(String number, int len) {
		if (number == null || number.trim() == null) {
			return false;
		}
		return (number.length() >= len);
	}

	public boolean isNumber(String addr) {
		return addr.matches("[0-9]+");
	}

	public boolean isAlphabet(String addr) {
		return addr.matches("^[a-zA-Z]+");
	}

	public boolean isAlphabetAndNumber(String addr) {
		return addr
				.matches("^[a-z-A-Z0-9<>~_;:\"!@#&\\(\\)\\^\\$\\*\\.\\s\\?]+");
	}

	public boolean isAlphanumeric(String addr) {
		return addr
				.matches("^(?=.*[a-z-A-Z<>~_;:\"!@#&\\(\\)\\^\\$\\*\\.\\s\\?]).*");
	}

	public boolean isGSM7BitCharacter(String addr) {
		return addr
				.matches("^[\\s\\w\\\\@\u00A3$\u00A5èéùìò\u00C7\u00D8\u00F8\u00C5\u00a0\u00E5@Δ_ΦΓΛΩΠΨΣΘΞ\\^\\{\\}\\[~\\]\\|\u20AC\u00C6\u00E6\u00DF\u00C9!\"#¤%&'\\(\\)*+,-./:;<=>?\u00A1\u00C4\u00D6\u00D1\u00DC§\u00BF\u00E4\u00F6\u00F1üà]+");
	}

	public String getCommonProperty(String key) {
		String result = properties.getProperty(key);
		if (result != null) {
			return result.trim();
		} else {
			return null;
		}
	}

	/**
	 * filter special characters of SQL statement
	 * \t	匹配一个制表符,等价于\x09和\cI
	 * \v	匹配一个垂直制表符,等价于\x0b和\cK
	 * \f	匹配一个换页符
	 * \xn	匹配n，其中n为十六进制转义值。十六进制转义值必须为确定的两个数字长。例如，「\x41」匹配「A」. 正則表达式中可以使用ASCII编码   
	 * */
	public String filterSpecialChara(String str){
		 String regEx ="[\\f\\n\\r\\t\\v\\x00]";  
         Pattern p = Pattern.compile(regEx);     
         Matcher m = p.matcher(str);     
         return  m.replaceAll(" ").trim(); 	
	}
	
	
	public String getCommonProperty(String key, String defaultValue) {
		String result = properties.getProperty(key, defaultValue);
		if (result != null) {
			return result.trim();
		} else {
			return null;
		}
	}

	public String getModuleProperty(String key) {
		String result = getCommonProperty(System.getProperty("module") + "."
				+ key);
		if (result != null) {
			return result.trim();
		} else {
			return null;
		}
	}

	public String getModuleProperty(String key, String defaultValue) {
		String result = getCommonProperty(System.getProperty("module") + "."
				+ key, defaultValue);
		if (result != null) {
			return result.trim();
		} else {
			return null;
		}
	}
	
	/**
	 * get the property value which have same module type, 
	 * e.g. CoreEngine.MinRouterProcessorNumber=5
	 * @param key
	 * @param defaultValue
	 * @return
	 */
		public String getFullModuleTypeProperty(String key, String defaultValue) {
		String moduleType = moduleManager.getFullModuleType(System.getProperty("module"));
		return getCommonProperty(moduleType + "." + key, defaultValue);
	}

	/****
	 * added by AMY to handle '\' & ''' in mysql on Jan. 8 2007 mode 1:replace
	 * '\' with '\\' mode 2:replace '\' with full width '\' default: mode 1
	 */
	public String modifybackslash(String str, int mode) {
		if (str == null || str.equals(""))
			return str;
		if (mode == 2)
			str = str.replace('\\', '\uFF3C');
		else
			str = str.replaceAll("\\\\", "\\\\\\\\");
		return str;
	}

	public MessageStoreManager getMessageStoreManager() {
		return messageStoreManager;
	}

	public CDRManager getCdrManager() {
		return cdrManager;
	}

	public A2PCustomerManager getCustomerManager() {
		return customerManager;
	}

	public MessageAddressInterpreter getMessageAddressInterpreter() {
		return messageAddressInterpreter;
	}

	public int getExpireTimeInMinute() {
		return expireTime;
	}

	public int getFinalExpireTimeInMinute() {
		return finalExpireTime;
	}

	public long getDrInterval(TimeUnit timeUnit) {
		return timeUnit.convert(drInterval, SECONDS);
	}

	public long getConnectionSilentTime(TimeUnit timeUnit) {
		return timeUnit.convert(connectionSilentTime, SECONDS);
	}

	public String getRouterModule() {
		return routerModule;
	}

	public int getInitPhoneLen() {
		return this.initPhoneLen;
	}

	public int getMaxSilentTime() {
		return maxSilentTime;
	}

	public int getCacheMsgTimeout() {
		return cacheMsgTimeout;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public void setGmmsCustomerManager(A2PCustomerManager customerManager) {
		this.customerManager = customerManager;
	}

	public void setCacheMsgTimeout(int cacheMsgTimeout) {
		this.cacheMsgTimeout = cacheMsgTimeout;
	}

	public int getCdrMaxTime() {
		return cdrMaxTime;
	}

	public int getCdrMaxSize() {
		return cdrMaxSize;
	}

	public boolean isRunningStoreDRMode() {
		return isRunningStoreDRMode;
	}

	public void resetRunningStoreDRMode() {
		isRunningStoreDRMode = !isRunningStoreDRMode;
	}

	public boolean isStoreDRModeEnable() {
		return storeDRModeEnable;
	}
	
	

	public String getRecipientRuleFile() {
		return recipientRuleFile;
	}

	public void setRecipientRuleFile(String recipientRuleFile) {
		this.recipientRuleFile = recipientRuleFile;
	}

	/**
	 * 
	 * @param cst
	 *            GmmsCustomer
	 * @param addr
	 *            String
	 * @return String
	 */
	public String replaceSenderAddress(A2PCustomerInfo cst, String addr) {
		ArrayList<String[]> sendMapping = cst.getAlSenderMapping();
		String[] stringArray;
		for (int i = 0; i < sendMapping.size(); i++) {
			stringArray = sendMapping.get(i);
			if (stringArray != null && stringArray.length == 2) {
				if (addr.startsWith(stringArray[0])) {
					return stringArray[1]
							+ addr.substring(stringArray[0].length());
				}
			}
		}
		return addr;
	}

	private boolean matchOriPrefixList(Pattern pattern, String sender) {

		if (pattern == null) {
			return true;
		}
		Matcher matcher = null;
		matcher = pattern.matcher(sender);
		if (matcher != null && matcher.matches()) {
			return true;
		} else
			return false;
	}

	public HttpInterfaceManager getHttpInterfaceManager() {
		return httpInterfaceManager;
	}

	public void setHttpInterfaceManager(
			HttpInterfaceManager httpInterfaceManager) {
		this.httpInterfaceManager = httpInterfaceManager;
	}

	public Date getGMTTime() {
		long now = new Date().getTime();

		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}
		long gmtNow = now - diff;
		return new Date(gmtNow);
	}

	public Date getGMTTime(Date date) {
		if (date == null) {
			date = new Date();
		}
		long now = date.getTime();

		long diff = local.getRawOffset();
		if (local.inDaylightTime(new Date(now))) {
			diff += local.getDSTSavings();
		}
		long gmtNow = now - diff;
		return new Date(gmtNow);
	}

	public void setCsmIntegrityCacheTimeout(int csmIntegrityCacheTimeout) {
		this.csmIntegrityCacheTimeout = csmIntegrityCacheTimeout;
	}

	public int getCsmIntegrityCacheTimeout() {
		return csmIntegrityCacheTimeout;
	}

	public void setCsmIntegrityCacheCapacity(int csmIntegrityCacheCapacity) {
		this.csmIntegrityCacheCapacity = csmIntegrityCacheCapacity;
	}

	public int getCsmIntegrityCacheCapacity() {
		return csmIntegrityCacheCapacity;
	}

	public int assignUniqueNumber() {
		synchronized (mutex) {
			if (count > 1000000000) {
				count = 0;
			}
			return count++;
		}
	}

	public void setEnquireLinkResponseTiem(int enquireLinkResponseTiem) {
		this.enquireLinkResponseTime = enquireLinkResponseTiem;
	}

	public int getEnquireLinkResponseTiem() {
		return enquireLinkResponseTime;
	}

	public boolean isSystemManageEnable() {
		return isSystemManageEnable;
	}

	public ModuleManager getModuleManager() {
		return moduleManager;
	}

	public void setContentTemplateFile(String contentTemplateFile) {
		this.contentTemplateFile = contentTemplateFile;
	}

	public String getContentTemplateFile() {
		return contentTemplateFile;
	}

	/**
	 * convert Unicode String to Hex format
	 * 
	 * @param strValue
	 * @return hex String
	 */
	public static String convert2HexFormat(String strValue) {

		// check param
		if (null == strValue || strValue.length() < 1) {
			return null;
		}

		byte[] raw = null;

		try {
			raw = strValue.getBytes(HEX_CHARSET);
		} catch (UnsupportedEncodingException e) {
			log.error("Exception raised in convert2HexFormat: {}", e);
			return null;
		}

		final StringBuilder hexStrBuilder = new StringBuilder(2 * raw.length);
		for (final byte bItem : raw) {
			hexStrBuilder.append(HEXES.charAt((bItem & 0xF0) >> 4)).append(
					HEXES.charAt((bItem & 0x0F)));
		}

		return hexStrBuilder.toString();
	}

	public String getRedisFile() {
		return redisFile;
	}

	public void setRedisFile(String redisFile) {
		this.redisFile = redisFile;
	}
	
	public RedisClient getRedisClient() {
		return redisClient;
	}

	public int getDNSTimeout(){
		return this.dnsTimeout;
	}
	
	public String getCmTempFile() {
		return cmTempFile;
	}

	public void setCmTempFile(String cmTempFile) {
		this.cmTempFile = cmTempFile;
	}

	public int getRedisExpireTime(GmmsMessage msg){
		if(msg == null){
			return 24*3600;
		}
		Date expirTime = msg.getExpiryDate();
		if(expirTime !=null){
			try {
				int time = (int)(expirTime.getTime() - getGMTTime().getTime())/1000;
				if(time>3600){
					return time;
				}else{
					return 3600;
				}		
			} catch (Exception e) {
				return 3600;
			}
				
		}else{
			return 24*3600;
		}
	}
	
	public String getRedisDateIn(GmmsMessage msg){
		if(msg == null || msg.getDateIn() == null){
			return null;
		}
		return new StringBuffer(50).append("DR").append(msg.getDateIn().getTime()/1000).toString();
	}

	
	  public int getBlackholeSsid() { 
		  if (blackholeSsid >0) { 
			  return blackholeSsid;	  
		  } 
		  return -1; 		  
	  }
	 

	public int getConSecThrottleWinNumToAlert() {
		if (conSecThrottleWinNumToAlert <= 1) {
			return 3; //default
		}
		return conSecThrottleWinNumToAlert;
	}

	public int getMaxThrottleAlertMailNum() {
		if (maxThrottleAlertMailNum <= 0) {
			return 1; //default
		}
		return maxThrottleAlertMailNum;
	}

	public int getDefaultCustIncomingThreshold() {
		if (defaultCustIncomingThreshold <=0 ) {
			return 20; //default;
		}
		return defaultCustIncomingThreshold;
	}

	public long getReportModuleIncomingMsgCountInterval() {
		if (reportModuleIncomingMsgCountInterval <=0 ) {
			return TimeUnit.SECONDS.toMillis(60);
		}
		return TimeUnit.SECONDS.toMillis(reportModuleIncomingMsgCountInterval);
	}

	public long getDynamicCustInThresholdExipreTime() {
		if (dynamicCustInThresholdExipreTime <=0 ) {
			return TimeUnit.SECONDS.toMillis(90);
		}
		return TimeUnit.SECONDS.toMillis(dynamicCustInThresholdExipreTime);
	}

	public int getSystemIncomingThreshold() {
		if (systemIncomingThreshold <=0 ) {
			return 2000;
		}
		return systemIncomingThreshold;
	}

	public int getMaxCustIncomingThresholdMagnification() {
		if (maxCustIncomingThresholdMagnification <=0 ) {
			return 5;
		}
		return maxCustIncomingThresholdMagnification;
	}
	
	public boolean isDBHandover(){
		try{
			return Boolean.parseBoolean(getCommonProperty("DataControl_HandOver", "false"));
		}catch(Exception e){
			return false;
		}
	}


	public SslConfiguration getSslConfiguration() {
		return sslConfiguration;
	}


	public int getMin_ID_expireTime() {
		return min_ID_expireTime;
	}

	public void setMin_ID_expireTime(int minIDExpireTime) {
		min_ID_expireTime = minIDExpireTime;
	}

	public DBLockConnection getDataConnection() {
		return dataConnection;
	}

	public void setDataConnection(DBLockConnection dataConnection) {
		this.dataConnection = dataConnection;
	}

	

	public ExecutorServiceManager getExecutorServiceManager() {
		return executorServiceManager;
	}

	public ThreadPoolProfile getDefaultThreadPoolProfile() {
		return defaultThreadPoolProfile;
	}

	public int getMaxServiceTypeID() {
		return maxServiceTypeID;
	}

	/**
	 * Unit: minute
	 * @return
	 */
	public int getMinMessageExpireTimeFromCust() {
		return minMessageExpireTimeFromCust;
	}

	/**
	 * Unit: minute
	 * @return
	 */
	public int getMaxMessageExpireTimeFromCust() {
		return maxMessageExpireTimeFromCust;
	}
	
	/**
	 * ExpiryDate should between MinMessageExpireTimeFromCust and MaxMessageExpireTimeFromCust
	 * @param date
	 * @return
	 */
	public boolean checkExpiryDateFromCust(Date date) {
  		if (date == null) {
  			return false;
  		}
  		long validityPeriodCheck = date.getTime() - new Date().getTime();
		// check
		if ((validityPeriodCheck >= 1000 * 60 * getMinMessageExpireTimeFromCust()) 
				&& (validityPeriodCheck <= 1000 * 60 * getMaxMessageExpireTimeFromCust())) {
			return true;
		}
		return false;
  	}
	
	public boolean checkReciptAddressRegions(List<String> rejectRegions, String addr){
		try {
			
			PhoneNumberInfo info = phoneNumberGeo.lookup(addr);
			log.info("region list:{}", rejectRegions);
			if (rejectRegions.contains(info.getProvince())) {
				return true;
			}
		} catch (Exception e) {
			log.error("do the checkReciptAddressRegions error, {}", addr,e);
		}
		
		return false;
	}
  
	/**
	 * ScheduleDeliveryTime should less than MaxMessageExpireTimeFromCust
	 * @param date
	 * @return
	 */
  	public boolean checkScheduleDeliveryTimeFromCust(Date date) {
  		if (date == null) {
  			return false;
  		}
  		long validityPeriodCheck = date.getTime() - new Date().getTime();
		// check
		if (validityPeriodCheck <= 1000 * 60 * getMaxMessageExpireTimeFromCust()) {
			return true;
		}
		return false;
  	}

	public short getSmppServiceTypeIDTag() {
		return smppServiceTypeIDTag;
	}
	
	
	
	public int getResendDRThrottle() {
		return resendDRThrottle;
	}

	public void setResendDRThrottle(int resendDRThrottle) {
		this.resendDRThrottle = resendDRThrottle;
	}

	public static boolean isModifySuccessDR(int ssid, int ratio, int squaredRatio){
		if(ratio==0){
			return false;
		}
		int sign = Math.random()<0.5? 1:-1;
    	int total = (int)((ratio+sign*Math.random()*squaredRatio));
		Random random = randomMap.get(ssid);
		if (random == null) {
			random = new Random();
			randomMap.put(ssid, random);
		}
		int i = random.nextInt(100);		
		if (i<total) {
			return true;
		}else{
			return false;
		}
	}
	
	public static int getRandomValue(int ssid, int weight){		
		Random random = routingMap.get(ssid);
		if (random == null) {
			random = new Random();
			routingMap.put(ssid, random);
		}
		return (int)(random.nextDouble()*weight);	
	}
	
	public static int getRandomValueByDelayTimeInterval(String delayTime){	
		try {
			if ("0".equals(delayTime)) {
				return 0;
			}
			String[] minMax = delayTime.split(",");
			int max = 0;
			int min = 0; 
			if (minMax.length==1) {
				max = Integer.parseInt(minMax[0].trim());
			}else if (minMax.length==2) {
				min = Integer.parseInt(minMax[0].trim());
				max = Integer.parseInt(minMax[1].trim());
			}
			
			return min + (int)(Math.random()*(max-min));
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		return 0;		
	}
	
	public static int calculateContentSize(String content, String charset){
		byte[] contentBytes;
		try {
			contentBytes = content.getBytes(charset);
			int unit = charset.equalsIgnoreCase("UnicodeBigUnmarked")? 140:160;
			if (contentBytes.length<=unit) {
				return 1;
			}
			int len = contentBytes.length/(unit-6)+(contentBytes.length%(unit-6)==0?0:1);
			return len;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("calculateContentSize error", e);
		}
		return 0;
	}
		
	
	
	
	public String getSenderActionDir() {
		return senderActionDir;
	}

	public void setSenderActionDir(String senderActionDir) {
		this.senderActionDir = senderActionDir;
	}

	
	public String getContentActionDir() {
		return contentActionDir;
	}

	public void setContentActionDir(String contentActionDir) {
		this.contentActionDir = contentActionDir;
	}	

	public String getRecipientActionDir() {
		return recipientActionDir;
	}

	public void setRecipientActionDir(String recipientActionDir) {
		this.recipientActionDir = recipientActionDir;
	}
	
	

	public String getVendorTemplateFile() {
		return vendorTemplateFile;
	}

	public void setVendorTemplateFile(String vendorTemplateFile) {
		this.vendorTemplateFile = vendorTemplateFile;
	}

	
	public String getVendorRoutingFile() {
		return vendorRoutingFile;
	}

	public void setVendorRoutingFile(String vendorRoutingFile) {
		this.vendorRoutingFile = vendorRoutingFile;
	}

	public static void main(String[] args) {
		Random random = new Random();
		for (int i = 0; i < 1000; i++) {				
			int t=GmmsUtility.getRandomValue(22, 1000);
			System.out.println(t);
		}
		
	}

	public String getNodeId() {
		if (nodeId == null || nodeId.trim().length() == 0) {
			return System.getProperty("NodeID", "0");
		}
		return nodeId;
	}

}
