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
import com.king.gmms.metrics.MetricsReporter;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java_cup.internal_error;

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
			if (controlSubscriber != null) {
				controlSubscriber.stop();
			}
			if (executorServiceManager != null) {
				executorServiceManager.shutdownAll();
			}
			
		} catch (Exception e) {
			log.error(e, e);
		}
	}

	public void startControlSubscriber() {
		if (controlSubscriber == null) {
			controlSubscriber = new com.king.framework.lifecycle.RedisControlSubscriber();
			Thread t = new Thread(controlSubscriber, "RedisControlSubscriberThread");
			t.setDaemon(true);
			t.start();
		}
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
    		redisClient = RedisClient.getInstance();
    		if("M".equalsIgnoreCase(flag)){
    			redisClient.setRedisHaFlag(true);
    		}else{
    			redisClient.setRedisHaFlag(false);
    		}
    		
    	}catch(Exception e){
            log.error("GmmsUtility init redisClient error!");
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

			this.cmTempFile = a2phome+"/temp/"+moduleName+"_tempCm.cfg."+new SimpleDateFormat("yyyy-MM-dd").format(new Date());
			
			// V4.0 Node ID initialization for distributed uniqueness
			int nodeId = Integer.parseInt(props.getProperty("NodeID", "0").trim());
			com.king.message.gmms.MessageIdGenerator.setNodeId(nodeId);
			
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
			try {
				if (myShardsStr.contains("-")) {
					String[] parts = myShardsStr.split("-");
					int start = Integer.parseInt(parts[0].trim());
					int end = Integer.parseInt(parts[1].trim());
					for (int i = start; i <= end; i++) {
						myShards.add(i);
					}
				} else {
					String[] parts = myShardsStr.split(",");
					for (String p : parts) {
						if (!p.trim().isEmpty()) {
							myShards.add(Integer.parseInt(p.trim()));
						}
					}
				}
			} catch (Exception e) {
				log.error("Failed to parse MyShards: " + myShardsStr, e);
				// Default to shard 0 if parsing fails
				myShards.add(0);
			}
			
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
			
			// Start metrics reporter
			int metricsInterval = Integer.parseInt(props.getProperty("Metrics.ReportIntervalSeconds", "30").trim());
			MetricsReporter.getInstance().start(metricsInterval);
			
			initialized = true;
		} catch (Exception e) {
			log.fatal("GmmsUtility initUtility failed", e);
			System.exit(-1);
		}
	}

	public int getTotalShards() {
		return totalShards;
	}

	public Set<Integer> getMyShards() {
		return myShards;
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

}
