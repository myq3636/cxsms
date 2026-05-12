package com.king.gmms.domain;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java_cup.internal_error;

import org.apache.axis.utils.StringUtils;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.LifecycleListener;
import com.king.framework.lifecycle.LifecycleSupport;
import com.king.framework.lifecycle.event.Event;
import com.king.gmms.BlackList;
import com.king.gmms.Constant;
import com.king.gmms.ContentScan;
import com.king.gmms.ContentTemplate;
import com.king.gmms.DuplicateMessageCheck;
import com.king.gmms.GmmsUtility;
import com.king.gmms.MailSender;
import com.king.gmms.RetryPolicyInfo;
import com.king.gmms.RetryPolicyManager;
import com.king.gmms.WhiteList;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForCore;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForMGT;
import com.king.gmms.customerconnectionfactory.AcceletClientFactory;
import com.king.gmms.customerconnectionfactory.ClickatellClientFactory;
import com.king.gmms.customerconnectionfactory.CommonHttpClientFactory;
import com.king.gmms.customerconnectionfactory.MultiSmppClientFactory;
import com.king.gmms.customerconnectionfactory.MultiSmppServerFactory;
import com.king.gmms.customerconnectionfactory.PeeringTcp2ClientFactory;
import com.king.gmms.customerconnectionfactory.PeeringTcp2ServerFactory;
import com.king.gmms.customerconnectionfactory.SpringClientFactory;
import com.king.gmms.customerconnectionfactory.SslSmppClientFactory;
import com.king.gmms.ha.ModuleURI;
import com.king.gmms.protocol.commonhttp.HttpUtils;
import com.king.gmms.protocol.tcp.ByteBuffer;
import com.king.gmms.throttle.DistributedThrottlingManager;
import com.king.gmms.throttle.ThrottlingControl;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.GmmsMessage;
import com.king.message.gmms.GmmsStatus;
import com.king.rest.util.StringUtility;

public class A2PCustomerManager implements LifecycleListener {
	private static final String A2P_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY = "A2PBackupContentKeywordRoutingRelay";

	private static final String A2P_CONTENT_KEYWORD_ROUTING_RELAY = "A2PContentKeywordRoutingRelay";

	private static final String A2P_BACKUP_SENDER_ROUTING_RELAY = "A2PBackupSenderRoutingRelay";

	private static final String A2P_SENDER_ROUTING_RELAY = "A2PSenderRoutingRelay";

	private static final String A2P_BACKUP_ROUTING_RELAY = "A2PBackupRoutingRelay";

	private static final String A2P_ROUTING_RELAY = "A2PRoutingRelay";
	
	private static final String A2P_SEC_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY = "A2PSecBackupContentKeywordRoutingRelay";

	private static final String A2P_SEC_CONTENT_KEYWORD_ROUTING_RELAY = "A2PSecContentKeywordRoutingRelay";

	private static final String A2P_SEC_BACKUP_SENDER_ROUTING_RELAY = "A2PSecBackupSenderRoutingRelay";

	private static final String A2P_SEC_SENDER_ROUTING_RELAY = "A2PSecSenderRoutingRelay";

	private static final String A2P_SEC_BACKUP_ROUTING_RELAY = "A2PSecBackupRoutingRelay";

	private static final String A2P_SEC_ROUTING_RELAY = "A2PSecRoutingRelay";
	
	private static final String A2P_OP_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY = "A2POPBackupContentKeywordRoutingRelay";

	private static final String A2P_OP_CONTENT_KEYWORD_ROUTING_RELAY = "A2POPContentKeywordRoutingRelay";

	private static final String A2P_OP_BACKUP_SENDER_ROUTING_RELAY = "A2POPBackupSenderRoutingRelay";

	private static final String A2P_OP_SENDER_ROUTING_RELAY = "A2POPSenderRoutingRelay";

	private static final String A2P_OP_BACKUP_ROUTING_RELAY = "A2POPBackupRoutingRelay";

	private static final String A2P_OP_ROUTING_RELAY = "A2POPRoutingRelay";
	private static final String A2P_NUMBER_ROUTING_RELAY = "A2PNumberRoutingRelay";
	private static final int A2P_DEFALUT_OP_ROUTING_INDEX = -2;

	private static SystemLogger log = SystemLogger
			.getSystemLogger(A2PCustomerManager.class);

	private String confFile = null;
	private String confFileVersion = null; // cm.cfg file version
	private String recipientRuleFileVersion = null; // recipientRule.cfg file version
	private Map<String, A2PCustomerConfig> confBlockMap = new ConcurrentHashMap<String, A2PCustomerConfig>();
	private Map<String, A2PCustomerConfig> confBlockMap4New = new ConcurrentHashMap<String, A2PCustomerConfig>();
	private Map<String, A2PCustomerConfig> confBlockMap2Reload = new ConcurrentHashMap<String, A2PCustomerConfig>();
	private Map<String, A2PCustomerConfig> confBlockMap2Remove = new ConcurrentHashMap<String, A2PCustomerConfig>();
	private Map<String, A2PCustomerConfig> routingInfoMap = new ConcurrentHashMap<String, A2PCustomerConfig>();
	private Map<String, A2PCustomerConfig> phonePrefixMap = new ConcurrentHashMap<String, A2PCustomerConfig>();
	private Map<String, A2PCustomerConfig> vendorTemplatePrefixMap = new ConcurrentHashMap<String, A2PCustomerConfig>();
	
	// The short names stored here is standard.
	private Map<String, Integer> shortName_ssid_map = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> check_shortName_ssid_map = new ConcurrentHashMap<String, Integer>();
	private Map<String, Integer> shortName_custId_map = new ConcurrentHashMap<String, Integer>();
	private Map<Integer, Integer> custId_ssid_map = new ConcurrentHashMap<Integer, Integer>();

	// Customers which chlInit=true and serverID!=null
	private ArrayList<A2PCustomerInfo> initServers = new ArrayList<A2PCustomerInfo>();
	/**
	 * cached customer iosmsSsid and ssid_map, Key: iosmsSsid
	 */
	private Map<Integer, Integer> iosmsSsid_ssid_map = new ConcurrentHashMap<Integer, Integer>();
	// cached customer objects, Key: SSID
	private Map<Integer, A2PCustomerInfo> ssid_cust_map = new ConcurrentHashMap<Integer, A2PCustomerInfo>();
	// cached customer objects, key: SP ID
	private Map<String, A2PCustomerInfo> spid_cust_map = new ConcurrentHashMap<String, A2PCustomerInfo>();
	// cached customer objects, key: Server ID
	private Map<String, A2PCustomerInfo> serverid_cust_map = new ConcurrentHashMap<String, A2PCustomerInfo>();
	// Cache all the info of sms connectrelay between A2P and CP or DC,
	private Map<Integer, Integer> smsConnectRelayinfo = new ConcurrentHashMap<Integer, Integer>();
	// Cache all the info of mms connectrelay between A2P and CP or DC,
	private Map<Integer, Integer> smsIPConnectRelayinfo = new ConcurrentHashMap<Integer, Integer>();
	// Cache all the info of sms connectrelay between A2P and CP or DC,
	private Map<Integer, Integer> smsSS7ConnectRelayinfo = new ConcurrentHashMap<Integer, Integer>();
	// Cache all the info of VP connectrelay
	private Map<Integer, Integer> vpConnectRelayinfo = new ConcurrentHashMap<Integer, Integer>();

	private Map<String, Boolean> policyInfo = new ConcurrentHashMap<String, Boolean>();
	private Set<Integer> oWhitelistInfo = new HashSet<Integer>(); // Cache
	// whitelist
	private Set<Integer> rWhitelistInfo = new HashSet<Integer>(); // Cache
	// whitelist

	private Set<Integer> a2pInfo = new HashSet<Integer>(); // Cache all A2Ps
	private Set<Integer> channelInfo = new HashSet<Integer>();
	private Map<Integer, Integer> hubInfo = new ConcurrentHashMap<Integer, Integer>();
	// Cache all operates
	private Map<Integer, Integer> operatorInfo = new ConcurrentHashMap<Integer, Integer>();
	// Cache all virtual operates
	private Set<Integer> virtualOperatorInfo = new HashSet<Integer>();
	// Cache all shortcodes
	private Set<String> shortcodeInfo = new HashSet<String>();
	private Map<String, Integer> shortcode_ssid_map = new ConcurrentHashMap<String, Integer>();

	private Map<String, Integer> charsetRelay = new ConcurrentHashMap<String, Integer>();

	// Key: SSID(IO-MMS),Value:List<phonePrefix Pattern>
	private Map<Integer, List<Pattern>> ssidToPhonePrefix = new ConcurrentHashMap<Integer, List<Pattern>>();
	//senderAddress replacement <ssid,<ossid_rssid,<prefix,<senderAddress, replaceTo>>>>
	private Map<Integer, Map<String,Map<String,Map<String, String>>>> senderReplacementPrefix = new ConcurrentHashMap<Integer, Map<String,Map<String,Map<String, String>>>>();
	private Map<Integer, List<Pattern>> senderReplacementPrefixRegex = new ConcurrentHashMap<Integer, List<Pattern>>();
	
	//base vendor template replacement <ssid,<ossid_rssid,<prefix,<senderAddress, templateId>>>>
	private Map<Integer, Map<String,Map<String,Map<String, String>>>> templateReplacementPrefix = new ConcurrentHashMap<Integer, Map<String,Map<String,Map<String, String>>>>();
	private Map<Integer, List<Pattern>> templateReplacementPrefixRegex = new ConcurrentHashMap<Integer, List<Pattern>>();
	
	// Cache the service type based on ssid.
	private Map<Integer, String> ssid_serviceType = Collections
			.synchronizedMap(new HashMap<Integer, String>());

	// key: MNC&MCC, value: ssid
	private Map<String, Integer> smsMncMcc = new ConcurrentHashMap<String, Integer>();
	// private Map<String, Integer> mmsMncMcc = new HashMap<String, Integer> ();
	// key:ssid, value: MNC&MCC
	private Hashtable<Integer, String[]> ssidOfMncMcc = new Hashtable<Integer, String[]>();

	private Set<Integer> oOpSupportSplitMsg = new HashSet<Integer>();
	private Set<Integer> chlSupportSplitMsg = new HashSet<Integer>();
	private Set<Integer> rOpSplitSuffix = new HashSet<Integer>();
	private Map<Integer, String> rOpSupportLength = new ConcurrentHashMap<Integer, String>();
	private Map<Integer, String> rOpSupportAsciiLength = new ConcurrentHashMap<Integer, String>();

	private int currentA2P = 0;
	private int peeringTcpVersion = 0;
	private Set<Integer> currentA2Ps = new HashSet<Integer>();
	private Set<Integer> rSsidsNotSupportDR = new HashSet<Integer>();

	// cache the rop of VFKK with 2G or 3G
	// Key (Integer) 2/3 (means 2G/3G) ,Value : (Interger) operator id
	private Map<String, Integer> vfkkInfo = new ConcurrentHashMap<String, Integer>();
	// cache (OOP,ROP),Rssid
	private Map<String, Integer> routingRelayInfo = new ConcurrentHashMap<String, Integer>();
	//<ossid,<prefix, rssid>
	private Map<String, Map<String,String>> customerRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,String>>();
	private Map<String, Map<String,String>> customerBackupRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,String>>();
	
	//<ossid,<sender_prefix, rssid>>
	private Map<String, Map<String,String>> customerSenderRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,String>>();
	private Map<String, Map<String,String>> customerBackupSenderRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,String>>();
	
	//<ossid,<prefix, <keywordId,rssid>>>
	private Map<String, Map<String,Map<String,String>>> customerContentKeywordRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,Map<String,String>>>();
	private Map<String, Map<String,Map<String,String>>> customerBackupContentKeywordRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,Map<String,String>>>();
	
	private Map<String,List<Pattern>> customerPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerBackupPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerSenderPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerBackupSenderPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerContentKeywordPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerBackupContentKeywordPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerDefaultPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerBackupDefaultPerfixInfo = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String, Map<String,String>> defaultBackupRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,String>>();
	private Map<String, Map<String,String>> defaultRoutingRelayInfo = new ConcurrentHashMap<String, Map<String,String>>();
    
	//2-time-routing
	private Map<String, Map<String,String>> customerRoutingRelayInfo2 = new ConcurrentHashMap<String, Map<String,String>>();
	private Map<String, Map<String,String>> customerBackupRoutingRelayInfo2 = new ConcurrentHashMap<String, Map<String,String>>();
	
	//2-time-routing-<ossid,<sender_prefix, rssid>>
	private Map<String, Map<String,String>> customerSenderRoutingRelayInfo2 = new ConcurrentHashMap<String, Map<String,String>>();
	private Map<String, Map<String,String>> customerBackupSenderRoutingRelayInfo2 = new ConcurrentHashMap<String, Map<String,String>>();
	
	//2-time-routing-<ossid,<prefix, <keywordId,rssid>>>
	private Map<String, Map<String,Map<String,String>>> customerContentKeywordRoutingRelayInfo2 = new ConcurrentHashMap<String, Map<String,Map<String,String>>>();
	private Map<String, Map<String,Map<String,String>>> customerBackupContentKeywordRoutingRelayInfo2 = new ConcurrentHashMap<String, Map<String,Map<String,String>>>();
	//2-time-routing
	private Map<String,List<Pattern>> customerPerfixInfo2 = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerBackupPerfixInfo2 = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerSenderPerfixInfo2 = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerBackupSenderPerfixInfo2 = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerContentKeywordPerfixInfo2 = new ConcurrentHashMap<String,List<Pattern>>();
	private Map<String,List<Pattern>> customerBackupContentKeywordPerfixInfo2 = new ConcurrentHashMap<String,List<Pattern>>();
    //op content routing
	//<ossid, opid, <keyword, priority, rssids>>
	private HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> customeropContentKeywordRoutingRelayInfo= HashBasedTable.create();
	private HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> customeropBackupContentKeywordRoutingRelayInfo= HashBasedTable.create();
	//op sender routing
	//<ossid, opid, <sender, priority, rssids>>
	private HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> customeropSenderRoutingRelayInfo= HashBasedTable.create();
	private HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> customeropBackupSenderRoutingRelayInfo= HashBasedTable.create();
	//op routing
	//<ossid, opid, <priority, rssids>>
	private HashBasedTable<String,Integer,Map<Integer,String>> customeropRoutingRelayInfo= HashBasedTable.create();
	private HashBasedTable<String,Integer,Map<Integer,String>> customeropBackupRoutingRelayInfo= HashBasedTable.create();
	
	//<ossid, phone, <priority, rssids>>
	private HashBasedTable<String,Long,Map<Integer,String>> customerNumberRoutingRelayInfo= HashBasedTable.create();
	private HashBasedTable<String,Long,Map<Integer,String>> customerNumberBackupRoutingRelayInfo= HashBasedTable.create();
	/**
	 * key: Ossid_ROP </br>
	 * value: backupDC
	 */
	private Map<String, Integer> backupRoutingRelayInfo = new ConcurrentHashMap<String, Integer>();

	/**
	 * key: O_OP/O_Ssid + R_OP + ServiceTypeID value: Relay
	 */
	private Map<String, Integer> serviceTypeIDRelayInfo = new ConcurrentHashMap<String, Integer>();

	// cache (ssid, DR mode)
	private Map<Integer, Integer> deliveryreportMode = new ConcurrentHashMap<Integer, Integer>();
	//<ssid,cc>
	private Map<String, List<Pattern>> ssidPrefixListPatternForSenderBlackList = new ConcurrentHashMap<String, List<Pattern>>();
	//<ssid, number>
	private Map<String, List<Pattern>> ssidPrefixListPatternForRecipientBlackList = new ConcurrentHashMap<String, List<Pattern>>();
	//<ssid_cc,blacklist>
	private Map<String, List<Pattern>> prefixBlackListPatternForSenderBlackList = new ConcurrentHashMap<String, List<Pattern>>();
	//<ssid,cc>
	private Map<String, List<Pattern>> ssidPrefixListPatternForSenderWLList = new ConcurrentHashMap<String, List<Pattern>>();
	
	//<ssid_action(wl;rp)_cc,wlsender:action(wl;rp):cc>
	private Map<String, List<Pattern>> prefixWLListPatternForSenderWLList = new ConcurrentHashMap<String, List<Pattern>>();
	//<wlsender:action(wl;rp):cc, sender>
	private Map<String, String> prefixWLSenderMappingForSenderWL = new ConcurrentHashMap<String, String>();
	
	//<ssid,cc>
	private Map<String, List<Pattern>> ssidPrefixListPatternForContentBlackList = new ConcurrentHashMap<String, List<Pattern>>();
	//<ssid_cc,blacklist>
	private Map<String, List<Pattern>> prefixBlackListPatternForContentBlackList = new ConcurrentHashMap<String, List<Pattern>>();
	//<ssid,cc>
	private Map<String, List<Pattern>> ssidPrefixListPatternForContentWhiteList = new ConcurrentHashMap<String, List<Pattern>>();
	//<ssid_cc,blacklist>
	private Map<String, List<Pattern>> prefixBlackListPatternForContentWhiteList = new ConcurrentHashMap<String, List<Pattern>>();

	private BlackList blacklist;
	private WhiteList whitelist;
	//<country, <ossid_rssid, rpssid>>
	private Map<String, Map<String, Integer>> systemRoutingReplacementMap = new ConcurrentHashMap<String, Map<String, Integer>>();
    private List<Pattern> systemRoutingReplacementPrefixPattern = new ArrayList<>();
	/**
	 * content template
	 */
	private ContentTemplate contentTpl;

	/**
	 * content white list map
	 * <p>
	 * key: Ossid_Oaddr or Ossid, value:List contentTplID
	 * </p>
	 */
	private Map<String, List<String>> contentWhiteListInfo = new ConcurrentHashMap<String, List<String>>();

	/**
	 * content relay info map
	 * <p>
	 * key:Ossid_Rop, value:List relay+contentTplID>
	 * </p>
	 */
	private Map<String, List<RelayMark>> contentRelayInfo = new ConcurrentHashMap<String, List<RelayMark>>();

	/**
	 * sender addr relay info map
	 * <p>
	 * key:Ossid_Rop, value:List<relay+Oaddr>
	 * </p>
	 */
	private Map<String, List<RelayMark>> senderAddrRelayInfo = new ConcurrentHashMap<String, List<RelayMark>>();

	/**
	 * sender addr replace info map
	 * <p>
	 * key:Ossid_OAddr_Relay or Ossid_Relay, value:OAddrReplace
	 * </p>
	 */
	private Map<String, String> senderAddrReplaceInfo = new ConcurrentHashMap<String, String>();

	/**
	 * recipient addr replace info map
	 * <p>
	 * key:Ossid_RAddr_Relay or Ossid_Relay, value:RAddrReplace
	 * </p>
	 */
	private Map<String, String> recipientAddrReplaceInfo = new ConcurrentHashMap<String, String>();

	/**
	 * content signature Map
	 * <p>
	 * key: rssid <br/>
	 * value rssidSignatureMap: key: O_SSID + R_SSID + R_OP + O_Addr etc. value:
	 * signatureMap
	 * </p>
	 * key:signature_charset, value: signature
	 */
	private Map<Integer, Map<String, Map<String, String>>> contentSignatureMap = new ConcurrentHashMap<Integer, Map<String, Map<String, String>>>();

	/**
	 * HashMap: contentKeywordMap<OSSID,HashMap<Keyword,ROP SSID>>
	 */
	private Map<Integer, Map<String, Integer>> contentKeywordMap = new ConcurrentHashMap<Integer, Map<String, Integer>>();
	
	/**
	 * contentTemplateMap<OSSID+RSSID,HashMap<countryCode,templateId>>
	 */
	private Map<String, Map<String, String>> contentTemplateMap = new ConcurrentHashMap<String, Map<String, String>>();

	private static final String CONF_SEPERATOR = "_";

	private Map<String, List<String>> opRequiredCharsets = new ConcurrentHashMap<String, List<String>>();
	private Map<Integer, String[]> customerLocalDNSPrefix = new ConcurrentHashMap<Integer, String[]>();
	private Map<Integer, ArrayList<ContentScan>> contentScanInfo = new ConcurrentHashMap<Integer, ArrayList<ContentScan>>();
	private ArrayList noticeInfo = new ArrayList();

	private Set<Integer> rSsidNotSupportUDH = new HashSet<Integer>();
	private Set<Integer> rSsidSupport7Bit = new HashSet<Integer>();

	private Set<Integer> isSupportExpiredDR = new HashSet<Integer>();

	private Map<String, String> connectionNodeMap = Collections
			.synchronizedMap(new HashMap<String, String>());
	private Map<String, String> connectionIPMap = Collections
			.synchronizedMap(new HashMap<String, String>());
	private Map<String, ArrayList<ConnectionInfo>> serverInfoMap = Collections
			.synchronizedMap(new HashMap<String, ArrayList<ConnectionInfo>>());
	private String a2pIP = null;

	private Set<Integer> partitionInfo = new HashSet<Integer>();

	private Map<String, ArrayList<Integer>> protocol_ssid_map = new Hashtable<String, ArrayList<Integer>>();
	// used by queryDR for commonHttp
	private ArrayList<Integer> queryMsg_ssid_list = new ArrayList<Integer>();
	private ArrayList<Integer> corePriority_ssid_list = new ArrayList<Integer>();

	private Set<Integer> ssidsInStoreMode = Collections
			.synchronizedSet(new HashSet<Integer>());
	private Set<Integer> ssidsInRopBusyMode = Collections
			.synchronizedSet(new HashSet<Integer>());
	private Set<Integer> ssidsInRopFailedMode = Collections
			.synchronizedSet(new HashSet<Integer>());

	private Set<Integer> ssidsInDRStoreMode = new HashSet<Integer>();
	private LifecycleSupport lifecycle;

	/**
	 * allowRoutingInfoMap is used to load RoutingInfo allow configure item
	 * Key:OSSID Value:List<Allow RSSID>
	 * */
	protected Map<Integer, List<Integer>> allowRoutingInfoMap = new ConcurrentHashMap<Integer, List<Integer>>();

	/**
	 * allowRecLengthMap is used to store recipient length Key:Customer SSID
	 * Value:List<Recipient length]>
	 * */
	protected Map<Integer, List<Integer>> recipientLengthMap = new ConcurrentHashMap<Integer, List<Integer>>();
	
	protected Map<String, RecipientAddressRule> recipientRuleMap = new ConcurrentHashMap<String, RecipientAddressRule>();
	
	private List<Pattern>  recipientRuleRegexs = new ArrayList<Pattern>();

	private  long phonePrefixTime;
	private  long vendorTemplateTime;
	//random for loadbalance routing;
	private int loadbalanceRandom = 0;
	
	public A2PCustomerManager(String conFile) {
		this.confFile = conFile;
		init();
		// clearCache();
		lifecycle = GmmsUtility.getInstance().getLifecycleSupport();
		lifecycle.addListener(Event.TYPE_ROUTINFO_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_CUSTOMER_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_PHONEPREFIX_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_RECEIPIENT_RULE_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_SENDER_WHITELIST_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_SENDER_BLACKLIST_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_RECIPIENT_BLACKLIST_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_SYSTEM_VENDOR_REPLACE_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_CONTENT_WHITELIST_RELOAD, this, 1);
		lifecycle.addListener(Event.TYPE_CONTENT_BLACKLIST_RELOAD, this, 1);
	}

	public int OnEvent(Event event) {
		log.info("Event Received. Type: {}", event.getEventType());
		if (event.getEventType() == Event.TYPE_ROUTINFO_RELOAD) {
			return loadRoutingInfo();
		} else if (event.getEventType() == Event.TYPE_CUSTOMER_RELOAD) {
			return loadCustomers();
		}else if (event.getEventType() == Event.TYPE_PHONEPREFIX_RELOAD) {
			return loadPhonePrefix();
		}else if (event.getEventType() == Event.TYPE_RECEIPIENT_RULE_RELOAD) {
			return loadRecipientAddressRule();
		}else if (event.getEventType() == Event.TYPE_SENDER_WHITELIST_RELOAD) {
			return loadSsidSenderWhitelist(false);
		}else if (event.getEventType() == Event.TYPE_SENDER_BLACKLIST_RELOAD) {
			return loadSsidSenderBlacklist(false);
		}else if (event.getEventType() == Event.TYPE_CONTENT_WHITELIST_RELOAD) {
			return loadSsidContentlist(false, false);
		}else if (event.getEventType() == Event.TYPE_CONTENT_BLACKLIST_RELOAD) {
			return loadSsidContentlist(false, true);
		}else if (event.getEventType() == Event.TYPE_VENDOR_CONTENT_REPLACE__RELOAD) {
			return loadVendorTemplateReplacement();
		}else if (event.getEventType() == Event.TYPE_RECIPIENT_BLACKLIST_RELOAD) {
			return loadSsidRecipientBlacklist(false);
		}else if (event.getEventType() == Event.TYPE_SYSTEM_VENDOR_REPLACE_RELOAD) {
			return loadVendorReplacement(false);
		}else {
			return 1;
		}
	}
	
	private int loadRecipientAddressRule() {
		String path = GmmsUtility.getInstance().getRecipientRuleFile();
		try {
			if (path != null) {
				File conf = new File(path);
				String md5Sum = this.getFileCheckSum(conf);
				if (md5Sum == null || md5Sum.equals(this.recipientRuleFileVersion)) {
					log.warn("No need to load recipientRule.cfg or md5Sum error. recipientRuleFileVersion={}, md5Sum={}",new Object[] { recipientRuleFileVersion, md5Sum });
					return 0;
				} else {
					if (log.isInfoEnabled()) {
						log.info("recipientRule.cfg change, recipientRuleFileVersion={}, md5Sum={}",new Object[] { recipientRuleFileVersion, md5Sum });
					}
					this.recipientRuleFileVersion = md5Sum;
				}
				
				List<RecipientAddressRule> list = A2PCustomerConfig.parseRecipientRuleInfo(conf);
				List<String> ccList = new ArrayList<String> ();
				Map<String, RecipientAddressRule> map = new ConcurrentHashMap();
				if(list!=null && !list.isEmpty()){
					for(RecipientAddressRule rule: list){
						ccList.add(rule.getCc());
						map.put(rule.getCc(), rule);
					}
				}
				Collections.sort(ccList,  new Comparator (){
					 @Override
				     public int compare(Object o1, Object o2)
				    {
				           String prefix1= (String )o1;
				           String prefix2= (String )o2;

				           return prefix2.length()-prefix1.length();
				    }
				});
				List<Pattern>  regexs  = new ArrayList<Pattern>();
				constructPhonePrefixPattern(ccList, regexs);
				this.recipientRuleRegexs = regexs;
				this.recipientRuleMap = map;
			}
		} catch (Exception e) {
			log.error("load recipientRule file failed", e);
		}
		return 0;
	}
	
	private int loadSsidSenderBlacklist(boolean isInit) {
		String path = GmmsUtility.getInstance().getSenderActionDir();
		
		try {
			if (path != null) {
				File conf = new File(path);				
				Map<String, Map<String, List<String>>> ssidSenderConfigMap = A2PCustomerConfig.parseSenderActionDir(conf);
				if(ssidSenderConfigMap!=null && !ssidSenderConfigMap.isEmpty()){
					Map<String, List<Pattern>> tempssidPrefixListPatternForSenderBlackList = new ConcurrentHashMap<String, List<Pattern>>();
					Map<String, List<Pattern>> tempprefixBlackListPatternForSenderBlackList = new ConcurrentHashMap<String, List<Pattern>>();
					for(Map.Entry<String, Map<String, List<String>>> entry: ssidSenderConfigMap.entrySet()){
						String ssid = entry.getKey();
						Map<String, List<String>> ccSenders = entry.getValue();
						if(ccSenders!=null && !ccSenders.isEmpty()){
							Set<String> ccSet = new HashSet<>();
							for(Map.Entry<String, List<String>> ccsenderEntry: ccSenders.entrySet()){
								String cc = ccsenderEntry.getKey();
								List<String> senders = ccsenderEntry.getValue();
								if(senders!=null && !senders.isEmpty()){
									Collections.sort(senders,  new Comparator (){
										 @Override
									     public int compare(Object o1, Object o2)
									    {
									           String prefix1= (String )o1;
									           String prefix2= (String )o2;

									           return prefix2.length()-prefix1.length();
									    }
									});
									List<Pattern>  regexs  = new ArrayList<Pattern>();
									constructSenderListPattern(senders, regexs);
									tempprefixBlackListPatternForSenderBlackList.put(ssid+"_"+cc, regexs);
								}
								ccSet.add(cc);
							}
							List<String> ccList = new ArrayList<>();
							ccList.addAll(ccSet);
							Collections.sort(ccList,  new Comparator (){
								 @Override
							     public int compare(Object o1, Object o2)
							    {
							           String prefix1= (String )o1;
							           String prefix2= (String )o2;

							           return prefix2.length()-prefix1.length();
							    }
							});
							List<Pattern>  regexs  = new ArrayList<Pattern>();
							constructSenderListPattern(ccList, regexs);
							tempssidPrefixListPatternForSenderBlackList.put(ssid, regexs);
						}
					}										
					Map<String, List<Pattern>> temp = this.ssidPrefixListPatternForSenderBlackList;
					this.ssidPrefixListPatternForSenderBlackList = tempssidPrefixListPatternForSenderBlackList;
					temp.clear();
					temp = this.prefixBlackListPatternForSenderBlackList;
					this.prefixBlackListPatternForSenderBlackList = tempprefixBlackListPatternForSenderBlackList;
					temp.clear();

					if(!isInit){
						String moduleName = System.getProperty("module");
				        if (ModuleManager.getInstance().getRouterModules().contains(moduleName)) {
				        	loadSenderBlacklistToRedis(moduleName);
				        }
					}					
				}				
			}
		} catch (Exception e) {
			log.error("load loadSsidSenderBlacklist file failed", e);
		}
		return 0;
	}
	
	private int loadSsidRecipientBlacklist(boolean isInit) {
		String path = GmmsUtility.getInstance().getRecipientActionDir();
		
		try {
			if (path != null) {
				File conf = new File(path);				
				Map<String, Set<String>> ssidRecipientConfigMap = A2PCustomerConfig.parseRecipientActionDir(conf);
				if(ssidRecipientConfigMap!=null && !ssidRecipientConfigMap.isEmpty()){
					Map<String, List<Pattern>> tempssidPrefixListPatternForSenderBlackList = new ConcurrentHashMap<String, List<Pattern>>();
					for(Map.Entry<String, Set<String>> entry: ssidRecipientConfigMap.entrySet()){
						String ssid = entry.getKey();
						Set<String> recipientSet = entry.getValue();
						if(recipientSet!=null && !recipientSet.isEmpty()){							
							List<String> ccList = new ArrayList<>();
							ccList.addAll(recipientSet);
							Collections.sort(ccList,  new Comparator (){
								 @Override
							     public int compare(Object o1, Object o2)
							    {
							           String prefix1= (String )o1;
							           String prefix2= (String )o2;

							           return prefix2.length()-prefix1.length();
							    }
							});
							List<Pattern>  regexs  = new ArrayList<Pattern>();
							constructSenderListPattern(ccList, regexs);
							tempssidPrefixListPatternForSenderBlackList.put(ssid, regexs);
						}
					}										
					Map<String, List<Pattern>> temp = this.ssidPrefixListPatternForRecipientBlackList;
					this.ssidPrefixListPatternForRecipientBlackList = tempssidPrefixListPatternForSenderBlackList;
					temp.clear();
					if(!isInit){
						String moduleName = System.getProperty("module");
				        if (ModuleManager.getInstance().getRouterModules().contains(moduleName)) {
				        	loadRecipientBlacklistToRedis(moduleName);
				        }
					}					
				}				
			}
		} catch (Exception e) {
			log.error("load loadSsidSenderBlacklist file failed", e);
		}
		return 0;
	}
	
	
	private int loadVendorReplacement(boolean isInit) {
		String path = GmmsUtility.getInstance().getVendorRoutingFile();		
		try {
			if (path != null) {
				File conf = new File(path);				
				Map<String, Map<String,Integer>> ccVendorReplacementMap = A2PCustomerConfig.parseVendorReplacement(conf);
							
				Map<String, Map<String,Integer>> temp = this.systemRoutingReplacementMap;
				this.systemRoutingReplacementMap = ccVendorReplacementMap;
				temp.clear();
				if(systemRoutingReplacementMap!=null && !systemRoutingReplacementMap.isEmpty()) {
					List<String> ccList = new ArrayList<>();
					ccList.addAll(systemRoutingReplacementMap.keySet());
					Collections.sort(ccList,  new Comparator (){
						 @Override
					     public int compare(Object o1, Object o2)
					    {
					           String prefix1= (String )o1;
					           String prefix2= (String )o2;

					           return prefix2.length()-prefix1.length();
					    }
					});
					List<Pattern>  regexs  = new ArrayList<Pattern>();
					constructPhonePrefixPattern(ccList, regexs);
					List<Pattern> tempPrefix = systemRoutingReplacementPrefixPattern;
					systemRoutingReplacementPrefixPattern = regexs;
					tempPrefix.clear();
				}else {
					systemRoutingReplacementPrefixPattern.clear();
				}
				if(!isInit){
					String moduleName = System.getProperty("module");
			        if (ModuleManager.getInstance().getRouterModules().contains(moduleName)) {
			        	loadSystemRoutingReplaceToRedis(moduleName);
			        }
				}					
			}							
		} catch (Exception e) {
			log.error("load VendorReplacement file failed", e);
		}
		return 0;
	}
	
	
	
	private int loadSsidContentlist(boolean isInit, boolean isContentBlack) {
		String path = GmmsUtility.getInstance().getContentActionDir();
		String suffer = Constant.CONTENT_BLACK_SUFFER_FEX;
		if(!isContentBlack){
			suffer = Constant.CONTENT_WHITLE_SUFFER_FEX;
		}
		try {
			if (path != null) {
				File conf = new File(path);				
				Map<String, Map<String, List<String>>> ssidSenderConfigMap = A2PCustomerConfig.parseContentActionDir(conf, suffer);
				if(ssidSenderConfigMap!=null && !ssidSenderConfigMap.isEmpty()){
					Map<String, List<Pattern>> tempssidPrefixListPatternForSenderBlackList = new ConcurrentHashMap<String, List<Pattern>>();
					Map<String, List<Pattern>> tempprefixBlackListPatternForSenderBlackList = new ConcurrentHashMap<String, List<Pattern>>();
					for(Map.Entry<String, Map<String, List<String>>> entry: ssidSenderConfigMap.entrySet()){
						String ssid = entry.getKey();
						Map<String, List<String>> ccSenders = entry.getValue();
						if(ccSenders!=null && !ccSenders.isEmpty()){
							Set<String> ccSet = new HashSet<>();
							for(Map.Entry<String, List<String>> ccsenderEntry: ccSenders.entrySet()){
								String cc = ccsenderEntry.getKey();
								List<String> senders = ccsenderEntry.getValue();
								if(senders!=null && !senders.isEmpty()){
									Collections.sort(senders,  new Comparator (){
										 @Override
									     public int compare(Object o1, Object o2)
									    {
									           String prefix1= (String )o1;
									           String prefix2= (String )o2;

									           return prefix2.length()-prefix1.length();
									    }
									});
									List<Pattern>  regexs  = new ArrayList<Pattern>();
									constructPhonePrefixPattern(senders, regexs);
									tempprefixBlackListPatternForSenderBlackList.put(ssid+"_"+cc, regexs);
								}
								ccSet.add(cc);
							}
							List<String> ccList = new ArrayList<>();
							ccList.addAll(ccSet);
							Collections.sort(ccList,  new Comparator (){
								 @Override
							     public int compare(Object o1, Object o2)
							    {
							           String prefix1= (String )o1;
							           String prefix2= (String )o2;

							           return prefix2.length()-prefix1.length();
							    }
							});
							List<Pattern>  regexs  = new ArrayList<Pattern>();
							constructPhonePrefixPattern(ccList, regexs);
							tempssidPrefixListPatternForSenderBlackList.put(ssid, regexs);
						}
					}	
					if(isContentBlack){
						Map<String, List<Pattern>> temp = this.ssidPrefixListPatternForContentBlackList;
						this.ssidPrefixListPatternForContentBlackList = tempssidPrefixListPatternForSenderBlackList;
						temp.clear();
						temp = this.prefixBlackListPatternForContentBlackList;
						this.prefixBlackListPatternForContentBlackList = tempprefixBlackListPatternForSenderBlackList;
						temp.clear();
					}else{
						Map<String, List<Pattern>> temp = this.ssidPrefixListPatternForContentWhiteList;
						this.ssidPrefixListPatternForContentWhiteList = tempssidPrefixListPatternForSenderBlackList;
						temp.clear();
						temp = this.prefixBlackListPatternForContentWhiteList;
						this.prefixBlackListPatternForContentWhiteList = tempprefixBlackListPatternForSenderBlackList;
						temp.clear();
					}
					

					if(!isInit){
						String moduleName = System.getProperty("module");
				        if (ModuleManager.getInstance().getRouterModules().contains(moduleName)) {
				        	loadContentActionlistToRedis(moduleName, isContentBlack);
				        }
					}					
				}				
			}
		} catch (Exception e) {
			log.error("load loadSsidSenderBlacklist file failed", e);
		}
		return 0;
	}
	
	public void loadSenderBlacklistToRedis(String core){
		Set<String> keys = GmmsUtility.getInstance().getRedisClient().smembers("senderBlacklist");
		if (keys != null && !keys.isEmpty()) {
			for (String key : keys) {
				if (key.contains(core)) {
					GmmsUtility.getInstance().getRedisClient().del(key);
				}				
			}			
		}
		
		if(ssidPrefixListPatternForSenderBlackList!= null && !ssidPrefixListPatternForSenderBlackList.isEmpty()){
			for (Map.Entry<String,List<Pattern>> entry : ssidPrefixListPatternForSenderBlackList.entrySet()) {
				List<Pattern> senderRegexList = entry.getValue();
				String ssidcc = entry.getKey();
				if(senderRegexList!=null && !senderRegexList.isEmpty()){
					for(Pattern pn: senderRegexList){
						String senderValue = pn.pattern();
						log.info("load sender ssid blacklist {},{}",ssidcc, senderValue);
					}
				}
			}			
		}		
		
		if(prefixBlackListPatternForSenderBlackList!= null && !prefixBlackListPatternForSenderBlackList.isEmpty()){
			for (Map.Entry<String,List<Pattern>> entry : prefixBlackListPatternForSenderBlackList.entrySet()) {
				List<Pattern> senderRegexList = entry.getValue();
				String ssidcc = entry.getKey();
				String ssid = ssidcc.split("_")[0];
				String cc = ssidcc.split("_")[1];
				Map<String, String> ccsenderMap = new HashMap<>();
				if(senderRegexList!=null && !senderRegexList.isEmpty()){
					for(Pattern pn: senderRegexList){
						String senderValue = pn.pattern();						
						ccsenderMap.put(cc+"_"+HttpUtils.encryptUP(senderValue, "MD5"), senderValue);
					}
				}
				GmmsUtility.getInstance().getRedisClient().setHashAndSet("senderBlacklist", "senderBlacklist"+core+"_"+ssid, ccsenderMap);
			}			
		}		
	}
	
	public void loadRecipientBlacklistToRedis(String core){
		Set<String> keys = GmmsUtility.getInstance().getRedisClient().smembers("recipientBlacklist");
		if (keys != null && !keys.isEmpty()) {
			for (String key : keys) {
				if (key.contains(core)) {
					GmmsUtility.getInstance().getRedisClient().del(key);
				}				
			}			
		}
		
		if(ssidPrefixListPatternForRecipientBlackList!= null && !ssidPrefixListPatternForRecipientBlackList.isEmpty()){
			for (Map.Entry<String,List<Pattern>> entry : ssidPrefixListPatternForRecipientBlackList.entrySet()) {
				List<Pattern> senderRegexList = entry.getValue();
				String ssid = entry.getKey();				
				Map<String, String> ccsenderMap = new HashMap<>();
				if(senderRegexList!=null && !senderRegexList.isEmpty()){
					for(Pattern pn: senderRegexList){
						String senderValue = pn.pattern();						
						ccsenderMap.put(ssid+"_"+HttpUtils.encryptUP(senderValue, "MD5"), senderValue);
					}
				}
				GmmsUtility.getInstance().getRedisClient().setHashAndSet("recipientBlacklist", "recipientBlacklist"+core+"_"+ssid, ccsenderMap);
			}			
		}		
	}
	
	public void loadSystemRoutingReplaceToRedis(String core){
		/*
		 * Set<String> keys = GmmsUtility.getInstance().getRedisClient().smembers(
		 * "systemRoutingReplacement."+core); if (keys != null && !keys.isEmpty()) { for
		 * (String key : keys) { if (key.contains(core)) {
		 * 
		 * } } }
		 */
		GmmsUtility.getInstance().getRedisClient().del("systemRoutingReplacement."+core);		
		if(systemRoutingReplacementMap!= null && !systemRoutingReplacementMap.isEmpty()){
			List<String> value = new ArrayList<>();
			for (Map.Entry<String,Map<String,Integer>> entry : systemRoutingReplacementMap.entrySet()) {
				Map<String,Integer> vendor = entry.getValue();
				String prefix = entry.getKey();				
				for(Map.Entry<String, Integer> entry1 : vendor.entrySet()) {
					value.add(prefix+":"+entry1.getKey()+":"+entry1.getValue());
				}				
			}
			GmmsUtility.getInstance().getRedisClient().sadd("systemRoutingReplacement."+core, value);
		}		
	}
	
	public void loadContentActionlistToRedis(String core, boolean isBlack){
		String prefix = "contentBlacklist";
		Map<String, List<Pattern>> temp = prefixBlackListPatternForContentBlackList;
		if(!isBlack){
			prefix = "contentWhitelist";
			temp = prefixBlackListPatternForContentWhiteList;
		}
		Set<String> keys = GmmsUtility.getInstance().getRedisClient().smembers(prefix);
		if (keys != null && !keys.isEmpty()) {
			for (String key : keys) {
				if (key.contains(core)) {
					GmmsUtility.getInstance().getRedisClient().del(key);
				}				
			}			
		}	
		
		if(temp!= null && !temp.isEmpty()){
			for (Map.Entry<String,List<Pattern>> entry : temp.entrySet()) {
				List<Pattern> senderRegexList = entry.getValue();
				String ssidcc = entry.getKey();
				String ssid = ssidcc.split("_")[0];
				String cc = ssidcc.split("_")[1];
				Map<String, String> ccsenderMap = new HashMap<>();
				if(senderRegexList!=null && !senderRegexList.isEmpty()){
					for(Pattern pn: senderRegexList){
						String senderValue = pn.pattern();						
						ccsenderMap.put(cc+"_"+HttpUtils.encryptUP(senderValue, "MD5"), senderValue);
					}
				}
				GmmsUtility.getInstance().getRedisClient().setHashAndSet(prefix, prefix+core+"_"+ssid, ccsenderMap);
			}			
		}		
	}
	
	public void loadSenderWhitelistToRedis(String core){
		Set<String> keys = GmmsUtility.getInstance().getRedisClient().smembers("senderWhitelist");
		if (keys != null && !keys.isEmpty()) {
			for (String key : keys) {
				if (key.contains(core)) {
					GmmsUtility.getInstance().getRedisClient().del(key);
				}				
			}			
		}
		
		
		if(ssidPrefixListPatternForSenderWLList!= null && !ssidPrefixListPatternForSenderWLList.isEmpty()){
			for (Map.Entry<String,List<Pattern>> entry : ssidPrefixListPatternForSenderWLList.entrySet()) {
				List<Pattern> senderRegexList = entry.getValue();
				String ssidcc = entry.getKey();
				if(senderRegexList!=null && !senderRegexList.isEmpty()){
					for(Pattern pn: senderRegexList){
						String senderValue = pn.pattern();
						log.info("load sender ssid whitelist {},{}",ssidcc, senderValue);
					}
				}
			}			
		}
				
		if(prefixWLListPatternForSenderWLList!= null && !prefixWLListPatternForSenderWLList.isEmpty()){
			for (Map.Entry<String,List<Pattern>> entry : prefixWLListPatternForSenderWLList.entrySet()) {
				List<Pattern> senderRegexList = entry.getValue();
				String ssidcc = entry.getKey();
				String ssid = ssidcc.split("_")[0];
				String cc = ssidcc.split("_")[2];
				Map<String, String> ccsenderMap = new HashMap<>();
				if(senderRegexList!=null && !senderRegexList.isEmpty()){
					for(Pattern pn: senderRegexList){
						String senderValue = pn.pattern();
						String senderValues[] = senderValue.split("\\|");
						StringBuilder builder = new StringBuilder();
						for(int i =0; i<senderValues.length; i++){
							String senderAction = senderValues[i];
							String rpSender = prefixWLSenderMappingForSenderWL.get(senderAction);
							String sender = senderAction.substring(0,senderAction.lastIndexOf(":"));
							if(rpSender!=null){
								sender = sender+":"+rpSender;
							}
							builder.append(sender).append("|");
						}
						ccsenderMap.put(cc+"_"+HttpUtils.encryptUP(builder.toString(), "MD5"), builder.toString());
					}
				}
				GmmsUtility.getInstance().getRedisClient().setHashAndSet("senderWhitelist", "senderWhitelist"+core+"_"+ssid, ccsenderMap);
			}			
		}		
	}
	
	public boolean doSenderBlacklistCheck(int ssid, GmmsMessage msg) {
		
		List<Pattern> prefixList = ssidPrefixListPatternForSenderBlackList.get(""+ssid);
		if (prefixList == null || prefixList.isEmpty() ) {
			return false;		    
		}
		String recipientAddress = msg.getRecipientAddress();
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(recipientAddress);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.trace(msg, "match cc {}", key);
					if (recipientAddress.startsWith(key)) {
						if(doSenderBlacklistRegex(msg, ""+ssid, key, msg.getSenderAddress())){
							return true;
						}
					}else{
						continue;
					}/*else {
						if(doSenderBlacklistCheckSecondMatch(msg, pattern, key, recipientAddress, ""+ssid)){
							return true;
						}
					}	*/											
				}
			}
		}		
		return doSenderBlacklistRegex(msg, ""+ssid, "all",  msg.getSenderAddress());												
	} 
	
    public boolean doRecipientBlacklistCheck(int ssid, GmmsMessage msg) {
		
		List<Pattern> prefixList = ssidPrefixListPatternForRecipientBlackList.get(""+ssid);
		if (prefixList == null || prefixList.isEmpty() ) {
			return false;		    
		}
		String recipientAddress = msg.getRecipientAddress();
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(recipientAddress);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.trace(msg, "match recipient {}", key);
					if (recipientAddress.startsWith(key)) {
						return true;
					}else{
						continue;
					}											
				}
			}
		}		
		return false;												
	} 
	
	
	
	private boolean doSenderBlacklistCheckSecondMatch(GmmsMessage msg, Pattern pattern, String matchString, 
			String recipientAddress, String ssid) {
		String senderPatternString = pattern.pattern();
		if (StringUtility.stringIsNotEmpty(senderPatternString)) {
			String newPatString = senderPatternString.replaceAll(matchString, "a-a-a");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(recipientAddress);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (recipientAddress.startsWith(key)) {
					if(doSenderBlacklistRegex(msg, ""+ssid, key, msg.getSenderAddress())){
						return true;
					}
				}else {
					return doSenderBlacklistCheckSecondMatch(msg, newPattern, key, recipientAddress, ssid);
				}												
			}								
		}
		return false;
	}
	
    public boolean doContentBlacklistCheck(int ssid, GmmsMessage msg) {
		
		List<Pattern> prefixList = ssidPrefixListPatternForContentBlackList.get(""+ssid);
		if (prefixList == null || prefixList.isEmpty() ) {
			return false;		    
		}
		String recipientAddress = msg.getRecipientAddress();
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(recipientAddress);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.trace(msg, "match cc {}", key);
					if (recipientAddress.startsWith(key)) {
						if(doContentBlacklistRegex(msg, ""+ssid, key, msg.getTextContent())){
							return true;
						}
					}else{
						continue;
					}											
				}
			}
		}		
		return doContentBlacklistRegex(msg, ""+ssid, "all",  msg.getTextContent());												
	}

	private boolean doContentBlacklistRegex(GmmsMessage msg, String ssid, String cc, String content) {
		log.trace(msg, "do login doContentBlacklistRegex method {}, {}, {}", ssid, cc, content);
		List<Pattern> senderRegex = prefixBlackListPatternForContentBlackList.get(ssid+"_"+cc);
		if (senderRegex == null) {
			return false;			
		}
		for (Pattern pattern : senderRegex) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(content);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.info(msg,"msg is policy deny by content blacklist is:{}", key);
					return true;											
				}
			}
		}	
		if(!"all".equalsIgnoreCase(content)){
			return doContentBlacklistRegex(msg, ssid, cc, "all");
		}
		return false;
	}
	
    public boolean doContentWhitelistCheck(int ssid, GmmsMessage msg) {
		boolean result = true;
		List<Pattern> prefixList = ssidPrefixListPatternForContentWhiteList.get(""+ssid);
		if (prefixList == null || prefixList.isEmpty() ) {
			if(ssid!=-1){
				return doContentWhitelistCheck(-1, msg);
			}else{
				return true;
			}					    
		}
		String recipientAddress = msg.getRecipientAddress();
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(recipientAddress);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.trace(msg, "match cc {}", key);
					if (recipientAddress.startsWith(key)) {
						result = false;
						if(doContentWhitelistRegex(msg, ""+ssid, key, msg.getTextContent())){
							return true;
						}
					}else{
						continue;
					}											
				}
			}
		}		
		List<Pattern> senderRegex = prefixBlackListPatternForContentWhiteList.get(ssid+"_all");
		if (senderRegex != null) {
			result = false;
			if(doContentWhitelistRegex(msg, ""+ssid, "all", msg.getTextContent())){
				return true;
			}			
		}
		if(result == false){
			return false;
		}else{
			//ssid = -1, default whitelist check
			if(ssid!=-1){
				return doContentWhitelistCheck(-1, msg);
			}
		}
		return result;
	}
    
    public boolean doVendorContentWhitelistCheck(int ssid, GmmsMessage msg) {
		boolean result = true;
		List<Pattern> prefixList = ssidPrefixListPatternForContentWhiteList.get(""+ssid);
		if (prefixList == null || prefixList.isEmpty() ) {
			if(ssid!=-2){
				return doVendorContentWhitelistCheck(-2, msg);
			}else{
				return true;
			}					    
		}
		String recipientAddress = msg.getRecipientAddress();
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(recipientAddress);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.trace(msg, "match cc {}", key);
					if (recipientAddress.startsWith(key)) {
						result = false;
						if(doContentWhitelistRegex(msg, ""+ssid, key, msg.getTextContent())){
							return true;
						}
					}else{
						continue;
					}											
				}
			}
		}		
		List<Pattern> senderRegex = prefixBlackListPatternForContentWhiteList.get(ssid+"_all");
		if (senderRegex != null) {
			result = false;
			if(doContentWhitelistRegex(msg, ""+ssid, "all", msg.getTextContent())){
				return true;
			}			
		}
		if(result == false){
			return false;
		}else{
			//ssid = -2, default whitelist check
			if(ssid!=-2){
				return doVendorContentWhitelistCheck(-2, msg);
			}
		}
		return result;
	}

	private boolean doContentWhitelistRegex(GmmsMessage msg, String ssid, String cc, String content) {
		log.trace(msg, "do login doContentWhitelistRegex method {}, {}, {}", ssid, cc, content);
		List<Pattern> senderRegex = prefixBlackListPatternForContentWhiteList.get(ssid+"_"+cc);
		if (senderRegex == null) {
			return false;			
		}
		for (Pattern pattern : senderRegex) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(content);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.info(msg,"msg is policy allow by content whitelist is:{}", key);
					return true;											
				}
			}
		}	
		if(!"all".equalsIgnoreCase(content)){
			return doContentBlacklistRegex(msg, ssid, cc, "all");
		}
		return false;
	}
	
	private boolean doSenderBlacklistRegex(GmmsMessage msg, String ssid, String cc, String sender) {
		log.trace(msg, "do login doSenderBlacklistRegex methon {}, {}, {}", ssid, cc, sender);
		List<Pattern> senderRegex = prefixBlackListPatternForSenderBlackList.get(ssid+"_"+cc);
		if (senderRegex == null) {
			return false;			
		}
		//sender = sender.replaceAll(" ", "");
		for (Pattern pattern : senderRegex) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(sender);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.trace(msg, "match key:{}", key);
					if (sender.equals(key)) {
						log.info(msg,"msg is policy deny by sender blacklist is:{}", key);
						return true;
					}else{
						continue;
					}/*else {
						return doSenderBlacklistRegexSecondMatch(msg, pattern, key, sender);
					}	*/											
				}
			}
		}	
		if(!"all".equalsIgnoreCase(sender)){
			return doSenderBlacklistRegex(msg, ssid, cc, "all");
		}
		return false;
	}
	
	private boolean doSenderBlacklistRegexSecondMatch(GmmsMessage msg, Pattern pattern, String matchString, 
			String sender) {
		String senderPatternString = pattern.pattern();
		if (StringUtility.stringIsNotEmpty(senderPatternString)) {
			String newPatString = senderPatternString.replaceAll(matchString, "a-a-a");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(sender);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (sender.equals(key)) {
					log.info(msg,"msg is policy deny by sender blacklist is:{}", key);
					return true;
				}else {
					return doSenderBlacklistRegexSecondMatch(msg, newPattern, key, sender);
				}												
			}								
		}
		return false;
	}
	
	private boolean doSenderWhitelistRegex(GmmsMessage msg, String ssid, String cc, String action, String sender) {
		List<Pattern> senderRegex = prefixWLListPatternForSenderWLList.get(ssid+"_"+action+"_"+cc);
		if (senderRegex == null) {
			return false;			
		}
		for (Pattern pattern : senderRegex) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(sender+":"+action+":"+cc);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					log.trace(msg, "do login doSenderWhitelistRegex methon {},", sender+":"+action+":"+cc);
					
					if (key.equals(sender+":"+action+":"+cc)) {
						if(action.equalsIgnoreCase("rp")){
							//msg.setOriginalSenderAddr(msg.getSenderAddress());
							msg.setSenderAddress(prefixWLSenderMappingForSenderWL.get(key));
						}
						return true;
					}else{
						continue;
					}
					/*else {
						return doSenderWhitelistRegexSecondMatch(msg, pattern, key, sender, cc, action);
					}*/												
				}
			}
		}				
		if(!"all".equalsIgnoreCase(sender)){
			return doSenderWhitelistRegex(msg, ssid, cc, action, "all");
		}
		return false;
	}
	
	private boolean doSenderWhitelistRegexSecondMatch(GmmsMessage msg, Pattern pattern, String matchString, 
			String sender, String cc, String action) {
		String senderPatternString = pattern.pattern();
		if (StringUtility.stringIsNotEmpty(senderPatternString)) {
			String newPatString = senderPatternString.replaceAll(matchString, "a-a-a");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(sender+":"+action+":"+cc);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (key.equals(sender+":"+action+":"+cc)) {
					if(action.equalsIgnoreCase("rp")){
						msg.setOriginalSenderAddr(msg.getSenderAddress());
						msg.setSenderAddress(prefixWLSenderMappingForSenderWL.get(key));
					}
					return true;
				}else {
					return doSenderWhitelistRegexSecondMatch(msg, newPattern, key, sender, cc, action);
				}												
			}								
		}
		return false;
	}
	
	private boolean doSenderWhitelistCheckSecondMatch(GmmsMessage msg, Pattern pattern, String matchString, 
			String recipientAddress, String ssid, String action) {
		String senderPatternString = pattern.pattern();
		if (StringUtility.stringIsNotEmpty(senderPatternString)) {
			String newPatString = senderPatternString.replaceAll(matchString, "a-a-a");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(recipientAddress);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (recipientAddress.startsWith(key)) {
					if(doSenderWhitelistRegex(msg, ssid, key, action, msg.getSenderAddress())){
						return true;
					}
				}else {
					return doSenderWhitelistCheckSecondMatch(msg, newPattern, key, recipientAddress, ssid, action);
				}												
			}								
		}
		return false;
	}
	
	public boolean doSenderWhitelistCheck(int ssid, GmmsMessage msg) {
		boolean result = true;
		String recipientAddress = msg.getRecipientAddress();
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		A2PCustomerInfo cst = getCustomerBySSID(msg.getOSsID());
		
		List<Pattern> prefixList = ssidPrefixListPatternForSenderWLList.get(""+ssid);
		if (prefixList == null || prefixList.isEmpty() ) {
			//NonPA deny all for SG
			if("NonPA".equalsIgnoreCase(cst.getCustomerTypeBySender()) 
        			&& recipientAddress.startsWith("65")){
				return false;
			}
			//ssid = -1, default whitelist check
			if(ssid!=-1){				
				return doSenderWhitelistCheck(-1, msg);
			}else{
				return true;
			}		    
		}
		
		
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(recipientAddress);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					if (recipientAddress.startsWith(key)) {
						result = false;
						//whitelist check
						log.trace("do the whitelist check {}", key, msg.getSenderAddress());
						if(doSenderWhitelistRegex(msg, ""+ssid, key, "wl", msg.getSenderAddress())){
							return true;
						}else{
							//replace check
							if(doSenderWhitelistRegex(msg, ""+ssid, key, "rp", msg.getSenderAddress())){
								return true;
							}else{
								result = false;
							}
						}
					}else{
						continue;
					}
					/*else {
						if(doSenderWhitelistCheckSecondMatch(msg, pattern, key, recipientAddress, ""+ssid, "wl")){
							return true;
						}
					}	*/											
				}
			}
		}
		//sender not in whitelist, do all country whitelist check.
		List<Pattern> senderRegex = prefixWLListPatternForSenderWLList.get(ssid+"_wl_all");
		if (senderRegex != null) {
			result = false;
			if(doSenderWhitelistRegex(msg, ""+ssid, "all", "wl", msg.getSenderAddress())){
				return true;
			}			
		}
		//if still not in all country whitelist, check replacement logic.
		senderRegex = prefixWLListPatternForSenderWLList.get(ssid+"_rp_all");
		if (senderRegex != null) {
			result = false;
			if(doSenderWhitelistRegex(msg, ""+ssid, "all", "rp", msg.getSenderAddress())){
				return true;
			}			
		}
		//ssid 级别的whitelist 存在，但是没有在whitelist 和替换策略里面，返回失败。
		if(result == false){
			return false;
		}else{
			//ssid = -1, default whitelist check
			if(ssid!=-1){
				if("NonPA".equalsIgnoreCase(cst.getCustomerTypeBySender()) 
	        			&& recipientAddress.startsWith("65")){
					return false;
				}
				return doSenderWhitelistCheck(-1, msg);
			}
		}				
		return result;										
	}
	
	public boolean doVendorSenderWhitelistCheck(int ssid, GmmsMessage msg) {
		boolean result = true;
		String recipientAddress = msg.getRecipientAddress();
		if (recipientAddress.startsWith("+")) {
			recipientAddress = recipientAddress.substring(1);
		}
		if (recipientAddress.startsWith("00")) {
			recipientAddress = recipientAddress.substring(2);
		}
		if (recipientAddress.startsWith("0")) {
			recipientAddress = recipientAddress.substring(1);
		}
		List<Pattern> prefixList = ssidPrefixListPatternForSenderWLList.get(""+ssid);
		if (prefixList == null || prefixList.isEmpty() ) {
			
			//ssid = -2, default whitelist check
			if(ssid!=-2){
				return doVendorSenderWhitelistCheck(-2, msg);
			}else{
				return true;
			}		    
		}
		
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(recipientAddress);
				while (matcher != null && matcher.find()) {
					String key = matcher.group();
					if (recipientAddress.startsWith(key)) {
						result = false;
						//whitelist check
						if(doSenderWhitelistRegex(msg, ""+ssid, key, "wl", msg.getSenderAddress())){
							return true;
						}else{
							//replace check
							if(doSenderWhitelistRegex(msg, ""+ssid, key, "rp", msg.getSenderAddress())){
								return true;
							}else{
								result = false;
							}
						}
					}else{
						continue;
					}
					/*else {
						if(doSenderWhitelistCheckSecondMatch(msg, pattern, key, recipientAddress, ""+ssid, "wl")){
							return true;
						}
					}	*/											
				}
			}
		}
		//sender not in whitelist, do all country whitelist check.
		List<Pattern> senderRegex = prefixWLListPatternForSenderWLList.get(ssid+"_wl_all");
		if (senderRegex != null) {
			result = false;
			if(doSenderWhitelistRegex(msg, ""+ssid, "all", "wl", msg.getSenderAddress())){
				return true;
			}			
		}
		//if still not in all country whitelist, check replacement logic.
		senderRegex = prefixWLListPatternForSenderWLList.get(ssid+"_rp_all");
		if (senderRegex != null) {
			result = false;
			if(doSenderWhitelistRegex(msg, ""+ssid, "all", "rp", msg.getSenderAddress())){
				return true;
			}			
		}
		//ssid 级别的whitelist 存在，但是没有在whitelist 和替换策略里面，返回失败。
		if(result == false){
			return false;
		}else{
			//ssid = -2, default whitelist check
			if(ssid!=-2){
				return doVendorSenderWhitelistCheck(-2, msg);
			}
		}				
		return result;										
	}
	
	
	
	private int loadSsidSenderWhitelist(boolean isInit) {
		String path = GmmsUtility.getInstance().getSenderActionDir();
		
		try {
			if (path != null) {
				File conf = new File(path);		
				//<ssid,<wl_cc,List<<sender:wl:cc:rpsender>>>
				Map<String, Map<String, List<String>>> ssidSenderConfigMap = A2PCustomerConfig.parseWhiteSenderActionDir(conf);
				if(ssidSenderConfigMap!=null && !ssidSenderConfigMap.isEmpty()){
					Map<String, List<Pattern>> tempssidPrefixListPatternForSenderWLList = new ConcurrentHashMap<String, List<Pattern>>();					
					//<ssid_action(wl;rp)_cc,wlsender:action(wl;rp):cc>
					Map<String, List<Pattern>> tempprefixWLListPatternForSenderWLList = new ConcurrentHashMap<String, List<Pattern>>();
					//<wlsender:action(wl;rp):cc, sender>
					Map<String, String> tempprefixWLSenderMappingForSenderWL = new ConcurrentHashMap<String, String>();
					for(Map.Entry<String, Map<String, List<String>>> entry: ssidSenderConfigMap.entrySet()){
						String ssid = entry.getKey();
						//<wl_cc,<sender:wl:cc:rpsender>>
						Map<String, List<String>> ccSenders = entry.getValue();
						if(ccSenders!=null && !ccSenders.isEmpty()){
							Set<String> ccSet = new HashSet<> ();
							for(Map.Entry<String, List<String>> ccsenderEntry: ccSenders.entrySet()){
								
								String cc = ccsenderEntry.getKey();
								List<String> senders = ccsenderEntry.getValue();
								if(senders!=null && !senders.isEmpty()){
									List<String> senderWLList = new ArrayList<String> ();
									for(String senderEntry: senders){
										String senderKye = senderEntry.substring(0, senderEntry.lastIndexOf(":"));
										String rpValue = senderEntry.substring(senderEntry.lastIndexOf(":")+1);
										senderWLList.add(senderKye);
										tempprefixWLSenderMappingForSenderWL.put(senderKye, rpValue);
									}
									Collections.sort(senderWLList,  new Comparator (){
										 @Override
									     public int compare(Object o1, Object o2)
									    {
									           String prefix1= (String )o1;
									           String prefix2= (String )o2;

									           return prefix2.length()-prefix1.length();
									    }
									});
									List<Pattern>  regexs  = new ArrayList<Pattern>();
									constructSenderListPattern(senderWLList, regexs);
									tempprefixWLListPatternForSenderWLList.put(ssid+"_"+cc, regexs);
									
								}
								ccSet.add(cc.split("_")[1]);
							}
							List<String> ccList = new ArrayList<String> ();
							ccList.addAll(ccSet);
							Collections.sort(ccList,  new Comparator (){
								 @Override
							     public int compare(Object o1, Object o2)
							    {
							           String prefix1= (String )o1;
							           String prefix2= (String )o2;

							           return prefix2.length()-prefix1.length();
							    }
							});
							List<Pattern>  regexs  = new ArrayList<Pattern>();
							constructSenderListPattern(ccList, regexs);
							tempssidPrefixListPatternForSenderWLList.put(ssid, regexs);
						}
					}										
					Map<String, List<Pattern>> temp = this.ssidPrefixListPatternForSenderWLList;
					this.ssidPrefixListPatternForSenderWLList = tempssidPrefixListPatternForSenderWLList;
					temp.clear();
					temp = this.prefixWLListPatternForSenderWLList;
					this.prefixWLListPatternForSenderWLList = tempprefixWLListPatternForSenderWLList;
					temp.clear();
					Map<String, String> temp1 = this.prefixWLSenderMappingForSenderWL;
					this.prefixWLSenderMappingForSenderWL = tempprefixWLSenderMappingForSenderWL;
					temp1.clear();
					if(!isInit){
						String moduleName = System.getProperty("module");
				        if (ModuleManager.getInstance().getRouterModules().contains(moduleName)) {
				        	loadSenderWhitelistToRedis(moduleName);
				        }
					}
				}				
			}
		} catch (Exception e) {
			log.error("load loadSsidSenderWhitelist file failed", e);
		}
		return 0;
	}

	private int loadRoutingInfo() {
		String path = GmmsUtility.getInstance().getRoutingFilePath();

		try {
			if (path != null) {
				File conf = new File(path);
				routingInfoMap.clear();
				this.routingInfoMap = A2PCustomerConfig.parseRoutingInfo(conf);

                if(routingInfoMap == null || routingInfoMap.isEmpty()){
                    log.info("Reload routingInfo warning, routingFile is null");
                    return 1;
                }
				
				Map<String, Map<String,String>> temp_routingRelay = new ConcurrentHashMap<String, Map<String,String>>();
				Map<String, Map<String,String>> temp_backupRoutingRelay = new ConcurrentHashMap<String, Map<String,String>>();
				Map<String, List<Pattern>> temp_customerPrefixList = new ConcurrentHashMap<String, List<Pattern>>();
				Map<String, List<Pattern>> temp_backupCustomerPrefixList = new ConcurrentHashMap<String, List<Pattern>>();
				Map<String, Map<String,String>> temp_defaultRoutingRelay = new ConcurrentHashMap<String, Map<String,String>>();
				Map<String, Map<String,String>> temp_backupDefaultRoutingRelay = new ConcurrentHashMap<String, Map<String,String>>();
				Map<String,List<Pattern>> temp_defaultPrefixList = new ConcurrentHashMap<String,List<Pattern>>();
				Map<String,List<Pattern>> temp_backupDefaultPrefixList = new ConcurrentHashMap<String,List<Pattern>>();
				Map<String, Map<String, String>> temp_contentTemplateMap = new ConcurrentHashMap<String, Map<String, String>>();

				
				//sender routing
				Map<String, Map<String,String>> temp_sendroutingRelay = new ConcurrentHashMap<String, Map<String,String>>();
				Map<String, Map<String,String>> temp_sendbackupRoutingRelay = new ConcurrentHashMap<String, Map<String,String>>();
				Map<String, List<Pattern>> temp_sendcustomerPrefixList = new ConcurrentHashMap<String, List<Pattern>>();
				Map<String, List<Pattern>> temp_sendBackupcustomerPrefixList = new ConcurrentHashMap<String, List<Pattern>>();
				
				//content keyword routing
				Map<String, Map<String,Map<String,String>>> temp_contentkeyroutingRelay = new ConcurrentHashMap<String, Map<String,Map<String,String>>>();
				Map<String, Map<String,Map<String,String>>> temp_contentkeybackupRoutingRelay = new ConcurrentHashMap<String, Map<String,Map<String,String>>>();
				Map<String, List<Pattern>> temp_contentkeycustomerPrefixList = new ConcurrentHashMap<String, List<Pattern>>();
				Map<String, List<Pattern>> temp_contentkeybackupcustomerPrefixList = new ConcurrentHashMap<String, List<Pattern>>();
				
				
			    //2-time routing 
				Map<String, Map<String, String>> temp_routingRelay2 = new ConcurrentHashMap<String, Map<String, String>>();
				Map<String, Map<String, String>> temp_backupRoutingRelay2 = new ConcurrentHashMap<String, Map<String, String>>();
				Map<String, List<Pattern>> temp_customerPrefixList2 = new ConcurrentHashMap<String, List<Pattern>>();
				Map<String, List<Pattern>> temp_backupCustomerPrefixList2 = new ConcurrentHashMap<String, List<Pattern>>();

				// 2-time sender routing
				Map<String, Map<String, String>> temp_sendroutingRelay2 = new ConcurrentHashMap<String, Map<String, String>>();
				Map<String, Map<String, String>> temp_sendbackupRoutingRelay2 = new ConcurrentHashMap<String, Map<String, String>>();
				Map<String, List<Pattern>> temp_sendcustomerPrefixList2 = new ConcurrentHashMap<String, List<Pattern>>();
				Map<String, List<Pattern>> temp_sendBackupcustomerPrefixList2 = new ConcurrentHashMap<String, List<Pattern>>();

				// 2-time content keyword routing
				Map<String, Map<String, Map<String, String>>> temp_contentkeyroutingRelay2 = new ConcurrentHashMap<String, Map<String, Map<String, String>>>();
				Map<String, Map<String, Map<String, String>>> temp_contentkeybackupRoutingRelay2 = new ConcurrentHashMap<String, Map<String, Map<String, String>>>();
				Map<String, List<Pattern>> temp_contentkeycustomerPrefixList2 = new ConcurrentHashMap<String, List<Pattern>>();
				Map<String, List<Pattern>> temp_contentkeybackupcustomerPrefixList2 = new ConcurrentHashMap<String, List<Pattern>>();
			    
				//op content keyword routing
				HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> temp_customeropContentKeywordRoutingRelayInfo= HashBasedTable.create();
				HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> temp_customeropBackupContentKeywordRoutingRelayInfo= HashBasedTable.create();
				//op sender routing
				//<ossid, opid, <sender, priority, rssids>>
				HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> temp_customeropSenderRoutingRelayInfo= HashBasedTable.create();
				HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> temp_customeropBackupSenderRoutingRelayInfo= HashBasedTable.create();
				//op routing
				//<ossid, opid, <priority, rssids>>
				HashBasedTable<String,Integer,Map<Integer,String>> temp_customeropRoutingRelayInfo= HashBasedTable.create();
				HashBasedTable<String,Integer,Map<Integer,String>> temp_customeropBackupRoutingRelayInfo= HashBasedTable.create();
				//<ossid, number, <priority, rssids>>
				HashBasedTable<String,Long,Map<Integer,String>> temp_customerNumberRoutingRelayInfo= HashBasedTable.create();
				
				for (A2PCustomerConfig block : routingInfoMap.values()) {
					try {
						int ssid = -1;
						try {
							ssid = block.getMandatoryInt("CCBSSID");							
						} catch (Exception e) {
							log.info("Reload routingInfo warning, ssid= {} {} Ignored {}",
									block.getSSID(), block.getShortName(),
									e);
						}
						
						block.setSsid(ssid);
						try {
							doLoadRoutingRelay(block, A2P_ROUTING_RELAY, temp_routingRelay,temp_customerPrefixList
									,temp_defaultRoutingRelay,temp_defaultPrefixList, temp_contentTemplateMap);						
						} catch (Exception e) {
							log.error("reload country routing error", e);
						}
						
						for (int i = 0; i < 20; i++) {
							String key = A2P_BACKUP_ROUTING_RELAY;
							if (i>0) {
								key= key+i;
							}
							try {
								doLoadRoutingRelay(block, key, temp_backupRoutingRelay, temp_backupCustomerPrefixList
										,temp_backupDefaultRoutingRelay,temp_backupDefaultPrefixList, temp_contentTemplateMap);						
							} catch (Exception e) {
								log.error("reload backup country routing error", e);
							}
						}																														
						
						try {							
							doLoadSenderRoutingRelay(block, A2P_SENDER_ROUTING_RELAY, temp_sendroutingRelay, temp_sendcustomerPrefixList, temp_contentTemplateMap);
						} catch (Exception e) {
							log.error("reload sender routing error", e);
						}
						
						for (int i = 0; i < 20; i++) {
							String key = A2P_BACKUP_SENDER_ROUTING_RELAY;
							if (i>0) {
								key= key+i;
							}
							try {							
								doLoadSenderRoutingRelay(block, key, temp_sendbackupRoutingRelay, temp_sendBackupcustomerPrefixList, temp_contentTemplateMap);
							} catch (Exception e) {
								log.error("reload backupsender routing error", e);
							}
						}												
						try {
							doLoadCustomerContentKeywordRoutingRelay(block, A2P_CONTENT_KEYWORD_ROUTING_RELAY, temp_contentTemplateMap,
									temp_contentkeyroutingRelay,temp_contentkeycustomerPrefixList);			
						} catch (Exception e) {
							log.error("reload contentkeyword routing error", e);
						}
						
						for (int i = 0; i < 20; i++) {
							String key = A2P_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
							if (i>0) {
								key= key+i;
							}
							try {
								doLoadCustomerContentKeywordRoutingRelay(block, key, temp_contentTemplateMap,
										temp_contentkeybackupRoutingRelay,temp_contentkeybackupcustomerPrefixList);			
							} catch (Exception e) {
								log.error("reload backupcontentkeyword routing error", e);
							}
						}												
						
						
						try {
							doLoadRoutingRelay(block, A2P_SEC_ROUTING_RELAY, temp_routingRelay2,
									temp_customerPrefixList2, temp_defaultRoutingRelay, temp_defaultPrefixList,
									temp_contentTemplateMap);
						} catch (Exception e) {
							log.error("reload 2-time country routing error", e);
						}

						for (int i = 0; i < 20; i++) {
							String key = A2P_SEC_BACKUP_ROUTING_RELAY;
							if (i > 0) {
								key = key + i;
							}
							try {
								doLoadRoutingRelay(block, key, temp_backupRoutingRelay2, temp_backupCustomerPrefixList2,
										temp_backupDefaultRoutingRelay, temp_backupDefaultPrefixList,
										temp_contentTemplateMap);
							} catch (Exception e) {
								log.error("reload 2-time backup country routing error", e);
							}
						}

						try {
							doLoadSenderRoutingRelay(block, A2P_SEC_SENDER_ROUTING_RELAY, temp_sendroutingRelay2,
									temp_sendcustomerPrefixList2, temp_contentTemplateMap);
						} catch (Exception e) {
							log.error("reload 2-time sender routing error", e);
						}

						for (int i = 0; i < 20; i++) {
							String key = A2P_SEC_BACKUP_SENDER_ROUTING_RELAY;
							if (i > 0) {
								key = key + i;
							}
							try {
								doLoadSenderRoutingRelay(block, key, temp_sendbackupRoutingRelay2,
										temp_sendBackupcustomerPrefixList2, temp_contentTemplateMap);
							} catch (Exception e) {
								log.error("reload 2-time backupsender routing error", e);
							}
						}
						try {
							doLoadCustomerContentKeywordRoutingRelay(block, A2P_SEC_CONTENT_KEYWORD_ROUTING_RELAY,
									temp_contentTemplateMap, temp_contentkeyroutingRelay2,
									temp_contentkeycustomerPrefixList2);
						} catch (Exception e) {
							log.error("reload 2-time contentkeyword routing error", e);
						}

						for (int i = 0; i < 20; i++) {
							String key = A2P_SEC_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
							if (i > 0) {
								key = key + i;
							}
							try {
								doLoadCustomerContentKeywordRoutingRelay(block, key, temp_contentTemplateMap,
										temp_contentkeybackupRoutingRelay2, temp_contentkeybackupcustomerPrefixList2);
							} catch (Exception e) {
								log.error("reload 2-time backupcontentkeyword routing error", e);
							}
						}
						
						//op routing
						try {
							doLoadCustomerOPContentKeywordRoutingRelay(block, A2P_OP_CONTENT_KEYWORD_ROUTING_RELAY,
									temp_customeropContentKeywordRoutingRelayInfo);
						} catch (Exception e) {
							log.error("reload operator contentkeyword routing error", e);
						}
						
						for (int i = 0; i < 20; i++) {
							String key = A2P_OP_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
							if (i > 0) {
								key = key + i;
							}
							try {
								doLoadCustomerOPContentKeywordRoutingRelay(block, key,
										temp_customeropBackupContentKeywordRoutingRelayInfo);
							} catch (Exception e) {
								log.error("reload operator backupcontentkeyword routing error", e);
							}
						}
						
						try {
							doLoadNumberRoutingRelay(block, A2P_NUMBER_ROUTING_RELAY,
									temp_customerNumberRoutingRelayInfo);
						} catch (Exception e) {
							log.error("reload number routing error", e);
						}
						
						try {
							doLoadOPRoutingRelay(block, A2P_OP_ROUTING_RELAY,
									temp_customeropRoutingRelayInfo);
						} catch (Exception e) {
							log.error("reload operator routing error", e);
						}
						
						for (int i = 0; i < 20; i++) {
							String key = A2P_OP_BACKUP_ROUTING_RELAY;
							if (i > 0) {
								key = key + i;
							}
							try {
								doLoadOPRoutingRelay(block, key,
										temp_customeropBackupRoutingRelayInfo);
							} catch (Exception e) {
								log.error("reload operator backup routing error", e);
							}
						}
						
						try {
							doLoadOPSenderRoutingRelay(block, A2P_OP_SENDER_ROUTING_RELAY,
									temp_customeropSenderRoutingRelayInfo);
						} catch (Exception e) {
							log.error("reload operator sender routing error", e);
						}
						
						for (int i = 0; i < 20; i++) {
							String key = A2P_OP_BACKUP_SENDER_ROUTING_RELAY;
							if (i > 0) {
								key = key + i;
							}
							try {
								doLoadOPSenderRoutingRelay(block, key,
										temp_customeropBackupSenderRoutingRelayInfo);
							} catch (Exception e) {
								log.error("reload operator backupsender routing error", e);
							}
						}
						 
						/*loadPolicyInfo(block, temp_policyInfo,
								temp_allowRoutingInfo);
						loadCharsetRelay(block, temp_charsetRelay);
						initContentWhiteList(block, temp_contentWhiteListInfo,
								true);
						initContentRelay(block, temp_contentRelayInfo, true);
						initSenderAddrRelay(block, temp_senderAddrRelayInfo,
								true);
						initSenderAddrReplace(block,
								temp_senderAddrReplaceInfo, true);
						initRecipientAddrReplace(block,
								temp_recipientAddrReplaceInfo, true);
						initContentSignature(block, temp_contentSignatureMap);
						initServiceTypeIDRelay(block, temp_serviceTypeIDRelayMap);
						initContentKeyword(block, temp_contentKeywordMap);*/
					} catch (Exception e) {
						log.info("Reload routingInfo error, ssid= {} {} Ignored {}",
										block.getSSID(), block.getShortName(),
										e);
						continue;
					}

				}

				
				Map routing = this.customerRoutingRelayInfo;
				this.customerRoutingRelayInfo = temp_routingRelay;
				
				Map prefixs = this.customerPerfixInfo;
				this.customerPerfixInfo = temp_customerPrefixList;
				
				Map backupRouting = this.customerBackupRoutingRelayInfo;
				this.customerBackupRoutingRelayInfo = temp_backupRoutingRelay;

				Map defaultRouting = this.defaultRoutingRelayInfo;
				this.defaultRoutingRelayInfo = temp_defaultRoutingRelay;
				
				Map defalutBackupRouting = this.defaultBackupRoutingRelayInfo;
				this.defaultBackupRoutingRelayInfo = temp_backupDefaultRoutingRelay;
				
				Map<String,List<Pattern>> defaultPrefixs = this.customerDefaultPerfixInfo;
				this.customerDefaultPerfixInfo = temp_defaultPrefixList;
				
				Map<String,List<Pattern>> backupDefaultPrefixs = this.customerBackupDefaultPerfixInfo;
				this.customerBackupDefaultPerfixInfo = temp_backupDefaultPrefixList;
				backupDefaultPrefixs.clear();
				
				Map backupPrefixs = this.customerBackupPerfixInfo;
				this.customerBackupPerfixInfo = temp_backupCustomerPrefixList;
				backupPrefixs.clear();
				
				Map<String, Map<String, String>> contentTemp = this.contentTemplateMap;
				this.contentTemplateMap = temp_contentTemplateMap;
				contentTemp.clear();
				
				Map sendRouting = this.customerSenderRoutingRelayInfo;
				this.customerSenderRoutingRelayInfo = temp_sendroutingRelay;
				
				Map sendprefixs = this.customerSenderPerfixInfo;
				this.customerSenderPerfixInfo = temp_sendcustomerPrefixList;
				
				Map sendbackupRouting = this.customerBackupSenderRoutingRelayInfo;
				this.customerBackupSenderRoutingRelayInfo = temp_sendbackupRoutingRelay;
				
				Map sendbackupprefixs = this.customerBackupSenderPerfixInfo;
				this.customerBackupSenderPerfixInfo = temp_sendBackupcustomerPrefixList;
				
				sendRouting.clear();
				sendprefixs.clear();
				sendbackupRouting.clear();
				sendbackupprefixs.clear();
				
				Map contentRouting = this.customerContentKeywordRoutingRelayInfo;
				this.customerContentKeywordRoutingRelayInfo = temp_contentkeyroutingRelay;
				
				Map contentprefixs = this.customerContentKeywordPerfixInfo;
				this.customerContentKeywordPerfixInfo = temp_contentkeycustomerPrefixList;
				
				Map contentbackupRouting = this.customerBackupContentKeywordRoutingRelayInfo;
				this.customerBackupContentKeywordRoutingRelayInfo = temp_contentkeybackupRoutingRelay;
				
				Map contentbackupprefixs = this.customerBackupContentKeywordPerfixInfo;
				this.customerBackupContentKeywordPerfixInfo = temp_contentkeybackupcustomerPrefixList;
				
				contentRouting.clear();
				contentprefixs.clear();
				contentbackupRouting.clear();
				contentbackupprefixs.clear();
				
				
				/*Map policy = this.policyInfo;
				this.policyInfo = temp_policyInfo;

				Map allowMap = this.allowRoutingInfoMap;
				this.allowRoutingInfoMap = temp_allowRoutingInfo;

				Map charset = this.charsetRelay;
				this.charsetRelay = temp_charsetRelay;

				Map<String, List<String>> contentWhitelist = this.contentWhiteListInfo;
				this.contentWhiteListInfo = temp_contentWhiteListInfo;

				Map<String, List<RelayMark>> contentRelay = this.contentRelayInfo;
				this.contentRelayInfo = temp_contentRelayInfo;

				Map<String, List<RelayMark>> senderAddrRelay = this.senderAddrRelayInfo;
				this.senderAddrRelayInfo = temp_senderAddrRelayInfo;

				Map<String, String> senderAddrReplace = this.senderAddrReplaceInfo;
				this.senderAddrReplaceInfo = temp_senderAddrReplaceInfo;

				Map<String, String> recipientAddrReplace = this.recipientAddrReplaceInfo;
				this.recipientAddrReplaceInfo = temp_recipientAddrReplaceInfo;

				Map<Integer, Map<String, Map<String, String>>> contentSignature = this.contentSignatureMap;
				this.contentSignatureMap = temp_contentSignatureMap;

				Map<String, Integer> serviceTypeIDRelay = this.serviceTypeIDRelayInfo;
				this.serviceTypeIDRelayInfo = temp_serviceTypeIDRelayMap;

				Map<Integer, Map<String, Integer>> contentKeyword = this.contentKeywordMap;
				this.contentKeywordMap = temp_contentKeywordMap;*/

				/*vp.clear();
				smsCon.clear();
				smsIP.clear();
				smsSS7.clear();				
				policy.clear();
				allowMap.clear();
				charset.clear();
				contentWhitelist.clear();
				contentRelay.clear();
				senderAddrRelay.clear();
				senderAddrReplace.clear();
				recipientAddrReplace.clear();
				contentSignature.clear();
				serviceTypeIDRelay.clear();
				contentKeyword.clear();*/
				
				routing.clear();
				prefixs.clear();
				defalutBackupRouting.clear();
				defaultRouting.clear();
				defaultPrefixs.clear();
				backupRouting.clear();
				
				//2-time retry				
				routing = this.customerRoutingRelayInfo2;
				this.customerRoutingRelayInfo2 = temp_routingRelay2;

				prefixs = this.customerPerfixInfo2;
				this.customerPerfixInfo2 = temp_customerPrefixList2;

				backupRouting = this.customerBackupRoutingRelayInfo2;
				this.customerBackupRoutingRelayInfo2 = temp_backupRoutingRelay2;
				backupPrefixs = this.customerBackupPerfixInfo2;
				this.customerBackupPerfixInfo2 = temp_backupCustomerPrefixList2;
				routing.clear();
				prefixs.clear();
				backupPrefixs.clear();
				backupRouting.clear();
				sendRouting = this.customerSenderRoutingRelayInfo2;
				this.customerSenderRoutingRelayInfo2 = temp_sendroutingRelay2;

				sendprefixs = this.customerSenderPerfixInfo2;
				this.customerSenderPerfixInfo2 = temp_sendcustomerPrefixList2;

				sendbackupRouting = this.customerBackupSenderRoutingRelayInfo2;
				this.customerBackupSenderRoutingRelayInfo2 = temp_sendbackupRoutingRelay2;

				sendbackupprefixs = this.customerBackupSenderPerfixInfo2;
				this.customerBackupSenderPerfixInfo2 = temp_sendBackupcustomerPrefixList2;
				sendRouting.clear();
				sendprefixs.clear();
				sendbackupRouting.clear();
				sendbackupprefixs.clear();

				contentRouting = this.customerContentKeywordRoutingRelayInfo2;
				this.customerContentKeywordRoutingRelayInfo2 = temp_contentkeyroutingRelay2;

				contentprefixs = this.customerContentKeywordPerfixInfo2;
				this.customerContentKeywordPerfixInfo2 = temp_contentkeycustomerPrefixList2;

				contentbackupRouting = this.customerBackupContentKeywordRoutingRelayInfo2;
				this.customerBackupContentKeywordRoutingRelayInfo2 = temp_contentkeybackupRoutingRelay2;

				contentbackupprefixs = this.customerBackupContentKeywordPerfixInfo2;
				this.customerBackupContentKeywordPerfixInfo2 = temp_contentkeybackupcustomerPrefixList2;

				contentRouting.clear();
				contentprefixs.clear();
				contentbackupRouting.clear();
				contentbackupprefixs.clear();
				
				//op routing
				HashBasedTable tempcustomeropContentKeywordRoutingRelayInfo = this.customeropContentKeywordRoutingRelayInfo;
				this.customeropContentKeywordRoutingRelayInfo = temp_customeropContentKeywordRoutingRelayInfo;
				HashBasedTable tempcustomeropBackupContentKeywordRoutingRelayInfo = this.customeropBackupContentKeywordRoutingRelayInfo;
				this.customeropBackupContentKeywordRoutingRelayInfo = temp_customeropBackupContentKeywordRoutingRelayInfo;
				//op sender routing
				//<ossid, opid, <sender, priority, rssids>>
				HashBasedTable tempcustomeropSenderRoutingRelayInfo= this.customeropSenderRoutingRelayInfo;
				this.customeropSenderRoutingRelayInfo = temp_customeropSenderRoutingRelayInfo;
				HashBasedTable tempcustomeropBackupSenderRoutingRelayInfo= this.customeropBackupSenderRoutingRelayInfo;
				this.customeropBackupSenderRoutingRelayInfo = temp_customeropBackupSenderRoutingRelayInfo;
				//op routing
				//<ossid, opid, <priority, rssids>>
				HashBasedTable tempcustomeropRoutingRelayInfo= this.customeropRoutingRelayInfo;
				this.customeropRoutingRelayInfo = temp_customeropRoutingRelayInfo;
				HashBasedTable tempcustomeropBackupRoutingRelayInfo= this.customeropBackupRoutingRelayInfo;
				this.customeropBackupRoutingRelayInfo = temp_customeropBackupRoutingRelayInfo;
				//number routing
				HashBasedTable tempcustomerNumberRoutingRelayInfo= this.customerNumberRoutingRelayInfo;
				this.customerNumberRoutingRelayInfo = temp_customerNumberRoutingRelayInfo;
				
				tempcustomeropContentKeywordRoutingRelayInfo.clear();
				tempcustomeropBackupContentKeywordRoutingRelayInfo.clear();
				tempcustomeropSenderRoutingRelayInfo.clear();
				tempcustomeropBackupSenderRoutingRelayInfo.clear();
				tempcustomeropRoutingRelayInfo.clear();
				tempcustomeropBackupRoutingRelayInfo.clear();
				tempcustomerNumberRoutingRelayInfo.clear();
				
			}
			String moduleName = System.getProperty("module");
	        if (ModuleManager.getInstance().getRouterModules().contains(moduleName)) {
	        	loadRoutingInfoToRedis(moduleName);
	        }
			
		} catch (Exception ex) {
			log.warn("reload routing info error.", ex);
			return 1;
		}

		return 0;
	}

	
	public void loadRoutingInfoToRedis(String core){
		Set<String> keys = GmmsUtility.getInstance().getRedisClient().smembers("routingkey");
		if (keys != null && !keys.isEmpty()) {
			for (String key : keys) {
				if (key.contains(core)) {
					GmmsUtility.getInstance().getRedisClient().del(key);
				}				
			}			
		}		
		if(customerRoutingRelayInfo!= null && !customerRoutingRelayInfo.isEmpty()){
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".RoutingRelayInfo", customerRoutingRelayInfo);
		}
		if(customerBackupRoutingRelayInfo!= null && !customerBackupRoutingRelayInfo.isEmpty()){
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".BackupRoutingRelayInfo", customerBackupRoutingRelayInfo);
		}
		
		if(customerSenderRoutingRelayInfo!= null && !customerSenderRoutingRelayInfo.isEmpty()){
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".SenderRoutingRelayInfo", customerSenderRoutingRelayInfo);
		}
		
		if(customerBackupSenderRoutingRelayInfo!= null && !customerBackupSenderRoutingRelayInfo.isEmpty()){
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".BackupSenderRoutingRelayInfo", customerBackupSenderRoutingRelayInfo);
		}
		
		if(customerContentKeywordRoutingRelayInfo!= null && !customerContentKeywordRoutingRelayInfo.isEmpty()){
			Map<String, Map<String, String>> redisMap  = new HashMap<String, Map<String,String>>();
			for (Map.Entry<String,Map<String,Map<String,String>>> entry : customerContentKeywordRoutingRelayInfo.entrySet()) {
				Map<String,Map<String,String>> prefixMap = entry.getValue();
				String ossid = entry.getKey();
				Map<String, String> prefixRedisMap = redisMap.get(ossid);
				if (prefixRedisMap == null) {
					prefixRedisMap = new HashMap<String, String>();
					redisMap.put(ossid, prefixRedisMap);
				}
				for (Map.Entry<String,Map<String,String>> prefixEntry : prefixMap.entrySet()) {
					Map<String, String> keywordIdMap = prefixEntry.getValue();
					StringBuilder keywordIdBuilder = new StringBuilder();
					for (Map.Entry<String,String> keywordIdEntry: keywordIdMap.entrySet()) {
						keywordIdBuilder.append(keywordIdEntry.getKey()+":"+keywordIdEntry.getValue())
						                .append("|");
					}
					String keywordIdString = keywordIdBuilder.substring(0, keywordIdBuilder.length()-1);
					prefixRedisMap.put(prefixEntry.getKey(), keywordIdString);
				}
			}
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".ContentKeywordRoutingRelayInfo", redisMap);
		}
		
		if(customerBackupContentKeywordRoutingRelayInfo!= null && !customerBackupContentKeywordRoutingRelayInfo.isEmpty()){
			Map<String, Map<String, String>> redisMap  = new HashMap<String, Map<String,String>>();
			for (Map.Entry<String,Map<String,Map<String,String>>> entry : customerBackupContentKeywordRoutingRelayInfo.entrySet()) {
				Map<String,Map<String,String>> prefixMap = entry.getValue();
				String ossid = entry.getKey();
				Map<String, String> prefixRedisMap = redisMap.get(ossid);
				if (prefixRedisMap == null) {
					prefixRedisMap = new HashMap<String, String>();
					redisMap.put(ossid, prefixRedisMap);
				}
				for (Map.Entry<String,Map<String,String>> prefixEntry : prefixMap.entrySet()) {
					Map<String, String> keywordIdMap = prefixEntry.getValue();
					StringBuilder keywordIdBuilder = new StringBuilder();
					for (Map.Entry<String,String> keywordIdEntry: keywordIdMap.entrySet()) {
						keywordIdBuilder.append(keywordIdEntry.getKey()+":"+keywordIdEntry.getValue())
						                .append("|");
					}
					String keywordIdString = keywordIdBuilder.substring(0, keywordIdBuilder.length()-1);
					prefixRedisMap.put(prefixEntry.getKey(), keywordIdString);
				}
			}
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".BackupContentKeywordRoutingRelayInfo", redisMap);
		}
		
		if(contentTemplateMap!= null && !contentTemplateMap.isEmpty()){
			GmmsUtility.getInstance().getRedisClient().setContentTemplate("routingInfo"+core+".ContentReplaceTemplateInfo", contentTemplateMap);
		}
		
		//2-time routing

		if (customerRoutingRelayInfo2 != null && !customerRoutingRelayInfo2.isEmpty()) {
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo" + core + ".SecRoutingRelayInfo",
					customerRoutingRelayInfo2);
		}
		if (customerBackupRoutingRelayInfo2 != null && !customerBackupRoutingRelayInfo2.isEmpty()) {
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo(
					"routingInfo" + core + ".SecBackupRoutingRelayInfo", customerBackupRoutingRelayInfo2);
		}

		if (customerSenderRoutingRelayInfo2 != null && !customerSenderRoutingRelayInfo2.isEmpty()) {
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo(
					"routingInfo" + core + ".SecSenderRoutingRelayInfo", customerSenderRoutingRelayInfo2);
		}

		if (customerBackupSenderRoutingRelayInfo2 != null && !customerBackupSenderRoutingRelayInfo2.isEmpty()) {
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo(
					"routingInfo" + core + ".SecBackupSenderRoutingRelayInfo", customerBackupSenderRoutingRelayInfo2);
		}

		if (customerContentKeywordRoutingRelayInfo2 != null && !customerContentKeywordRoutingRelayInfo2.isEmpty()) {
			Map<String, Map<String, String>> redisMap = new HashMap<String, Map<String, String>>();
			for (Map.Entry<String, Map<String, Map<String, String>>> entry : customerContentKeywordRoutingRelayInfo2
					.entrySet()) {
				Map<String, Map<String, String>> prefixMap = entry.getValue();
				String ossid = entry.getKey();
				Map<String, String> prefixRedisMap = redisMap.get(ossid);
				if (prefixRedisMap == null) {
					prefixRedisMap = new HashMap<String, String>();
					redisMap.put(ossid, prefixRedisMap);
				}
				for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
					Map<String, String> keywordIdMap = prefixEntry.getValue();
					StringBuilder keywordIdBuilder = new StringBuilder();
					for (Map.Entry<String, String> keywordIdEntry : keywordIdMap.entrySet()) {
						keywordIdBuilder.append(keywordIdEntry.getKey() + ":" + keywordIdEntry.getValue()).append("|");
					}
					String keywordIdString = keywordIdBuilder.substring(0, keywordIdBuilder.length() - 1);
					prefixRedisMap.put(prefixEntry.getKey(), keywordIdString);
				}
			}
			GmmsUtility.getInstance().getRedisClient()
					.setRoutingInfo("routingInfo" + core + ".SecContentKeywordRoutingRelayInfo", redisMap);
		}

		if (customerBackupContentKeywordRoutingRelayInfo2 != null
				&& !customerBackupContentKeywordRoutingRelayInfo2.isEmpty()) {
			Map<String, Map<String, String>> redisMap = new HashMap<String, Map<String, String>>();
			for (Map.Entry<String, Map<String, Map<String, String>>> entry : customerBackupContentKeywordRoutingRelayInfo2
					.entrySet()) {
				Map<String, Map<String, String>> prefixMap = entry.getValue();
				String ossid = entry.getKey();
				Map<String, String> prefixRedisMap = redisMap.get(ossid);
				if (prefixRedisMap == null) {
					prefixRedisMap = new HashMap<String, String>();
					redisMap.put(ossid, prefixRedisMap);
				}
				for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
					Map<String, String> keywordIdMap = prefixEntry.getValue();
					StringBuilder keywordIdBuilder = new StringBuilder();
					for (Map.Entry<String, String> keywordIdEntry : keywordIdMap.entrySet()) {
						keywordIdBuilder.append(keywordIdEntry.getKey() + ":" + keywordIdEntry.getValue()).append("|");
					}
					String keywordIdString = keywordIdBuilder.substring(0, keywordIdBuilder.length() - 1);
					prefixRedisMap.put(prefixEntry.getKey(), keywordIdString);
				}
			}
			GmmsUtility.getInstance().getRedisClient()
					.setRoutingInfo("routingInfo" + core + ".SecBackupContentKeywordRoutingRelayInfo", redisMap);
		}
		
		if(customerNumberRoutingRelayInfo!= null && !customerNumberRoutingRelayInfo.isEmpty()){
			//op routing
			//<ossid, opid, <priority, rssids>> => <ossid, <opid_prority, rssids>>
			Map<String, Map<Long, Map<Integer, String>>> oproutingmap  = customerNumberRoutingRelayInfo.rowMap();
			Map<String, Map<String, String>> opNewRouting = new HashMap<>();
			for(Map.Entry<String, Map<Long, Map<Integer, String>>> oproutingentry : oproutingmap.entrySet()) {
				String oproutingKey = oproutingentry.getKey();
				Map<Long, Map<Integer, String>> oproutingValue = oproutingentry.getValue();
				for(Map.Entry<Long, Map<Integer, String>> opidentry : oproutingValue.entrySet()) {
					Long opidKey = opidentry.getKey();
					Map<Integer, String> prorityentry = opidentry.getValue();
					Map<String, String> opidNewMap = opNewRouting.get(oproutingKey);
					if(opidNewMap == null) {
						opidNewMap = new HashMap<>();						
					}
					for(Map.Entry<Integer, String> rssidentry : prorityentry.entrySet()) {
						Integer priorityKey = rssidentry.getKey();
						String rssid = rssidentry.getValue();
						opidNewMap.put(opidKey+"_"+priorityKey, rssid);
						opNewRouting.put(oproutingKey, opidNewMap);
					}
				}
			}
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".NumberRoutingRelayInfo", opNewRouting);
		}
		
		if(customeropRoutingRelayInfo!= null && !customeropRoutingRelayInfo.isEmpty()){
			//op routing
			//<ossid, opid, <priority, rssids>> => <ossid, <opid_prority, rssids>>
			Map<String, Map<Integer, Map<Integer, String>>> oproutingmap  = customeropRoutingRelayInfo.rowMap();
			Map<String, Map<String, String>> opNewRouting = new HashMap<>();
			for(Map.Entry<String, Map<Integer, Map<Integer, String>>> oproutingentry : oproutingmap.entrySet()) {
				String oproutingKey = oproutingentry.getKey();
				Map<Integer, Map<Integer, String>> oproutingValue = oproutingentry.getValue();
				for(Map.Entry<Integer, Map<Integer, String>> opidentry : oproutingValue.entrySet()) {
					Integer opidKey = opidentry.getKey();
					Map<Integer, String> prorityentry = opidentry.getValue();
					Map<String, String> opidNewMap = opNewRouting.get(oproutingKey);
					if(opidNewMap == null) {
						opidNewMap = new HashMap<>();						
					}
					for(Map.Entry<Integer, String> rssidentry : prorityentry.entrySet()) {
						Integer priorityKey = rssidentry.getKey();
						String rssid = rssidentry.getValue();
						opidNewMap.put(opidKey+"_"+priorityKey, rssid);
						opNewRouting.put(oproutingKey, opidNewMap);
					}
				}
			}
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".OPRoutingRelayInfo", opNewRouting);
		}
		if(customeropBackupRoutingRelayInfo!= null && !customeropBackupRoutingRelayInfo.isEmpty()){
			//op routing
			//<ossid, opid, <priority, rssids>> => <ossid, <opid_prority, rssids>>
			Map<String, Map<Integer, Map<Integer, String>>> oproutingmap  = customeropBackupRoutingRelayInfo.rowMap();
			Map<String, Map<String, String>> opNewRouting = new HashMap<>();
			for(Map.Entry<String, Map<Integer, Map<Integer, String>>> oproutingentry : oproutingmap.entrySet()) {
				String oproutingKey = oproutingentry.getKey();
				Map<Integer, Map<Integer, String>> oproutingValue = oproutingentry.getValue();
				for(Map.Entry<Integer, Map<Integer, String>> opidentry : oproutingValue.entrySet()) {
					Integer opidKey = opidentry.getKey();
					Map<Integer, String> prorityentry = opidentry.getValue();
					Map<String, String> opidNewMap = opNewRouting.get(oproutingKey);
					if(opidNewMap == null) {
						opidNewMap = new HashMap<>();						
					}
					for(Map.Entry<Integer, String> rssidentry : prorityentry.entrySet()) {
						Integer priorityKey = rssidentry.getKey();
						String rssid = rssidentry.getValue();
						opidNewMap.put(opidKey+"_"+priorityKey, rssid);
						opNewRouting.put(oproutingKey, opidNewMap);
					}
				}
			}
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".OPBackupRoutingRelayInfo", opNewRouting);
		}
		
		if(customeropSenderRoutingRelayInfo!= null && !customeropSenderRoutingRelayInfo.isEmpty()){
			//op sender routing
			//<ossid, opid, <sender, priority, rssids>>=> <ossid, <opid_sender_prority, rssids>>
			Map<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingmap  = customeropSenderRoutingRelayInfo.rowMap();
			Map<String, Map<String, String>> opNewRouting = new HashMap<>();
			for(Map.Entry<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingentry : oproutingmap.entrySet()) {
				String oproutingKey = oproutingentry.getKey();
				Map<Integer,HashBasedTable<String,Integer,String>> oproutingValue = oproutingentry.getValue();
				for(Map.Entry<Integer,HashBasedTable<String,Integer,String>> opidentry : oproutingValue.entrySet()) {
					Integer opidKey = opidentry.getKey();
					HashBasedTable<String,Integer,String> prorityentry = opidentry.getValue();
					Map<String, String> opidNewMap = opNewRouting.get(oproutingKey);
					if(opidNewMap == null) {
						opidNewMap = new HashMap<>();						
					}
					for(Table.Cell<String,Integer,String> rssidentry : prorityentry.cellSet()) {
						Integer priorityKey = rssidentry.getColumnKey();
						String sender = rssidentry.getRowKey();
						String rssid = rssidentry.getValue();
						opidNewMap.put(opidKey+"_"+sender+"_"+priorityKey, rssid);
						opNewRouting.put(oproutingKey, opidNewMap);
					}
				}
			}
			
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".OPSenderRoutingRelayInfo", opNewRouting);
		}
		
		if(customeropBackupSenderRoutingRelayInfo!= null && !customeropBackupSenderRoutingRelayInfo.isEmpty()){
			//op sender routing
			//<ossid, opid, <sender, priority, rssids>>=> <ossid, <opid_sender_prority, rssids>>
			Map<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingmap  = customeropBackupSenderRoutingRelayInfo.rowMap();
			Map<String, Map<String, String>> opNewRouting = new HashMap<>();
			for(Map.Entry<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingentry : oproutingmap.entrySet()) {
				String oproutingKey = oproutingentry.getKey();
				Map<Integer,HashBasedTable<String,Integer,String>> oproutingValue = oproutingentry.getValue();
				for(Map.Entry<Integer,HashBasedTable<String,Integer,String>> opidentry : oproutingValue.entrySet()) {
					Integer opidKey = opidentry.getKey();
					HashBasedTable<String,Integer,String> prorityentry = opidentry.getValue();
					Map<String, String> opidNewMap = opNewRouting.get(oproutingKey);
					if(opidNewMap == null) {
						opidNewMap = new HashMap<>();						
					}
					for(Table.Cell<String,Integer,String> rssidentry : prorityentry.cellSet()) {
						Integer priorityKey = rssidentry.getColumnKey();
						String sender = rssidentry.getRowKey();
						String rssid = rssidentry.getValue();
						opidNewMap.put(opidKey+"_"+sender+"_"+priorityKey, rssid);
						opNewRouting.put(oproutingKey, opidNewMap);
					}
				}
			}
			
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".OPBackupSenderRoutingRelayInfo", opNewRouting);
		}
		
		if(customeropContentKeywordRoutingRelayInfo!= null && !customeropContentKeywordRoutingRelayInfo.isEmpty()){
			//op content routing
			//<ossid, opid, <keyword, priority, rssids>>=> <ossid, <opid_keyword_prority, rssids>>
			Map<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingmap  = customeropContentKeywordRoutingRelayInfo.rowMap();
			Map<String, Map<String, String>> opNewRouting = new HashMap<>();
			for(Map.Entry<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingentry : oproutingmap.entrySet()) {
				String oproutingKey = oproutingentry.getKey();
				Map<Integer,HashBasedTable<String,Integer,String>> oproutingValue = oproutingentry.getValue();
				for(Map.Entry<Integer,HashBasedTable<String,Integer,String>> opidentry : oproutingValue.entrySet()) {
					Integer opidKey = opidentry.getKey();
					HashBasedTable<String,Integer,String> prorityentry = opidentry.getValue();
					Map<String, String> opidNewMap = opNewRouting.get(oproutingKey);
					if(opidNewMap == null) {
						opidNewMap = new HashMap<>();						
					}
					for(Table.Cell<String,Integer,String> rssidentry : prorityentry.cellSet()) {
						Integer priorityKey = rssidentry.getColumnKey();
						String sender = rssidentry.getRowKey();
						String rssid = rssidentry.getValue();
						opidNewMap.put(opidKey+"_"+sender+"_"+priorityKey, rssid);
						opNewRouting.put(oproutingKey, opidNewMap);
					}
				}
			}
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".OPContentKeywordRoutingRelayInfo", opNewRouting);
		}
		
		if(customeropBackupContentKeywordRoutingRelayInfo!= null && !customeropBackupContentKeywordRoutingRelayInfo.isEmpty()){
			//op content routing
			//<ossid, opid, <keyword, priority, rssids>>=> <ossid, <opid_keyword_prority, rssids>>
			Map<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingmap  = customeropBackupContentKeywordRoutingRelayInfo.rowMap();
			Map<String, Map<String, String>> opNewRouting = new HashMap<>();
			for(Map.Entry<String,Map<Integer,HashBasedTable<String,Integer,String>>> oproutingentry : oproutingmap.entrySet()) {
				String oproutingKey = oproutingentry.getKey();
				Map<Integer,HashBasedTable<String,Integer,String>> oproutingValue = oproutingentry.getValue();
				for(Map.Entry<Integer,HashBasedTable<String,Integer,String>> opidentry : oproutingValue.entrySet()) {
					Integer opidKey = opidentry.getKey();
					HashBasedTable<String,Integer,String> prorityentry = opidentry.getValue();
					Map<String, String> opidNewMap = opNewRouting.get(oproutingKey);
					if(opidNewMap == null) {
						opidNewMap = new HashMap<>();						
					}
					for(Table.Cell<String,Integer,String> rssidentry : prorityentry.cellSet()) {
						Integer priorityKey = rssidentry.getColumnKey();
						String sender = rssidentry.getRowKey();
						String rssid = rssidentry.getValue();
						opidNewMap.put(opidKey+"_"+sender+"_"+priorityKey, rssid);
						opNewRouting.put(oproutingKey, opidNewMap);
					}
				}
			}
			GmmsUtility.getInstance().getRedisClient().setRoutingInfo("routingInfo"+core+".OPBackupContentKeywordRoutingRelayInfo", opNewRouting);
		}		 
		
	}

	private void loadPhonePrefixInfo(A2PCustomerConfig config){
		A2PCustomerConfig phonePrefixBlock = phonePrefixMap.get(config.getShortName());
		if(phonePrefixBlock != null){
			try {
				//initSsidtoPhonePrefix(phonePrefixBlock);
				initSenderReplacementPrefix(config);
			} catch (Exception e) {
				log.info("Init phonePrefix info error when reload phonePrefix.cfg, ssid={} {} Ignored {}",
								phonePrefixBlock.getSSID(), phonePrefixBlock
										.getShortName(), e);
			}
		}
	}

	private int loadPhonePrefix() {

		try {
			if (this.comparePhonePrefixConfBlocks()) {
				if (!confBlockMap2Remove.isEmpty()
						|| !confBlockMap2Reload.isEmpty()) {
					try {
						this.initPhonePrefixBlocks();
					} catch (CustomerConfigurationException e1) {
						log.error("init PhonePrefixInfoBlocks error when invoke loadPhonePrefix() ",
										e1.getMessage());
					}

					for (A2PCustomerConfig block : this.confBlockMap2Remove.values()) {
						// clear phonePrefix info
						clearSsidtoPhonePrefix(block.getSSID());
						String shortName = block.getShortName();
						//update phone prefix info:
						if (this.confBlockMap2Reload.containsKey(shortName)) {
							A2PCustomerConfig newBlock = confBlockMap2Reload.remove(shortName);
							this.loadPhonePrefixInfo(newBlock);
						}
					}

					//new added or update phone prefix info
					for (A2PCustomerConfig block : this.confBlockMap2Reload.values()) {
						this.loadPhonePrefixInfo(block);
					}
				}
			}else{
				if(log.isDebugEnabled()){
					log.debug("there is no change for phonePrefix.cfg file, A2P will quit.");
				}
			}

		} catch (Exception ex) {
			return 1;
		} finally {
			confBlockMap2Remove.clear();
			confBlockMap2Reload.clear();
		}

		return 0;
	}

	public void init() {
		try {
			a2pIP = GmmsUtility.getInstance().getCommonProperty("ServiceIP");
			if (a2pIP == null || a2pIP.length() == 0) {
				log.error("Missing Mandatory configure item: ServiceIP!");
				return;
			}

			// get cm.cfg information
			initConfBlocks();

			// get routingInfo from cfg
			initRoutingInfoBlocks();

			// get senderReplacement from cfg
			initPhonePrefixBlocks();
			
			//get recipientRuleFile.cfg
			loadRecipientAddressRule();

			initCustomerInfo();

			blacklist = initBlacklistInfo();
			whitelist = initWhitelistInfo();

			contentTpl = initContentTemplate();

			// Will use currentA2P and vpConnectRelayinfo
			initCurrentA2Ps();
			
			//load senderWhitelist.cfg
			loadSsidSenderWhitelist(true);
			loadSsidSenderBlacklist(true);
			loadSsidRecipientBlacklist(true);
			//content blacklist
			loadSsidContentlist(true, true);
			//content whitelist
			loadSsidContentlist(true, false);
			
			//vendorTemplateReplacement.cfg
			loadVendorTemplateReplacement();
			loadVendorReplacement(true);

			ConfigMonitor configMonitor = new ConfigMonitor();
			new Thread(A2PThreadGroup.getInstance(), configMonitor,
					"OPModeConfigMonitor").start();
		} catch (CustomerConfigurationException ce) {
			log.error("Failed to initCustomerConfigure!{}", ce.getMessage());
		}
	}

	private void initConfBlocks() throws CustomerConfigurationException {
		File conf = new File(confFile);
		confBlockMap = A2PCustomerConfig.parse(conf);
		confFileVersion = getFileCheckSum(conf);
		initConfBlockMaps();

	}	

	private void initConfBlockMaps() throws CustomerConfigurationException {
		for (A2PCustomerConfig block : confBlockMap.values()) {
			try {
				int ssid = block.getMandatoryInt("CCBSSID");
				block.setSsid(ssid);
				this.shortName_ssid_map.put(block.getShortNameWithoutService(),ssid);
				this.check_shortName_ssid_map.put(block.getShortNameWithoutService(), ssid);
			} catch (Exception e) {// modified by Jianming in v1.0.1
				log.error("Exception ignored:" + e.getMessage(), e);
				continue;
			}
		}
	}

	private void initPhonePrefixBlocks() throws CustomerConfigurationException {
		String path = GmmsUtility.getInstance().getPhonePrefixFile();
		if (path != null) {
			File conf = new File(path);
			this.phonePrefixTime = conf.lastModified();
			this.phonePrefixMap = A2PCustomerConfig.parse(conf);
			for (A2PCustomerConfig block : phonePrefixMap.values()) {
				try {

					int ssid = -1;
					ssid = block.getMandatoryInt("CCBSSID");					
					block.setSsid(ssid);
				} catch (Exception e) {
					log.error("Exception ignored:" + e.getMessage(), e);
					continue;
				}
			}
		}
	}

	private void initRoutingInfoBlocks() throws CustomerConfigurationException {
		String path = GmmsUtility.getInstance().getRoutingFilePath();
		if (path != null) {
			File conf = new File(path);

			this.routingInfoMap = A2PCustomerConfig.parseRoutingInfo(conf);
			initRoutingBlockMaps();
		}
	}

	private void initRoutingBlockMaps() throws CustomerConfigurationException {
		for (A2PCustomerConfig block : routingInfoMap.values()) {
			int ssid = -1;
			try {
				ssid = block.getMandatoryInt("CCBSSID");
				log.debug("short name and ssid is {},{}", block.getShortName(), ssid);								
				block.setSsid(ssid);
			} catch (Exception e) {// modified by Jianming in v1.0.1
				log.error("Ignore block error with CCBSSID={},shortName={}",
						ssid, block.getShortName(), e);
				continue;
			}

		}

	}

	private String getFileCheckSum(File file) {
		String md5Sum = null;
		FileInputStream fin = null;
		BufferedReader reader = null;
		try {
			fin = new FileInputStream(file);
			reader = new BufferedReader(new InputStreamReader(fin));
			ByteBuffer buffer = new ByteBuffer();
			String line = null;
			Map<String, String> ssidMap = new HashMap<>();
			while ((line = reader.readLine()) != null) {
				buffer.appendString(line);
				if(line.startsWith("CCBSSID")){
					String[] lines = line.split(":");
					if(ssidMap.get(lines[1].trim()) ==null){
						ssidMap.put(lines[1], lines[1]);
					}else{
						log.error("CCBSSID duplicate, please change it first,{}", lines[1]);
						return null;
					}
				}
			}
			md5Sum = HttpUtils.encrypt(buffer.getBuffer(), "MD5");
			reader.close();
			fin.close();
		} catch (IOException ex) {
			log.error(ex, ex);
		}
		return md5Sum;
	}

	/**
	 * reload customers
	 * 
	 * @return
	 */
	private int loadCustomers() {
		try {
			boolean success = compareConfBlocks();
			if (!success) {
				return 1;
			}
			if (!confBlockMap2Remove.isEmpty()
					|| !confBlockMap2Reload.isEmpty()) {
				this.reloadCustomerInfo();
			}
		} catch (Exception e) {
			log.error("loadCustomers error:", e);
			return 1;
		} finally {
			confBlockMap2Remove.clear();
			confBlockMap2Reload.clear();
		}
		return 0;
	}

	/**
	 * clear for ConnectionManagement
	 */
	private void clearConnectionManagement(int ssid) {
		ArrayList<Integer> ssids = new ArrayList<Integer>();
		ssids.add(ssid);
		String module = System.getProperty("module");
		String moduleType = ModuleManager.getInstance().getFullModuleType(
				module);
		if (SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(moduleType)) {
			ConnectionManagementForCore.getInstance().clearConnection(ssids);
		} else if (SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)) {
			ConnectionManagementForMGT.getInstance().clearConnection(ssids);
		}
	}

	/**
	 * init for ConnectionManagement
	 */
	private void reloadConnectionManagement(int ssid) {
		ArrayList<Integer> ssids = new ArrayList<Integer>();
		ssids.add(ssid);
		String module = System.getProperty("module");
		String moduleType = ModuleManager.getInstance().getFullModuleType(
				module);
		if (SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(moduleType)) {
			ConnectionManagementForCore.getInstance().initConnection(ssids);
		} else if (SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)) {
			ConnectionManagementForMGT.getInstance().initConnection(ssids);
		}
	}

	/**
	 * compareConfBlocks
	 * 
	 * @throws CustomerConfigurationException
	 */
	private boolean compareConfBlocks() throws CustomerConfigurationException {
		File conf = new File(confFile);
		String md5Sum = this.getFileCheckSum(conf);
		if (md5Sum == null || this.confFileVersion.equals(md5Sum)) {
			log.warn("No need to load cm.cfg or md5Sum error. confFileVersion={}, md5Sum={}",new Object[] { confFileVersion, md5Sum });
			return false;
		} else {
			if (log.isInfoEnabled()) {
				log.info("ConfBlock change, confFileVersion={}, md5Sum={}",new Object[] { confFileVersion, md5Sum });
			}
			this.confBlockMap4New = A2PCustomerConfig.parse(conf);
			this.confFileVersion = md5Sum;
		}

		check_shortName_ssid_map.clear();
		for (A2PCustomerConfig newCust : confBlockMap4New.values()) {
			A2PCustomerConfig oldCust = this.confBlockMap.remove(newCust.getShortName());// remove
			if (oldCust != null) {
				if (newCust.compareTo(oldCust) != 0) {// changed
					confBlockMap2Reload.put(newCust.getShortName(), newCust);
					confBlockMap2Remove.put(oldCust.getShortName(), oldCust);
				}
			} else {// added
				confBlockMap2Reload.put(newCust.getShortName(), newCust);
			}

			try {
				int ssid = newCust.getMandatoryInt("CCBSSID");
				newCust.setSsid(ssid);
				check_shortName_ssid_map.put(newCust.getShortNameWithoutService(), ssid);
			} catch (Exception e) {
				log.error("Exception ignored:" + e.getMessage(), e);
				continue;
			}
		}
		confBlockMap2Remove.putAll(confBlockMap);// remove others
		confBlockMap.clear();
		confBlockMap = confBlockMap4New;

		return true;
	}

	/**
	 * comparePhonePrefixConfBlocks
	 * 
	 * @throws CustomerConfigurationException
	 * 
	 * @throws CustomerConfigurationException
	 */
	private boolean comparePhonePrefixConfBlocks() throws CustomerConfigurationException {
		Map<String, A2PCustomerConfig> phonePrefixConfBlockMap4New = null;
		String path = GmmsUtility.getInstance().getPhonePrefixFile();

		if (path != null) {
			File conf = new File(path);
			long modifiedTime = conf.lastModified();
			if(phonePrefixTime == modifiedTime){
				return false;
			}else{
				phonePrefixTime = modifiedTime;
			}
			
			phonePrefixConfBlockMap4New = A2PCustomerConfig.parse(conf);
		}
		for (A2PCustomerConfig newCust : phonePrefixConfBlockMap4New.values()) {
			A2PCustomerConfig oldCust = this.phonePrefixMap.remove(newCust.getShortName());// remove
			if (oldCust != null) {
				if (newCust.compareTo(oldCust) != 0) {// changed
					confBlockMap2Reload.put(newCust.getShortName(), newCust);
					confBlockMap2Remove.put(oldCust.getShortName(), oldCust);
				}
			} else {// added
				confBlockMap2Reload.put(newCust.getShortName(), newCust);
			}

			try {
				int ssid = newCust.getMandatoryInt("CCBSSID");
				newCust.setSsid(ssid);
			} catch (Exception e) {
				log.error("invoke comparePhonePrefixConfBlocks() function, the Exception ignored:"
								+ e.getMessage(), e);
				continue;
			}
		}// end of for
		confBlockMap2Remove.putAll(phonePrefixMap);// add removed customers
		phonePrefixMap.clear();
		phonePrefixMap = phonePrefixConfBlockMap4New;
		return true;
	}
	
	private boolean compareVendorTemplateConfBlocks() throws CustomerConfigurationException {
		Map<String, A2PCustomerConfig> phonePrefixConfBlockMap4New = null;
		String path = GmmsUtility.getInstance().getVendorTemplateFile();

		if (path != null) {
			File conf = new File(path);
			long modifiedTime = conf.lastModified();
			if(vendorTemplateTime == modifiedTime){
				return false;
			}else{
				vendorTemplateTime = modifiedTime;
			}
			
			phonePrefixConfBlockMap4New = A2PCustomerConfig.parse(conf);
		}
		for (A2PCustomerConfig newCust : phonePrefixConfBlockMap4New.values()) {
			A2PCustomerConfig oldCust = this.phonePrefixMap.remove(newCust.getShortName());// remove
			if (oldCust != null) {
				if (newCust.compareTo(oldCust) != 0) {// changed
					confBlockMap2Reload.put(newCust.getShortName(), newCust);
					confBlockMap2Remove.put(oldCust.getShortName(), oldCust);
				}
			} else {// added
				confBlockMap2Reload.put(newCust.getShortName(), newCust);
			}

			try {
				int ssid = newCust.getMandatoryInt("CCBSSID");
				newCust.setSsid(ssid);
			} catch (Exception e) {
				log.error("invoke comparePhonePrefixConfBlocks() function, the Exception ignored:"
								+ e.getMessage(), e);
				continue;
			}
		}// end of for
		confBlockMap2Remove.putAll(phonePrefixMap);// add removed customers
		phonePrefixMap.clear();
		phonePrefixMap = phonePrefixConfBlockMap4New;
		return true;
	}

	/**
	 * init all customer blocks
	 * 
	 * @throws CustomerConfigurationException
	 */
	private void initCustomerInfo() throws CustomerConfigurationException {
		initCustomerInfo(this.confBlockMap);
	}

	/**
	 * clear old blocks reload new customer blocks
	 * 
	 * @param customerBlockMap
	 * @throws CustomerConfigurationException
	 */
	private void reloadCustomerInfo() {
		StringBuffer cmString = new StringBuffer().append("begin reload cm.cfg on:" + new Date() + "\r\n");
		Set<String> rmShortName = new HashSet<String>();
		Set<Integer> ssids = new HashSet<Integer>();

		// A2P will check each customer info is right by traversing
		// routingInfoMap and phonePrefixMap
		try {
			this.initRoutingInfoBlocks();
		} catch (CustomerConfigurationException e1) {
			log.error("init RoutinfInfoBlocks error in reloadCustomerInfo() function.",e1.getMessage());
		}
		try {
			this.initPhonePrefixBlocks();
		} catch (CustomerConfigurationException e1) {
			log.error("init PhonePrefixInfoBlocks error in reloadCustomerInfo() function.",e1.getMessage());
		}

		// remove customers:
		for (A2PCustomerConfig block : this.confBlockMap2Remove.values()) {
			clearCustomerInfo(block);
			// clear for ConnectionManagement
			clearConnectionManagement(block.getSSID());
			this.smsConnectRelayinfo.remove(block.getSSID());
			// clear phonePrefix info
			//clearSsidtoPhonePrefix(block.getSSID());
			String shortName = block.getShortName();
			rmShortName.add(shortName);
			//update customers:
			if (this.confBlockMap2Reload.containsKey(shortName)) {
				A2PCustomerConfig newBlock = confBlockMap2Reload.remove(shortName);
				this.loadCustomerInfo(newBlock);
				int ssid = newBlock.getSSID();
				initConnectedRelayInfo(newBlock);
				ssids.add(block.getSSID());
				// map.put(block.getSSID(), ssid_cust_map.get(block.getSSID()));
				// reload for sessions
				this.reloadConnectionManagement(block.getSSID());
				this.createCustomerSessions(block);
				DistributedThrottlingManager.getInstance().updateRate(ssid, newBlock.getOutgoingThrottlingNum());
//				RetryPolicyManager.getInstance().updateRetryPolicy(block.getSSID());
				rmShortName.remove(shortName);
				if ("partition".equalsIgnoreCase(newBlock.getString("A2PPlayerType"))) {
					int o_ssid = getVPConnectingRelaySsid(ssid);
					if (o_ssid >0 && o_ssid == currentA2P){
						currentA2Ps.add(ssid);
					}
				}
			}
		}
		//new added customers:
		for (A2PCustomerConfig block : this.confBlockMap2Reload.values()) {
			this.loadCustomerInfo(block);
			Integer ssid = this.shortName_ssid_map.get(block.getShortNameWithoutService());
			ssids.add(block.getSSID());
			initConnectedRelayInfo(block);
			// reload for sessions
			this.reloadConnectionManagement(ssid);
			this.createCustomerSessions(block);
			if ("partition".equalsIgnoreCase(block.getString("A2PPlayerType"))) {
				int o_ssid = getVPConnectingRelaySsid(ssid);
				if (o_ssid >0 && o_ssid == currentA2P){
					currentA2Ps.add(ssid);
				}
			}			
		}
		if (log.isInfoEnabled()) {

			if (rmShortName.size() == 0) {
				cmString.append("No customers remove!\r\n");
			} else {
				cmString.append("Remove customers shortName list as follows:\r\n");
			}
			for (String shortName : rmShortName) {
				cmString.append(shortName).append("\r\n");
			}
			cmString.append("Add or Modify customers list as follows: \r\n");
			// String fileName =
			// System.getProperty("a2p_home")+"/conf/"+System.getProperty("module")+"_tempCm.cfg";
			writerCust2File(cmString.toString(), ssids, GmmsUtility
					.getInstance().getCmTempFile());
		}
	}

	private void clearCustomerInfo(A2PCustomerConfig block) {
		A2PCustomerInfo cust = null;
		String spID = null;
		String serverID = null;
		try {
			Integer ssid = this.shortName_ssid_map.get(block
					.getShortNameWithoutService());
			if (ssid == null) {
				log.warn("clearCustomerInfo with null map for ", block
						.getShortNameWithoutService());
				return;
			}
			cust = this.ssid_cust_map.get(ssid);
			// clear ConnectionMap
			clearConnectionMap(cust);
			// at first to clear customer connection
			breakCustomerSessions(cust);
			// clear maps
			this.shortName_ssid_map.remove(cust.getShortName());
			this.shortName_custId_map.remove(cust.getShortName());
			this.ssid_cust_map.remove(cust.getSSID());
			this.ssid_serviceType.remove(cust.getSSID());
			
			// clear retry policy map
			Map<Integer, RetryPolicyInfo> map = RetryPolicyManager.getInstance().getSsid_policy_map();
			RetryPolicyInfo rcust = map.remove(cust.getSSID());
			if(rcust != null){
				rcust.getSenderPrefixPolicyMap().clear();
				rcust.getServiceTypeIDPolicyMap().clear();
				if(rcust.getMoPolicyList() != null) rcust.getMoPolicyList().clear();
				if(rcust.getMtPolicyList() != null) rcust.getMtPolicyList().clear();
			}		
			if (cust.getIosmsSsid() > 0) {
				iosmsSsid_ssid_map.remove(cust.getIosmsSsid());
			}
			if (cust.getCustomerId() > 0) {
				custId_ssid_map.remove(cust.getCustomerId());
			}

			// Common init for IO-SMS, GMMS, IO-MMS
			clearMCCMNCMap(block);

			clearRequiredCharsets(block);

			// Init role map
			clearRoleMap(cust);

			// clearSsidtoPhonePrefix(block);

			spID = block.getString("SPID");
			if (spID != null)
				this.spid_cust_map.remove(spID);

			serverID = block.getString("ServerID");
			if (serverID != null) {
				this.serverid_cust_map.remove(serverID);
				// InitServers: ServerID!=null and ChlInit=true
				if (block.getBool("ChlInit", false)) {// TODO: for modify
					this.initServers.remove(cust);
				}
			}

			clearShortCodeInfo(block);

			clearSupportSplitMsg(cust, block);

			clearRMap(cust, block);

			clearDCNotSupportDR(block);

			clearVfkkInfo(block);

			clearDRModeInfo(block);

			clearCustomerLocalDNSPrefix(block);

			clearContentScan(block);

			clearNotSupportUDH(block);

			clearSupport7Bit(block);

			clearSupportExpiredDR(block);

			clearCurrentA2P(block);
			
			clearCorePrioritySSIDList(block);

			clearProtocolSSIDMap(block);

			clearQueryMsgSSIDList(block);

			clearNoticeInfo(cust);
			// clear ThrottlingControl at last
			ThrottlingControl.getInstance().clearThrottlingControlCache(
					cust.getSSID());

			DuplicateMessageCheck.getInstance().clearCache(cust.getSSID());

			// clear Recipient number length info
			this.clearRecipientLengthInfo(cust.getSSID());

		} catch (Exception e) {// added by Jianming in v1.0.1
			if (log.isInfoEnabled()) {
				log.info("clear CustomerInfo error,ignored!", e);
			}

		}
	}

	/**
	 * breakCustomerSessions
	 * 
	 * @param cust
	 */
	private void breakCustomerSessions(A2PCustomerInfo cust) {
		String module = System.getProperty("module");
		String moduleType = ModuleManager.getInstance().getFullModuleType(
				module);
		String protocol = cust.getProtocol();
		if ("SMPP".equalsIgnoreCase(protocol)) {
			if (SystemConstants.MULTISMPPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				MultiSmppClientFactory clientFct = MultiSmppClientFactory
						.getInstance();
				clientFct.clearConnectionFactory(cust);
			} else if (SystemConstants.MULTISMPPSERVER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				MultiSmppServerFactory serverFct = MultiSmppServerFactory
						.getInstance();
				serverFct.clearConnectionFactory(cust);
			}
		} else if ("SSLSMPP".equalsIgnoreCase(protocol)) {
			if (SystemConstants.SSLSMPPSERVER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				MultiSmppServerFactory serverFct = MultiSmppServerFactory
						.getInstance();
				serverFct.clearConnectionFactory(cust);
			} else if (SystemConstants.SSLSMPPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				SslSmppClientFactory clientFct = SslSmppClientFactory
						.getInstance();
				clientFct.clearConnectionFactory(cust);
			}

		} else if ("Peering2".equalsIgnoreCase(protocol)) {
			if (SystemConstants.PEERING2CLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				PeeringTcp2ClientFactory clientFct = PeeringTcp2ClientFactory
						.getInstance();
				clientFct.clearConnectionFactory(cust);
			} else if (SystemConstants.PEERING2SERVER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				PeeringTcp2ServerFactory serverFct = PeeringTcp2ServerFactory
						.getInstance();
				serverFct.clearConnectionFactory(cust);
			}
		} else {
			int ssid = cust.getSSID();
			if (SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)
					&& SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE
							.equalsIgnoreCase(cust.getChlQueue())) {
				CommonHttpClientFactory commonFct = CommonHttpClientFactory
						.getInstance();
				if ((((A2PSingleConnectionInfo) cust).isChlInit())
						&& this.inCurrentA2P(this.getConnectedRelay(ssid,
								GmmsMessage.AIC_MSG_TYPE_TEXT))) {
					commonFct.removeOperatorMessageQueue(ssid);
					commonFct.clearThreads(cust.getSSID());
				}
			} else if (SystemConstants.CLICKATELLHTTPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)
					&& SystemConstants.CLICKATELLHTTPCLIENT_MODULE_TYPE
							.equalsIgnoreCase(cust.getChlQueue())) {
				ClickatellClientFactory clickFct = ClickatellClientFactory
						.getInstance();
				if ((((A2PSingleConnectionInfo) cust).isChlInit())
						&& this.inCurrentA2P(this.getConnectedRelay(ssid,
								GmmsMessage.AIC_MSG_TYPE_TEXT))) {
					clickFct.removeOperatorMessageQueue(ssid);
				}
			} else if (SystemConstants.SPRINGHTTPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)
					&& SystemConstants.SPRINGHTTPCLIENT_MODULE_TYPE
							.equalsIgnoreCase(cust.getChlQueue())) {
				SpringClientFactory sprFct = SpringClientFactory.getInstance();
				if ((((A2PSingleConnectionInfo) cust).isChlInit())
						&& this.inCurrentA2P(this.getConnectedRelay(ssid,
								GmmsMessage.AIC_MSG_TYPE_TEXT))) {
					sprFct.removeOperatorMessageQueue(ssid);
				}
			} else if (SystemConstants.ACCELETHTTPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)
					&& SystemConstants.ACCELETHTTPCLIENT_MODULE_TYPE
							.equalsIgnoreCase(cust.getChlQueue())) {
				AcceletClientFactory actFct = AcceletClientFactory
						.getInstance();
				if ((((A2PSingleConnectionInfo) cust).isChlInit())
						&& this.inCurrentA2P(this.getConnectedRelay(ssid,
								GmmsMessage.AIC_MSG_TYPE_TEXT))) {
					actFct.removeOperatorMessageQueue(ssid);
				}
			}
		}
	}

	/**
	 * createCustomerSessions
	 * 
	 * @param block
	 */
	private void createCustomerSessions(A2PCustomerConfig block) {
		String module = System.getProperty("module");
		String moduleType = ModuleManager.getInstance().getFullModuleType(
				module);
		Integer ssid = shortName_ssid_map.get(block
				.getShortNameWithoutService());
		if (ssid == null) {
			log.warn("Failed to get shortname {} for {}", block
					.getShortNameWithoutService(), ssid);
			return;
		}
		A2PCustomerInfo custInfo = ssid_cust_map.get(ssid);
		if (custInfo == null) {
			log.warn("Failed to get custInfo for {}", ssid);
			return;
		}
		if ("SMPP".equalsIgnoreCase(custInfo.getProtocol())) {
			if (SystemConstants.MULTISMPPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				MultiSmppClientFactory clientFct = MultiSmppClientFactory
						.getInstance();
				int connectionType = custInfo.getConnectionType();
				if (custInfo.getConnectionType() == 1) {
					if ((((A2PSingleConnectionInfo) custInfo).isChlInit())
							&& this.inCurrentA2P(this.getConnectedRelay(ssid,
									GmmsMessage.AIC_MSG_TYPE_TEXT))) {
						clientFct.initConnectionFactory(ssid, connectionType);
					}
				} else {
					if (((A2PMultiConnectionInfo) custInfo).isInit()
							&& this.inCurrentA2P(this.getConnectedRelay(ssid,
									GmmsMessage.AIC_MSG_TYPE_TEXT))) {
						clientFct.initConnectionFactory(ssid, connectionType);
					}
				}
				clientFct.initializeSession();
			} else if (SystemConstants.MULTISMPPSERVER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				MultiSmppServerFactory serverFct = MultiSmppServerFactory
						.getInstance();
				int connectionType = custInfo.getConnectionType();
				serverFct.initConnectionFactory(ssid, connectionType);
			}
		}
		if ("SSLSMPP".equalsIgnoreCase(custInfo.getProtocol())) {
			if (SystemConstants.SSLSMPPSERVER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				MultiSmppServerFactory serverFct = MultiSmppServerFactory
						.getInstance();
				int connectionType = custInfo.getConnectionType();
				serverFct.initConnectionFactory(ssid, connectionType);
			} else if (SystemConstants.SSLSMPPCLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				SslSmppClientFactory clientFct = SslSmppClientFactory
						.getInstance();
				int connectionType = custInfo.getConnectionType();
				if (custInfo.getConnectionType() == 1) {
					if ((((A2PSingleConnectionInfo) custInfo).isChlInit())
							&& this.inCurrentA2P(this.getConnectedRelay(ssid,
									GmmsMessage.AIC_MSG_TYPE_TEXT))) {
						clientFct.initConnectionFactory(ssid, connectionType);
					}
				} else {
					if (((A2PMultiConnectionInfo) custInfo).isInit()
							&& this.inCurrentA2P(this.getConnectedRelay(ssid,
									GmmsMessage.AIC_MSG_TYPE_TEXT))) {
						clientFct.initConnectionFactory(ssid, connectionType);
					}
				}
				clientFct.initializeSession();
			}
		} else if ("Peering2".equalsIgnoreCase(custInfo.getProtocol())) {
			if (SystemConstants.PEERING2CLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				PeeringTcp2ClientFactory clientFct = PeeringTcp2ClientFactory
						.getInstance();
				int connectionType = custInfo.getConnectionType();
				if (custInfo.getConnectionType() == 1) {
					if ((((A2PSingleConnectionInfo) custInfo).isChlInit())) {
						clientFct.initConnectionFactory(ssid, connectionType);
					}
				} else {
					if (((A2PMultiConnectionInfo) custInfo).isInit()) {
						clientFct.initConnectionFactory(ssid, connectionType);
					}
				}
			} else if (SystemConstants.PEERING2SERVER_MODULE_TYPE
					.equalsIgnoreCase(moduleType)) {
				PeeringTcp2ServerFactory serverFct = PeeringTcp2ServerFactory
						.getInstance();
				int connectionType = custInfo.getConnectionType();
				serverFct.initConnectionFactory(ssid, connectionType);
			}
		}else{
			if(SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE.equalsIgnoreCase(moduleType)
					&& SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE.equalsIgnoreCase(custInfo.getChlQueue())) {
				CommonHttpClientFactory commonFct = CommonHttpClientFactory.getInstance();
				if ((((A2PSingleConnectionInfo) custInfo).isChlInit())
						&& this.inCurrentA2P(this.getConnectedRelay(ssid,GmmsMessage.AIC_MSG_TYPE_TEXT))) {
					commonFct.reloadQueryMessageThread(ssid);
				}
			}
		}
	}

	/**
	 * init Customer Info
	 * 
	 * @param customerBlockMap
	 * @throws CustomerConfigurationException
	 */
	private void initCustomerInfo(
			Map<String, A2PCustomerConfig> customerBlockMap) {
		for (A2PCustomerConfig block : customerBlockMap.values()) {
			initCustomerInfo(block, false);
		}
		
		for (A2PCustomerConfig block : customerBlockMap.values()) {
			initConnectedRelayInfo(block);
		}

		for (A2PCustomerConfig block : phonePrefixMap.values()) {
			initSenderReplacementPrefix(block);
		}

		for (A2PCustomerConfig block : routingInfoMap.values()) {
			try {				
				initRoutingRelay(block);				
				initSecRoutingRelay(block);				
				//loadBackupRoutingRelay(block, backupRoutingRelayInfo);
				initPolicyInfo(block);
				initCharsetRelay(block);
				initContentWhiteList(block, null, false);
				initContentRelay(block, null, false);
				initSenderAddrRelay(block, null, false);
				initSenderAddrReplace(block, null, false);
				initRecipientAddrReplace(block, null, false);

				initContentSignature(block, contentSignatureMap);
				initServiceTypeIDRelay(block, serviceTypeIDRelayInfo);
				initContentKeyword(block, contentKeywordMap);
				
				initSenderRoutingRelay(block);
				initSecSenderRoutingRelay(block);
				initContentKeywordRoutingRelay(block);
				initSecContentKeywordRoutingRelay(block);
				
				//op routing
				initNumberRoutingRelay(block);
				initOPRoutingRelay(block);
				initOPSenderRoutingRelay(block);
				initOPContentRoutingRelay(block);

			} catch (Exception e) {// added by Jianming
				log.info("Init routingInfo error, ssid={} {} Ignored {}", block
						.getSSID(), block.getShortName(), e);
				continue;
			}
		}		

		if (currentA2P <= 0) {
			log.error("Error getting the SSID of Current A2P by serviceIP:{}",
					a2pIP);
		}
	}

	private void loadCustomerInfo(A2PCustomerConfig config) {
		initCustomerInfo(config, true);

		/*A2PCustomerConfig phonePrefixBlock = phonePrefixMap.get(config
				.getShortName());
		if (phonePrefixBlock != null) {
			try {
				initSsidtoPhonePrefix(phonePrefixBlock);
			} catch (Exception e) {
				log.info("Init phonePrefix info error when reload cm.cfg, ssid={} {} Ignored {}",
								phonePrefixBlock.getSSID(), phonePrefixBlock
										.getShortName(), e);
			}
		}*/

		A2PCustomerConfig block = routingInfoMap.get(config.getShortName());
		if (block != null) {
			try {
				initPolicyInfo(block);
			} catch (Exception e) {
				log.info("Init PolicyInfo error when reload cm.cfg, ssid={} {} Ignored {}",
								block.getSSID(), block.getShortName(), e);
			}
		}
		if (currentA2P <= 0) {
			log.error("Error getting the SSID of Current A2P by serviceIP:{}",
					a2pIP);
		}
	}

	/**
	 * initCustomerInfo by block
	 * 
	 * @param block
	 */
	private void initCustomerInfo(A2PCustomerConfig block, boolean isReload) {
		A2PCustomerInfo cust = null;
		try {
			int ssid = block.getMandatoryInt("CCBSSID");
			block.setSsid(ssid);
			String connectionType = block.getString("ConnectionType");
			if ("MN".equalsIgnoreCase(connectionType)) {
				cust = new MultiNodeCustomerInfo();
				cust.assignValue(block, shortName_ssid_map);
				initConnectionMap(cust);
			} else if ("MC".equalsIgnoreCase(connectionType)) {
				cust = new SingleNodeCustomerInfo();
				cust.assignValue(block, shortName_ssid_map);
				initConnectionMap(cust);
			} else {
				cust = new A2PSingleConnectionInfo();
				cust.assignValue(block, shortName_ssid_map);
			}

			if (isReload) {
				this.shortName_ssid_map.put(cust.getShortName(), cust.getSSID());
			}

			this.shortName_custId_map.put(cust.getShortName(), cust.getCustomerId());

			this.ssid_cust_map.put(cust.getSSID(), cust);

			this.ssid_serviceType.put(cust.getSSID(), cust.getService());

			if (cust.getIosmsSsid() > 0) {
				iosmsSsid_ssid_map.put(cust.getIosmsSsid(), cust.getSSID());
			}
			if (cust.getCustomerId() > 0) {
				custId_ssid_map.put(cust.getCustomerId(), cust.getSSID());
			}

			// Common init for IO-SMS, GMMS, IO-MMS
			initMCCMNCMap(block);

			initRequiredCharsets(block);

			// Init role map
			initRoleMap(cust);

			// initSsidtoPhonePrefix(block);

			String spID = block.getString("SPID");
			if (spID != null)
				this.spid_cust_map.put(spID, cust);

			String serverID = block.getString("ServerID");
			if (serverID != null) {
				this.serverid_cust_map.put(serverID, cust);
				// InitServers: ServerID!=null and ChlInit=true
				if (block.getBool("ChlInit", false))
					this.initServers.add(cust);
			}

			initShortCodeInfo(block);

			initSupportSplitMsg(cust, block);

			initRMap(cust, block);

			initDCNotSupportDR(block);

			initVfkkInfo(block);

			initDRModeInfo(block);

			initCustomerLocalDNSPrefix(block);

			initContentScan(block);

			initNotSupportUDH(block);

			initSupport7Bit(block);

			initSupportExpiredDR(block);

			initCurrentA2P(block);
			
			initCorePrioritySSIDList(block);

			initProtocolSSIDMap(block);

			initQueryMsgSSIDList(block);

			initNoticeInfo(cust);

			initRecipientLen(recipientLengthMap,ssid);
			initSsidtoPhonePrefix(block);			

		} catch (Exception e) {// added by Jianming in v1.0.1
			if (log.isInfoEnabled()) {
				log.info("initCustomerInfo error,ignored!", e);
			}

		}
	}

	public void initNoticeInfo(A2PCustomerInfo cust) {

		String shortName = cust.getShortName();
		ArrayList<String> prefixes = cust.getNoticePrefix();
		if (prefixes != null) {
			for (int i = 0; i < prefixes.size(); i++) {
				noticeInfo.add(shortName + "." + prefixes.get(i).trim());
			}
		}
	}

	public void clearNoticeInfo(A2PCustomerInfo cust) {

		String shortName = cust.getShortName();
		ArrayList<String> prefixes = cust.getNoticePrefix();
		if (prefixes != null) {
			for (int i = 0; i < prefixes.size(); i++) {
				noticeInfo.remove(shortName + "." + prefixes.get(i).trim());
			}
		}
	}

	public BlackList initBlacklistInfo() {
		BlackList bl = new BlackList();
		GmmsUtility util = GmmsUtility.getInstance();
		String path = util.getBlacklistFilePath();
		if (bl.loadBlacklist(path) != 0) {
			log.error("Parse blacklist error!");// modified by Jianming in
			// v1.0.1
		}
		return bl;
	}
	
	public WhiteList initWhitelistInfo() {
		WhiteList bl = new WhiteList();
		GmmsUtility util = GmmsUtility.getInstance();
		String path = util.getWhitelistFilePath();
		if (bl.loadWhitelist(path) != 0) {
			log.error("Parse whitelist error!");// modified by Jianming in
			// v1.0.1
		}
		return bl;
	}

	/**
	 * init ContentTemplate with configuration file
	 * 
	 * @return ContentTemplate
	 */
	public ContentTemplate initContentTemplate() {
		ContentTemplate contentTpl = new ContentTemplate();
		GmmsUtility util = GmmsUtility.getInstance();
		String path = util.getContentTemplateFile();
		if (contentTpl.loadContentTemplate(path)) {
			if (log.isInfoEnabled()) {
				log.info("Load Content Template success!");
			}

		} else {
			log.error("Load Content Template error!");
		}

		return contentTpl;
	}

	public boolean allowSplit(GmmsMessage msg) {
		return chlSupportSplitMsg.contains(msg.getRSsID())
				&& oOpSupportSplitMsg.contains(msg.getOoperator());
	}

	/**
	 * To get ASCII Length which Roperator support if not configture then return
	 * 0
	 */
	private int getRopSupportAsciiLength(int ssid) {
		int length = 0;
		if (this.rOpSupportAsciiLength.containsKey(ssid)) {
			try {
				length = Integer.parseInt(rOpSupportAsciiLength.get(ssid));
			} catch (Exception ex) {
				log.warn(ex, ex);
			}
		}
		return length;
	}

	private static final int MSG_SPLIT_LENGTH_MIN = 20;
	private static final int MSG_SPLIT_LENGTH_DEFAULT = 160;

	/**
	 * this method is to get the length that Roperator support
	 * 
	 * @param msg
	 *            GmmsMessage
	 * @return int added by Sean on 2006,3,15
	 */
	public int getRopSupportLength(GmmsMessage msg) {
		int length = 0;
		try {
			length = Integer.parseInt(GmmsUtility.getInstance()
					.getModuleProperty("ContentLength",
							MSG_SPLIT_LENGTH_DEFAULT + ""));
		} catch (NumberFormatException e) {
			log.warn(e, e);
			length = MSG_SPLIT_LENGTH_DEFAULT;
		}

		int ssid = msg.getRSsID();
		String contentType = msg.getContentType();
		if (msg.isGsm7bit()) {
			contentType = GmmsMessage.AIC_CS_ASCII;
		}
		// get by rssid
		int supportLength = getRopSupportLength(ssid, contentType);
		if (supportLength >= MSG_SPLIT_LENGTH_MIN) {
			return supportLength;
		}

		// get by rop
		ssid = msg.getRoperator();
		supportLength = getRopSupportLength(ssid, contentType);
		if (supportLength >= MSG_SPLIT_LENGTH_MIN) {
			return supportLength;
		}

		return length;
	}

	private int getRopSupportLength(int ssid, String contentType) {
		if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(contentType)) {
			int msgSupportAsciiLen = this.getRopSupportAsciiLength(ssid);
			if (msgSupportAsciiLen >= MSG_SPLIT_LENGTH_MIN) {
				return msgSupportAsciiLen;
			}
		}

		if (rOpSupportLength.containsKey(ssid)) {
			try {
				int messageSupportedLength = Integer.parseInt(rOpSupportLength
						.get(ssid));
				if (messageSupportedLength >= MSG_SPLIT_LENGTH_MIN) {
					return messageSupportedLength;
				}
			} catch (Exception ex) {
				log.warn(ex, ex);
			}
		}

		return 0;
	}

	public boolean supportSplitSuffix(GmmsMessage msg) {
		int rop = msg.getRSsID();
		if (rOpSplitSuffix.contains(new Integer(rop))) {
			return true;
		} else {
			rop = msg.getRoperator();
			return rOpSplitSuffix.contains(new Integer(rop));
		}
	}

	private void initProtocolSSIDMap(A2PCustomerConfig cfg) {
		String protocol = cfg.getString("Protocol");
		if (protocol == null)
			return;

		if (this.protocol_ssid_map.get(protocol.trim().toLowerCase()) != null) {
			ArrayList<Integer> ssids = this.protocol_ssid_map.get(protocol
					.trim().toLowerCase());
			ssids.add(cfg.getSSID());
		} else {
			ArrayList<Integer> ssids = new ArrayList<Integer>();
			ssids.add(cfg.getSSID());
			this.protocol_ssid_map.put(protocol.trim().toLowerCase(), ssids);
		}
	}

	private void clearProtocolSSIDMap(A2PCustomerConfig cfg) {
		String protocol = cfg.getString("Protocol");
		if (protocol == null)
			return;

		if (this.protocol_ssid_map.get(protocol.trim().toLowerCase()) != null) {
			ArrayList<Integer> ssids = this.protocol_ssid_map.get(protocol
					.trim().toLowerCase());
			int index = ssids.indexOf(cfg.getSSID());
			if (index >= 0) {
				ssids.remove(index);
			}
		}
	}

	private void initQueryMsgSSIDList(A2PCustomerConfig cfg) {
		String queryMsgFlag = cfg.getString(
				"SMSOptinIsSupportHttpQueryMessage", "");
		if ("".equalsIgnoreCase(queryMsgFlag))
			return;
		if (this.queryMsg_ssid_list == null) {
			queryMsg_ssid_list = new ArrayList<Integer>();
		}
		queryMsg_ssid_list.add(cfg.getSSID());
	}

	private void clearQueryMsgSSIDList(A2PCustomerConfig cfg) {
		String queryMsgFlag = cfg.getString(
				"SMSOptinIsSupportHttpQueryMessage", "");
		if ("".equalsIgnoreCase(queryMsgFlag))
			return;
		if (this.queryMsg_ssid_list != null && !queryMsg_ssid_list.isEmpty()) {
			queryMsg_ssid_list.remove(Integer.valueOf(cfg.getSSID()));
		}
	}
	
	private void initCorePrioritySSIDList(A2PCustomerConfig cfg) {
		String queryMsgFlag = cfg.getString(
				"SMSOptionCoreProcessorThrottlingNum", "");
		if ("".equalsIgnoreCase(queryMsgFlag))
			return;
		if (this.corePriority_ssid_list == null) {
			corePriority_ssid_list = new ArrayList<Integer>();
		}
		corePriority_ssid_list.add(cfg.getSSID());
		log.info("core priority ssid is: {}", corePriority_ssid_list);
	}
	
	private void clearCorePrioritySSIDList(A2PCustomerConfig cfg) {
		String corePriorityFlag = cfg.getString(
				"SMSOptionCoreProcessorThrottlingNum", "");
		if ("".equalsIgnoreCase(corePriorityFlag))
			return;
		if (this.corePriority_ssid_list != null && !corePriority_ssid_list.isEmpty()) {
			corePriority_ssid_list.remove(Integer.valueOf(cfg.getSSID()));
		}
	}

	private void initSsidtoPhonePrefix(A2PCustomerConfig cfg) {
		List<Pattern> list = new ArrayList<Pattern>();

		int ssid = cfg.getSSID();

		A2PCustomerMultiValue prefixes = cfg.parseMultiValue("PhoneSubPrefix");
		this.constructPhonePrefixPattern(prefixes, list, ssid);

		if (list.size() == 0) {
			prefixes = cfg.parseMultiValue("PhonePrefix");
			this.constructPhonePrefixPattern(prefixes, list, ssid);
		}
		ssidToPhonePrefix.put(ssid, list);
	}
	
	private void initSenderReplacementPrefix(A2PCustomerConfig cfg) {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue("SenderReplacement");
		if (relays != null) {
			int ssid = cfg.getSSID();
			List<String> prefixLists = new ArrayList<String>();			
			for (Group g : relays.getAllGroups()) {
				try {
					String prefixString = g.getAttr("PhonePrefix").getStringValue();
					String replaceTo = g.getAttr("ReplaceTo").getStringValue();					
					String senderAddress = g.getAttr("SenderAddress").getStringValue();					
					String ossid = g.getAttr("Ossid").getStringValue();					
					String rssid = g.getAttr("Rssid").getStringValue();	
					log.info("ossid={},prefixString={},replaceTo={},senderAddress={},rssid={},ssid={}",
							ossid,prefixString,replaceTo,senderAddress,rssid,ssid);
					if (prefixString == null 
							|| replaceTo == null || senderAddress == null
							|| ossid == null || rssid == null || ssid ==0) {
						continue;
					}
					Map<String, Map<String, Map<String, String>>> ssidMap = senderReplacementPrefix.get(ssid);
					if (ssidMap == null) {
						ssidMap = new HashMap<String, Map<String,Map<String,String>>>();
						senderReplacementPrefix.put(ssid, ssidMap);
					}
					Map<String, Map<String, String>> prefixMap = ssidMap.get(ossid+"_"+rssid);
					if (prefixMap == null) {
						prefixMap = new HashMap<String, Map<String,String>>();
						ssidMap.put(ossid+"_"+rssid, prefixMap);
					}										
					if(prefixString!= null) {
						String [] prefixList = prefixString.split(A2PCustomerConfig.REGEX_VALUE_SEP);						
						for(String prefix: prefixList) {
							Map<String, String> replaceMap = prefixMap.get(prefix);
							if (replaceMap == null) {
								replaceMap = new HashMap<String, String>();
								prefixMap.put(prefix, replaceMap);
							}
							replaceMap.put(senderAddress, replaceTo);							
							prefixLists.add(prefix);							
						}
					}
					
				} catch (Exception e) {// modified by Jianming in v1.0.1
					log.error("initRoutingRelay Exception ignored:{}", g.toString());
					continue;
				}
			}
			Collections.sort(prefixLists,  new Comparator (){
				 @Override
			     public int compare(Object o1, Object o2)
			    {
			           String prefix1= (String )o1;
			           String prefix2= (String )o2;

			           return prefix2.length()-prefix1.length();
			    }
			});
			List<Pattern>  regexs  = new ArrayList<Pattern>();
			constructPhonePrefixPattern(prefixLists, regexs);
			senderReplacementPrefixRegex.put(ssid, regexs);
		}

		
	}
	
	private int loadVendorTemplateReplacement() {
		String path = GmmsUtility.getInstance().getVendorTemplateFile();
		try {
			if (path != null) {
				File conf = new File(path);
				this.vendorTemplatePrefixMap = A2PCustomerConfig.parse(conf);
				for (A2PCustomerConfig block : vendorTemplatePrefixMap.values()) {
					try {
						int ssid = -1;
						ssid = block.getMandatoryInt("CCBSSID");					
						block.setSsid(ssid);
					} catch (Exception e) {
						log.error("Exception ignored:" + e.getMessage(), e);
						continue;
					}
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		
		if(vendorTemplatePrefixMap!=null && !vendorTemplatePrefixMap.isEmpty()) {
			Map<Integer, Map<String,Map<String,Map<String, String>>>> temptemplateReplacementPrefix = new HashMap();
			Map<Integer, List<Pattern>> temptemplateReplacementPrefixRegex = new HashMap();
			for(A2PCustomerConfig cfg : vendorTemplatePrefixMap.values()) {
				A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue("ContentReplacement");
				if (relays != null) {
					int ssid = cfg.getSSID();
					List<String> prefixLists = new ArrayList<String>();			
					for (Group g : relays.getAllGroups()) {
						try {
							String prefixString = g.getAttr("PhonePrefix").getStringValue();
							String templateId = g.getAttr("TemplateId").getStringValue();					
							String senderAddress = g.getAttr("SenderAddress").getStringValue();					
							String ossid = g.getAttr("Ossid").getStringValue();					
							String rssid = g.getAttr("Rssid").getStringValue();	
							log.info("ossid={},prefixString={},TemplateId={},senderAddress={},rssid={},ssid={}",
									ossid,prefixString,templateId,senderAddress,rssid,ssid);
							if (prefixString == null 
									|| templateId == null || senderAddress == null
									|| ossid == null || rssid == null || ssid ==0) {
								continue;
							}
							Map<String, Map<String, Map<String, String>>> ssidMap = temptemplateReplacementPrefix.get(ssid);
							if (ssidMap == null) {
								ssidMap = new HashMap<String, Map<String,Map<String,String>>>();
								temptemplateReplacementPrefix.put(ssid, ssidMap);
							}
							Map<String, Map<String, String>> prefixMap = ssidMap.get(ossid+"_"+rssid);
							if (prefixMap == null) {
								prefixMap = new HashMap<String, Map<String,String>>();
								ssidMap.put(ossid+"_"+rssid, prefixMap);
							}										
							if(prefixString!= null) {
								String [] prefixList = prefixString.split(A2PCustomerConfig.REGEX_VALUE_SEP);						
								for(String prefix: prefixList) {
									Map<String, String> replaceMap = prefixMap.get(prefix);
									if (replaceMap == null) {
										replaceMap = new HashMap<String, String>();
										prefixMap.put(prefix, replaceMap);
									}
									replaceMap.put(senderAddress, templateId);							
									prefixLists.add(prefix);							
								}
							}
							
						} catch (Exception e) {// modified by Jianming in v1.0.1
							log.error("initVendorContentReplacementPrefix Exception ignored:{}", g.toString());
							continue;
						}
					}
					Collections.sort(prefixLists,  new Comparator (){
						 @Override
					     public int compare(Object o1, Object o2)
					    {
					           String prefix1= (String )o1;
					           String prefix2= (String )o2;

					           return prefix2.length()-prefix1.length();
					    }
					});
					List<Pattern>  regexs  = new ArrayList<Pattern>();
					constructPhonePrefixPattern(prefixLists, regexs);
					temptemplateReplacementPrefixRegex.put(ssid, regexs);
				}
			}
			Map temp = this.templateReplacementPrefix;
			this.templateReplacementPrefix = temptemplateReplacementPrefix;
			temp.clear();
			temp = this.templateReplacementPrefixRegex;
			this.templateReplacementPrefixRegex= temptemplateReplacementPrefixRegex;
			temp.clear();
		}
		
		if(vendorTemplatePrefixMap!=null) {
			vendorTemplatePrefixMap.clear();
		}
		return 0;
	}

	private void constructPhonePrefixPattern(A2PCustomerMultiValue prefixes,
			List<Pattern> list, int ssid) {
		StringBuffer buffer = null;
		if (prefixes != null) {
			ArrayList<AttrPair> attrs = prefixes.getAllAttrs();
			for (int i = 0; i < attrs.size(); i++) {
				if (i % 50 == 0) {
					buffer = new StringBuffer();
					buffer.append(attrs.get(i).getStringValue());
					buffer.append("[\\d]+");
				} else if ((i + 1) % 50 == 0 || (i + 1) == attrs.size()) {
					buffer.append("|");
					buffer.append(attrs.get(i).getStringValue());
					buffer.append("[\\d]+");
					String regular = buffer.toString();
					Pattern p = Pattern.compile(regular);
					list.add(p);
					buffer = null;

				} else {
					buffer.append("|");
					buffer.append(attrs.get(i).getStringValue());
					buffer.append("[\\d]+");
				}
			}
			if (buffer != null) {
				String regular = buffer.toString();
				//if (log.isInfoEnabled()) {
					// log.info("ssid:{} create PhonePrefix regular:{}",ssid,regular);
				//}
				Pattern p = Pattern.compile(regular);
				list.add(p);
			}
		}
	}

	private void clearRecipientLengthInfo(int ssid) {
		recipientLengthMap.remove(ssid);
	}

	// private void clearAllowRoutingInfo(int ssid){
	// allowRoutingInfoMap.remove(ssid);
	// for(List<Integer> list:allowRoutingInfoMap.values()){
	// if(list.contains(ssid)) list.remove(ssid);
	// }
	// }
	
	public boolean doCheckRecipientAddressRule(GmmsMessage msg){
    	String recipientAddr = msg.getRecipientAddress();
        boolean result = false;
		if (recipientRuleRegexs == null || recipientRuleRegexs.isEmpty()) {
			log.trace(msg, "do the recipientRule is null");
		    return false;
		}else {			
			for (Pattern pattern : recipientRuleRegexs) {
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getRecipientAddress());
					if (matcher != null && matcher.find()) {
						String key = matcher.group();
						if (msg.getRecipientAddress().startsWith(key)) {
							log.trace("recipientRuleRegexs rule is:{}", key);
						}else{
							continue;
						}
						RecipientAddressRule rule = recipientRuleMap.get(key);
						if(rule!=null){
							List<Integer> lens = rule.getLens();
							if(lens!=null && !lens.isEmpty()){
								if(!lens.contains(recipientAddr.length())){
									log.info(msg, "recipient len is invalid");
									return true;
								}
							}
							String formatRegex = rule.getFormatRegex();
							if(!StringUtils.isEmpty(formatRegex)){
								Pattern p = Pattern.compile(formatRegex);
						        Matcher m = p.matcher(recipientAddr);
						        if(!m.matches()){
						        	log.info(msg, "recipient format regex is invalid");
						        	return true;
						        }
							}
							//black list, contain it will reject.
							List<String> recipientRule = rule.getBlackPrefixList();
							if(recipientRule!=null && !recipientRule.isEmpty()){
								for (String prefix : recipientRule) {
									if(recipientAddr.startsWith(prefix)){
										log.info(msg, "recipient is in black list");
										return true;
									}
								}					
							}
							recipientRule = rule.getAllowPrefixList();
							//allow list, not in will reject.
							if(recipientRule!=null && !recipientRule.isEmpty()){
								for (String prefix : recipientRule) {
									if(recipientAddr.startsWith(prefix)){
										return false;
									}
								}
								log.info(msg, "recipient is not in allow list");
								return true;
							}
						}
					}
				}				
			}
			return result;
		}
    }

	private void clearSsidtoPhonePrefix(int ssid) {
		//ssidToPhonePrefix.remove(ssid);
		senderReplacementPrefix.remove(ssid);
		senderReplacementPrefixRegex.remove(ssid);
	}

	public boolean checkRecPrefixAndLen(String rec, int ossid) {
		boolean checkLength = false;
		boolean checkPhonePrefix = false;
		int recLength = rec.length();
		try {
			// get Allow Customer SSID
			List<Pattern> listPrefix = ssidToPhonePrefix.get(ossid);
			if (listPrefix != null && listPrefix.size()>0) {
				for (Pattern pattern : listPrefix) {
					if (pattern != null) {
						Matcher matcher = null;
						matcher = pattern.matcher(rec);
						if (matcher != null && matcher.matches()) {
							checkPhonePrefix = true;
							break;
						}
					}
				}// end of listPrefix for loop
			}else {
				return true;
			}

			if (checkPhonePrefix) {
				// check length
				List<Integer> list = recipientLengthMap.get(ossid);
				if (list != null) {
					if (list.contains(recLength)) {
						checkLength = true;
					}
				} else {
					if (recLength >= 6 && recLength <= 15) {
						checkLength = true;
					}
				}
			}
			if (checkPhonePrefix && checkLength) {
				return true;
			} else {
				checkPhonePrefix = false;
			}
			/*List<Integer> allowList = this.allowRoutingInfoMap.get(ossid);
			for (int rssid : allowList) {
				// check phonePrefix
				
			}// end of allowList for loop
*/
		} catch (Exception ex) {
			if (log.isInfoEnabled()) {
				log.info("check recipient number prefix and length exception:{}",
								ex);
			}
		}
		return false;
	}
	
	public Integer getSsidByRecPrefixAndLen(String rec, int ossid) {
		boolean checkLength = false;
		boolean checkPhonePrefix = false;
		int recLength = rec.length();
		int ssid=0;
		try {
			// get Allow Customer SSID
			List<Integer> allowList = this.allowRoutingInfoMap.get(ossid);
			for (int rssid : allowList) {
				// check phonePrefix
				List<Pattern> listPrefix = ssidToPhonePrefix.get(rssid);
				if (listPrefix != null) {
					for (Pattern pattern : listPrefix) {
						if (pattern != null) {
							Matcher matcher = null;
							matcher = pattern.matcher(rec);
							if (matcher != null && matcher.matches()) {
								checkPhonePrefix = true;
								break;
							}
						}
					}// end of listPrefix for loop
				}

				if (checkPhonePrefix) {
					// check length
					List<Integer> list = recipientLengthMap.get(rssid);
					if (list != null) {
						if (list.contains(recLength)) {
							checkLength = true;
						}
					} else {
						if (recLength >= 6 && recLength <= 15) {
							checkLength = true;
						}
					}
				}
				if (checkPhonePrefix && checkLength) {
					return rssid;
				} else {
					checkPhonePrefix = false;
				}
			}// end of allowList for loop

		} catch (Exception ex) {
			if (log.isInfoEnabled()) {
				log.info("check recipient number prefix and length exception:{}",
								ex);
			}
		}
		return ssid;
	}

	private void initConnectionMap(A2PCustomerInfo cust) {
		modifyConnectionMap(cust, false);
	}

	private void clearConnectionMap(A2PCustomerInfo cust) {
		modifyConnectionMap(cust, true);
	}

	private void modifyConnectionMap(A2PCustomerInfo cust, boolean toClear) {
		ArrayList temp = null;

		if (cust instanceof SingleNodeCustomerInfo) {
			SingleNodeCustomerInfo singleCust = (SingleNodeCustomerInfo) cust;

			// incomingConnection
			Map<String, ConnectionInfo> connMap = singleCust
					.getConnectionMap(true);
			if (toClear) {
				cleanConnectionMap(null, connMap);
			} else {
				processConnectionMap(null, connMap);
			}
			// outgoingConnection
			connMap = singleCust.getConnectionMap(false);
			if (toClear) {
				cleanConnectionMap(null, connMap);
			} else {
				processConnectionMap(null, connMap);
			}
		} else if (cust instanceof MultiNodeCustomerInfo) {
			MultiNodeCustomerInfo multi_cust = (MultiNodeCustomerInfo) cust;

			Map nodeMap = multi_cust.getNodeMap();
			if (nodeMap == null)
				return;

			String nodeName;
			NodeInfo nodeInfo;

			temp = new ArrayList(nodeMap.keySet());
			for (int i = 0; i < temp.size(); i++) {
				nodeName = (String) temp.get(i);
				nodeInfo = (NodeInfo) nodeMap.get(nodeName);
				if (nodeInfo == null)
					continue;

				// incomingConnection
				Map<String, ConnectionInfo> connMap = nodeInfo
						.getConnectionMap(true);
				if (toClear) {
					cleanConnectionMap(nodeName, connMap);
				} else {
					processConnectionMap(nodeName, connMap);
				}
				// outgoingConnection
				connMap = nodeInfo.getConnectionMap(false);
				if (toClear) {
					cleanConnectionMap(nodeName, connMap);
				} else {
					processConnectionMap(nodeName, connMap);
				}
			}
		}
	}

	private void processConnectionMap(String nodeName,
			Map<String, ConnectionInfo> connMap) {
		if (connMap == null)
			return;

		Iterator iterator = connMap.entrySet().iterator();
		Map.Entry entry = null;
		String connName;
		ConnectionInfo connInfo;
		while (iterator.hasNext()) {
			entry = (Map.Entry) iterator.next();
			connName = entry.getKey().toString();

			if (connName != null && nodeName != null) {
				this.connectionNodeMap.put(connName, nodeName);
			}

			if (connName != null) {
				connInfo = (ConnectionInfo) entry.getValue();
				if (connInfo != null) {
					connectionIPMap.put(connInfo.getConnectionName(), connInfo
							.getURL());
					addServerInfoMap(connInfo);
				}
			}
		}
	}

	private void cleanConnectionMap(String nodeName,
			Map<String, ConnectionInfo> connMap) {
		if (connMap == null)
			return;

		Iterator iterator = connMap.entrySet().iterator();
		Map.Entry entry = null;
		String connName;
		ConnectionInfo connInfo;
		while (iterator.hasNext()) {
			entry = (Map.Entry) iterator.next();
			connName = entry.getKey().toString();

			if (connName != null && nodeName != null) {
				this.connectionNodeMap.remove(connName);
			}

			if (connName != null) {
				connInfo = (ConnectionInfo) entry.getValue();
				if (connInfo != null) {
					connectionIPMap.remove(connInfo.getConnectionName());
					clearServerInfoMap(connInfo);
				}
			}
		}
	}

	public void addConnNodeMapping(String connName, String nodeName) {
		this.connectionNodeMap.put(connName, nodeName);
	}

	public void addConnIPMapping(String connName, String url) {
		connectionIPMap.put(connName, url);
	}

	public void clearConnNodeMapping(String connName) {
		this.connectionNodeMap.remove(connName);
	}

	public void clearConnIPMapping(String connName) {
		connectionIPMap.remove(connName);
	}

	public int getVFKKssid(String nameshort) {
		try {
			if (vfkkInfo.containsKey(nameshort)) {
				return vfkkInfo.get(nameshort);
			} else {
				return 0;
			}
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("Failed to getVFKKssid: {}", nameshort);
			}

			return 0;
		}
	}

	private void loadConnectedRelayInfo(A2PCustomerConfig cfg, Map vp,
			Map smsIp, Map smsSS7, Map smsCon) {
		A2PCustomerMultiValue relays = cfg.parseMultiValue("A2PConnectedRelay");
		if (relays != null) {
			String relay = null;
			ArrayList<AttrPair> attrs = relays.getAllAttrs();
			for (AttrPair attr : attrs) {

				relay = attr.getStringValue(); // Has been trimed off here
				if (relay == null)
					continue;

				int relay_ssid;
				String playerType = null;
				try {
					playerType = this.getCustomerBySSID(cfg.getSSID()).getA2PPlayerType();
				} catch (Exception e) {
					log.warn("there is an exception happened in loadConnectedRelayInfo(), error:"+ e);
				}
				if ("partition".equalsIgnoreCase(playerType)) {
					relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					if (relay_ssid > 0)
						vp.put(cfg.getSSID(), relay_ssid);
				} else if (relay.endsWith("(IP)")) {
					relay = relay.substring(0, relay.length() - 4);
					relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					if (relay_ssid > 0)
						smsIp.put(cfg.getSSID(), relay_ssid);
				} else if (relay.endsWith("(SS7)")) {
					relay = relay.substring(0, relay.length() - 5);
					relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					if (relay_ssid > 0)
						smsSS7.put(cfg.getSSID(), relay_ssid);
				} else {
					relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					if (relay_ssid > 0)
						smsCon.put(cfg.getSSID(), relay_ssid);
				}
			}
		}
	}

	/**
	 * 1. A2PPlayerType=Partition => vpConnectRelayinfo, else: 2. The value ends
	 * with "(IP)" => smsIPConnectRelayInfo, else 3. The value ends with "(SS7)"
	 * => smsSS7ConnectRelayInfo, else 4. ==> smsConnectRelayinfo
	 * 
	 * @param cfg
	 *            A2PCustomerConfig
	 */
	private void initConnectedRelayInfo(A2PCustomerConfig cfg) {
		if (!"A2P".equalsIgnoreCase(cfg.getString("A2PPlayerType"))) {
			this.smsConnectRelayinfo.put(cfg.getSSID(), currentA2P);
		}
		
	}

	private void clearMCCMNCMap(A2PCustomerConfig cfg) {
		String values = cfg.getString("MCCMNC");
		int ssid = cfg.getSSID();
		if (values != null) {
			String[] mccmncs = values.split(",");
			String mccmnc = null;
			Map<String, Integer> map = null;
			map = smsMncMcc;

			for (int i = 0; i < mccmncs.length; i++) {
				mccmnc = mccmncs[i];
				if (mccmnc == null || mccmnc.trim().length() != 6) {
					if (log.isInfoEnabled()) {
						log.info("Error mccmnc configure: {} for {}", mccmnc,
								cfg.getShortName());
					}

					continue;
				}
				String mcc = mccmnc.trim().substring(0, 3);
				String mnc = mccmnc.trim().substring(3);
				map.remove(mnc + "_" + mcc);
				ssidOfMncMcc.remove(ssid);
			}
		}
	}

	/**
	 * No deny configured?
	 * 
	 * @param cfg
	 *            A2PCustomerConfig
	 */

	private void initMCCMNCMap(A2PCustomerConfig cfg) {
		String values = cfg.getString("MCCMNC");
		String service = cfg.getString("Service");
		int ssid = cfg.getSSID();

		if (values != null) {
			String[] mccmncs = values.split(",");
			String mccmnc = null;
			String[] temp = null;

			Map<String, Integer> map = null;
			// if ("IO-SMS".equalsIgnoreCase(service) ||
			// "GMMS".equalsIgnoreCase(service))
			// map = this.smsMncMcc;
			// else
			// map = this.mmsMncMcc;

			map = smsMncMcc;

			for (int i = 0; i < mccmncs.length; i++) {
				mccmnc = mccmncs[i];
				if (mccmnc == null || mccmnc.trim().length() != 6) {
					if (log.isInfoEnabled()) {
						log.info("Error mccmnc configure: {} for {}", mccmnc,
								cfg.getShortName());

					}
					continue;
				}

				String mcc = mccmnc.trim().substring(0, 3);
				String mnc = mccmnc.trim().substring(3);

				if (ssidOfMncMcc.containsKey(ssid)) { // ssid already in hashmap
					temp = ssidOfMncMcc.get(ssid);
					if (!mcc.equalsIgnoreCase(temp[1])) {
						log.warn("Two different MCC values for SSID {}", ssid);
						continue;
					}
				}

				map.put(mnc + "_" + mcc, ssid);

				ssidOfMncMcc.put(ssid, new String[] { mnc, mcc });
			}
		}

	}

	/*
	 * Initialize the customer allow info map from routingInfo.cfg file
	 * 
	 * @param cfg A2PCustomerConfig
	 */
	private void initPolicyInfo(A2PCustomerConfig cfg) {
		A2PCustomerMultiValue allows = cfg.parseMultiValue("ALLOW");
		if (allows != null) {
			A2PCustomerInfo info = ssid_cust_map.get(cfg.getSSID());
			if (info == null) {
				log.warn("can not get customer info by ssid:{}", cfg.getSSID());
				return;
			}
			ArrayList<AttrPair> attrs = allows.getAllAttrs();
			int ossid = cfg.getSSID();
			ArrayList<Integer> allowList = new ArrayList<Integer>();
			for (AttrPair attr : attrs) {
				int r_ssid = this.getSSidbyCustomerNameshort(getShortNameByValue(attr.getStringValue()));
				if (r_ssid > 0) {
					this.policyInfo.put(ossid + ":" + r_ssid, true);
					allowList.add(r_ssid);
				}
			}
			this.allowRoutingInfoMap.put(ossid, allowList);
		}
	}

	@SuppressWarnings("unchecked")
	private void initRecipientLen(Map<Integer, List<Integer>> map, int recSsid) {
		A2PCustomerInfo recinfo = ssid_cust_map.get(recSsid);
		if (recinfo == null) {
			return;
		}
		List<Integer> lens = (recinfo.getRecUserPhoneLens() != null ? recinfo
				.getRecUserPhoneLens() : recinfo.getRecNumberLens());

		if (lens == null || lens.isEmpty()) {
			if (!map.containsKey(recSsid)) {
				List<Integer> list = new ArrayList<Integer>();
				for (int i = 6; i <= 15; i++) {
					list.add(i);
				}
				map.put(recSsid, list);
			} else {
				List<Integer> list = map.get(recSsid);
				if (list == null) {
					list = new ArrayList<Integer>();
					map.put(recSsid, list);
				}
				for (int i = 6; i <= 15; i++) {
					if (!list.contains(i)) {
						list.add(i);
					}
				}
			}
		} else {
			List<Integer> newLens = new ArrayList<Integer>(lens);
			List<Integer> oldLens = map.get(recSsid);

			if (oldLens == null) {
				map.put(recSsid, newLens);
			} else {
				for (Integer len : newLens) {
					if (!oldLens.contains(len)) {
						oldLens.add(len);
					}
				}
			}
		}// end of else
	}

	/**
	 * judge if the VP is on the same A2P
	 * 
	 * @param c_A2P
	 *            : int
	 * @param r_A2P
	 *            : int
	 * @return boolean
	 */
	public boolean vpOnSameA2P(int c_A2P, int r_A2P) {
		return (partitionInfo.contains(r_A2P) && c_A2P == getVPConnectingRelaySsid(r_A2P))
				|| (partitionInfo.contains(c_A2P) && r_A2P == getVPConnectingRelaySsid(c_A2P))
				|| (partitionInfo.contains(c_A2P)
						&& partitionInfo.contains(r_A2P) && getVPConnectingRelaySsid(r_A2P) == getVPConnectingRelaySsid(c_A2P));
	}

	private String getShortNameByValue(String value) {
		if (value == null)
			return null;

		int c = value.lastIndexOf("_");
		if (c > 0)
			return value.substring(0, c);
		else
			return value;
	}

	private void loadPolicyInfo(A2PCustomerConfig cfg,
			Map<String, Boolean> policy, Map<Integer, List<Integer>> allowMap) throws Exception {
		A2PCustomerInfo info = ssid_cust_map.get(cfg.getSSID());
		if (info == null) {
			log.warn("can not reload customer info by ssid:{}", cfg.getSSID());
			return;
		}
		A2PCustomerMultiValue allows = cfg.parseMultiValue("ALLOW");
		if (allows != null) {
			ArrayList<AttrPair> attrs = allows.getAllAttrs();
			ArrayList<Integer> allowList = new ArrayList<Integer>();
			for (AttrPair attr : attrs) {
				String shortName = getShortNameByValue(attr.getStringValue());

				int r_ssid = getSSidbyCustomerNameshort(shortName);
				if (r_ssid > 0) {
					policy.put(cfg.getSSID() + ":" + r_ssid, true);
					allowList.add(r_ssid);
				}
			}
			allowMap.put(cfg.getSSID(), allowList);
		}
	}

	private void initRoleMap(A2PCustomerInfo cust) {
		if ("A2P".equalsIgnoreCase(cust.getA2PPlayerType())) {
			a2pInfo.add(cust.getSSID());
		}
		// else if ("ASG".equalsIgnoreCase(cust.getA2PPlayerType()) ||
		// "KingSMSHub".equalsIgnoreCase(cust.getA2PPlayerType())) {
		// asgInfo.add(cust.getSSID());
		// }
		// else if ("AMR".equalsIgnoreCase(cust.getA2PPlayerType())) {
		// amrInfo.add(cust.getSSID());
		// }
		else if ("operator".equalsIgnoreCase(cust.getA2PPlayerType())) {
			// Note: TransparencyMode default value is set to 0;
			operatorInfo.put(cust.getSSID(), cust.getTransparencyMode());
		} else if ("VirtualOperator".equalsIgnoreCase(cust.getA2PPlayerType())) {
			// Note: TransparencyMode default value is set to 0;
			operatorInfo.put(cust.getSSID(), cust.getTransparencyMode());
			virtualOperatorInfo.add(cust.getSSID());
		} else if ("channel".equalsIgnoreCase(cust.getA2PPlayerType())) {
			channelInfo.add(cust.getSSID());
		} else if ("3rdPartyHUB".equalsIgnoreCase(cust.getA2PPlayerType())
				|| "3rdHUB".equalsIgnoreCase(cust.getA2PPlayerType())) {
			// Note: TransparencyMode default value is set to 0;
			hubInfo.put(cust.getSSID(), cust.getTransparencyMode());
		} else if ("partition".equalsIgnoreCase(cust.getA2PPlayerType())) {
			partitionInfo.add(cust.getSSID());
		}
	}

	private void clearRoleMap(A2PCustomerInfo cust) {
		if ("A2P".equalsIgnoreCase(cust.getA2PPlayerType())) {
			a2pInfo.remove(cust.getSSID());
		} else if ("operator".equalsIgnoreCase(cust.getA2PPlayerType())) {
			// Note: TransparencyMode default value is set to 0;
			operatorInfo.remove(cust.getSSID());
		} else if ("VirtualOperator".equalsIgnoreCase(cust.getA2PPlayerType())) {
			// Note: TransparencyMode default value is set to 0;
			operatorInfo.remove(cust.getSSID());
			virtualOperatorInfo.remove(cust.getSSID());
		} else if ("channel".equalsIgnoreCase(cust.getA2PPlayerType())) {
			channelInfo.remove(cust.getSSID());
		} else if ("3rdPartyHUB".equalsIgnoreCase(cust.getA2PPlayerType())
				|| "3rdHUB".equalsIgnoreCase(cust.getA2PPlayerType())) {
			// Note: TransparencyMode default value is set to 0;
			hubInfo.remove(cust.getSSID());
		} else if ("partition".equalsIgnoreCase(cust.getA2PPlayerType())) {
			partitionInfo.remove(cust.getSSID());
		}
	}

	private void initShortCodeInfo(A2PCustomerConfig cfg) {
		A2PCustomerMultiValue shortCodeValue = cfg.parseMultiValue("ShortCode");
		if (shortCodeValue != null) {
			if (shortCodeValue.getAttr("code") != null) {
				String code = shortCodeValue.getAttr("code").getStringValue();
				if (code != null) {
					this.shortcodeInfo.add(code);
					this.shortcode_ssid_map.put(code, cfg.getSSID());
				}
				if (log.isDebugEnabled()) {
					log.debug("shortcode_ssid_map get ssid:{} by ShortCode:{}",shortcode_ssid_map.get(code), code);
				}
			}
		}
	}

	private void clearShortCodeInfo(A2PCustomerConfig cfg) {
		A2PCustomerMultiValue shortCodeValue = cfg.parseMultiValue("ShortCode");
		if (shortCodeValue != null) {
			if (shortCodeValue.getAttr("code") != null) {
				String code = shortCodeValue.getAttr("code").getStringValue();
				if (code != null) {
					this.shortcodeInfo.remove(code);
					this.shortcode_ssid_map.remove(code);
				}
			}
		}
	}

	private void initCurrentA2P(A2PCustomerConfig cfg) {
		if ("A2P".equalsIgnoreCase(cfg.getString("A2PPlayerType"))) {
			String chlURL = cfg.getString("ChlURL");
			if (a2pIP != null && chlURL != null && chlURL.trim().length() > 0) {
				if (chlURL.indexOf(a2pIP) >= 0) {
					currentA2P = cfg.getSSID();
					currentA2Ps.add(currentA2P);

					String PeeringTcpVersionStr = cfg
							.getString("PeeringTcpVersion");
					if (PeeringTcpVersionStr != null) {
						peeringTcpVersion = getPeeringTcpVersionInt(PeeringTcpVersionStr);
					}
				}
			}
		}
	}

	private void clearCurrentA2P(A2PCustomerConfig cfg) {
		if ("A2P".equalsIgnoreCase(cfg.getString("A2PPlayerType"))) {
			String chlURL = cfg.getString("ChlURL");
			if (a2pIP != null && chlURL != null && chlURL.trim().length() > 0) {
				if (chlURL.indexOf(a2pIP) >= 0) {
					currentA2P = cfg.getSSID();
					currentA2Ps.remove(currentA2P);
				}
			}
		} else if ("partition".equalsIgnoreCase(cfg.getString("A2PPlayerType"))) {
			currentA2Ps.remove(cfg.getSSID());
		}
	}

	private void initCurrentA2Ps() {
		if (currentA2P <= 0)
			return;

		ArrayList<Integer> al = new ArrayList<Integer>(this.vpConnectRelayinfo
				.keySet());

		for (Integer self : al) {
			int o_ssid = getVPConnectingRelaySsid(self);
			if (o_ssid == currentA2P)
				this.currentA2Ps.add(self);
		}
	}

	private void initSupportSplitMsg(A2PCustomerInfo cust, A2PCustomerConfig cfg) {
		String playerType = cust.getA2PPlayerType();
		if ("Operator".equalsIgnoreCase(playerType)
				|| "VirtualOperator".equalsIgnoreCase(playerType)
				|| "3rdpartyhub".equalsIgnoreCase(playerType)) {
			if ("yes".equalsIgnoreCase(cfg.getString("SupportMsgSplit", "yes"))) {
				this.oOpSupportSplitMsg.add(cust.getSSID());
			}
		}
		if ("yes".equalsIgnoreCase(cfg.getString("ChlMsgSplit"))) {
			this.chlSupportSplitMsg.add(cust.getSSID());
		}
	}

	private void clearSupportSplitMsg(A2PCustomerInfo cust,
			A2PCustomerConfig cfg) {
		String playerType = cust.getA2PPlayerType();
		if ("Operator".equalsIgnoreCase(playerType)
				|| "VirtualOperator".equalsIgnoreCase(playerType)
				|| "3rdpartyhub".equalsIgnoreCase(playerType)) {
			if ("yes".equalsIgnoreCase(cfg.getString("SupportMsgSplit", "yes"))) {
				this.oOpSupportSplitMsg.remove(cust.getSSID());
			}
		}

		if ("yes".equalsIgnoreCase(cfg.getString("ChlMsgSplit"))) {
			this.chlSupportSplitMsg.remove(cust.getSSID());
		}
	}

	private void initRMap(A2PCustomerInfo cust, A2PCustomerConfig cfg) {
		String playerType = cust.getA2PPlayerType();
		if ("Operator".equalsIgnoreCase(playerType)
				|| "OperatorAndChannel".equalsIgnoreCase(playerType)
				|| "3rdpartyhub".equalsIgnoreCase(playerType)) {

			String t = cfg.getString("SplitSuffix", "no");
			if ("yes".equalsIgnoreCase(t)) {
				this.rOpSplitSuffix.add(cfg.getSSID());
			}

			t = cfg.getString("SMSOptionAsciiMsgSplitLen");
			if (t != null)
				this.rOpSupportAsciiLength.put(cfg.getSSID(), t);

			t = cfg.getString("MsgSplitLen");
			if (t != null)
				this.rOpSupportLength.put(cfg.getSSID(), t);
		}

	}

	private void clearRMap(A2PCustomerInfo cust, A2PCustomerConfig cfg) {
		String playerType = cust.getA2PPlayerType();
		if ("Operator".equalsIgnoreCase(playerType)
				|| "OperatorAndChannel".equalsIgnoreCase(playerType)
				|| "3rdpartyhub".equalsIgnoreCase(playerType)) {

			String t = cfg.getString("SplitSuffix", "no");
			if ("yes".equalsIgnoreCase(t)) {
				this.rOpSplitSuffix.remove(cfg.getSSID());
			}

			t = cfg.getString("SMSOptionAsciiMsgSplitLen");
			if (t != null)
				this.rOpSupportAsciiLength.remove(cfg.getSSID());

			t = cfg.getString("MsgSplitLen");
			if (t != null)
				this.rOpSupportLength.remove(cfg.getSSID());
		}
	}

	private void initCharsetRelay(A2PCustomerConfig cfg) {
		A2PCustomerMultiValue relays = cfg.parseMultiValue("A2PCharsetRelay");
		if (relays != null) {
			for (Group g : relays.getAllGroups()) {
				try {
					String rOp = g.getAttr("R_OP").getStringValue();
					int rSsid = this.getSSidbyCustomerNameshort(getShortNameByValue(rOp));
					String relay = g.getAttr("relay").getStringValue();
					int relay_ssid = this.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					String charset = g.getAttr("charset").getStringValue();
					if (rSsid > 0 && relay_ssid > 0 && charset != null) {
						String item = cfg.getSSID() + "_" + rSsid + "_"
								+ charset.trim().toLowerCase();
						this.charsetRelay.put(item, relay_ssid);
					}
				} catch (Exception e) {// modified by Jianming in v1.0.1
					log.error("Exception ignored:{}", g.toString());
					continue;
				}
			}
		}
	}

	private void loadCharsetRelay(A2PCustomerConfig cfg,
			Map<String, Integer> charsetRe) {

		A2PCustomerMultiValue relays = cfg.parseMultiValue("A2PCharsetRelay");
		if (relays != null) {
			for (Group g : relays.getAllGroups()) {
				try {
					String rOp = g.getAttr("R_OP").getStringValue();
					int rSsid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(rOp));
					String relay = g.getAttr("relay").getStringValue();
					int relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					String charset = g.getAttr("charset").getStringValue();
					if (rSsid > 0 && relay_ssid > 0 && charset != null) {
						String item = cfg.getSSID() + "_" + rSsid + "_"
								+ charset.trim().toLowerCase();
						charsetRe.put(item, relay_ssid);
					}
				} catch (Exception e) {// modified by Jianming in v1.0.1
					log.error("Exception ignored:{}", g.toString());
					continue;
				}
			}
		}
	}

	public String getCurrentA2Ps() {
		String a2ps = currentA2Ps.toString();
		return a2ps.substring(1, a2ps.length() - 1);
	}

	public boolean inCurrentA2P(int ssid) {
		return currentA2Ps.contains(ssid);
	}

	public int getCurrentA2P() {
		return currentA2P;
	}

	private void initDCNotSupportDR(A2PCustomerConfig cfg) {
		String t = cfg.getString("ChlIsSupportDeliveryReport");
		if ("No".equalsIgnoreCase(t))
			this.rSsidsNotSupportDR.add(cfg.getSSID());
	}

	private void clearDCNotSupportDR(A2PCustomerConfig cfg) {
		String t = cfg.getString("ChlIsSupportDeliveryReport");
		if ("No".equalsIgnoreCase(t))
			this.rSsidsNotSupportDR.remove(cfg.getSSID());
	}

	private void initNotSupportUDH(A2PCustomerConfig cfg) {
		String t = cfg.getString("SmsOptionIsSupportUDH");
		if ("No".equalsIgnoreCase(t))
			this.rSsidNotSupportUDH.add(cfg.getSSID());
	}

	private void clearNotSupportUDH(A2PCustomerConfig cfg) {
		String t = cfg.getString("SmsOptionIsSupportUDH");
		if ("No".equalsIgnoreCase(t))
			this.rSsidNotSupportUDH.remove(cfg.getSSID());
	}

	private void initSupport7Bit(A2PCustomerConfig cfg) {
		String t = cfg.getString("SmsOptionIsSupport7Bit");
		if ("yes".equalsIgnoreCase(t))
			this.rSsidSupport7Bit.add(cfg.getSSID());
	}

	private void clearSupport7Bit(A2PCustomerConfig cfg) {
		String t = cfg.getString("SmsOptionIsSupport7Bit");
		if ("yes".equalsIgnoreCase(t))
			this.rSsidSupport7Bit.remove(cfg.getSSID());
	}

	private void initSupportExpiredDR(A2PCustomerConfig cfg) {
		String t = cfg.getString("SmsOptionIsSupportExpiredDR");
		if ("yes".equalsIgnoreCase(t))
			this.isSupportExpiredDR.add(cfg.getSSID());
	}

	private void clearSupportExpiredDR(A2PCustomerConfig cfg) {
		String t = cfg.getString("SmsOptionIsSupportExpiredDR");
		if ("yes".equalsIgnoreCase(t))
			this.isSupportExpiredDR.remove(cfg.getSSID());
	}

	private void initVfkkInfo(A2PCustomerConfig cfg) {
		String VFKK_2G = "VFKK2G_JP";
		String VFKK_3G = "Jphone_JP";

		if (cfg.getShortName().startsWith(VFKK_2G)
				|| cfg.getShortName().startsWith(VFKK_3G))
			vfkkInfo.put(cfg.getShortName(), cfg.getSSID());
	}

	private void clearVfkkInfo(A2PCustomerConfig cfg) {
		String VFKK_2G = "VFKK2G_JP";
		String VFKK_3G = "Jphone_JP";

		if (cfg.getShortName().startsWith(VFKK_2G)
				|| cfg.getShortName().startsWith(VFKK_3G))
			vfkkInfo.remove(cfg.getShortName());
	}

	private int getPeeringTcpVersionInt(String peeringTcpVersionStr) {
		int result = 0;
		if (peeringTcpVersionStr == null) {
			return result;
		} else {
			if (!peeringTcpVersionStr.startsWith("V")
					&& !peeringTcpVersionStr.startsWith("v")) {
				return result;
			} else {
				try {
					result = Integer.parseInt(peeringTcpVersionStr.substring(1,
							peeringTcpVersionStr.length()));
					return result;
				} catch (Exception e) {
					return 0;
				}
			}
		}
	}
		
	public int getCustomerRoutingRelay(int ossid, GmmsMessage msg) {
		return doGetCustomerRoutingRelay(A2P_ROUTING_RELAY, ossid, msg,customerPerfixInfo,customerRoutingRelayInfo);						
	}
	
	public int getCustomerBackupRoutingRelay(String routingSsids, int ossid, GmmsMessage msg) {
		int result = 0;
		if (customerBackupPerfixInfo.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_BACKUP_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerRoutingRelay(key, ossid, msg,customerBackupPerfixInfo,customerBackupRoutingRelayInfo);								
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;						
	}
	
	public int getSecCustomerRoutingRelay(int ossid, GmmsMessage msg) {
		return doGetCustomerRoutingRelay(A2P_SEC_ROUTING_RELAY, ossid, msg,customerPerfixInfo2,customerRoutingRelayInfo2);						
	}
	
	public int getSecCustomerBackupRoutingRelay(String routingSsids, int ossid, GmmsMessage msg) {
		int result = 0;
		if (customerBackupPerfixInfo2.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_SEC_BACKUP_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerRoutingRelay(key, ossid, msg,customerBackupPerfixInfo2,customerBackupRoutingRelayInfo2);								
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;						
	}

	private int doGetCustomerRoutingRelay(String keyword, int ossid, GmmsMessage msg,
			Map<String, List<Pattern>> prefixInfo, Map<String, Map<String, String>> relayInfo) {
		int rssid = 0;
		List<Pattern> prefixList = prefixInfo.get(keyword+"_"+ossid);
		Map<String, String> routing = relayInfo.get(keyword+"_"+ossid);
		if (prefixList == null || prefixList.isEmpty()) {
			log.trace(msg, "the customer didn't has special routing, use default routing config");
		    return getDefaultRoutingRelay(ossid, msg);
		}else {			
			for (Pattern pattern : prefixList) {
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getRecipientAddress());
					if (matcher != null && matcher.find()) {
						String key = matcher.group();
						if (msg.getRecipientAddress().startsWith(key)) {
							log.trace(msg, "routing key is:{}", key);
							if (routing.get(key) !=null) {
								rssid = getMixRoutingSsid(ossid, routing.get(key), msg);
								log.trace(msg, "get the customer special routing success");
							}
							return rssid;
						}else {
							String recipit = msg.getRecipientAddress();
							if (recipit.startsWith("+")) {
								recipit = recipit.substring(1);
							}
							rssid = doSecondMatch(pattern, key, recipit, routing, ossid);
							if (rssid>0) {
								return rssid;
							}
						}												
					}
				}
			}			
			if (routing != null && !routing.isEmpty()) {
				if(routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY)!=null) {
					rssid = getMixRoutingSsid(ossid, routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY), msg);
				}
			}
			if (rssid == -1 || rssid == 0) {
				log.trace(msg, "can't found routing info from special routing, use default routing config");
			    return getDefaultRoutingRelay(ossid, msg);
			}
			return rssid;
		}
	}
	
	public boolean getSystemRoutingReplace(GmmsMessage msg) {
		return doSystemRoutingReplacement(msg);
	}
	
	private boolean doSystemRoutingReplacement(GmmsMessage msg) {
		
		if (systemRoutingReplacementPrefixPattern == null || systemRoutingReplacementPrefixPattern.isEmpty()) {
			log.trace(msg, "no configure system routing replacement ,not need to do routing replacement");
		    return false;
		}else {			
			for (Pattern pattern : systemRoutingReplacementPrefixPattern) {
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getRecipientAddress());
					if (matcher != null && matcher.find()) {
						String key = matcher.group();
						if (msg.getRecipientAddress().startsWith(key)) {
							log.trace(msg, "system routing key is:{}", key);
							if (systemRoutingReplacementMap.get(key) !=null) {
								Map<String, Integer> vendorMap = systemRoutingReplacementMap.get(key);
								Integer replaceSsid = vendorMap.get(msg.getOSsID()+":"+msg.getRSsID());
								if(replaceSsid != null) {
									log.debug(msg, "get the system replace special routing success, routing ssid is:{}",replaceSsid);
									msg.setRSsID(replaceSsid);
									return true;
								}else {
									Integer defaultSsid = vendorMap.get(-1+":"+msg.getRSsID());
									if(defaultSsid!=null) {
										msg.setRSsID(defaultSsid);
										log.debug(msg, "get the system replace special routing success, routing ssid is:{}",defaultSsid);
										return true;
									}									
								}
								
							}							
						}else {
							String recipit = msg.getRecipientAddress();
							if (recipit.startsWith("+")) {
								recipit = recipit.substring(1);
							}
							if (doVendorReplacementSecondMatch(pattern, key, recipit, msg)){
								return true;
							}							
						}												
					}
				}
			}			
			if (systemRoutingReplacementMap.get("all") !=null) {
				Map<String, Integer> vendorMap = systemRoutingReplacementMap.get("all");
				Integer replaceSsid = vendorMap.get(msg.getOSsID()+":"+msg.getRSsID());
				if(replaceSsid != null) {
					msg.setRSsID(replaceSsid);
					log.debug(msg, "get the system replace special routing success, routing ssid is:{}",replaceSsid);
					return true;
				}else {
					Integer defaultSsid = vendorMap.get(-1+":"+msg.getRSsID());
					if(defaultSsid!=null) {
						msg.setRSsID(defaultSsid);
						log.debug(msg, "get the system replace special routing success, routing ssid is:{}",defaultSsid);
						return true;
					}									
				}
				
			}			
			return false;
		}
	}
	
	private boolean doVendorReplacementSecondMatch(Pattern pattern, String matchString, String recipit, GmmsMessage msg) {
		String prefixString = pattern.pattern();
		int rssid = -1;
		if (StringUtility.stringIsNotEmpty(prefixString)) {
			String newPatString = prefixString.replaceAll(matchString, "aaa");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(recipit);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (recipit.startsWith(key)) {
					log.debug("do system routing second match, then routing key is:{}", key);
					if (systemRoutingReplacementMap.get(key) !=null) {
						Map<String, Integer> vendorMap = systemRoutingReplacementMap.get(key);
						Integer replaceSsid = vendorMap.get(msg.getOSsID()+":"+msg.getRSsID());
						if(replaceSsid != null) {
							msg.setRSsID(replaceSsid);
							log.debug(msg, "get the system replace special routing success, routing ssid is:{}",replaceSsid);
							return true;
						}else {
							Integer defaultSsid = vendorMap.get(-1+":"+msg.getRSsID());
							if(defaultSsid!=null) {
								msg.setRSsID(defaultSsid);
								log.debug(msg, "get the system replace special routing success, routing ssid is:{}",defaultSsid);
								return true;
							}									
						}
					}
					return false;
				}else {
					return doVendorReplacementSecondMatch(newPattern, key, recipit, msg);
				}												
			}								
		}
		return false;
	}
	
	public void doSenderAddressReplace(int ssid, GmmsMessage msg) {
		List<Pattern> prefixList = senderReplacementPrefixRegex.get(ssid);
		Map<String,Map<String,Map<String,String>>> ssidMap = senderReplacementPrefix.get(ssid);
		if (prefixList == null || prefixList.isEmpty() || ssidMap == null || ssidMap.isEmpty()) {
			log.trace(msg, "senderAdress replace not configuration.");
		    return;
		}else {
			boolean isReplace = false;
			for (Pattern pattern : prefixList) {
				log.trace(msg,"do the senderAddress replace pattern is:{}", pattern);
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getRecipientAddress());
					if (matcher != null && matcher.find()) {
						String key = matcher.group();						
						if (msg.getRecipientAddress().startsWith(key)) {
							log.trace(msg,"do the senderAddress replace key is:{}", key);							
							isReplace = doReplaceSenderAddressRegex(msg, ssidMap, key);
						}else {
							String recipit = msg.getRecipientAddress();
							if (recipit.startsWith("+")) {
								recipit = recipit.substring(1);
							}
							isReplace = doSenderReplaceSecondMatch(pattern, key, recipit, msg, ssidMap);
						}												
					}
				}
				if(isReplace) {
					return;
				}
			}
			if(!isReplace) {
				log.trace(msg,"do the senderAddress replace key is null, do replace by 'all' key");
				doReplaceSenderAddressRegex(msg, ssidMap, "all");
			}
			return;
		}						
	}
	
	public void doVendorTemplateReplace(int ssid, GmmsMessage msg) {
		List<Pattern> prefixList = templateReplacementPrefixRegex.get(ssid);
		Map<String,Map<String,Map<String,String>>> ssidMap = templateReplacementPrefix.get(ssid);
		if (prefixList == null || prefixList.isEmpty() || ssidMap == null || ssidMap.isEmpty()) {
			log.trace(msg, "vendor template replace not configuration.");
		    return;
		}else {		
			boolean isReplace = false;
			for (Pattern pattern : prefixList) {
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getRecipientAddress());
					if (matcher != null && matcher.find()) {
						String key = matcher.group();
						if (msg.getRecipientAddress().startsWith(key)) {
							log.trace(msg,"do vendor template replace key is:{}", key);
							isReplace = doVenderTemplateReplaceRegex(msg, ssidMap, key);
						}else {
							String recipit = msg.getRecipientAddress();
							if (recipit.startsWith("+")) {
								recipit = recipit.substring(1);
							}
							isReplace = doVendorTemplateReplaceSecondMatch(pattern, key, recipit, msg, ssidMap);
						}												
					}
				}
				if(isReplace) {
					return;
				}
			}
			if(!isReplace) {
				log.trace(msg,"do vendor template replace key is null, do replace by 'all' key");
				doVenderTemplateReplaceRegex(msg, ssidMap, "all");
			}
			return;
		}						
	}
	
	private boolean doVenderTemplateReplaceRegex(GmmsMessage msg,
			Map<String, Map<String, Map<String, String>>> ssidMap, String key) {
		Map<String, Map<String, String>> prefixMap = ssidMap.get(msg.getOSsID()+"_"+msg.getRSsID());
		if (prefixMap == null) {
			prefixMap = ssidMap.get("-1_"+msg.getRSsID());
			if (prefixMap == null) {
				return false;
			}			
		}
		boolean isprefixAll = false;
		Map<String, String> vendorTemplateReplaceMap = prefixMap.get(key);
		if (vendorTemplateReplaceMap==null) {
			vendorTemplateReplaceMap = prefixMap.get("all");
			if (vendorTemplateReplaceMap == null) {
				
			}else{
				isprefixAll= true;
			}
			
		}
		boolean result = false;
		if(vendorTemplateReplaceMap !=null){
			result = doVendorTemplateReplaceMap(msg, vendorTemplateReplaceMap);
			
			//for prefix=all
			if (!isprefixAll && !result) {
				vendorTemplateReplaceMap = prefixMap.get("all");
				if (vendorTemplateReplaceMap != null) {
					result = doVendorTemplateReplaceMap(msg, vendorTemplateReplaceMap);
				}				
			}
		}
		//for add -1_rssid case	
		if(!result){
			isprefixAll = false;
			prefixMap = ssidMap.get("-1_"+msg.getRSsID());
			if (prefixMap == null) {
				return false;
			}else{
				vendorTemplateReplaceMap = prefixMap.get(key);
				if (vendorTemplateReplaceMap==null) {
					vendorTemplateReplaceMap = prefixMap.get("all");
					if (vendorTemplateReplaceMap == null) {
						return false;
					}else{
						isprefixAll= true;
					}
				}
			}
			
            result = doVendorTemplateReplaceMap(msg, vendorTemplateReplaceMap);
			
			//for prefix=all
			if (!isprefixAll && !result) {
				vendorTemplateReplaceMap = prefixMap.get("all");
				if (vendorTemplateReplaceMap == null) {
					return false;
				}
				result = doVendorTemplateReplaceMap(msg, vendorTemplateReplaceMap);
			}
		}
		return result;
	}

	private boolean doReplaceSenderAddressRegex(GmmsMessage msg,
			Map<String, Map<String, Map<String, String>>> ssidMap, String key) {
		Map<String, Map<String, String>> prefixMap = ssidMap.get(msg.getOSsID()+"_"+msg.getRSsID());
		if (prefixMap == null) {
			prefixMap = ssidMap.get("-1_"+msg.getRSsID());
			if (prefixMap == null) {
				return false;
			}			
		}
		boolean isprefixAll = false;
		Map<String, String> senderReplaceMap = prefixMap.get(key);
		if (senderReplaceMap==null) {
			senderReplaceMap = prefixMap.get("all");
			if (senderReplaceMap == null) {
				
			}else{
				isprefixAll= true;
			}
			
		}
		boolean result = false;
		if(senderReplaceMap !=null){
			result = doSenderReplaceMap(msg, senderReplaceMap);
			
			//for prefix=all
			if (!isprefixAll && !result) {
				senderReplaceMap = prefixMap.get("all");
				if (senderReplaceMap != null) {
					result = doSenderReplaceMap(msg, senderReplaceMap);
				}				
			}
		}
		//for add -1_rssid case	
		if(!result){
			isprefixAll = false;
			prefixMap = ssidMap.get("-1_"+msg.getRSsID());
			if (prefixMap == null) {
				return false;
			}else{
				senderReplaceMap = prefixMap.get(key);
				if (senderReplaceMap==null) {
					senderReplaceMap = prefixMap.get("all");
					if (senderReplaceMap == null) {
						return false;
					}else{
						isprefixAll= true;
					}
				}
			}
			
            result = doSenderReplaceMap(msg, senderReplaceMap);
			
			//for prefix=all
			if (!isprefixAll && !result) {
				senderReplaceMap = prefixMap.get("all");
				if (senderReplaceMap == null) {
					return false;
				}
				result = doSenderReplaceMap(msg, senderReplaceMap);
			}
		}
		return result;
	}
	
	private boolean doVendorTemplateReplaceMap(GmmsMessage msg,
			Map<String, String> senderReplaceMap) {
		for(Map.Entry<String, String> entry: senderReplaceMap.entrySet()){
			String senderPrefix = entry.getKey();
			String replaceTo = entry.getValue();
			String sender = msg.getSenderAddress();
			if(sender == null){
			   sender = "";
			}
			if (senderPrefix.contains("|")) {
				String[] senders = senderPrefix.split("\\|");
				for (int i = 0; i < senders.length; i++) {
					
					if (sender.trim().equals(senders[i])) {
						doVendorTemplateRepaceByTemplateId(replaceTo, msg);
						return true;
					}
				}
			}else{
				if (sender.trim().equals(senderPrefix)) {
					doVendorTemplateRepaceByTemplateId(replaceTo, msg);
					return true;
				}
			}								
		}
		String replaceTo = senderReplaceMap.get("all");
		if (replaceTo!=null) {
			doVendorTemplateRepaceByTemplateId(replaceTo, msg);
			return true;
		}
		return false;
	}

	private boolean doSenderReplaceMap(GmmsMessage msg,
			Map<String, String> senderReplaceMap) {
		for(Map.Entry<String, String> entry: senderReplaceMap.entrySet()){
			String senderPrefix = entry.getKey();
			String replaceTo = entry.getValue();
			String sender = msg.getSenderAddress();
			if(sender == null){
			   sender = "";
			}
			if (senderPrefix.contains("|")) {
				String[] senders = senderPrefix.split("\\|");
				for (int i = 0; i < senders.length; i++) {
					
					if (sender.trim().equals(senders[i])) {
						msg.setSenderAddress(replaceTo);
						return true;
					}
				}
			}else{
				if (sender.trim().equals(senderPrefix)) {
					msg.setSenderAddress(replaceTo);
					return true;
				}
			}								
		}
		String replaceTo = senderReplaceMap.get("all");
		if (replaceTo!=null) {
			msg.setSenderAddress(replaceTo);
			return true;
		}
		return false;
	}
	
	public int getCustomerSenderRoutingRelay(int ossid, GmmsMessage msg) {
		return doGetCustomerSenderRoutingRelay(A2P_SENDER_ROUTING_RELAY, ossid, msg,customerSenderPerfixInfo,customerSenderRoutingRelayInfo);						
	}
	
	public int getCustomerBackupSenderRoutingRelay(String routingSsids, int ossid, GmmsMessage msg) {
		int result = 0;
		if (customerBackupSenderPerfixInfo.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_BACKUP_SENDER_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerSenderRoutingRelay(key, ossid, msg,customerBackupSenderPerfixInfo,customerBackupSenderRoutingRelayInfo);						
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;
	}
	
	public int getSecCustomerSenderRoutingRelay(int ossid, GmmsMessage msg) {
		return doGetCustomerSenderRoutingRelay(A2P_SEC_SENDER_ROUTING_RELAY, ossid, msg,customerSenderPerfixInfo2,customerSenderRoutingRelayInfo2);						
	}
	
	public int getSecCustomerBackupSenderRoutingRelay(String routingSsids, int ossid, GmmsMessage msg) {
		int result = 0;
		if (customerBackupSenderPerfixInfo2.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_SEC_BACKUP_SENDER_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerSenderRoutingRelay(key, ossid, msg,customerBackupSenderPerfixInfo2,customerBackupSenderRoutingRelayInfo2);						
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;
	}

	private int doGetCustomerSenderRoutingRelay(String keyword, int ossid, GmmsMessage msg,
			Map<String, List<Pattern>> prefixInfo, Map<String, Map<String, String>> relayInfo) {
		int rssid = 0;
		List<Pattern> prefixList = prefixInfo.get(keyword+"_"+ossid);
		Map<String, String> routing = relayInfo.get(keyword+"_"+ossid);
		if (prefixList == null || prefixList.isEmpty()) {
		    return rssid;
		}else {			
			for (Pattern pattern : prefixList) {
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getSenderAddress()+"_"+msg.getRecipientAddress());
					if (matcher != null && matcher.find()) {
						String key = matcher.group();
						if (key!=null &&msg.getRecipientAddress().startsWith(key.substring(msg.getSenderAddress().length()+1))) {
							log.trace(msg, "routing key is:{}", key);
							if (routing.get(key) !=null) {
								rssid = getMixRoutingSsid(ossid, routing.get(key), msg);
								log.trace(msg, "get the customer special routing success");
							}
							return rssid;
						}else {
							String recipit = msg.getRecipientAddress();
							if (recipit.startsWith("+")) {
								recipit = recipit.substring(1);
							}
							rssid = doSecondMatch(pattern, key, msg.getSenderAddress()+"_"+recipit, routing, ossid);
							if (rssid>0) {
								return rssid;
							}
						}												
					}
				}
			}			
			return rssid;
		}
	}
	
	public int getCustomerContentKeywordRoutingRelay(int ossid, GmmsMessage msg) {
		return doGetCustomerContentKeywordRoutingRelay(A2P_CONTENT_KEYWORD_ROUTING_RELAY, ossid, msg,customerContentKeywordPerfixInfo,customerContentKeywordRoutingRelayInfo);						
	}
	
	public int getCustomerBackupContentKeywordRoutingRelay(String routingSsids, int ossid, GmmsMessage msg) {
		int result = 0;
		if (customerBackupContentKeywordPerfixInfo.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerContentKeywordRoutingRelay(key, ossid, msg,customerBackupContentKeywordPerfixInfo,customerBackupContentKeywordRoutingRelayInfo);							
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;						
	}
	
	public int getSecCustomerContentKeywordRoutingRelay(int ossid, GmmsMessage msg) {
		return doGetCustomerContentKeywordRoutingRelay(A2P_SEC_CONTENT_KEYWORD_ROUTING_RELAY, ossid, msg,customerContentKeywordPerfixInfo2,customerContentKeywordRoutingRelayInfo2);						
	}
	
	public int getSecCustomerBackupContentKeywordRoutingRelay(String routingSsids, int ossid, GmmsMessage msg) {
		int result = 0;
		if (customerBackupContentKeywordPerfixInfo2.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_SEC_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerContentKeywordRoutingRelay(key, ossid, msg,customerBackupContentKeywordPerfixInfo2,customerBackupContentKeywordRoutingRelayInfo2);							
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;						
	}

	private int doGetCustomerContentKeywordRoutingRelay(String keyword, int ossid, GmmsMessage msg,
			Map<String, List<Pattern>> prefixInfo, Map<String, Map<String, Map<String, String>>>  relayInfo) {
		int rssid = 0;
		List<Pattern> prefixList = prefixInfo.get(keyword+"_"+ossid);
		Map<String, Map<String, String>> routing = relayInfo.get(keyword+"_"+ossid);
		if (prefixList == null || prefixList.isEmpty()) {
			return rssid;
		}else {			
			for (Pattern pattern : prefixList) {
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getRecipientAddress());
					if (matcher != null && matcher.find()) {
						String key = matcher.group();
						if (msg.getRecipientAddress().startsWith(key)) {
							//log.trace("routing content keyword key is:{}", key);
							if (routing.get(key) !=null) {
								Map<String, String> contentKeyWordMap = routing.get(key);
								for (String contentKey : contentKeyWordMap.keySet()) {
									//add otp and none otp routing check
									if(contentTpl.checkContent(msg, contentKey)){
										rssid = getMixRoutingSsid(ossid, contentKeyWordMap.get(contentKey), msg);
										log.debug(msg, "get the customer content routing success");
										return rssid;
									}
								}								
							}
							return rssid;
						}else {
							String recipit = msg.getRecipientAddress();
							if (recipit.startsWith("+")) {
								recipit = recipit.substring(1);
							}
							rssid = doContentSecondMatch(pattern, key, recipit, routing, ossid, msg);
							if (rssid>0) {
								return rssid;
							}
						}												
					}
				}
			}						
			return rssid;
		}
	}

	private int doSecondMatch(Pattern pattern, String matchString, String recipit, Map<String, String> routing, int ossid) {
		String prefixString = pattern.pattern();
		int rssid = -1;
		if (StringUtility.stringIsNotEmpty(prefixString)) {
			String newPatString = prefixString.replaceAll(matchString, "aaa");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(recipit);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (recipit.startsWith(key)) {
					log.debug("do second match, then routing key is:{}", key);
					if (routing.get(key) !=null) {
						GmmsMessage msg = new GmmsMessage();
						msg.setRecipientAddress(recipit);
						rssid = getMixRoutingSsid(ossid, routing.get(key), msg);						
					}
					return rssid;
				}else {
					return doSecondMatch(newPattern, key, recipit, routing, ossid);
				}												
			}								
		}
		return rssid;
	}
	
	private boolean doVendorTemplateReplaceSecondMatch(Pattern pattern, String matchString, 
			String recipit, GmmsMessage msg,Map<String,Map<String,Map<String,String>>> ssidMap) {
		String prefixString = pattern.pattern();
		boolean isReplace = false;
		if (StringUtility.stringIsNotEmpty(prefixString)) {
			String newPatString = prefixString.replaceAll(matchString, "aaa");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(recipit);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (recipit.startsWith(key)) {
					log.debug("doVendorTemplateReplaceSecondMatch key is:{}", key);
					isReplace = doVenderTemplateReplaceRegex(msg, ssidMap, key);
				}else {
					return doVendorTemplateReplaceSecondMatch(newPattern, key, recipit, msg, ssidMap);
				}												
			} /*
				 * else { doVenderTemplateReplaceRegex(msg, ssidMap, "all"); }
				 */							
		}
		return isReplace;
	}
	
	private boolean doSenderReplaceSecondMatch(Pattern pattern, String matchString, 
			String recipit, GmmsMessage msg,Map<String,Map<String,Map<String,String>>> ssidMap) {
		String prefixString = pattern.pattern();
		boolean isReplace = false;
		if (StringUtility.stringIsNotEmpty(prefixString)) {
			String newPatString = prefixString.replaceAll(matchString, "aaa");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(recipit);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (recipit.startsWith(key)) {					
					log.debug("doSenderReplaceSecondMatch key is:{}", key);
					isReplace = doReplaceSenderAddressRegex(msg, ssidMap, key);
				}else {
					return doSenderReplaceSecondMatch(newPattern, key, recipit, msg, ssidMap);
				}												
			} /*
				 * else { doReplaceSenderAddressRegex(msg, ssidMap, "all"); }
				 */							
		}
		return isReplace;
	}
	
	private int doContentSecondMatch(Pattern pattern, String matchString, String recipit, 
			Map<String, Map<String, String>> routing, int ossid, GmmsMessage msg) {
		String prefixString = pattern.pattern();
		int rssid = -1;
		if (StringUtility.stringIsNotEmpty(prefixString)) {
			String newPatString = prefixString.replaceAll(matchString, "aaa");
			Pattern newPattern = Pattern.compile(newPatString);
			Matcher matcher = newPattern.matcher(recipit);
			if (matcher != null && matcher.find()) {
				String key = matcher.group();
				if (recipit.startsWith(key)) {
					Map<String, String> contentKeyWordMap = routing.get(key);
					for (String contentKey : contentKeyWordMap.keySet()) {
						if(contentTpl.checkContent(msg, contentKey)){
							rssid = getMixRoutingSsid(ossid, contentKeyWordMap.get(contentKey), msg);
							log.debug(msg, "get the customer content routing success");
							return rssid;
						}
					}
					return rssid;
				}else {
					return doContentSecondMatch(newPattern, key, recipit, routing, ossid, msg);
				}												
			}								
		}
		return rssid;
	}
	
	private int getMixRoutingSsid(int ossid,  String routingValue, GmmsMessage msg){
		try {
			int index = routingValue.indexOf(A2PCustomerConfig.VALUE_SEP);
			if (index>0) {
				String[] ssid_rate = routingValue.split(A2PCustomerConfig.VALUE_SEP);
				A2PCustomerInfo oCustomer = getCustomerBySSID(ossid);			
				//support recipitAddress white list
				if(whitelist.allowWhenRouting(msg) 
						|| (oCustomer.isSmsOptionRecipientNotContinueFilter() 
								&& oCustomer.getReicipientCache()!=null 
								&& oCustomer.getReicipientCache().asMap().containsKey(msg.getRecipientAddress()))){
					for (int i = 0; i < ssid_rate.length; i++) {
						index = ssid_rate[i].indexOf(A2PCustomerConfig.ATTR_SEP);
						if (index>0) {		
							int ssid = Integer.parseInt(ssid_rate[i].split(A2PCustomerConfig.ATTR_SEP)[0]);
							A2PCustomerInfo rCustomer = getCustomerBySSID(ssid);
							if(!"blackhole".equalsIgnoreCase(rCustomer.getRole())){
								return ssid;
							}
						}else {							
							int ssid =  Integer.parseInt(ssid_rate[i]);
							A2PCustomerInfo rCustomer = getCustomerBySSID(ssid);
							if(!"blackhole".equalsIgnoreCase(rCustomer.getRole())){
								return ssid;
							}
						}
					}
				}
				int totalRate = 0;
				for (int i = 0; i < ssid_rate.length; i++) {
					int id = ssid_rate[i].indexOf(A2PCustomerConfig.ATTR_SEP);
					if (id>0) {
						totalRate += Integer.parseInt(ssid_rate[i].split(A2PCustomerConfig.ATTR_SEP)[1]);
					}else{
						totalRate += 0;
					}
				}
				int random = GmmsUtility.getRandomValue(ossid, totalRate);	
				int rate = 0;
				for (int i = 0; i < ssid_rate.length; i++) {
					index = ssid_rate[i].indexOf(A2PCustomerConfig.ATTR_SEP);
					int ssidRate = 0;
					if (index>0) {		
						ssidRate = Integer.parseInt(ssid_rate[i].split(A2PCustomerConfig.ATTR_SEP)[1]);												
						if (random>=rate && random <= (ssidRate+rate)) {
							return Integer.parseInt(ssid_rate[i].split(A2PCustomerConfig.ATTR_SEP)[0]);
						}else {
							rate = ssidRate+rate;
						}
					}else {
						//loadbalace case, rate=0
						index = loadbalanceRandom%ssid_rate.length;
						int ssid =  Integer.parseInt(ssid_rate[index]);
						loadbalanceRandom++;
						if (loadbalanceRandom> 100000000) {
							loadbalanceRandom=0;
						}
						return ssid;
					}										
				}
				return Integer.parseInt(ssid_rate[0].split(A2PCustomerConfig.ATTR_SEP)[0]);
			}else {
				index = routingValue.indexOf(A2PCustomerConfig.ATTR_SEP);
				if (index>0) {
					String[] ssid_rate = routingValue.split(A2PCustomerConfig.ATTR_SEP);
					int rate = GmmsUtility.getRandomValue(ossid, Integer.parseInt(ssid_rate[1]));
					if (rate>Integer.parseInt(ssid_rate[1])) {
						return 0;
					}else {
						return Integer.parseInt(ssid_rate[0]);
					}
				}else {
					return Integer.valueOf(routingValue);
				}
			}
		} catch (Exception e) {
			log.error("getmixroutingssid error! {}", routingValue, e);
			return 0;
		}
		
	}
	
	
	private int getDefaultRoutingRelay(int ossid, GmmsMessage msg) {
		int rssid= 0;
		A2PCustomerInfo customerInfo = this.getCustomerBySSID(ossid);
		List<Pattern> prefixList = customerDefaultPerfixInfo.get(customerInfo.getCustomerServiceType()+Constant.DEFAULT_ROUTING_SUFFER);
		Map<String, String> routing = defaultRoutingRelayInfo.get(customerInfo.getCustomerServiceType()+Constant.DEFAULT_ROUTING_SUFFER);
		if (prefixList == null || prefixList.isEmpty()) {			
			log.trace(msg,"can't found routing config, please config default routing config first.");
			if (routing != null && !routing.isEmpty()) {
				if(routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY)!=null) {
					rssid = getMixRoutingSsid(ossid, routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY), msg);
				}
			}				
			if (rssid == 0) {
				log.trace(msg, "can't found routing config, please config default routing config first.");
			}
			return rssid;			
		}else {
			log.debug(msg, "get the default routing success");
		}		
		
		if (routing == null) {
			return rssid;
		}
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(msg.getRecipientAddress());
				if (matcher != null && matcher.find()) {
					String key = matcher.group();
					if (msg.getRecipientAddress().startsWith(key)) {
						log.debug("routing key is:{}", key);
						if (routing.get(key) !=null) {
							rssid = getMixRoutingSsid(ossid, routing.get(key), msg);
						}
						return rssid;
					}else {
						String recipit = msg.getRecipientAddress();
						if (recipit.startsWith("+")) {
							recipit = recipit.substring(1);
						}
						rssid = doSecondMatch(pattern, key, recipit, routing, ossid);
						if (rssid>0) {
							return rssid;
						}
					}						
				}
			}
		}
		if (routing != null && !routing.isEmpty()) {
			if(routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY)!=null) {
				rssid = getMixRoutingSsid(ossid, routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY), msg);
			}
		}
		return rssid;
	}
	
	private int getBackupDefaultRoutingRelay(int ossid, GmmsMessage msg) {
		int rssid= 0;
		A2PCustomerInfo customerInfo = this.getCustomerBySSID(ossid);
		List<Pattern> prefixList = customerDefaultPerfixInfo.get(customerInfo.getCustomerServiceType()+Constant.DEFAULT_ROUTING_SUFFER);;
		Map<String, String> routing = defaultBackupRoutingRelayInfo.get(customerInfo.getCustomerServiceType()+Constant.DEFAULT_ROUTING_SUFFER);;
		if (prefixList == null || prefixList.isEmpty()) {
			log.error(msg,"can't found routing config, please config default routing config first.");
			if (routing != null && !routing.isEmpty()) {
				if(routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY)!=null) {
					rssid = getMixRoutingSsid(ossid, routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY), msg);
				}
			}
			if (rssid == 0) {
				log.error(msg, "can't found routing config, please config default routing config first.");
			}
			return rssid;
		}
		
		if (routing == null || routing.isEmpty()){
			return rssid;			
		}		
		log.debug(msg, "get the default backup routing success");
		for (Pattern pattern : prefixList) {
			if (pattern != null) {
				Matcher matcher = null;
				matcher = pattern.matcher(msg.getRecipientAddress());
				if (matcher != null && matcher.find()) {
					String key = matcher.group();
					if (msg.getRecipientAddress().startsWith(key)) {
						log.debug("routing key is:{}", key);
						if (routing.get(key) !=null) {
							rssid = getMixRoutingSsid(ossid, routing.get(key), msg);
						}					
						return rssid;
					}else {
						String recipit = msg.getRecipientAddress();
						if (recipit.startsWith("+")) {
							recipit = recipit.substring(1);
						}
						rssid = doSecondMatch(pattern, key, recipit, routing, ossid);
						if (rssid>0) {
							return rssid;
						}
					}
					
				}
			}
		}
		if (routing != null && !routing.isEmpty()) {
			if(routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY)!=null) {
				rssid = getMixRoutingSsid(ossid, routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY), msg);
			}
		}
		
		return rssid;
	}
	
	public int getBackupCustomerRoutingRelay(int ossid, GmmsMessage msg) {
		int rssid = 0;
		List<Pattern> prefixList = customerBackupPerfixInfo.get(ossid);
		Map<String, String> routing = customerBackupRoutingRelayInfo.get(ossid);
		if (prefixList == null || prefixList.isEmpty()) {
			return this.getBackupDefaultRoutingRelay(ossid, msg);			
		}else {
			log.trace("get the customer special routing success");
			for (Pattern pattern : prefixList) {
				if (pattern != null) {
					Matcher matcher = null;
					matcher = pattern.matcher(msg.getRecipientAddress());
					if (matcher != null && matcher.matches()) {
						String key = matcher.group();
						if (msg.getRecipientAddress().startsWith(key)) {
							if (routing.get(key) !=null) {
								rssid = getMixRoutingSsid(ossid, routing.get(key), msg);
							}
							return rssid;
						}else {
							String recipit = msg.getRecipientAddress();
							if (recipit.startsWith("+")) {
								recipit = recipit.substring(1);
							}
							rssid = doSecondMatch(pattern, key, recipit, routing, ossid);
							if (rssid>0) {
								return rssid;
							}
						}						
					}
				}
			}
			
			if (routing != null && !routing.isEmpty()) {
				if(routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY)!=null) {
					rssid = getMixRoutingSsid(ossid, routing.get(A2PCustomerConfig.ALL_ROUTING_INFO_KEY), msg);
					return rssid;
				}
			}
			
			if (rssid == -1 || rssid == 0) {
				log.info(msg, "can't found routing info from special routing, use default routing config");
			    return getBackupDefaultRoutingRelay(ossid, msg);
			}
			return rssid;
		}						
	}

	/**
	 * get ralay ssid Routing Priority: serviceTypeID > Content > SenderId >
	 * Traditional Routing
	 * 
	 * @param o_op
	 * @param r_op
	 * @param msg
	 * @return
	 */
	public int getRoutingRelay(int o_op, int r_op, GmmsMessage msg) {
		try {
			// serviceTypeID based routing
			String keyStr = o_op + CONF_SEPERATOR + r_op + CONF_SEPERATOR + msg.getServiceTypeID();
			if (serviceTypeIDRelayInfo.containsKey(keyStr)) {
				if (log.isInfoEnabled()) {
					log.info(msg, "Got relay by serviceTypeIDRelayInfo");
				}

				return serviceTypeIDRelayInfo.get(keyStr);
			}

			keyStr = o_op + CONF_SEPERATOR + r_op;

			// content based routing
			if (contentRelayInfo.containsKey(keyStr)) {
				List<RelayMark> contentRelayMarkList = contentRelayInfo
						.get(keyStr);
				for (RelayMark contentRelayMark : contentRelayMarkList) {
					String tplId = contentRelayMark.getContentTplID();
					if (contentTpl.checkContent(msg, tplId)) {
						if (log.isInfoEnabled()) {
							log.info(msg,
									"Got relay by contentRelayInfo, tplName="
											+ tplId);
						}
						return contentRelayMark.getRelay();
					}
				}
			}

			// senderID base routing
			if (senderAddrRelayInfo.containsKey(keyStr)) {
				List<RelayMark> senderIdRelayMarkList = senderAddrRelayInfo
						.get(keyStr);
				if (getCustomerBySSID(msg.getOoperator())
						.isMatchFullWhenRouteReplaceOAddr()
						|| getCustomerBySSID(msg.getOSsID())
								.isMatchFullWhenRouteReplaceOAddr()) {
					// match full
					for (RelayMark senderIdRelayMark : senderIdRelayMarkList) {
						String senderAddr = senderIdRelayMark.getSenderAddr();
						if (msg.getSenderAddress().equals(senderAddr)) {
							if (log.isInfoEnabled()) {
								log.info(msg,
										"Got relay by senderAddrRelayInfo, full match, conf O_Addr="
												+ senderAddr);
							}

							return senderIdRelayMark.getRelay();
						}
					}
				} else {
					// for longest prefix match,
					// e.g.5403_3456_abcde, 5403_3456_abcd, 5403_3456_9879,
					// 5403_3456_987
					Collections.sort(senderIdRelayMarkList);
					for (RelayMark senderIdRelayMark : senderIdRelayMarkList) {
						String senderAddr = senderIdRelayMark.getSenderAddr();
						if (msg.getSenderAddress().startsWith(senderAddr)) {
							if (log.isInfoEnabled()) {
								log.info(msg,
										"Got relay by senderAddrRelayInfo, prefix match, conf O_Addr="
												+ senderAddr);
							}

							return senderIdRelayMark.getRelay();
						}
					}
				}
			}

			// Traditional routing
			if (routingRelayInfo.containsKey(keyStr)) {
				log.info(msg, "Got relay by routingRelayInfo");
				return routingRelayInfo.get(keyStr);
			}
		} catch (Exception e) {
			log.warn(e, e);
		}
		return 0;
	}
	
	public int getBackupRoutingRelay(int o_op, int r_op) {
		String keyStr = o_op + CONF_SEPERATOR + r_op;
		if (backupRoutingRelayInfo.containsKey(keyStr)) {
			return backupRoutingRelayInfo.get(keyStr);
		}
		return 0;
	}
	
	private void loadBackupRoutingRelay(A2PCustomerConfig cfg, Map<String, Integer> backupRoutingRelayMap) {
		A2PCustomerMultiValue relays = cfg.parseMultiValue(A2P_BACKUP_ROUTING_RELAY);
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			for (Group g : relays.getAllGroups()) {
				try {
					String r_op = g.getAttr("R_OP").getStringValue();
					int r_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(r_op));
					String relay = g.getAttr("Relay").getStringValue();
					int relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					if (r_ssid > 0 && relay_ssid > 0)
						backupRoutingRelayMap.put(o_ssid + "_" + r_ssid, relay_ssid);
				} catch (Exception e) {
					log.error("loadBackupRoutingRelay Exception ignored:{}", g.toString());
					continue;
				}
			}
		}

	}
	
	@SuppressWarnings("unchecked")
	private void doLoadRoutingRelay(A2PCustomerConfig cfg, String routingKey,
			Map<String, Map<String,String>> routingRelay, 
			Map<String, List<Pattern>> temp_customerPrefixList, Map<String, Map<String,String>> routingDefaultRelay,
			Map<String,List<Pattern>> temp_defaultPrefixList,
			Map<String, Map<String,String>> temp_contentTemplateMap) {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue(routingKey);		
		if (relays != null) {
			String shortName = cfg.getShortName();
			int o_ssid = cfg.getSSID();
			Map<String, String> msaterPrefixMap = new HashMap<String, String>();
			List<String> prefixLists = new ArrayList<String>();
			for (Group g : relays.getAllGroups()) {
				try {
					String prefixString = g.getAttr("PhonePrefix").getStringValue();
					String relay = g.getAttr("Relay").getStringValue();
					if (g.getAttr("ReplaceContentByTemplate")!=null) {						
						this.initContentTemplateId(o_ssid, prefixString, relay, g.getAttr("ReplaceContentByTemplate").getStringValue(), temp_contentTemplateMap, null);
					}
					if(prefixString!= null) {
						String [] prefixList = prefixString.split(A2PCustomerConfig.SUB_VALUE_SEP);						
						for(String prefix: prefixList) {
							if(prefix == null || prefix.isEmpty()) {
								continue;
							}
							if(relay != null) {
								int index = relay.indexOf(A2PCustomerConfig.SUB_VALUE_SEP);
								String masterRelayShortName = relay;
								if(index >0) {
									masterRelayShortName = relay.substring(0, index);
								}
								String masterRelayString = getMixRouting(masterRelayShortName);																
								if (StringUtility.stringIsNotEmpty(masterRelayString)) {									
									msaterPrefixMap.put(prefix, masterRelayString);
									}								
								}
							prefixLists.add(prefix);																					
						}
					}
					
				} catch (Exception e) {// modified by Jianming in v1.0.1
					log.error("loadRoutingRelay Exception ignored:{}", e);
					continue;
				}
			}
			Collections.sort(prefixLists,  new Comparator (){
				 @Override
			     public int compare(Object o1, Object o2)
			    {
			           String prefix1= (String )o1;
			           String prefix2= (String )o2;

			           return prefix2.length()-prefix1.length();
			    }
			});
			List<Pattern>  regexs  = new ArrayList<Pattern>();
			constructPhonePrefixPattern(prefixLists, regexs);
            if (o_ssid<=0) {
            	routingDefaultRelay.put(shortName, msaterPrefixMap);
            	temp_defaultPrefixList.put(shortName, regexs);
			}else {
				routingRelay.put(routingKey+"_"+o_ssid, msaterPrefixMap);
				temp_customerPrefixList.put(routingKey+"_"+o_ssid, regexs);
			}
			
		}

	}
	
	private void doLoadSenderRoutingRelay(A2PCustomerConfig cfg, String sendRoutingKey,
			Map<String, Map<String,String>> routingRelay, 
			Map<String, List<Pattern>> temp_customerPrefixList, Map<String, Map<String,String>> temp_contentTemplateMap) throws Exception {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue(sendRoutingKey);
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			Map<String, String> msaterPrefixMap = new HashMap<String, String>();
			List<String> prefixLists = new ArrayList<String>();
			for (Group g : relays.getAllGroups()) {
				try {
					String prefixString = g.getAttr("PhonePrefix").getStringValue();
					String senderString = g.getAttr("Sender").getStringValue();
					String relay = g.getAttr("Relay").getStringValue();
					if (g.getAttr("ReplaceContentByTemplate")!=null) {	
						if (g.getAttr("IsReplaceTemplateBySender")!= null && "true".equalsIgnoreCase(g.getAttr("IsReplaceTemplateBySender").getStringValue())) {
							this.initContentTemplateId(o_ssid, prefixString, relay, g.getAttr("ReplaceContentByTemplate").getStringValue(), temp_contentTemplateMap,senderString);
						}else {
							this.initContentTemplateId(o_ssid, prefixString, relay, g.getAttr("ReplaceContentByTemplate").getStringValue(), temp_contentTemplateMap, null);
						}
						
					}
					if(prefixString!= null) {
						String [] prefixList = prefixString.split(A2PCustomerConfig.SUB_VALUE_SEP);
						
						for(String prefix: prefixList) {
							if(prefix == null || prefix.isEmpty()) {
								continue;
							}
							if(relay != null) {
								int index = relay.indexOf(A2PCustomerConfig.SUB_VALUE_SEP);
								String masterRelayShortName = relay;
								String slaveRelayShortName = "";
								if(index >0) {
									masterRelayShortName = relay.substring(0, index);
									slaveRelayShortName = relay.substring(index+1, relay.length());
								}
								//add mix feature; sub by 'a:20,b:30,c:50'
								String masterRelayString = getMixRouting(masterRelayShortName);																
								if (StringUtility.stringIsNotEmpty(masterRelayString)) {	
									String [] senderList = senderString.split(A2PCustomerConfig.SUB_VALUE_SEP);
									for (String sender : senderList) {
										msaterPrefixMap.put(sender+"_"+prefix, masterRelayString);
									}
									
								}							
							}
							String [] senderList = senderString.split(A2PCustomerConfig.SUB_VALUE_SEP);
							for (String sender : senderList) {
								prefixLists.add(sender+"_"+prefix);
							}							
						}
					}
					
				} catch (Exception e) {
					log.error("initSenderRoutingRelay Exception ignored:{}", g.toString());
					continue;
				}
			}
			Collections.sort(prefixLists,  new Comparator (){
				 @Override
			     public int compare(Object o1, Object o2)
			    {
			           String prefix1= (String )o1;
			           String prefix2= (String )o2;

			           return prefix2.length()-prefix1.length();
			    }
			});
			List<Pattern>  regexs  = new ArrayList<Pattern>();
			constructPhonePrefixPattern(prefixLists, regexs);
			if (o_ssid >0) {
				routingRelay.put(sendRoutingKey+"_"+o_ssid, msaterPrefixMap);				
				temp_customerPrefixList.put(sendRoutingKey+"_"+o_ssid, regexs);				
			}		
		}

	}
	
	private void initContentTemplateId(int ossid, String phonePrefixAttr, String rshortNameAttr, String templateId, 
			Map<String, Map<String,String>> temp_contentTemplateMap, String sender){
		String[] phonePrefixs = phonePrefixAttr.split(A2PCustomerConfig.SUB_VALUE_SEP);
		String[] rshortNames = rshortNameAttr.split(A2PCustomerConfig.VALUE_SEP);
		List<String> rssids = new ArrayList<String>();
		for (int i = 0; i < rshortNames.length; i++) {
			String rssid = getMixRouting(rshortNames[i]);
			if (rssid.contains(A2PCustomerConfig.ATTR_SEP)) {
				String[] rssidRate = rssid.split(A2PCustomerConfig.ATTR_SEP);
				rssids.add(rssidRate[0]);
			}else {
				rssids.add(rssid);
			}			
		}
		for (String rssid : rssids) {
			String contentTemplateKey = ossid+":"+rssid;
			if (sender!=null && !sender.isEmpty()) {
				String[]senderStrings = sender.split(";");
				for (int j = 0; j < senderStrings.length; j++) {
					contentTemplateKey = ossid+":"+rssid+":"+senderStrings[j];
					Map<String, String> countryTemplateIdMap = temp_contentTemplateMap.get(contentTemplateKey);
					if (countryTemplateIdMap == null) {
						countryTemplateIdMap = new HashMap<String, String>();
						temp_contentTemplateMap.put(contentTemplateKey, countryTemplateIdMap);
					}
					for (int i = 0; i < phonePrefixs.length; i++) {
						countryTemplateIdMap.put(phonePrefixs[i], templateId);
					}
				}								
			}else {
				Map<String, String> countryTemplateIdMap = temp_contentTemplateMap.get(contentTemplateKey);
				if (countryTemplateIdMap == null) {
					countryTemplateIdMap = new HashMap<String, String>();
					temp_contentTemplateMap.put(contentTemplateKey, countryTemplateIdMap);
				}
				for (int i = 0; i < phonePrefixs.length; i++) {
					countryTemplateIdMap.put(phonePrefixs[i], templateId);
				}
			}		
		}		
	}
	
	private void constructPhonePrefixPattern(List<String> prefixes,
			List<Pattern> list) {
		StringBuffer buffer = null;
		if (prefixes != null) {
			for (int i = 0; i < prefixes.size(); i++) {
				if (i % 99 == 0) {
					buffer = new StringBuffer();
					buffer.append(prefixes.get(i));
				} else if ((i + 1) % 99 == 0 || (i + 1) == prefixes.size()) {
					buffer.append("|");
					buffer.append(prefixes.get(i));
					String regular = buffer.toString();
					Pattern p = Pattern.compile(regular, Pattern.CASE_INSENSITIVE);
					list.add(p);
					buffer = null;

				} else {
					buffer.append("|");
					buffer.append(prefixes.get(i));
				}
			}
			if (buffer != null) {
				String regular = buffer.toString();
				//if (log.isInfoEnabled()) {
					// log.info("ssid:{} create PhonePrefix regular:{}",ssid,regular);
				//}
				Pattern p = Pattern.compile(regular, Pattern.CASE_INSENSITIVE);
				list.add(p);
			}
		}
	}
	
	
	private void constructSenderListPattern(List<String> prefixes,
			List<Pattern> list) {
		StringBuffer buffer = null;
		String spString = "$^()[]{}*+?.\\";
		if (prefixes != null) {
			for (int i = 0; i < prefixes.size(); i++) {
				String exp = prefixes.get(i);
				StringBuilder sb = new StringBuilder();
		        for(int j=0; j<exp.length();j++){
		        	String ch = exp.substring(j, j+1);
		        	if(spString.contains(ch)){
		        		sb.append("\\").append(ch);
		        	}else{
		        		sb.append(ch);
		        	}
		        }
				if (i % 50 == 0) {
					buffer = new StringBuffer();
					buffer.append(sb);
				} else if ((i + 1) % 50 == 0 || (i + 1) == prefixes.size()) {
					buffer.append("|");
					buffer.append(sb);
					String regular = buffer.toString();
					Pattern p = Pattern.compile(regular);
					list.add(p);
					buffer = null;

				} else {
					buffer.append("|");
					buffer.append(sb);
				}
			}
			if (buffer != null) {
				String regular = buffer.toString();
				//if (log.isInfoEnabled()) {
					// log.info("ssid:{} create PhonePrefix regular:{}",ssid,regular);
				//}
				Pattern p = Pattern.compile(regular);
				list.add(p);
			}
		}
	}
	

	@SuppressWarnings("unchecked")
	private void initRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadRoutingRelay(cfg, A2P_ROUTING_RELAY,
					customerRoutingRelayInfo, 
					customerPerfixInfo, defaultRoutingRelayInfo,
					customerDefaultPerfixInfo,
					contentTemplateMap);
		} catch (Exception e) {
			log.error("init routing relay error", e);
		}
		
		for (int i = 0; i < 20; i++) {
			String key = A2P_BACKUP_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			try {
				doLoadRoutingRelay(cfg, key,
						customerBackupRoutingRelayInfo, 
						customerBackupPerfixInfo, defaultBackupRoutingRelayInfo,
						customerBackupDefaultPerfixInfo,
						contentTemplateMap);					
			} catch (Exception e) {
				log.error("init backup routing relay error", e);
			}
		}					
	}
	
	private void initSenderRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadSenderRoutingRelay(cfg, A2P_SENDER_ROUTING_RELAY, customerSenderRoutingRelayInfo, 
					customerSenderPerfixInfo, contentTemplateMap);
		} catch (Exception e) {
			log.error("init sender routing relay error", e);
		}
		for (int i = 0; i < 20; i++) {
			String key = A2P_BACKUP_SENDER_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			try {							
				doLoadSenderRoutingRelay(cfg, key, customerBackupSenderRoutingRelayInfo, 
						customerBackupSenderPerfixInfo, contentTemplateMap);
			} catch (Exception e) {
				log.error("init backup sender routing relay error", e);
			}
		}				
				
	}
	
	private void initContentKeywordRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadCustomerContentKeywordRoutingRelay(cfg, A2P_CONTENT_KEYWORD_ROUTING_RELAY, contentTemplateMap,
					customerContentKeywordRoutingRelayInfo,customerContentKeywordPerfixInfo);
		} catch (Exception e) {
			log.error("init content routing relay error", e);
		}
		
		for (int i = 0; i < 20; i++) {
			String key = A2P_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			try {
				doLoadCustomerContentKeywordRoutingRelay(cfg, key, contentTemplateMap,
						customerBackupContentKeywordRoutingRelayInfo,customerBackupContentKeywordPerfixInfo);		
			} catch (Exception e) {
				log.error("init backup content routing relay error", e);
			}
		}				
	}

	@SuppressWarnings("unchecked")
	private void initSecRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadRoutingRelay(cfg, A2P_SEC_ROUTING_RELAY,
					customerRoutingRelayInfo2, 
					customerPerfixInfo2, defaultRoutingRelayInfo,
					customerDefaultPerfixInfo,
					contentTemplateMap);
		} catch (Exception e) {
			log.error("init 2-time routing relay error", e);
		}
		
		for (int i = 0; i < 20; i++) {
			String key = A2P_SEC_BACKUP_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			try {
				doLoadRoutingRelay(cfg, key,
						customerBackupRoutingRelayInfo2, 
						customerBackupPerfixInfo2, defaultBackupRoutingRelayInfo,
						customerBackupDefaultPerfixInfo,
						contentTemplateMap);					
			} catch (Exception e) {
				log.error("init 2-time backup routing relay error", e);
			}
		}					
	}
	
	private void initSecSenderRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadSenderRoutingRelay(cfg, A2P_SEC_SENDER_ROUTING_RELAY, customerSenderRoutingRelayInfo2, 
					customerSenderPerfixInfo2, contentTemplateMap);
		} catch (Exception e) {
			log.error("init 2-time sender routing relay error", e);
		}
		for (int i = 0; i < 20; i++) {
			String key = A2P_SEC_BACKUP_SENDER_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			try {							
				doLoadSenderRoutingRelay(cfg, key, customerBackupSenderRoutingRelayInfo2, 
						customerBackupSenderPerfixInfo2, contentTemplateMap);
			} catch (Exception e) {
				log.error("init 2-time backup sender routing relay error", e);
			}
		}				
				
	}
	
	private void initSecContentKeywordRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadCustomerContentKeywordRoutingRelay(cfg, A2P_SEC_CONTENT_KEYWORD_ROUTING_RELAY, contentTemplateMap,
					customerContentKeywordRoutingRelayInfo2,customerContentKeywordPerfixInfo2);
		} catch (Exception e) {
			log.error("init 2-time content routing relay error", e);
		}
		
		for (int i = 0; i < 20; i++) {
			String key = A2P_SEC_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			try {
				doLoadCustomerContentKeywordRoutingRelay(cfg, key, contentTemplateMap,
						customerBackupContentKeywordRoutingRelayInfo2,customerBackupContentKeywordPerfixInfo2);		
			} catch (Exception e) {
				log.error("init 2-time backup content routing relay error", e);
			}
		}				
	}
	
	//init number routing
		private void initNumberRoutingRelay(A2PCustomerConfig cfg) throws Exception {
			try {
				doLoadNumberRoutingRelay(cfg, A2P_NUMBER_ROUTING_RELAY,
						customerNumberRoutingRelayInfo);
			} catch (Exception e) {
				log.error("reload number routing error", e);
			}
											
		}
	
	//init op routing
	private void initOPRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		
		
		try {
			doLoadOPRoutingRelay(cfg, A2P_OP_ROUTING_RELAY,
					customeropRoutingRelayInfo);
		} catch (Exception e) {
			log.error("reload operator routing error", e);
		}
		
		for (int i = 0; i < 20; i++) {
			String key = A2P_OP_BACKUP_ROUTING_RELAY;
			if (i > 0) {
				key = key + i;
			}
			try {
				doLoadOPRoutingRelay(cfg, key,
						customeropBackupRoutingRelayInfo);
			} catch (Exception e) {
				log.error("reload operator backup routing error", e);
			}
		}									
	}
	
	private void initOPSenderRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadOPSenderRoutingRelay(cfg, A2P_OP_SENDER_ROUTING_RELAY,
					customeropSenderRoutingRelayInfo);
		} catch (Exception e) {
			log.error("reload operator sender routing error", e);
		}
		
		for (int i = 0; i < 20; i++) {
			String key = A2P_OP_BACKUP_SENDER_ROUTING_RELAY;
			if (i > 0) {
				key = key + i;
			}
			try {
				doLoadOPSenderRoutingRelay(cfg, key,
						customeropBackupSenderRoutingRelayInfo);
			} catch (Exception e) {
				log.error("reload operator backupsender routing error", e);
			}
		}					
	}
	private void initOPContentRoutingRelay(A2PCustomerConfig cfg) throws Exception {
		try {
			doLoadCustomerOPContentKeywordRoutingRelay(cfg, A2P_OP_CONTENT_KEYWORD_ROUTING_RELAY,
					customeropContentKeywordRoutingRelayInfo);
		} catch (Exception e) {
			log.error("reload operator contentkeyword routing error", e);
		}
		
		for (int i = 0; i < 20; i++) {
			String key = A2P_OP_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
			if (i > 0) {
				key = key + i;
			}
			try {
				doLoadCustomerOPContentKeywordRoutingRelay(cfg, key,
						customeropBackupContentKeywordRoutingRelayInfo);
			} catch (Exception e) {
				log.error("reload operator backupcontentkeyword routing error", e);
			}
		}		
	}
	
	
	private void doLoadCustomerContentKeywordRoutingRelay(A2PCustomerConfig cfg, String contentRelayKey,
			Map<String, Map<String, String>> tempContentTemplateMap, 
			Map<String, Map<String,Map<String, String>>> routingRelay,
			Map<String, List<Pattern>> temp_customerPrefixList) {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue(contentRelayKey);
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			Map<String, Map<String, String>> msaterPrefixMap = new HashMap<String, Map<String, String>>();
			Map<String, Map<String, String>> slavePrefixMap = new HashMap<String, Map<String, String>>();
			List<String> prefixLists = new ArrayList<String>();
			for (Group g : relays.getAllGroups()) {
				try {
					String prefixString = g.getAttr("PhonePrefix").getStringValue();
					String keywordString = g.getAttr("KeywordId").getStringValue();
					String relay = g.getAttr("Relay").getStringValue();
					if (g.getAttr("ReplaceContentByTemplate")!=null) {						
						this.initContentTemplateId(o_ssid, prefixString, relay, g.getAttr("ReplaceContentByTemplate").getStringValue(), tempContentTemplateMap, null);
					}
					if(prefixString!= null) {
						String [] prefixList = prefixString.split(A2PCustomerConfig.SUB_VALUE_SEP);
						
						for(String prefix: prefixList) {
							if(prefix == null || prefix.isEmpty()) {
								continue;
							}
							if(relay != null) {
								int index = relay.indexOf(A2PCustomerConfig.SUB_VALUE_SEP);
								String masterRelayShortName = relay;
								String slaveRelayShortName = "";
								if(index >0) {
									masterRelayShortName = relay.substring(0, index);
									slaveRelayShortName = relay.substring(index+1, relay.length());
								}
								//add mix feature; sub by 'a:20,b:30,c:50'
								String masterRelayString = getMixRouting(masterRelayShortName);																
								if (StringUtility.stringIsNotEmpty(masterRelayString)) {									
									Map<String, String> kMap = msaterPrefixMap.get(prefix);
									if (kMap == null) {
										kMap = new HashMap<String, String>();
										msaterPrefixMap.put(prefix, kMap);
									}											
									kMap.put(keywordString, masterRelayString);
								}								
							}
							prefixLists.add(prefix);							
						}
					}
					
				} catch (Exception e) {
					log.error("initContentKeywordRoutingRelay Exception ignored:{}", g.getAllAttrs());
					continue;
				}
			}
			Collections.sort(prefixLists,  new Comparator (){
				 @Override
			     public int compare(Object o1, Object o2)
			    {
			           String prefix1= (String )o1;
			           String prefix2= (String )o2;

			           return prefix2.length()-prefix1.length();
			    }
			});
			List<Pattern>  regexs  = new ArrayList<Pattern>();
			constructPhonePrefixPattern(prefixLists, regexs);
			if (o_ssid >0) {
				routingRelay.put(contentRelayKey+"_"+o_ssid, msaterPrefixMap);				
				temp_customerPrefixList.put(contentRelayKey+"_"+o_ssid, regexs);				
			}		
		}
	}
	
	
	
	
	private String getMixRouting(String relay){
		//add mix feature; sub by 'a:20,b:30,c:50'
		StringBuilder routingValue = new StringBuilder();
		int index = relay.indexOf(A2PCustomerConfig.VALUE_SEP);
		if (index >0) {
			String[] mixRelay = relay.split(A2PCustomerConfig.VALUE_SEP);
			for (int i = 0; i < mixRelay.length; i++) {
				if (mixRelay[i].contains(A2PCustomerConfig.ATTR_SEP)) {
					String mixRateRelay[] = mixRelay[i].split(A2PCustomerConfig.ATTR_SEP);
					int ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(mixRateRelay[0]));
					routingValue.append(ssid).append(A2PCustomerConfig.ATTR_SEP).append(mixRateRelay[1])
					.append(A2PCustomerConfig.VALUE_SEP);
				}else {
					int master_relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(mixRelay[i]));
					routingValue.append(master_relay_ssid).append(A2PCustomerConfig.VALUE_SEP);
				}				
			}
		}else {
			if (relay.contains(A2PCustomerConfig.ATTR_SEP)) {
				String mixRateRelay[] = relay.split(A2PCustomerConfig.ATTR_SEP);
				int ssid = this
						.getSSidbyCustomerNameshort(getShortNameByValue(mixRateRelay[0]));
				routingValue.append(ssid).append(A2PCustomerConfig.ATTR_SEP).append(mixRateRelay[1])
				.append(A2PCustomerConfig.VALUE_SEP);
			}else {
				int master_relay_ssid = this
						.getSSidbyCustomerNameshort(getShortNameByValue(relay));
				routingValue.append(master_relay_ssid).append(A2PCustomerConfig.VALUE_SEP);
			}			
		}		
		return routingValue.substring(0, routingValue.length()-1);
		
	}

	private void initServiceTypeIDRelay(A2PCustomerConfig cfg,
			Map<String, Integer> serviceTypeIDRelayMap) throws Exception {
		A2PCustomerMultiValue relays = cfg
				.parseMultiValue("A2PServiceTypeIDRoutingRelay");
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			for (Group g : relays.getAllGroups()) {
				try {
					String rop = g.getAttr("R_OP").getStringValue();
					int rop_ssid = this.getSSidbyCustomerNameshort(getShortNameByValue(rop));

					int serviceTypeID = Integer.parseInt(g.getAttr("ServiceTypeID").getStringValue());

					String relay = g.getAttr("Relay").getStringValue();
					int relay_ssid = this.getSSidbyCustomerNameshort(getShortNameByValue(relay));

					if (rop_ssid > 0 && relay_ssid > 0) {
						String key = o_ssid + CONF_SEPERATOR + rop_ssid + CONF_SEPERATOR + serviceTypeID;
						serviceTypeIDRelayMap.put(key, relay_ssid);
					}

				} catch (Exception e) {// 
					log.warn("Init A2PServiceTypeIDRoutingRelay error, ssid={} {} Ignored {} {}",
									o_ssid, cfg.getShortName(), g.toString(), e);
					continue;
				}
			}
		}
	}

	private void initRequiredCharsets(A2PCustomerConfig cfg) {
		A2PCustomerMultiValue value = cfg.parseMultiValue("RequiredCharsets");
		if (value != null) {
			int r_ssid = cfg.getSSID();
			for (Group g : value.getAllGroups()) {
				String r_relay = g.getAttr("R_RELAY").getStringValue();
				int o_ssid = this.getSSidbyCustomerNameshort(getShortNameByValue(r_relay));
				String charsets = g.getAttr("Charset").getStringValue();
				if (o_ssid > 0 && charsets != null) {
					this.opRequiredCharsets.put(o_ssid + "_" + r_ssid, Arrays
							.asList(charsets.split(",")));
					if (log.isTraceEnabled()) {
						log.trace("opRequiredCharsets.put: {}_{},{}", o_ssid,
								r_ssid, charsets);
					}

				}
			}
		}
	}

	private void clearRequiredCharsets(A2PCustomerConfig cfg) {
		A2PCustomerMultiValue value = cfg.parseMultiValue("RequiredCharsets");
		if (value != null) {
			int r_ssid = cfg.getSSID();
			for (Group g : value.getAllGroups()) {
				String r_relay = g.getAttr("R_RELAY").getStringValue();
				int o_ssid = this.getSSidbyCustomerNameshort(getShortNameByValue(r_relay));
				String charsets = g.getAttr("Charset").getStringValue();
				if (o_ssid > 0 && charsets != null) {
					this.opRequiredCharsets.remove(o_ssid + "_" + r_ssid);
					if (log.isTraceEnabled()) {
						log.trace("opRequiredCharsets.remove: {}_{}", o_ssid,
								r_ssid);
					}

				}
			}
		}
	}

	public int getDRMode(int ssid) {
		try {
			if (deliveryreportMode.containsKey(ssid)) {
				return deliveryreportMode.get(ssid);
			} else {
				return -1;
			}
		} catch (Exception e) {
			log.info("Failed to getDRMode for SSID {}", ssid);
			return -1;
		}
	}

	private void initDRModeInfo(A2PCustomerConfig cfg) {
		int t = cfg.getInt("DeliveryReportMode", -1);
		if (t > -1)
			deliveryreportMode.put(cfg.getSSID(), t);
	}

	private void clearDRModeInfo(A2PCustomerConfig cfg) {
		int t = cfg.getInt("DeliveryReportMode", -1);
		if (t > -1)
			deliveryreportMode.remove(cfg.getSSID());
	}

	private void initCustomerLocalDNSPrefix(A2PCustomerConfig cfg) {
		String dns = cfg.getString("LocalDNSPrefix");
		if (dns != null) {
			String[] prefixes = dns.split(",");
			this.customerLocalDNSPrefix.put(cfg.getSSID(), prefixes);
		}
	}

	private void clearCustomerLocalDNSPrefix(A2PCustomerConfig cfg) {
		String dns = cfg.getString("LocalDNSPrefix");
		if (dns != null) {
			String[] prefixes = dns.split(",");
			this.customerLocalDNSPrefix.remove(cfg.getSSID());
		}
	}

	private void initContentScan(A2PCustomerConfig cfg) {

		int ossid = cfg.getSSID();
		A2PCustomerMultiValue v = cfg.parseMultiValue("ContentScan");
		if (v != null) {

			for (Group g : v.getAllGroups()) {
				String rPrefix = g.getAttr("Prefix").getStringValue();
				String oCharset = g.getAttr("Charset").getStringValue();
				String charset = g.getAttr("Except").getStringValue();
				ContentScan cs = new ContentScan(oCharset, rPrefix, charset);

				ArrayList<ContentScan> contentScan;
				if (contentScanInfo != null
						&& contentScanInfo.get(ossid) != null) {
					contentScan = contentScanInfo.get(ossid);
					contentScan.add(cs);
				} else {
					contentScan = new ArrayList<ContentScan>();
					contentScan.add(cs);
				}
				contentScanInfo.put(ossid, contentScan);
			}
		}
	}

	private void clearContentScan(A2PCustomerConfig cfg) {
		int ossid = cfg.getSSID();
		A2PCustomerMultiValue v = cfg.parseMultiValue("ContentScan");
		if (v != null) {
			contentScanInfo.remove(ossid);
		}
	}

	public A2PCustomerInfo getCustomerByServerID(String serverId) {
		return serverid_cust_map.get(serverId);
	}

	public A2PCustomerInfo getCustomerBySSID(int ssid) {
		return ssid_cust_map.get(ssid);
	}

	public A2PCustomerInfo getCustomerBySpID(String spid) {
		return spid_cust_map.get(spid);
	}

	public String getIpByConnection(String connectionID) {
		return connectionIPMap.get(connectionID);
	}

	public void addServerInfoMap(ConnectionInfo connInfo) {
		if (connInfo == null)
			return;
		String username = connInfo.getUserName();
		String password = connInfo.getPassword();
		String unionKey = username + "|" + password;
		add2ServerInfoMap(connInfo, username);
		add2ServerInfoMap(connInfo, unionKey);
	}

	// only used by addServerInfoMap()
	private void add2ServerInfoMap(ConnectionInfo connInfo, String key) {
		ArrayList list = null;
		if (serverInfoMap.get(key) != null) {
			list = serverInfoMap.get(key);
			list.add(connInfo);
		} else {
			list = new ArrayList();
			list.add(connInfo);
		}

		serverInfoMap.put(key, list);
	}

	public void clearServerInfoMap(ConnectionInfo connInfo) {
		if (connInfo == null)
			return;
		String username = connInfo.getUserName();
		String password = connInfo.getPassword();
		String unionKey = username + "|" + password;
		clear4ServerInfoMap(connInfo, username);
		clear4ServerInfoMap(connInfo, unionKey);
	}

	// only used by clearServerInfoMap()
	private void clear4ServerInfoMap(ConnectionInfo connInfo, String key) {
		if (serverInfoMap.get(key) != null) {
			ArrayList<ConnectionInfo> list = serverInfoMap.get(key);
			if (list == null) {
				return;
			}
			list.remove(connInfo);
		}
	}

	// Need implements in cm.cfg, currently not implement it.
	public int getSsidByMmsDomain(String domain) {
		int ssid = -1;
		return ssid;
	}

	// Need implements in cm.cfg, currently not implement it.
	public String getMmsDomainBySsid(int ssid) {
		String domain = null;
		return domain;
	}

	// maybe useless
	public int getSsidByShortcode(String shortCode) {
		return -1;
	}

	public void clearCache() {
		// confBlockList.clear();
		routingInfoMap.clear();
	}

	// Current useless
	private ArrayList<Integer> getAllSsIdForIOMMS() {
		return null;
	}

	public int getVPConnectingRelaySsid(int ssid) {
		try {
			if (this.vpConnectRelayinfo.containsKey(ssid)) {
				return this.vpConnectRelayinfo.get(ssid);
			} else {
				return -1;
			}
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("Failed to getVPConnectingRelaySsid({})", ssid);
			}

			return -1;
		}
	}

	public String[] getMncMccbySsid(int ssid) {
		if (ssidOfMncMcc.containsKey(ssid))
			return ssidOfMncMcc.get(ssid);
		return null;
	}

	public int getSmsSsidByMncMcc(String mncmcc) {
		try {
			if (smsMncMcc.containsKey(mncmcc)) {
				return smsMncMcc.get(mncmcc);
			} else {
				return -1;
			}
		} catch (Exception e) {
			log.warn("Can't get the operator for MNC/MCC:{}", mncmcc);
			return -1;
		}
	}

	// Priority
	public int getConnectedRelay(int ssid, String gmmsMsgType) {
		try {
			return smsConnectRelayinfo.get(ssid);
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("Failed to get getConnectedRelay by ({},{}", ssid,
						gmmsMsgType);
			}

		}
		return 0;
	}

	public boolean isA2P(int ssid) {
		return a2pInfo.contains(ssid);
	}

	public boolean isChannel(Integer ssid) {
		return channelInfo.contains(ssid);
	}

	public boolean isHub(Integer ssid) {
		return hubInfo.containsKey(ssid);
	}

	public boolean isPartition(Integer ssid) {
		return partitionInfo.contains(ssid);
	}

	public boolean isOperator(Integer ssid) {
		return operatorInfo.containsKey(ssid);
	}

	public boolean isVirtualOperator(Integer ssid) {
		return virtualOperatorInfo.contains(ssid);
	}

	public boolean isNotDRSupport(int ssid) {
		return rSsidsNotSupportDR.contains(ssid);
	}

	public String getNoDRRssidsString() {
		if (rSsidsNotSupportDR == null || rSsidsNotSupportDR.size() == 0)
			return "0";
		String text = rSsidsNotSupportDR.toString();
		return text.substring(1, text.length() - 1);
	}

	public ArrayList getNoticeInfo() {
		return this.noticeInfo;
	}

	public int getPeeringTcpVersion() {
		return peeringTcpVersion;
	}

	public List<String> getRequiredCharsets(int rssid, int rop) {
		return opRequiredCharsets.get(rssid + "_" + rop);
	}

	public boolean isRssidNotSupportDR(int rSsID) {
		return rSsidsNotSupportDR.contains(rSsID);
	}

	public void setBlackList(BlackList bl) {
		this.blacklist = bl;
	}

	public BlackList getBlackList() {
		return this.blacklist;
	}

	public int getTransparency(int ssid) {
		if (this.isHub(ssid)) {
			return hubInfo.get(ssid);
		}
		if (this.isOperator(ssid)) {
			return operatorInfo.get(ssid);
		}
		return 0;
	}

	public boolean checkRecNumberLen(int ssid, int numbLen) {
		A2PCustomerInfo cst = null;
		cst = getCustomerBySSID(ssid);

		if (cst == null) {
			return false;
		}
		ArrayList<Integer> numLen;
		numLen = cst.getRecNumberLens();
		if (numLen == null || numLen.isEmpty() || numLen.contains(numbLen)) {
			return true;
		}
		return false;
	}

	public boolean checkOriNumberLen(int ssid, int numbLen) {
		A2PCustomerInfo cst = null;
		cst = getCustomerBySSID(ssid);

		if (cst == null) {
			return false;
		}
		ArrayList<Integer> numLen;
		numLen = cst.getOriNumberLens();
		if (numLen == null || numLen.isEmpty() || numLen.contains(numbLen)) {
			return true;
		}
		return false;
	}

	public boolean checkNumberLen(int ssid, int numbLen) {
		return checkRecNumberLen(ssid, numbLen);
	}

	public int getCharsetRelay(int oop, int ossid, int rop, String charset) {
		try {
			if (charset != null && !"".equals(charset)) {
				charset = charset.trim().toLowerCase();
				String item = oop + "_" + rop + "_" + charset;
				if (charsetRelay.containsKey(item)) {
					return charsetRelay.get(item);
				}
				item = ossid + "_" + rop + "_" + charset;
				if (charsetRelay.containsKey(item)) {
					return charsetRelay.get(item);
				}
			}
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("Failed to getCharsetRelay({},{},{},{}", oop, ossid,
						rop, charset);
			}

		}
		return -1;
	}

	public Map<Integer, String[]> getCustomerLocalDNSPrefix() {
		return customerLocalDNSPrefix;
	}

	public void setCustomerLocalDNSPrefix(
			ConcurrentHashMap<Integer, String[]> customerLocalDNSPrefix) {
		this.customerLocalDNSPrefix = customerLocalDNSPrefix;
	}

	public void setContentScanInfo(ConcurrentHashMap contentScanInfo) {
		this.contentScanInfo = contentScanInfo;
	}

	public Map<Integer, ArrayList<ContentScan>> getContentScanInfo() {
		return contentScanInfo;
	}

	/**
	 * 
	 * @param CustomerNameshort
	 *            String
	 * @return int added by cassie
	 */
	public int getSSidbyCustomerNameshort(String customerNameshort) {
		try {
			if (this.shortName_ssid_map.containsKey(customerNameshort)) {
				return this.shortName_ssid_map.get(customerNameshort).intValue();
			} else {
				return -1;
			}
		} catch (Exception e) {
			log.info("Failed to getSSidbyCustomerNameshort by {}",
					customerNameshort);
			return -1;
		}
	}

	public int getCustIdbyCustomerNameshort(String customerNameshort) {
		try {
			if (this.shortName_custId_map.containsKey(customerNameshort)) {
				return this.shortName_custId_map.get(customerNameshort).intValue();
			} else {
				return -1;
			}
		} catch (Exception e) {
			if (log.isInfoEnabled()) {
				log.info("Failed to getCustIdbyCustomerNameshort by {}",
						customerNameshort);
			}

			return -1;
		}
	}

	public String getShortNamebySSID(int ssid) {
		A2PCustomerInfo cst = this.ssid_cust_map.get(ssid);
		if (cst != null)
			return cst.getShortName();

		if (log.isInfoEnabled()) {
			log.info("Failed to getShortNamebySSID by {}", ssid);
		}

		return null;
	}

	public ArrayList<Integer> getSsidByProtocol(String protocol) {
		if (protocol == null || protocol.trim().length() == 0)
			return null;

		return this.protocol_ssid_map.get(protocol.trim().toLowerCase());
	}

	public ArrayList<Integer> getSsidByQueryMsg() {
		return this.queryMsg_ssid_list;
	}

	public String getNodeIDByConnectionID(String connectionID) {
		return connectionNodeMap.get(connectionID);
	}

	public ArrayList getConnectionInfobyServerInfo(String username,
			String password, String ip) {
		ArrayList resultList = new ArrayList();
		if (username == null) {
			return null;
		}

		if (password != null) {
			String temp = username + "|" + password;
			resultList = serverInfoMap.get(temp);
		} else {
			resultList = serverInfoMap.get(username);
		}

		return checkIPAddress(resultList, ip);
	}

	// check the IP address of incoming connections.
	private ArrayList checkIPAddress(ArrayList<ConnectionInfo> list, String ip) {
		if (list == null || list.size() == 0) {
			return null;
		}
		if ((list != null && list.size() == 1) || (ip == null)) {
			return list;
		}
		ArrayList connList = new ArrayList();
		for (ConnectionInfo connInfo : list) {
			String url = connInfo.getURL();
			if (ip.equalsIgnoreCase(url)) {
				connList.add(connInfo);
			}
		}
		return connList;
	}

	public boolean isSupportExpiredDR(int ssid) {
		return this.isSupportExpiredDR.contains(ssid);
	}

	public String getServiceNameBySsid(int oSsid) {
		return ssid_serviceType.get(oSsid);
	}

	public boolean isNotSupportUDH(int ssid) {
		return this.rSsidNotSupportUDH.contains(ssid);
	}

	public boolean isSupport7Bit(int ssid) {
		return this.rSsidSupport7Bit.contains(ssid);
	}

	public A2PCustomerInfo[] getInitServers() {
		return (A2PCustomerInfo[]) initServers.toArray(new A2PCustomerInfo[0]);
	}

	public Map<String, Integer> getAllShortcode() {
		return this.shortcode_ssid_map;
	}

	public boolean isShortcode(String address) {
		for (String shortCode : shortcodeInfo) {
			if (address.startsWith(shortCode)) {
				return true;
			}
		}
		return false;
	}

	public synchronized void addCustomerInfoBySsid(int ssid,
			A2PCustomerInfo cusInfo) {
		ssid_cust_map.put(ssid, cusInfo);
	}

	public synchronized void clearCustomerInfoBySsid(int ssid) {
		ssid_cust_map.remove(ssid);
	}

	public boolean isPolicyControlPermit(GmmsMessage message) {
		if (!blacklist.allowWhenReceived(message)
				|| !blacklist.allowWhenRouting(message)) {
			return false;
		}

		int oop = message.getOoperator();
		int ossid = message.getOSsID();
		int rop = message.getRoperator();

		if (this.oWhitelistInfo.contains(oop)
				|| this.rWhitelistInfo.contains(rop)) {
			return true;
		}

		boolean permit = false;
		String ossid_rop = ossid + ":" + rop;
		String oop_rop = oop + ":" + rop;
		if (policyInfo.containsKey(ossid_rop)) {
			if (policyInfo.get(ossid_rop)) {
				permit = true;
			} else {
				return false;
			}
		}

		if (policyInfo.containsKey(oop_rop)) {
			return policyInfo.get(oop_rop);
		} else {
			return permit;
		}
	}
	
	public boolean whitleListCheck(GmmsMessage message) {
		if (whitelist.allowWhenRouting(message)) {
			return true;
		}
		return false;
	}
	
	public void test() {
		A2PSingleConnectionInfo cust = (A2PSingleConnectionInfo) this
				.getCustomerBySSID(489);
		System.out.println(cust.getPriorityFlag());

		// int ssid = getSmsSsidByMncMcc("001_321");
		// System.out.println(ssid);
		// System.out.println(this.opRequiredCharsets.get("1071_1232"));
		// MultiNodeCustomerInfo cust =
		// (MultiNodeCustomerInfo)this.getCustomerBySSID(851);
		// Map m = cust.getNodeMap();
		// NodeInfo nodeInfo = (NodeInfo)m.get("CMCC_BJ");
		// nodeInfo.print();
		//
		// nodeInfo = (NodeInfo)m.get("CMCC_GD");
		// nodeInfo.print();
		//
		// Iterator it = this.serverInfoMap.entrySet().iterator();
		// Map.Entry entry = null;
		// String key = null;
		// ArrayList<ConnectionInfo> conns = null;
		// while(it.hasNext()){
		// entry = (Map.Entry)it.next();
		// key = entry.getKey().toString();
		// conns = (ArrayList)entry.getValue();
		// if(conns == null){
		// System.out.println("No conns for " + key);
		// }
		// String t = null;
		// for(ConnectionInfo conInfo : conns){
		// System.out.println("key:" + key + "; connName:" +
		// conInfo.getConnectionName());
		// }
		// }
	}

	public boolean isInStoreMode(int ssid) {
		return ssidsInStoreMode.contains(ssid);
	}

	public boolean isInRopBusyMode(int ssid) {
		return ssidsInRopBusyMode.contains(ssid);
	}

	public boolean isInRopFailedMode(int ssid) {
		return ssidsInRopFailedMode.contains(ssid);
	}

	public boolean isInDRStoreMode(int ssid) {
		return this.ssidsInDRStoreMode.contains(ssid);
	}

	class ConfigMonitor implements Runnable {
		String configFileName = System.getProperty("a2p_home")
				+ "conf/OperatorMode.properties";
		public long lastModifiedTime = -1;

		public void run() {
			File aFile = null;
			while (true) {
				aFile = new File(configFileName);
				if (aFile.exists()) {
					if (this.lastModifiedTime != aFile.lastModified()) {
						log
								.info("Operator Mode configuration file changed, reload");
						loadOperatorMode();
						lastModifiedTime = aFile.lastModified();
					}
				} else {
					try {
						log.warn("Operator Mode configuration file not exist!");
						aFile.createNewFile();
						lastModifiedTime = aFile.lastModified();
					} catch (IOException e) {
						log.error(e, e);
					}
				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
				}
			}
		}

		private boolean loadOperatorMode() {// modified by Jianming in v1.0.1
			try {
				Properties properties = new Properties();
				FileInputStream fis = new FileInputStream(configFileName);
				properties.load(fis);
				fis.close();
				ssidsInStoreMode.clear();
				String storeModeOp = properties.getProperty("StoreMode");
				trackModeOp(storeModeOp, ssidsInStoreMode);

				ssidsInDRStoreMode.clear();
				String drStoreModeOp = properties.getProperty("DRStoreMode");
				trackModeOp(drStoreModeOp, ssidsInDRStoreMode);

				ssidsInRopBusyMode.clear();
				String ROP_BusyModeOp = properties.getProperty("ROP-BusyMode");
				trackModeOp(ROP_BusyModeOp, ssidsInRopBusyMode);

				ssidsInRopFailedMode.clear();
				String ROP_FailedModeOp = properties
						.getProperty("ROP-FailedMode");
				trackModeOp(ROP_FailedModeOp, ssidsInRopFailedMode);
				return true;
			} catch (Exception e) {
				log.error("Load OperatorMode configuration file failed", e);
			}
			return false;
		}
	}

	// added by Jianming in v1.0.1
	private void trackModeOp(String modeOp, Set ssidsSet) {
		if (modeOp != null && !"".equalsIgnoreCase(modeOp)) {
			String[] ops = modeOp.split(",");
			for (String opShortName : ops) {
				if (opShortName != null && !"".equalsIgnoreCase(opShortName)) {
					try {
						int opSsid = Integer.parseInt(opShortName.trim());
						ssidsSet.add(opSsid);
						log.info("add mode: {}", opSsid);
					} catch (Exception e) {
						log.error(
								"Integer parse error for modeOp:{},ignore it.",
								modeOp);
						continue;
					}
				}
			}
		}
	}

	

	public int getIosmsSsidBySsid(int ssid) {
		A2PCustomerInfo cstInfo = getCustomerBySSID(ssid);
		if (cstInfo != null) {
			return cstInfo.getIosmsSsid();
		}
		return -1;
	}

	public int getSsidByIosmsSsid(int iosmsSsid) {
		if (this.iosmsSsid_ssid_map.containsKey(iosmsSsid)) {
			return this.iosmsSsid_ssid_map.get(iosmsSsid);
		}

		return -1;
	}

	public int getSsidByCustID(int custID) {
		if (this.custId_ssid_map.containsKey(custID)) {
			return this.custId_ssid_map.get(custID);
		}

		return -1;
	}

	public void setContentTpl(ContentTemplate contentTpl) {
		this.contentTpl = contentTpl;
	}

	public ContentTemplate getContentTpl() {
		return contentTpl;
	}

	private void initContentWhiteList(A2PCustomerConfig cfg,
			Map<String, List<String>> tmpWhiteListInfo, boolean isReload) {
		A2PCustomerMultiValue relays = cfg
				.parseMultiValue("A2PSMSContentWhitelist");
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			for (Group g : relays.getAllGroups()) {
				try {
					String o_addr = null;
					if (g.getAttr("O_Addr") != null) {
						o_addr = g.getAttr("O_Addr").getStringValue();
					}

					String contentTplID = g.getAttr("contentTmpl")
							.getStringValue();

					if (contentTplID != null && contentTplID.length() > 0) {
						if (o_addr != null && o_addr.length() > 0) {
							String keyStr = o_ssid + "_" + o_addr;
							initContentWhiteList(tmpWhiteListInfo, isReload,
									contentTplID, keyStr);

						} else {
							String keyStr = o_ssid + "";
							initContentWhiteList(tmpWhiteListInfo, isReload,
									contentTplID, keyStr);
						}
					}
				} catch (Exception e) {
					log
							.warn(
									"Init routingInfo content whitelist error, ssid={} {} Ignored {} {}",
									o_ssid, cfg.getShortName(), g.toString(), e);
					continue;
				}
			}
		}
	}

	private void initContentWhiteList(
			Map<String, List<String>> tmpWhiteListInfo, boolean isReload,
			String contentTplID, String keyStr) {
		List<String> tplList = null;

		if (isReload) {
			if (tmpWhiteListInfo.containsKey(keyStr)) {
				tplList = tmpWhiteListInfo.get(keyStr);
			} else {
				tplList = new ArrayList<String>();
				tmpWhiteListInfo.put(keyStr, tplList);
			}
		} else {
			if (this.contentWhiteListInfo.containsKey(keyStr)) {
				tplList = this.contentWhiteListInfo.get(keyStr);
			} else {
				tplList = new ArrayList<String>();
				this.contentWhiteListInfo.put(keyStr, tplList);
			}
		}
		tplList.add(contentTplID);
	}

	private void initContentRelay(A2PCustomerConfig cfg,
			Map<String, List<RelayMark>> tmpContentRelayInfo, boolean isReload) {
		A2PCustomerMultiValue relays = cfg
				.parseMultiValue("A2PContentRoutingRelay");
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			for (Group g : relays.getAllGroups()) {
				try {
					String r_op = g.getAttr("R_OP").getStringValue();
					int r_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(r_op));

					String relay = g.getAttr("Relay").getStringValue();
					int relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));

					String contentTplID = g.getAttr("contentTmpl")
							.getStringValue();

					if (r_ssid > 0 && relay_ssid > 0 && contentTplID != null
							&& contentTplID.length() > 0) {
						String keyStr = o_ssid + "_" + r_ssid;
						List<RelayMark> relayMarkList = null;

						if (isReload) {
							if (tmpContentRelayInfo.containsKey(keyStr)) {
								relayMarkList = tmpContentRelayInfo.get(keyStr);
							} else {
								relayMarkList = new ArrayList<RelayMark>();
								tmpContentRelayInfo.put(keyStr, relayMarkList);
							}
						} else {
							if (this.contentRelayInfo.containsKey(keyStr)) {
								relayMarkList = this.contentRelayInfo
										.get(keyStr);
							} else {
								relayMarkList = new ArrayList<RelayMark>();
								this.contentRelayInfo
										.put(keyStr, relayMarkList);
							}
						}

						RelayMark mark = new RelayMark();
						mark.setRelay(relay_ssid);
						mark.setContentTplID(contentTplID);
						relayMarkList.add(mark);
					}

				} catch (Exception e) {
					log
							.warn(
									"Init routingInfo content relay error, ssid={} {} Ignored {} {}",
									o_ssid, cfg.getShortName(), g.toString(), e);
					continue;
				}
			}
		}
	}

	private void initSenderAddrRelay(A2PCustomerConfig cfg,
			Map<String, List<RelayMark>> tmpSenderAddrRelayInfo,
			boolean isReload) {
		A2PCustomerMultiValue relays = cfg
				.parseMultiValue("A2PSenderAddrRoutingRelay");
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			for (Group g : relays.getAllGroups()) {
				try {
					String r_op = g.getAttr("R_OP").getStringValue();
					int r_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(r_op));

					String relay = g.getAttr("Relay").getStringValue();
					int relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));

					String o_addr = g.getAttr("O_Addr").getStringValue();

					if (r_ssid > 0 && relay_ssid > 0 && o_addr != null
							&& o_addr.length() > 0) {
						String keyStr = o_ssid + "_" + r_ssid;
						List<RelayMark> relayMarkList = null;

						if (isReload) {
							if (tmpSenderAddrRelayInfo.containsKey(keyStr)) {
								relayMarkList = tmpSenderAddrRelayInfo
										.get(keyStr);
							} else {
								relayMarkList = new ArrayList<RelayMark>();
								tmpSenderAddrRelayInfo.put(keyStr,
										relayMarkList);
							}
						} else {
							if (this.senderAddrRelayInfo.containsKey(keyStr)) {
								relayMarkList = this.senderAddrRelayInfo
										.get(keyStr);
							} else {
								relayMarkList = new ArrayList<RelayMark>();
								this.senderAddrRelayInfo.put(keyStr,
										relayMarkList);
							}
						}

						RelayMark mark = new RelayMark();
						mark.setRelay(relay_ssid);
						mark.setSenderAddr(o_addr);
						relayMarkList.add(mark);
					}

				} catch (Exception e) {
					log
							.warn(
									"Init routingInfo senderAddr relay error, ssid={} {} Ignored {} {}",
									o_ssid, cfg.getShortName(), g.toString(), e);
					continue;
				}
			}
		}
	}

	private void initSenderAddrReplace(A2PCustomerConfig cfg,
			Map<String, String> tmpSenderAddrReplaceInfo, boolean isReload) {
		A2PCustomerMultiValue relays = cfg
				.parseMultiValue("A2PSenderAddrReplace");
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			for (Group g : relays.getAllGroups()) {
				try {
					String o_addr = null;
					if (g.getAttr("O_Addr") != null) {
						o_addr = g.getAttr("O_Addr").getStringValue();
					}

					int relay_ssid = -1;
					if (g.getAttr("Relay") != null) {
						String relay = g.getAttr("Relay").getStringValue();
						relay_ssid = this.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					}

					int ropSsid = -1;
					if (g.getAttr("Rop") != null) {
						String rop = g.getAttr("Rop").getStringValue();
						ropSsid = this
								.getSSidbyCustomerNameshort(getShortNameByValue(rop));
					}

					String o_addr_replace = g.getAttr("O_AddrReplace").getStringValue();

					if (o_addr_replace != null && o_addr_replace.length() > 0
							&& o_addr != null && o_addr.length() > 0) {
						if (relay_ssid > 0 && ropSsid > 0) {
							if ("All".equalsIgnoreCase(o_addr)) {
								if (isReload) {
									tmpSenderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid + "_" + ropSsid,
											o_addr_replace);
								} else {
									this.senderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid + "_" + ropSsid,
											o_addr_replace);
								}
							} else {
								if (isReload) {
									tmpSenderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid + "_" + ropSsid + "_"
											+ o_addr, o_addr_replace);
								} else {
									this.senderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid + "_" + ropSsid + "_"
											+ o_addr, o_addr_replace);
								}
							}
						} else if (relay_ssid > 0) {
							if ("All".equalsIgnoreCase(o_addr)) {
								if (isReload) {
									tmpSenderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid, o_addr_replace);
								} else {
									this.senderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid, o_addr_replace);
								}
							} else {
								if (isReload) {
									tmpSenderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid + "_" + o_addr,
											o_addr_replace);
								} else {
									this.senderAddrReplaceInfo.put(o_ssid + "_"
											+ relay_ssid + "_" + o_addr,
											o_addr_replace);
								}
							}
						} else if (ropSsid > 0) {
							if ("All".equalsIgnoreCase(o_addr)) {
								if (isReload) {
									tmpSenderAddrReplaceInfo.put(o_ssid + "_"
											+ ropSsid, o_addr_replace);
								} else {
									this.senderAddrReplaceInfo.put(o_ssid + "_"
											+ ropSsid, o_addr_replace);
								}
							} else {
								if (isReload) {
									tmpSenderAddrReplaceInfo.put(o_ssid + "_"
											+ ropSsid + "_" + o_addr,
											o_addr_replace);
								} else {
									this.senderAddrReplaceInfo.put(o_ssid + "_"
											+ ropSsid + "_" + o_addr,
											o_addr_replace);
								}
							}
						}
					}
				} catch (Exception e) {
					log.warn("Init routingInfo senderAddr replace error, o_ssid={} {} Ignored {} {}",
									o_ssid, cfg.getShortName(), g.toString(), e);
					continue;
				}
			}
		}
	}

	private void initRecipientAddrReplace(A2PCustomerConfig cfg,
			Map<String, String> tmpRecipientAddrReplaceInfo, boolean isReload) {
		A2PCustomerMultiValue relays = cfg
				.parseMultiValue("A2PRecipientAddrReplace");
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			for (Group g : relays.getAllGroups()) {
				try {
					String r_addr = null;
					if (g.getAttr("R_Addr") != null) {
						r_addr = g.getAttr("R_Addr").getStringValue();
					}
					String relay = g.getAttr("Relay").getStringValue();
					int relay_ssid = this
							.getSSidbyCustomerNameshort(getShortNameByValue(relay));
					String r_addr_replace = g.getAttr("R_AddrReplace")
							.getStringValue();

					if (relay_ssid > 0 && r_addr_replace != null
							&& r_addr_replace.length() > 0) {
						if (r_addr != null && r_addr.length() > 0) {
							if (isReload) {
								tmpRecipientAddrReplaceInfo.put(o_ssid + "_"
										+ relay_ssid + "_" + r_addr,
										r_addr_replace);
							} else {
								this.recipientAddrReplaceInfo.put(o_ssid + "_"
										+ relay_ssid + "_" + r_addr,
										r_addr_replace);
							}
						} else {
							if (isReload) {
								tmpRecipientAddrReplaceInfo.put(o_ssid + "_"
										+ relay_ssid, r_addr_replace);
							} else {
								this.recipientAddrReplaceInfo.put(o_ssid + "_"
										+ relay_ssid, r_addr_replace);
							}
						}
					}

				} catch (Exception e) {
					log
							.warn(
									"Init routingInfo recipient replace error, ssid={} {} Ignored {} {}",
									o_ssid, cfg.getShortName(), g.toString(), e);
					continue;
				}
			}
		}
	}

	private List<String> getTplIdFromContentWhitelist(int o_op, String o_addr) {
		if (contentWhiteListInfo.containsKey(o_op + "_" + o_addr)) {
			return contentWhiteListInfo.get(o_op + "_" + o_addr);
		} else if (contentWhiteListInfo.containsKey(o_op + "")) {
			return contentWhiteListInfo.get(o_op + "");
		}
		// String searchKey = o_op + "_" + o_addr;
		// // for longest prefix match,
		// // e.g.5403_3456_abcde, 5403_3456_abcd, 5403_3456_9879, 5403_3456_987
		// List<String> keyList = new
		// ArrayList<String>(contentWhiteListInfo.keySet());
		// Collections.sort(keyList);
		// Collections.reverse(keyList);
		//		
		// for (String item : keyList) {
		// if (item.split("_").length > 1 && searchKey.startsWith(item)) {
		// return contentWhiteListInfo.get(item);
		// }
		// }
		//		
		// searchKey = o_op + "";
		// if (contentWhiteListInfo.containsKey(searchKey)){
		// return contentWhiteListInfo.get(searchKey);
		// }
		return null;

	}

	/**
	 * check if message content matches the content whitelist
	 * 
	 * @param msg
	 * @return
	 */
	public boolean isAllowByContentWhiteList(GmmsMessage msg) {
		List<String> tplList = getTplIdFromContentWhitelist(msg.getOoperator(),
				msg.getSenderAddress());
		if (tplList == null || tplList.size() < 1) {
			tplList = getTplIdFromContentWhitelist(msg.getOSsID(), msg
					.getSenderAddress());
		}

		boolean isAscii = false;
		if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(msg.getContentType())) {
			isAscii = true;
		}

		// check valid
		if (tplList != null) {
			for (Iterator<String> it = tplList.iterator(); it.hasNext();) {
				String tplName = (String) it.next();
				if (!isContentTplValid(tplName, isAscii)) {
					it.remove();
					if (log.isTraceEnabled()) {
						log
								.trace(
										msg,
										"Discard ContentWhiteList configuration. tplName={}",
										tplName);
					}
				}
			}
		}

		// no content whitelist defined, no need to check
		if (tplList == null || tplList.size() < 1) {
			return true;
		}

		for (String tplId : tplList) {
			if (this.contentTpl.checkContent(msg, tplId)) {
				return true;
			}
		}

		return false;
	}

	public boolean isContentTplValid(String tplName, boolean isAscii) {

		if (isAscii) {
			// ASCII
			return contentTpl.getRuleAscii().containsKey(tplName);
		} else {
			if (contentTpl.getRuleAscii().containsKey(tplName)) {
				return true;
			}
			// Unicode
			return contentTpl.getRuleUnicode().containsKey(tplName);
		}
	}
	
	public boolean isContentReplace(int ossid, int rssid, String raddr, String sender) {
		Map<String, String> contentTemplate = this.contentTemplateMap.get(ossid+":"+rssid+":"+sender);
		if (contentTemplate == null || contentTemplate.isEmpty()) {
			contentTemplate = this.contentTemplateMap.get(ossid+":"+rssid);
			if (contentTemplate == null || contentTemplate.isEmpty()) {
				return false;
			}			
		}		
		for (String country : contentTemplate.keySet()) {
			if (raddr.startsWith(country)) {
				return true;
			}
		}
		
		return false;
		
	}
	
	public List<String> getTemplateContent(String code, String contentType, int ossid, int rssid, String raddr, String sender) {

		Map<String, String> contentTemplate = this.contentTemplateMap.get(ossid+":"+rssid+":"+sender);
		if (contentTemplate == null || contentTemplate.isEmpty()) {
			contentTemplate = this.contentTemplateMap.get(ossid+":"+rssid);
			if (contentTemplate == null || contentTemplate.isEmpty()) {
				log.error("contentTemplate not found by key {}", ossid+":"+rssid);
				return null;
			}						
		}
		String templateId = null;
		for (Entry<String, String> entry: contentTemplate.entrySet()) {
			if (raddr.startsWith(entry.getKey())) {
				templateId = entry.getValue();
				break;
			}
		}
		
		if (templateId == null) {
			log.error("templateId is null ", ossid+":"+rssid);
			return null;
		}
		String type = contentType;
		String text = null;
		String templateSender = null;
		Map<String, String> content = contentTpl.getTemplateContentMaps().get(templateId);
		if (content == null) {
			//do check the template pool
			List<String> templatePool = new ArrayList<String>();
			for (String key : contentTpl.getTemplateContentMaps().keySet()) {
				if (key.startsWith(templateId)) {
					templatePool.add(key);
				}
			}
			if (templatePool.isEmpty()) {
				log.error("content is null by templateId {}", templateId);
				return null;
			}
			int len = templatePool.size();
			int random = (int)(Math.random()*len);
			content = contentTpl.getTemplateContentMaps().get(templatePool.get(random));
			if (content.get(contentType) == null) {
				for (Map.Entry<String,String> entry: content.entrySet()) {
					type = entry.getKey();
					text = entry.getValue();
					if ("sender".equalsIgnoreCase(type)) {
						templateSender = text;
						continue;
					}					
					if (type !=null && text != null) {
						break;
					}
				}
			}else {
				text = content.get(contentType);
				templateSender = content.get("sender");
			}
			
			
		}else {
			if (content.get(contentType) == null) {
				for (Map.Entry<String,String> entry: content.entrySet()) {
					type = entry.getKey();
					if ("sender".equalsIgnoreCase(type)) {
						continue;
					}
					text = entry.getValue();
					if (type !=null && text != null) {
						break;
					}
				}
			}else {
				text = content.get(contentType);
			}			
		}
		
		if (text == null) {
			log.error("content is null by templateId {}", templateId);
			return null;
		}
		code = code.replace("-", "");
		
		//add random value
		text = getRandomValue(text);
		//add replace short code logic
		String replaceCode = replaceShortCode(code, text);
		if(!code.equalsIgnoreCase(replaceCode) && text.indexOf("$srdm$")>-1){
			text = text.substring(0, text.indexOf("$srdm$"));
		}
		text = text.replaceFirst("\\$shortcode\\$", replaceCode);
		List<String> response = new ArrayList<String>();
		response.add(type);
		response.add(text);
		if (templateSender!=null) {
			response.add(templateSender);
		}		
		return response;
		
	}
	
	
	public List<String> getTemplateContent(String code, String templateId, String contentType) {
		String type = contentType;
		String text = null;
		String templateSender = null;
		Map<String, String> content = contentTpl.getTemplateContentMaps().get(templateId);
		if (content == null) {
			//do check the template pool
			List<String> templatePool = new ArrayList<String>();
			for (String key : contentTpl.getTemplateContentMaps().keySet()) {
				if (key.startsWith(templateId)) {
					templatePool.add(key);
				}
			}
			if (templatePool.isEmpty()) {
				log.error("content is null by templateId {}", templateId);
				return null;
			}
			int len = templatePool.size();
			int random = (int)(Math.random()*len);
			content = contentTpl.getTemplateContentMaps().get(templatePool.get(random));
			if (content.get(contentType) == null) {
				for (Map.Entry<String,String> entry: content.entrySet()) {
					type = entry.getKey();
					text = entry.getValue();
					if ("sender".equalsIgnoreCase(type)) {
						templateSender = text;
						continue;
					}					
					if (type !=null && text != null) {
						break;
					}
				}
			}else {
				text = content.get(contentType);
				templateSender = content.get("sender");
			}
			
			
		}else {
			if (content.get(contentType) == null) {
				for (Map.Entry<String,String> entry: content.entrySet()) {
					type = entry.getKey();
					if ("sender".equalsIgnoreCase(type)) {
						continue;
					}
					text = entry.getValue();
					if (type !=null && text != null) {
						break;
					}
				}
			}else {
				text = content.get(contentType);
			}			
		}
		
		if (text == null) {
			log.error("content is null by templateId {}", templateId);
			return null;
		}
		code = code.replace("-", "");
		
		//add random value
		text = getRandomValue(text);
		//add replace short code logic
		String replaceCode = replaceShortCode(code, text);
		if(!code.equalsIgnoreCase(replaceCode) && text.indexOf("$srdm$")>-1){
			text = text.substring(0, text.indexOf("$srdm$"));
		}
		text = text.replaceFirst("\\$shortcode\\$", replaceCode);
		List<String> response = new ArrayList<String>();
		response.add(type);
		response.add(text);
		if (templateSender!=null) {
			response.add(templateSender);
		}		
		return response;
		
	}
	
	public void doVendorTemplateRepaceByTemplateId(String templateId, GmmsMessage message) {
		//do replace Content by TemplateId in routing config	
				try {
					if(templateId!=null){
						String textContent = message.getTextContent();
						A2PCustomerInfo ocustomer = getCustomerBySSID(message.getOSsID());
						String regex = ocustomer.getTemplatecoderegex();
						String rex = "(?:[0-9-]){4,}";
						if(regex!=null && !"".equalsIgnoreCase(regex)) {
							rex = regex;
						}						
						Pattern p = Pattern.compile(rex);
						  Matcher m = p.matcher(textContent);
						 if(m.find()) {
							 String textContentType = null;
							 if (GmmsMessage.AIC_CS_ASCII.equalsIgnoreCase(message.getContentType())
									 || message.getContentType().startsWith("ISO-8859")) {
								 textContentType = GmmsMessage.AIC_CS_ASCII;
							  }else{
								  textContentType = GmmsMessage.AIC_CS_UCS2;
							  }				 
							 List<String> replaceContent = getTemplateContent(m.group(), templateId, textContentType);
							if (replaceContent!=null) {
								String type = replaceContent.get(0);
								String text = replaceContent.get(1);
								 message.setTextContent(text);
								 message.setContentType(type);
								 //模板替换后，要把7bit的标志去掉
								 if(GmmsMessage.AIC_CS_UCS2.equalsIgnoreCase(type)){
									 message.setGsm7bit(false);
								 }
								 message.setMessageSize(text.getBytes(type).length);
								 if (replaceContent.size()==3) {
									message.setSenderAddress(replaceContent.get(2));							
								 }
							}else {
								log.error(message, "the template Content is null in contentTemplate.cfg");						
								message.setStatus(GmmsStatus.TEMPLATE_FAIL);								
								return;
							}
							 
						 }else {
							 log.info(message, "do the replace content template failed, the code can't found in content [{}]", message.getTextContent());
							 message.setStatus(GmmsStatus.TEMPLATE_FAIL);							 					
							 return;
						 }
							
					}
				} catch (Exception e) {
					log.error(message,"do replace content template error", e);
				}
	}
	
	
	
	

	/**
	 * get senderId replacement based on Oop + OAddr + Relay or Oop + Relay
	 * 
	 * @param msg
	 * @return List<O_addr_conf, O_addr_replacement>
	 */
	public List<String> getSenderIdRelace(GmmsMessage msg) {
		boolean isMatchFull = getCustomerBySSID(msg.getOoperator())
				.isMatchFullWhenRouteReplaceOAddr()
				|| getCustomerBySSID(msg.getOSsID())
						.isMatchFullWhenRouteReplaceOAddr();

		List<String> replaceInfo = getSenderIdRelace(msg.getOoperator(), msg
				.getSenderAddress(), msg.getRSsID(), msg.getRoperator(),
				isMatchFull);
		if (replaceInfo == null || replaceInfo.size() < 1) {
			replaceInfo = getSenderIdRelace(msg.getOSsID(), msg
					.getSenderAddress(), msg.getRSsID(), msg.getRoperator(),
					isMatchFull);
		}
		return replaceInfo;
	}

	/**
	 * get recpientId replacement based on Oop + RAddr + Relay or Oop + Relay
	 * 
	 * @param msg
	 * @return List<r_addr_conf, r_addr_replacement>
	 */
	public List<String> getRecipientIdRelace(GmmsMessage msg) {
		List<String> replaceInfo = getRecipientIdRelace(msg.getOoperator(), msg
				.getRecipientAddress(), msg.getRSsID());
		if (replaceInfo == null || replaceInfo.size() < 1) {
			replaceInfo = getRecipientIdRelace(msg.getOSsID(), msg
					.getRecipientAddress(), msg.getRSsID());
		}
		return replaceInfo;
	}

	/**
	 * Support full match and longest prefix match
	 * 
	 * @param o_op
	 * @param o_addr
	 * @param relay
	 * @param r_op
	 * @return List<O_addr_conf, O_addr_replacement>
	 * 
	 *         The route priority: OOP->A2P->Relay->R_OP is the highest
	 *         OOP->A2P->Relay is the 2nd OOP->A2P->R_OP is the 3rd
	 */
	private List<String> getSenderIdRelace(int o_op, String o_addr, int relay,
			int r_op, boolean isMatchFull) {
		List<String> ret = new ArrayList<String>();
		// according to priority, define 3 hash map key:
		String searchKey1 = o_op + "_" + relay + "_" + r_op;
		String searchKey2 = o_op + "_" + relay;
		String searchKey3 = o_op + "_" + r_op;

		if (isMatchFull) {
			// full match
			if ((ret = this.getFullMatchSenderIdRelaceList(searchKey1, o_addr)) != null) {
				return ret;
			} else if ((ret = this.getFullMatchSenderIdRelaceList(searchKey2,
					o_addr)) != null) {
				return ret;
			} else if ((ret = this.getFullMatchSenderIdRelaceList(searchKey3,
					o_addr)) != null) {
				return ret;
			}
		} else {
			if ((ret = this.getSenderIdRelaceList(searchKey1, o_addr)) != null) {
				return ret;
			} else if ((ret = this.getSenderIdRelaceList(searchKey2, o_addr)) != null) {
				return ret;
			} else if ((ret = this.getSenderIdRelaceList(searchKey3, o_addr)) != null) {
				return ret;
			}
		}
		return ret;
	}

	private List<String> getFullMatchSenderIdRelaceList(String searchKey,
			String o_addr) {
		List<String> ret = new ArrayList<String>();
		if (senderAddrReplaceInfo.containsKey(searchKey + "_" + o_addr)) {
			// oaddr prefix in conf
			ret.add(o_addr);
			// oaddr replacement
			ret.add(senderAddrReplaceInfo.get(searchKey + "_" + o_addr));
			return ret;
		} else if (senderAddrReplaceInfo.containsKey(searchKey)) {
			// oaddr prefix in conf
			ret.add("All");
			// oaddr replacement
			ret.add(senderAddrReplaceInfo.get(searchKey));
			return ret;
		}
		return null;
	}

	private List<String> getSenderIdRelaceList(String searchKey, String o_addr) {
		List<String> ret = new ArrayList<String>();

		// for longest prefix match,
		// e.g.5403_3456_abcde, 5403_3456_abcd, 5403_3456_9879,
		// 5403_3456_987
		List<String> keyList = new ArrayList<String>(senderAddrReplaceInfo
				.keySet());
		Collections.sort(keyList);
		Collections.reverse(keyList);

		for (String item : keyList) {
			if (item.split("_").length > 3 && searchKey.split("_").length == 3
					&& (searchKey + "_" + o_addr).startsWith(item)) {
				// oaddr prefix in conf
				ret.add(item.split("_")[3]);
				// oaddr replacement
				ret.add(senderAddrReplaceInfo.get(item));
				return ret;
			} else if (item.split("_").length > 2
					&& searchKey.split("_").length == 2
					&& (searchKey + "_" + o_addr).startsWith(item)) {
				// oaddr prefix in conf
				ret.add(item.split("_")[2]);
				// oaddr replacement
				ret.add(senderAddrReplaceInfo.get(item));
				return ret;
			}
		}

		if (senderAddrReplaceInfo.containsKey(searchKey)) {
			// oaddr prefix in conf
			ret.add("All");
			// oaddr replacement
			ret.add(senderAddrReplaceInfo.get(searchKey));
			return ret;
		}

		return null;
	}

	private List<String> getRecipientIdRelace(int o_op, String r_addr, int relay) {
		// if (recipientAddrReplaceInfo.containsKey(o_op + "_" + r_addr + "_" +
		// relay)) {
		// return recipientAddrReplaceInfo.get(o_op + "_" + r_addr + "_" +
		// relay);
		// } else if (recipientAddrReplaceInfo.containsKey(o_op + "_" + relay)){
		// return recipientAddrReplaceInfo.get(o_op + "_" + relay);
		// }
		List<String> ret = new ArrayList<String>();
		String searchKey = o_op + "_" + relay + "_" + r_addr;
		// for longest prefix match,
		// e.g.5403_3456_abcde, 5403_3456_abcd, 5403_3456_9879, 5403_3456_987
		List<String> keyList = new ArrayList<String>(recipientAddrReplaceInfo
				.keySet());
		Collections.sort(keyList);
		Collections.reverse(keyList);

		for (String item : keyList) {
			if (item.split("_").length > 2 && searchKey.startsWith(item)) {
				// raddr prefix in conf
				ret.add(item.split("_")[2]);
				// raddr replacement
				ret.add(recipientAddrReplaceInfo.get(item));
				return ret;
			}
		}

		searchKey = o_op + "_" + relay;
		if (recipientAddrReplaceInfo.containsKey(searchKey)) {
			// oaddr prefix in conf
			ret.add("All");
			// oaddr replacement
			ret.add(recipientAddrReplaceInfo.get(searchKey));
			return ret;
		}
		return ret;
	}

	/**
	 * @param msg
	 * @return Map: key:signature_charset </br> value: signature
	 */
	public Map<String, String> getContentSignatureMap(GmmsMessage msg) {
		int r_ssid = msg.getRSsID();
		Map<String, Map<String, String>> rssidSignatureMap = contentSignatureMap
				.get(r_ssid);
		if (rssidSignatureMap == null || rssidSignatureMap.keySet().size() < 1) {
			return null;
		}

		int o_ssid = msg.getOSsID();
		int r_op = msg.getRoperator();
		String r_charset = msg.getContentType();
		String o_addr = msg.getSenderAddress();

		String key = o_ssid + CONF_SEPERATOR + r_ssid + CONF_SEPERATOR + r_op
				+ CONF_SEPERATOR + r_charset + CONF_SEPERATOR + o_addr;

		// 1) O_SSID + R_SSID + R_OP + O_Addr
		key = o_ssid + CONF_SEPERATOR + r_ssid + CONF_SEPERATOR + r_op
				+ CONF_SEPERATOR + o_addr;
		if (rssidSignatureMap.containsKey(key)) {
			Map<String, String> signatureMap = rssidSignatureMap.get(key);
			if (log.isTraceEnabled()) {
				log.trace(msg, "getContentSignature by key {}", key);
			}
			return signatureMap;
		}

		// 2) O_SSID + R_SSID + R_OP
		key = o_ssid + CONF_SEPERATOR + r_ssid + CONF_SEPERATOR + r_op;
		if (rssidSignatureMap.containsKey(key)) {
			Map<String, String> signatureMap = rssidSignatureMap.get(key);
			if (log.isTraceEnabled()) {
				log.trace(msg, "getContentSignature by key {}", key);
			}
			return signatureMap;
		}

		// 3) O_SSID + R_SSID + O_Addr
		key = o_ssid + CONF_SEPERATOR + r_ssid + CONF_SEPERATOR + o_addr;
		if (rssidSignatureMap.containsKey(key)) {
			Map<String, String> signatureMap = rssidSignatureMap.get(key);
			if (log.isTraceEnabled()) {
				log.trace(msg, "getContentSignature by key {}", key);
			}
			return signatureMap;
		}

		// 4) O_SSID + R_SSID
		key = o_ssid + CONF_SEPERATOR + r_ssid;
		if (rssidSignatureMap.containsKey(key)) {
			Map<String, String> signatureMap = rssidSignatureMap.get(key);
			if (log.isTraceEnabled()) {
				log.trace(msg, "getContentSignature by key {}", key);
			}
			return signatureMap;
		}

		// 5) R_SSID
		key = r_ssid + "";
		if (rssidSignatureMap.containsKey(key)) {
			Map<String, String> signatureMap = rssidSignatureMap.get(key);
			if (log.isTraceEnabled()) {
				log.trace(msg, "getContentSignature by key {}", key);
			}
			return signatureMap;
		}

		return null;
	}

	/**
	 * HashMap: contentKeywordMap key: OSSID value: ConcurrentHashMap<keyword,
	 * ROP_SSID>
	 */
	public Map<String, Integer> getKeywordMap(int oop) {
		if (contentKeywordMap.containsKey(oop)) {
			return contentKeywordMap.get(oop);
		} else {
			return null;
		}
	}

	//old method
	private void initContentKeyword(A2PCustomerConfig cfg,
			Map<Integer, Map<String, Integer>> map) {
		A2PCustomerMultiValue infos = cfg
				.parseMultiValue("A2PContentRouting");
		Map<String, Integer> keywordMap = new ConcurrentHashMap<String, Integer>();
		if (infos != null) {
			int o_ssid = cfg.getSSID();
			String keyword = null;
			for (Group g : infos.getAllGroups()) {
				int r_op = -1;
				if (g.getAttr("ROP") != null) {
					String r_op_str = g.getAttr("ROP").getStringValue();
					r_op = this
							.getSSidbyCustomerNameshort(getShortNameByValue(r_op_str));
				}

				if (g.getAttr("keyword") != null) {
					keyword = g.getAttr("keyword").getStringValue()
							.toLowerCase();
				}
				if (r_op > 0 && keyword != null) {
					keywordMap.put(keyword, Integer.valueOf(r_op));
				}
			}
			if (keywordMap.size() > 0) {
				map.put(Integer.valueOf(o_ssid), keywordMap);
			}
		}
	}

	private void initContentSignature(A2PCustomerConfig cfg,
			Map<Integer, Map<String, Map<String, String>>> contentSignatureMap) {
		A2PCustomerMultiValue infos = cfg
				.parseMultiValue("A2PContentSignature");

		if (infos != null) {
			int r_ssid = cfg.getSSID();
			for (Group g : infos.getAllGroups()) {
				try {
					int o_ssid = -1;
					if (g.getAttr("O_SSID") != null) {
						String o_ssid_str = g.getAttr("O_SSID")
								.getStringValue();
						o_ssid = this
								.getSSidbyCustomerNameshort(getShortNameByValue(o_ssid_str));
					}

					int r_op = -1;
					if (g.getAttr("R_OP") != null) {
						String r_op_str = g.getAttr("R_OP").getStringValue();
						r_op = this
								.getSSidbyCustomerNameshort(getShortNameByValue(r_op_str));
					}

					String o_addr = null;
					if (g.getAttr("O_Addr") != null) {
						o_addr = g.getAttr("O_Addr").getStringValue();
					}

					// Mandatory
					String sig_charset = g.getAttr("Signature_Charset")
							.getStringValue();
					String signature = g.getAttr("Signature").getStringValue();

					if (signature != null
							&& signature.length() > 0
							&& sig_charset != null
							&& sig_charset.length() > 0
							&& (sig_charset
									.equalsIgnoreCase(GmmsMessage.AIC_CS_ASCII) || sig_charset
									.equalsIgnoreCase(GmmsMessage.AIC_CS_UCS2))) {

						Map<String, Map<String, String>> rssidSignatureMap = contentSignatureMap
								.get(r_ssid);
						if (rssidSignatureMap == null) {
							rssidSignatureMap = new ConcurrentHashMap<String, Map<String, String>>();
							contentSignatureMap.put(r_ssid, rssidSignatureMap);
						}

						if (o_ssid > 0 && r_op > 0) {
							// 1) O_SSID + R_SSID + R_OP + O_Addr
							if (o_addr != null && o_addr.length() > 0) {
								String key = o_ssid + CONF_SEPERATOR + r_ssid
										+ CONF_SEPERATOR + r_op
										+ CONF_SEPERATOR + o_addr;
								Map<String, String> signatureMap = rssidSignatureMap
										.get(key);
								if (signatureMap == null) {
									signatureMap = new ConcurrentHashMap<String, String>();
									rssidSignatureMap.put(key, signatureMap);
								}
								signatureMap.put(sig_charset, signature);
								continue;
							}

							// 2) O_SSID + R_SSID + R_OP
							String key = o_ssid + CONF_SEPERATOR + r_ssid
									+ CONF_SEPERATOR + r_op;
							Map<String, String> signatureMap = rssidSignatureMap
									.get(key);
							if (signatureMap == null) {
								signatureMap = new ConcurrentHashMap<String, String>();
								rssidSignatureMap.put(key, signatureMap);
							}
							signatureMap.put(sig_charset, signature);
							continue;
						}

						if (o_ssid > 0) {
							// 3) O_SSID + R_SSID + O_Addr
							if (o_addr != null && o_addr.length() > 0) {
								String key = o_ssid + CONF_SEPERATOR + r_ssid
										+ CONF_SEPERATOR + o_addr;
								Map<String, String> signatureMap = rssidSignatureMap
										.get(key);
								if (signatureMap == null) {
									signatureMap = new ConcurrentHashMap<String, String>();
									rssidSignatureMap.put(key, signatureMap);
								}
								signatureMap.put(sig_charset, signature);
								continue;
							}

							// 4) O_SSID + R_SSID
							String key = o_ssid + CONF_SEPERATOR + r_ssid;
							Map<String, String> signatureMap = rssidSignatureMap
									.get(key);
							if (signatureMap == null) {
								signatureMap = new ConcurrentHashMap<String, String>();
								rssidSignatureMap.put(key, signatureMap);
							}
							signatureMap.put(sig_charset, signature);
							continue;

						}

						// 5) R_SSID
						String key = r_ssid + "";
						Map<String, String> signatureMap = rssidSignatureMap
								.get(key);
						if (signatureMap == null) {
							signatureMap = new ConcurrentHashMap<String, String>();
							rssidSignatureMap.put(key, signatureMap);
						}
						signatureMap.put(sig_charset, signature);
						continue;
					}

				} catch (Exception e) {
					log
							.warn(
									"Init content signature error, ssid={} {} Ignored {} {}",
									r_ssid, cfg.getShortName(), g.toString(), e);
					continue;
				}
			}
		}
	}

	public String getConfFileVersion() {
		return confFileVersion;
	}

	
	public Map<String, A2PCustomerConfig> getPhonePrefixMap() {
		return phonePrefixMap;
	}

	public void setPhonePrefixMap(Map<String, A2PCustomerConfig> phonePrefixMap) {
		this.phonePrefixMap = phonePrefixMap;
	}

	
	public void writerCust2File(String rcm, Set<Integer> ssids, String fileName) {
		File file = new File(fileName);
		FileOutputStream fos = null;
		OutputStreamWriter osw = null;
		BufferedWriter bw = null;
		try {
			if (!file.getParentFile().exists()) {
				if (!file.getParentFile().mkdirs()) {
					log.error("create temp file dir error!");
				}
			}
			if (!file.exists()) {
				file.createNewFile();
			}
			fos = new FileOutputStream(file, true);
			osw = new OutputStreamWriter(fos, "UTF-8");
			bw = new BufferedWriter(osw);
			bw.write(rcm);
			bw.flush();
			for (int ssid : ssids) {
				A2PCustomerInfo info = this.ssid_cust_map.get(ssid);
				bw.write(new StringBuffer().append("[").append(
						info.getShortName()).append("] \r\n").append(info)
						.append("\r\n").toString());
				bw.flush();
			}
			bw.flush();
		} catch (IOException e) {
			log.error("write cm info to temp file error!");
		} finally {
			try {
				bw.close();
				osw.close();
				fos.close();
			} catch (IOException e) {
				log.error("write cm info to temp file error!");
			}

		}

	}

	public ArrayList<Integer> getCorePriority_ssid_list() {
		return corePriority_ssid_list;
	}

	public void setCorePriority_ssid_list(ArrayList<Integer> corePriority_ssid_list) {
		this.corePriority_ssid_list = corePriority_ssid_list;
	}
	
	public String getRandomValue(String text){	
		try {
			if(!text.contains("$rdm$")){
				return text;
			}
			Pattern p = Pattern.compile("\\$rdm\\$");
		    Matcher m = p.matcher(text);
		    List<Integer> indexs  = new ArrayList<Integer>();
		    while (m.find()) {
		        indexs.add(m.start());
		    }

		    List<String> replace = new ArrayList<>();
		    for(int i = 0; i<indexs.size()-1; i=i+2){
		    	String numbers = text.substring(indexs.get(i)+5, indexs.get(i+1));
		    	int len = 1;
		    	String [] lens = numbers.split(",");
		    	if(lens.length>1){
		    		len = Integer.parseInt(lens[1].trim());
		    	}
		    	int start = Integer.parseInt((lens[0].split("-"))[0].trim());
		    	int end = Integer.parseInt((lens[0].split("-"))[1].trim());
		    	StringBuilder builder = new StringBuilder();
		    	for(int t = 0; t<len; t++){
		    		builder.append((char)((int)(Math.random()*(end-start)+start)));
		    	}
		    	replace.add(builder.toString());
		    	builder.trimToSize();
		    }
		    List<String> remainText = new ArrayList<>();
		    remainText.add(text.substring(0, indexs.get(0)));
		    for(int i = 1; i<indexs.size()-1; i=i+2){
		    	remainText.add(text.substring(indexs.get(i)+5, indexs.get(i+1)));
		    }
		    remainText.add(text.substring(indexs.get(indexs.size()-1)+5));
		    StringBuilder value = new StringBuilder();
		    for(int i = 0; i<remainText.size(); i++){
		    	value.append(remainText.get(i));
		    	if(i<replace.size()){
		    		value.append(replace.get(i));
		    	}
		    }
			//return (int)(Math.random()*(end-start)+start);
		    return value.toString();
		} catch (Exception e) {
			log.error("replace randmo value failed, {}", text, e);
			return text;
		}
		
	}
	
	private String replaceShortCode(String code, String text){
		try {
			Pattern p = Pattern.compile("\\$srdm\\$");
		    Matcher m = p.matcher(text);
		    List<Integer> indexs  = new ArrayList<Integer>();
		    while (m.find()) {
		        indexs.add(m.start());
		    }
		    if(indexs.size()<2){
		    	return code;
		    }
		    StringBuilder builder = new StringBuilder();
		    for(int i = 0; i<indexs.size()-1; i=i+2){
		    	String numbers = text.substring(indexs.get(i)+6, indexs.get(i+1));
		    	int len = 1;
		    	String [] lens = numbers.split(",");
		    	if(lens.length>1){
		    		len = Integer.parseInt(lens[1].trim());
		    	}
		    	int start = Integer.parseInt((lens[0].split("-"))[0].trim());
		    	int end = Integer.parseInt((lens[0].split("-"))[1].trim());	    	
		    	for(int t = 0; t<len; t++){
		    		builder.append((char)((int)(Math.random()*(end-start)+start)));
		    	}	    	
		    }
		    
		    StringBuilder value = new StringBuilder();
		    
		    for(int i = 0; i<code.length(); i++){
		    	value.append(code.charAt(i));
		    	if(i<builder.length()){
		    		value.append(builder.charAt(i));
		    	}else{
		    		value.append(builder.charAt(builder.length()-1));
		    	}	    	
		    }
		    String firstMergeValue = value.substring(0, value.length()-1);
		    int vLen = firstMergeValue.length();
		    int length = builder.length()-code.length()+2;
		    int start =0;
		    StringBuilder mergeValue = new StringBuilder();
		    for(int i = 0; i < vLen; i++){
		    	mergeValue.append(firstMergeValue.charAt(i));
		    	if(length-start>=0 && i%2==0){
		    		int end = (int)(Math.random()*(length-start));
		    		System.out.println("start:"+start+", end:"+end);
		    		mergeValue.append(builder.subSequence(start, start+end));
			    	start = end;
			    	length = length -start;
		    	}	    	
		    } 		    
			return mergeValue.toString();
		} catch (Exception e) {
			log.error("replace shortcode value failed, {}", text, e);
			return code;
		}
		
	}
	
	
	
	private void doLoadCustomerOPContentKeywordRoutingRelay(A2PCustomerConfig cfg, String contentRelayKey,			
			HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> customeropContentKeywordRoutingInfo
			) {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue(contentRelayKey);
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			
			List<String> prefixLists = new ArrayList<String>();
			for (Group g : relays.getAllGroups()) {
				try {
					String operatorIdString = g.getAttr("Opid").getStringValue();
					String keywordString = g.getAttr("KeywordId").getStringValue();
					String relay = g.getAttr("Relay").getStringValue();
					int priority = g.getAttr("Priority").getIntValue();
					if(operatorIdString!= null) {
						String [] opIdList = operatorIdString.split(A2PCustomerConfig.SUB_VALUE_SEP);						
						for(String opId: opIdList) {
							if(relay != null) {								
								HashBasedTable<String, Integer, String> opContentMap = customeropContentKeywordRoutingInfo.get(contentRelayKey+"_"+o_ssid, Integer.parseInt(opId.trim()));
                                if(opContentMap == null) { 
                                	opContentMap = HashBasedTable.create();
                                }                                
								opContentMap.put(keywordString, priority, relay);	
								customeropContentKeywordRoutingInfo.put(contentRelayKey+"_"+o_ssid, Integer.parseInt(opId.trim()), opContentMap);
							}
														
						}
					}
					
				} catch (Exception e) {
					log.error("initOPContentKeywordRoutingRelay Exception ignored:{}", g.getAllAttrs());
					continue;
				}
			}					
		}
	}
	
	
	private int doGetCustomerOPContentKeywordRoutingRelay(String keyword, int ossid, GmmsMessage msg,
			HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> routingTable, int opId) {
		int rssid = 0;
		rssid = doGetRoutingRssidByOpIdContent(keyword, ossid, msg, routingTable, opId);
		if(rssid == 0) {
			rssid = doGetRoutingRssidByOpIdContent(keyword, ossid, msg, routingTable, opId/10000*10000);
			if(rssid == 0) {
				rssid = doGetRoutingRssidByOpIdContent(keyword, ossid, msg, routingTable, A2P_DEFALUT_OP_ROUTING_INDEX);
			}
		}
		return rssid;
	}

	private int doGetRoutingRssidByOpIdContent(String keyword, int ossid, GmmsMessage msg,
			HashBasedTable<String, Integer, HashBasedTable<String, Integer, String>> routingTable, int opId) {
		int rssid = 0;
		HashBasedTable<String,Integer,String> relayInfo = routingTable.get(keyword+"_"+ossid, opId);
		if (relayInfo == null || relayInfo.isEmpty()) {
			return rssid;			
		}			
		Set<Integer> proritySet = relayInfo.columnKeySet();
		List<Integer> list = new ArrayList<Integer>(proritySet);
		Collections.sort(list);
		for (Integer prority : list) {
			//add otp and none otp routing check
			Map<String, String> contentRoutingMap = relayInfo.columnMap().get(prority);
			for(Map.Entry<String, String> entry: contentRoutingMap.entrySet()) {
				String contentKey = entry.getKey();
				String contentRouting = entry.getValue();
				if(contentTpl.checkContent(msg, contentKey)){
					rssid = getMixRoutingSsid(ossid, contentRouting, msg);
					log.debug(msg, "get the customer op content routing success");
					return rssid;
				}
			}
			
		}	
		return rssid;
	}
	
	private void doLoadOPSenderRoutingRelay(A2PCustomerConfig cfg, String sendRoutingKey,
			HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> customeropSenderRoutingRelayInfo) throws Exception {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue(sendRoutingKey);
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			Map<String, String> msaterPrefixMap = new HashMap<String, String>();
			List<String> prefixLists = new ArrayList<String>();
			for (Group g : relays.getAllGroups()) {
				try {
					String operatorIdString = g.getAttr("Opid").getStringValue();
					String senderString = g.getAttr("Sender").getStringValue();
					String relay = g.getAttr("Relay").getStringValue();
					int priority = g.getAttr("Priority").getIntValue();
					if(operatorIdString!= null) {
						String [] opIdList = operatorIdString.split(A2PCustomerConfig.SUB_VALUE_SEP);						
						String [] senderList = senderString.split(A2PCustomerConfig.SUB_VALUE_SEP);						
						for(String opId: opIdList) {
							for(String sender: senderList) {
								if(relay != null) {
									HashBasedTable<String, Integer, String> senderContentMap = customeropSenderRoutingRelayInfo.get(sendRoutingKey+"_"+o_ssid, Integer.parseInt(opId.trim()));
	                                if(senderContentMap == null) { 
	                                	senderContentMap = HashBasedTable.create();
	                                }
									senderContentMap.put(sender, priority, relay);	
									customeropSenderRoutingRelayInfo.put(sendRoutingKey+"_"+o_ssid, Integer.parseInt(opId.trim()), senderContentMap);
								}
															
							}							
						}
					}
				} catch (Exception e) {
					log.error("doLoadOPSenderRoutingRelay Exception ignored:{}", g.toString());
					continue;
				}
			}			
		}

	}
	
	private int doGetCustomerOPSenderRoutingRelay(String keyword, int ossid, GmmsMessage msg,
			HashBasedTable<String,Integer,HashBasedTable<String,Integer,String>> routingTable, int opId) {
		int rssid = 0;
		rssid = doGetRoutingRssidByOpIdSender(keyword, ossid, msg, routingTable, opId);
		if(rssid==0) {
			rssid = doGetRoutingRssidByOpIdSender(keyword, ossid, msg, routingTable, opId/10000*10000);
			if(rssid==0) {
				rssid = doGetRoutingRssidByOpIdSender(keyword, ossid, msg, routingTable, A2P_DEFALUT_OP_ROUTING_INDEX);				
			}
		}
		return rssid;
								
	}

	private int doGetRoutingRssidByOpIdSender(String keyword, int ossid, GmmsMessage msg,
			HashBasedTable<String, Integer, HashBasedTable<String, Integer, String>> routingTable, int opId) {
		int rssid = 0;
		HashBasedTable<String,Integer,String> relayInfo = routingTable.get(keyword+"_"+ossid, opId);
		if (relayInfo == null || relayInfo.isEmpty()) {
			return rssid;			
		}		
		Set<Integer> proritySet = relayInfo.columnKeySet();
		List<Integer> list = new ArrayList<Integer>(proritySet);
		Collections.sort(list);
		for (Integer prority : list) {
			//add otp and none otp routing check
			Map<String, String> senderRoutingMap = relayInfo.columnMap().get(prority);
			for(Map.Entry<String, String> entry: senderRoutingMap.entrySet()) {
				String senderKey = entry.getKey();
				String senderRouting = entry.getValue();
				if(msg.getSenderAddress().equals(senderKey)){
					rssid = getMixRoutingSsid(ossid, senderRouting, msg);
					log.debug(msg, "get the customer op sender routing success");
					return rssid;
				}
			}				
		}	
		return rssid;
	}
	
	private void doLoadOPRoutingRelay(A2PCustomerConfig cfg, String routingKey,
			HashBasedTable<String,Integer,Map<Integer,String>> customerOPRoutingTable) throws Exception {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue(routingKey);
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			Map<String, String> msaterPrefixMap = new HashMap<String, String>();
			List<String> prefixLists = new ArrayList<String>();
			for (Group g : relays.getAllGroups()) {
				try {
					String operatorIdString = g.getAttr("Opid").getStringValue();					
					String relay = g.getAttr("Relay").getStringValue();
					int priority = g.getAttr("Priority").getIntValue();
					if(operatorIdString!= null) {
						String [] opIdList = operatorIdString.split(A2PCustomerConfig.SUB_VALUE_SEP);						
						for(String opId: opIdList) {
							if(relay != null) {								
								Map<Integer,String> routingMap = customerOPRoutingTable.get(routingKey+"_"+o_ssid, Integer.parseInt(opId.trim()));
								if(routingMap == null) {
									routingMap = new HashMap<>();
								}
								routingMap.put(priority, relay);	
								customerOPRoutingTable.put(routingKey+"_"+o_ssid, Integer.parseInt(opId.trim()), routingMap);
							}							
						}
					}
				} catch (Exception e) {
					log.error("doLoadOPRoutingRelay Exception ignored:{}", g.toString());
					continue;
				}
			}			
		}
	}
	
	private void doLoadNumberRoutingRelay(A2PCustomerConfig cfg, String routingKey,
			HashBasedTable<String,Long,Map<Integer,String>> customerNumberRoutingTable) throws Exception {
		A2PCustomerMultiValue relays = cfg.parseRoutingMultiValue(routingKey);
		if (relays != null) {
			int o_ssid = cfg.getSSID();
			Map<String, String> msaterPrefixMap = new HashMap<String, String>();
			List<String> prefixLists = new ArrayList<String>();
			for (Group g : relays.getAllGroups()) {
				try {
					String operatorIdString = g.getAttr("Number").getStringValue();					
					String relay = g.getAttr("Relay").getStringValue();
					int priority = g.getAttr("Priority").getIntValue();
					if(operatorIdString!= null) {
						String [] opIdList = operatorIdString.split(A2PCustomerConfig.SUB_VALUE_SEP);						
						for(String opId: opIdList) {
							if(relay != null) {								
								Map<Integer,String> routingMap = customerNumberRoutingTable.get(routingKey+"_"+o_ssid, Long.parseLong(opId.trim()));
								if(routingMap == null) {
									routingMap = new HashMap<>();
								}
								routingMap.put(priority, relay);	
								customerNumberRoutingTable.put(routingKey+"_"+o_ssid, Long.parseLong(opId.trim()), routingMap);
							}							
						}
					}
				} catch (Exception e) {
					log.error("doLoadNumberRoutingRelay Exception ignored:{}", g.toString());
					continue;
				}
			}			
		}
	}
	
	private int doGetRoutingRssidByNumber(String keyword, int ossid, GmmsMessage msg, 
			HashBasedTable<String,Long,Map<Integer,String>> customerNumberRoutingTable) {
		int rssid=0;
		Map<Integer, String> routing = customerNumberRoutingTable.get(keyword+"_"+ossid, Long.parseLong(msg.getRecipientAddress()));
		if (routing == null || routing.isEmpty()) {
			return rssid;			
		}
		Set<Integer> proritySet = routing.keySet();
		List<Integer> list = new ArrayList<Integer>(proritySet);
		Collections.sort(list);
		for (Integer prority : list) {
			//add otp and none otp routing check
			if (routing.get(prority) !=null) {
				rssid = getMixRoutingSsid(ossid, routing.get(prority), msg);
				log.debug(msg, "get the customer special number routing success");
			}
			return rssid;				
		}
		return rssid;
	}
	
	
	private int doGetCustomerOPRoutingRelay(String keyword, int ossid, GmmsMessage msg,
			HashBasedTable<String,Integer,Map<Integer,String>> customerOPRoutingTable, int opId) {
		int rssid = 0;		
		rssid = doGetRoutingRssidByOpIdCountry(keyword, ossid, msg, customerOPRoutingTable, opId);
		if(rssid==0) {
			rssid = doGetRoutingRssidByOpIdCountry(keyword, ossid, msg, customerOPRoutingTable, opId/10000*10000);
			if(rssid == 0) {
				rssid = doGetRoutingRssidByOpIdCountry(keyword, ossid, msg, customerOPRoutingTable, A2P_DEFALUT_OP_ROUTING_INDEX);
			}
		}
		return rssid;
		
	}

	private int doGetRoutingRssidByOpIdCountry(String keyword, int ossid, GmmsMessage msg, 
			HashBasedTable<String,Integer,Map<Integer,String>> customerOPRoutingTable, int opId) {
		int rssid=0;
		Map<Integer, String> routing = customerOPRoutingTable.get(keyword+"_"+ossid, opId);
		if (routing == null || routing.isEmpty()) {
			return rssid;			
		}
		Set<Integer> proritySet = routing.keySet();
		List<Integer> list = new ArrayList<Integer>(proritySet);
		Collections.sort(list);
		for (Integer prority : list) {
			//add otp and none otp routing check
			if (routing.get(prority) !=null) {
				rssid = getMixRoutingSsid(ossid, routing.get(prority), msg);
				log.debug(msg, "get the customer special op routing success");
			}
			return rssid;				
		}
		return rssid;
	}
	
	public int getCustomerOPSenderRoutingRelay(int ossid, GmmsMessage msg, int opId) {
		return doGetCustomerOPSenderRoutingRelay(A2P_OP_SENDER_ROUTING_RELAY, ossid, msg, customeropSenderRoutingRelayInfo, opId);						
	}
	
	public int getCustomerOPContentRoutingRelay(int ossid, GmmsMessage msg, int opId) {
		return doGetCustomerOPContentKeywordRoutingRelay(A2P_OP_CONTENT_KEYWORD_ROUTING_RELAY, ossid, msg, customeropContentKeywordRoutingRelayInfo, opId);						
	}
	
	public int getCustomerOPRoutingRelay(int ossid, GmmsMessage msg, int opId) {
		return doGetCustomerOPRoutingRelay(A2P_OP_ROUTING_RELAY, ossid, msg, customeropRoutingRelayInfo, opId);						
	}
	
	public int getCustomerNumberRoutingRelay(int ossid, GmmsMessage msg) {
		return doGetRoutingRssidByNumber(A2P_NUMBER_ROUTING_RELAY, ossid, msg, customerNumberRoutingRelayInfo);						
	}
	
	public int getCustomerOPBackupSenderRoutingRelay(String routingSsids, int ossid, GmmsMessage msg, int opId) {
		int result = 0;
		if (customeropBackupSenderRoutingRelayInfo.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_OP_BACKUP_SENDER_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerOPSenderRoutingRelay(key, ossid, msg, customeropBackupSenderRoutingRelayInfo, opId);								
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;						
	}
	
	public int getCustomerOPBackupContentRoutingRelay(String routingSsids, int ossid, GmmsMessage msg, int opId) {
		int result = 0;
		if (customeropBackupContentKeywordRoutingRelayInfo.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_OP_BACKUP_CONTENT_KEYWORD_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerOPContentKeywordRoutingRelay(key, ossid, msg, customeropBackupContentKeywordRoutingRelayInfo, opId);								
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;	
	}
	
	public int getCustomerOPBackupRoutingRelay(String routingSsids, int ossid, GmmsMessage msg, int opId) {
		int result = 0;
		if (customeropBackupRoutingRelayInfo.isEmpty()) {
			return result;
		}
		for (int i = 0; i < 10; i++) {
			String key = A2P_OP_BACKUP_ROUTING_RELAY;
			if (i>0) {
				key= key+i;
			}
			result = doGetCustomerOPRoutingRelay(key, ossid, msg, customeropBackupRoutingRelayInfo, opId);								
			if ((routingSsids == null || (routingSsids!=null && !routingSsids.contains(","+result+","))) && result >0) {
				return result;
			}
		}
		return 0;
	}

	public static void main(String[] args) {
		/*String text = "D:$rdm$5-13,10$rdm$$srdm$32-32,10$srdm$";
		String code ="1234556";
		Pattern p = Pattern.compile("\\$rdm\\$");
	    Matcher m = p.matcher(text);
	    List<Integer> indexs  = new ArrayList<Integer>();
	    while (m.find()) {
	        indexs.add(m.start());
	    }

	    if(indexs.size()<2){
	    	return;
	    }
	    StringBuilder builder = new StringBuilder();
	    for(int i = 0; i<indexs.size()-1; i=i+2){
	    	String numbers = text.substring(indexs.get(i)+6, indexs.get(i+1));
	    	int len = 1;
	    	String [] lens = numbers.split(",");
	    	if(lens.length>1){
	    		len = Integer.parseInt(lens[1].trim());
	    	}
	    	int start = Integer.parseInt((lens[0].split("-"))[0].trim());
	    	int end = Integer.parseInt((lens[0].split("-"))[1].trim());	    	
	    	for(int t = 0; t<len; t++){
	    		builder.append((char)((int)(Math.random()*(end-start)+start)));
	    	}	    	
	    }
	    
	    StringBuilder value = new StringBuilder();
	    
	    for(int i = 0; i<code.length(); i++){
	    	value.append(code.charAt(i));
	    	if(i<builder.length()){
	    		value.append(builder.charAt(i));
	    	}else{
	    		value.append(builder.charAt(builder.length()-1));
	    	}	    	
	    }
	    String firstMergeValue = value.substring(0, value.length()-1);
	    int vLen = firstMergeValue.length();
	    int length = builder.length()-code.length()+2;
	    int start =0;
	    StringBuilder mergeValue = new StringBuilder();
	    for(int i = 0; i < vLen; i++){
	    	mergeValue.append(firstMergeValue.charAt(i));
	    	if(length-start>=0 && i%2==0){
	    		int end = (int)(Math.random()*(length-start));
	    		System.out.println("start:"+start+", end:"+end);
	    		mergeValue.append(builder.subSequence(start, start+end));
		    	start = end;
		    	length = length -start;
	    	}	    	
	    }
	    
	    System.out.println(mergeValue);
	    text = text.substring(0, text.indexOf("$srdm$"));
	    System.out.println(text);*/
		String text = "D:$rdm$8-8,10$rdm$$srdm$32-32,10$srdm$";
		if(!text.contains("$rdm$")){
			//return text;
		}
		Pattern p = Pattern.compile("\\$rdm\\$");
	    Matcher m = p.matcher(text);
	    List<Integer> indexs  = new ArrayList<Integer>();
	    while (m.find()) {
	        indexs.add(m.start());
	    }

	    List<String> replace = new ArrayList<>();
	    for(int i = 0; i<indexs.size()-1; i=i+2){
	    	String numbers = text.substring(indexs.get(i)+5, indexs.get(i+1));
	    	int len = 1;
	    	String [] lens = numbers.split(",");
	    	if(lens.length>1){
	    		len = Integer.parseInt(lens[1].trim());
	    	}
	    	int start = Integer.parseInt((lens[0].split("-"))[0].trim());
	    	int end = Integer.parseInt((lens[0].split("-"))[1].trim());
	    	StringBuilder builder = new StringBuilder();
	    	for(int t = 0; t<len; t++){
	    		builder.append((char)((int)(Math.random()*(end-start)+start)));
	    	}
	    	replace.add(builder.toString());
	    	builder.trimToSize();
	    }
	    List<String> remainText = new ArrayList<>();
	    remainText.add(text.substring(0, indexs.get(0)));
	    for(int i = 1; i<indexs.size()-1; i=i+2){
	    	remainText.add(text.substring(indexs.get(i)+5, indexs.get(i+1)));
	    }
	    remainText.add(text.substring(indexs.get(indexs.size()-1)+5));
	    StringBuilder value = new StringBuilder();
	    for(int i = 0; i<remainText.size(); i++){
	    	value.append(remainText.get(i));
	    	if(i<replace.size()){
	    		value.append(replace.get(i));
	    	}
	    }
		//return (int)(Math.random()*(end-start)+start);
	    System.out.println(value.toString()) ;
	    String spString = "$^()[]{}*+?.\\";
	    //String exp = "\\^Hello\\.21yi\\$";
	    String exp = "876|26";
        String input = "26876";
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<exp.length();i++){
        	String ch = exp.substring(i, i+1);
        	if(spString.contains(ch)){
        		sb.append("\\").append(ch);
        	}else{
        		sb.append(ch);
        	}
        }
        /* StringBuilder isb = new StringBuilder();
        for(int i=0; i<input.length();i++){
        	String ch = input.substring(i, i+1);
        	if(spString.contains(ch)){
        		isb.append("\\").append(ch);
        	}else{
        		isb.append(ch);
        	}
        }
        System.out.println(sb);
        System.out.println(isb);*/
        Pattern pattern = Pattern.compile(sb.toString());
        Matcher matcher = pattern.matcher(input);
        while(matcher!=null && matcher.find()){
        	System.out.println("匹配: " + matcher.group());
        }
        String senderEntry = "abc:123:456";
        String senderKye = senderEntry.substring(0, senderEntry.lastIndexOf(":"));
		String rpValue = senderEntry.substring(senderEntry.lastIndexOf(":"));
        System.out.println("sn "+senderKye+" "+rpValue);
        
        //Cache<Object, Object> cache  =CacheBuilder.newBuilder().expireAfterWrite(3,TimeUnit.SECONDS).build();
        String rssids=",111,,8,";
        int oldLen = rssids.length();
		int newLen = rssids.replaceAll(",", "").length();
		System.out.println(oldLen-newLen);
		
	}
}
