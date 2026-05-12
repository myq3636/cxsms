package com.king.gmms.domain;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.axis.utils.StringUtils;
import org.apache.soap.providers.com.Log;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeMap;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.RetryPolicyInfo;
import com.king.gmms.RetryPolicyManager;
import com.king.gmms.connectionpool.BindMode;
import com.king.gmms.connectionpool.ssl.SslConfiguration;
import com.king.gmms.protocol.smpp.version.SMPPVersion;
import com.king.gmms.util.SystemConstants;
import com.king.gmms.util.prefix.AddPrefix;
import com.king.gmms.util.prefix.Prefix;
import com.king.gmms.util.prefix.RemovePrefix;
import com.king.gmms.util.prefix.ReplacePrefix;
import com.king.message.gmms.GmmsMessage;

public abstract class A2PCustomerInfo {

	private static SystemLogger log = SystemLogger
			.getSystemLogger(A2PCustomerInfo.class);
	protected int ssid = -1;

	/**
	 * for A2P P2P peering
	 */
	protected int iosmsSsid = -1;

	protected String service = null;
	protected String shortName = null;

	protected String serverID = null;
	protected String a2pPlayerType = null;
	protected String receiverQueue;
	protected String chlQueue = null;
	protected MessageMode messageMode;
	
	//modify by kevin for REST
//	protected String inClientPull = null;
	
	protected int inClientPull = 0;
	
	
	protected String outClientPull = null;
	protected boolean supportDeliverReport = false;
	protected boolean supportTemplateParemeterDelivery = false;
	protected boolean supportTemplateParemeterSignature = false;
	protected List<String> supportedCharsets = new ArrayList<String>();

	/**
	 * ID of customer <br>
	 * get relationship with ssid from subscribedservice
	 */
	protected int customerId = -1;

	/**
	 * <p>
	 * use in the scenario:<br>
	 * MO: user -> operator partner ->King --> 1.5way/ott
	 * </p>
	 * <p>
	 * conf on operator partner side.
	 * </p>
	 * <p>
	 * decide whether use ADS or NMG to route
	 * </p>
	 */
	protected List<String> rentAddrPrefixList = new ArrayList<String>();

	/**
	 * <p>
	 * use in the scenario:<br>
	 * MT: 1.5way/ott -> King -> operator partner --> user
	 * </p>
	 * <p>
	 * conf on OTT/1.5 way side.
	 * </p>
	 * <p>
	 * decide whether apply number from Partner
	 * </p>
	 */
	protected List<RentAddrCondition> rentAddrConditionList = new ArrayList<RentAddrCondition>();

	/**
	 * key: ossid_rop<br/>
	 * value: percent
	 */
	protected Map<String, Integer> blackholePercentMap = new HashMap<String, Integer>();
	
	/**
	 * key: priority
	 * value: percent 
	 * configure it in rssid
	 */
	protected Map<Integer, Integer> priorityPercentMap = new HashMap<Integer, Integer>();
	
	protected int priority = 0;
	protected int blackholeDrStatusCode = 10000;
	protected int blackholebatchMinNumberForPortalCust = 500;
	protected int blackholeMinNumberByTimer = 500;
	protected int blackholeTimerInSec = 60;
	
	protected int minSenderNumber = -1;
	protected int prioritryHanderNumber = -1;
	protected int maxSenderNumber = -1;
	protected int queueNumber = 1;
	protected int senderThroughput = 0;
	protected int senderAliveTime = 5;
	protected double senderRiseRatio;
	protected int minReceiverNumber = -1;
	protected int maxReceiverNumber = -1;
	protected int connectionSilentTime = 300 * 1000;
	protected int windowSize = 1000;
	protected int enquireLinkTime = -1;
	protected int bufferTimeout = 60 * 1000;
	protected int connectionType = 1;
	protected boolean isServerEnquireLink = false;
	protected boolean isEnquireLink = false;
	protected boolean isKeepEnquireLink = false;
	protected boolean isEnableGuavaBuffer = false;
	protected Pattern pAllowOriPrefixList = null;

	protected ArrayList<Integer> oriNumberLens = new ArrayList<Integer>();
	protected ArrayList<Integer> recUserPhoneLens = null;

	protected ArrayList<Integer> recNumberLens = new ArrayList<Integer>();
	protected int oriMinNumberLen = -1;
	protected int recMinNumberLen = -1;

	protected boolean parseOoperator = false;
	protected boolean udhConcatenated = false;
	protected boolean supportConcatenatedMsg = false;
	protected int operatorPriority = 0;

	protected int transparencyMode = 0; // SMSOptionNeedTransparency

	protected String foreword;
	protected String ottContentAddOaddr;

	protected boolean supportIncomingAntiSpam = false;
	protected boolean supportOutgoingAntiSpam = false;

	protected boolean supportIncomingBinary = false;
	protected boolean supportOutgoingBinary = false;
	protected boolean supportIncomingA2P = false;
	protected boolean supportOutgoingA2P = false;
	protected boolean isSmppMapErrCod4DR = false;

	protected int incomingThrottlingNum = 0;
	
	/** if the flag is true, 
	 * the cust can apply dynamic incoming throttle quota.
	 * <p> dynamic value
	 * */
	protected volatile boolean applyInThrottleFlag = true;
	
	protected int outgoingThrottlingNum = 0;
	protected int coreProcessorThrottlingNum = 0;

	protected boolean needAntiSpam = false; // default

	protected boolean checkAlphaWhenReplaceOAddr = false;

	protected boolean matchFullWhenRouteReplaceOAddr = false;

	protected int expireTime = 0;
	protected int finalExpireTime = 0;
//	protected String smsOptionRetryPolicy;

	protected boolean bIsHandlePrefix4NewMO = false;
	protected boolean bIsHandlePrefix4DRMO = false;
	protected boolean bIsHandlePrefix4NewMT = false;

	protected ArrayList<Prefix> lNewMoPrefixList = new ArrayList<Prefix>();
	protected ArrayList<Prefix> lDRMoPrefixList = new ArrayList<Prefix>();
	protected ArrayList<Prefix> lNewMTPrefixList = new ArrayList<Prefix>();
	

	protected Hashtable shortCodes = new Hashtable(); // Key: prefix, Value:
	// short code

	//ori number len control, if len >10, cutoff, or len<3, add pri
	//key:min, max, suffix
	protected Map<String, String> oriNumberLenController = new HashMap(); //
	
	protected String shortCodeIfExpand = null;
	// protected ArrayList domainNames = new ArrayList(); // domain names

	protected ArrayList<String> noticePrefix = new ArrayList<String>();
	protected String noticeCustomer;

	// phonePrefix and phoneSubPrefix
	protected ArrayList<String> phonePrefixs = new ArrayList<String>();
	protected Map<String, Integer> phonePrefixsMap = new HashMap();

	public final static String Iusacell_ShortName = "Iusacell_MX";
	public final static String Globe_ShortName = "Globe_PH";
	public final static String TIS_ShortName = "TISDC_IT";

	protected Properties extProperties = new Properties();
	protected int pendingAliveCount = 3;
	protected ArrayList<String[]> alSenderMapping = new ArrayList<String[]>();

	protected int reconnectInterval = 60 * 1000;
	protected boolean isDRStatusInOptionPara = false;

	// Consider Globe (SingleConnection) will use the protocol value
	protected String protocol;

	// add for CT support KSC5601
	protected List<String> convertToBinaryCharsets = new ArrayList<String>();
	// Eagle
	protected String removeRecPrefix = null;

	// SMPP
	protected boolean bSmppIsPutMsgId4DSR = false;
	protected boolean bSmppIsGenHexMsgId = false;
	protected boolean chlSMPPMsgIDParse = false;
	protected boolean bSmppIsPadZero4SR = true;
	protected boolean drSwapAddr = true;
	protected int dSmppParaTonFlag = 0; //0= ton=1 npi=1;1=ton=5,npi=0;2 ton=0;npi=0
	protected int deliveryReportMode = -1;
	protected boolean dealExceptionDR = true;
	protected String chlAcctNamer = null;
	protected String chlPasswordr = null;
	protected String port2 = null; // recevier port,
	protected int clientSessionNumber = 1;
	protected BindMode bindMode = BindMode.Transceiver;

	protected String serviceType = null;
	protected String systemType = null;
	protected byte sourceAddrTon = 1;
	protected byte sourceAddrNpi = 1;
	protected byte addrTon = 0;
	protected byte addrNpi = 0;
	protected byte destAddrTon = 1;
	protected byte destAddrNpi = 1;
	protected byte priorityFlag = 1;
	protected byte replaceIfFlag = 0;
	protected byte protocolId = 0;
	protected SMPPVersion smppVersion;
	// added for Spring by Will
	protected String senderId = "";

	protected int smppReceiveTimeout = 40 * 1000;

	protected boolean supportDCS = false; // added by Jianming for Polar
											// wireless, v1.0.1
	protected boolean needReceiptedMsgId = false; // added by Jianming for Polar
													// wireless, v1.0.1
	protected TimeZone smsOptionTimeZone = TimeZone.getTimeZone("GMT");

	// added by King for WeMedia
	protected int msgQueryInterval = 60000;
	protected String httpQueryMessageFlag = "";
	// added by King for http method
	protected String httpMethod = "";
	protected String httpQueryDRMethod = "";

	// added by King for Http custom parameter
	protected String SMSOptionHttpCustomParameter = "";

	// added by King for choice session

	protected String SMSOptionChoiceSecurityHttpSession = "";

	// add SMSOptionHttpType item for Emay
	protected String SMSOptionHttpType = "";

	// add specialServiceNumList for 2 way
	protected String[] specialServiceNumList = null;
	
	protected boolean SmsOptionIsVirtualDC = false;
	
	protected boolean parseValidityPeriod;
	protected boolean transferValidityPeriod;
	protected boolean isRelateValidityPeriod;

	
	protected int smsOptionIncomingGSM7bit = 0;
	protected int smsOptionOutgoingGSM7bit = 0;
	protected boolean smsOptionForceTrans2ASCII = false;
	protected boolean smsOptionConvert2GSM7bit = false;

	protected boolean smsOptionIsMaxASCIILenTo7BitFlag = false;
	
	//use to distinguish https or http 
	protected boolean smsOptionIsSupportHttps = false;

	// check duplicate message configurations
	protected List<Integer> duplicateMsgServiceTypeIDList = new ArrayList<Integer>();
	protected int duplicateMsgPeriod = 0;
	protected boolean checkDuplicateMsgContent = false;
	
	protected boolean parseServiceTypeID = false;

	protected String smsOptionTemplateSignature = null;
	
	// schedule delivery: for SMPP only
	protected boolean parseScheduleDeliveryTime;
	
	// schedule delivery time window
	protected int deliveryStartHH = -1;
	protected int deliveryStartmm = -1;
	protected int deliveryEndHH = -1;
	protected int deliveryEndmm = -1;

	protected int MCCMNCLength = 5;
	
	//serviceType for customer routing
	protected String customerServiceType="all";
	protected int customerSupportLength=510;
	
	//support ajdust success DR ratio.
	protected int drSucRatio=0; //adjust in dr by out dr
	protected int blackholeDRSucRatio=0; //adjust blackhole dr in dr by out dr
	protected int drBiasRatio=0; //adjust in dr by out dr
	protected int sendDelayDRImmediately=0; //0=normal, 1= immediately send, 2=not sent. default =0
	protected String drDelayTimeInSec; //adjust in dr by out dr
	protected boolean smsOptionSendFakeDR = false;
    protected boolean smsOptionSendDRByInSubmitInCU = false;
	protected boolean smsOptionSendDRBeforeOutSubmitInDC = false;
	protected ArrayList<String> smsRecPrefixForFakeDR = new ArrayList<String>();
	//send fake dr by insubmt with country
	protected ArrayList<String> smsSendFakeDRByInSubmitRecPrefixInCU = new ArrayList<String>();
	//support gateway charge feature
	protected boolean smsOptionChargeByGateWay = false; 
	protected boolean smsOptionChargeCountryByGateWay = false; 
	//disable system auto adjust the throttling number
	protected boolean smsOptionDisableAutoAdjustThrottlingNumFlag = false; 	
	protected List<Integer> smsOptionSubmitRetryCode= new ArrayList<Integer>();
    protected List<String> smsOptionDRNoCreditCode= new ArrayList<String>();
    protected List<Integer> smsOptionDRRerouteCode= new ArrayList<Integer>();
	protected List<Integer> smsOptionSubmitErrorCodesForFakeDR= new ArrayList<Integer>();
	protected List<String> smsOptionRejectRegions= new ArrayList<String>();
	protected Map<String, Map<String, String>> smsOptionReplaceContent = new HashMap<String, Map<String,String>>();
	
	protected boolean smsOptionOnlySupportDRRetry = false; 
	protected boolean smsOptionRemoveUdh = false; 
	protected boolean drSuccForTemplateReplaceFailed = false; 
	protected boolean smsOptionAdvancedSenderReplacement = false; 
	protected boolean needDrReroute = false; 
	protected String role = ""; 
	protected boolean needCheckSession = false;
	protected boolean needStatisticMsgCount = false;
	protected boolean needStatisticMsgCountByCountry = false;
	protected boolean needCheckMsgSize = false;
	
	protected boolean needSupportBlackList = false;
	//support max inmsgid for reslove duplicate inmsgid issue. 20230602
	protected boolean isSupportMaxInMsgID = false;
	
	private int maxRerouteDRDelayTimeInSec=0;
	private int smsOptionRecipientMaxSendCountIn24H=0;
	
	//VIP,NonPA,PA for SG
	private String customerTypeBySender;
	
	private SslConfiguration sslConfiguration = null;
	
	private Cache<Object, Object> reicipientCache  = null;
	private Cache<Object, Object> reicipienSecondRoutingtCache  = null;
	protected boolean smsOptionRecipientNotContinueFilter = false;
	protected boolean smsOptionNeedSupportSecondRouting = false;
	
	protected boolean smsOptionWrMonitorCDR = false;	
	protected Map<String, TreeRangeMap<Integer, String>> smsRSupportLenghtCheck = new HashMap<String, TreeRangeMap<Integer, String>>();
	//need do second routing recipient prefix
    protected ArrayList<String> smsSecondRoutingRecPrefix = new ArrayList<String>();
    protected ArrayList<String> enumRoutingRecPrefix = new ArrayList<String>();
    protected ArrayList<String> enumIR21RoutingRecPrefix = new ArrayList<String>();
    
    private String enumRoutingURL;
    private String enumIR21RoutingURL;
    
    protected Map<String, List<Integer>> recipNumberLenCheck = new HashMap();
    private boolean isRecipNumberLenCheck = false;
    private boolean isNotSupportUnknowDRStatus = false;
    protected Map<String, String> drStatusMapping = new HashMap();
    protected Map<Integer, Integer> submitNotPaidStatusMapping = new HashMap();
    private String templatecoderegex;
	public int getMCCMNCLength() {
		return MCCMNCLength;
	}

	public void setMCCMNCLength(int length) {
		MCCMNCLength = length;
	}
	

	
	public boolean isSMSOptionIsSupportHttps() {
		return smsOptionIsSupportHttps;
	}

	public void setSMSOptionIsSupportHttps(boolean sMSOptionIsSupportHttps) {
		smsOptionIsSupportHttps = sMSOptionIsSupportHttps;
	}


	public boolean isSmsOptionIsMaxASCIILenTo7BitFlag() {
		return smsOptionIsMaxASCIILenTo7BitFlag;
	}

	public void setSmsOptionIsMaxASCIILenTo7BitFlag(
			boolean smsOptionIsMaxASCIILenTo7BitFlag) {
		this.smsOptionIsMaxASCIILenTo7BitFlag = smsOptionIsMaxASCIILenTo7BitFlag;
	}

	
	public boolean isSmsOptionIsVirtualDC() {
		return SmsOptionIsVirtualDC;
	}

	public void setSmsOptionIsVirtualDC(boolean smsOptionIsVirtualDC) {
		SmsOptionIsVirtualDC = smsOptionIsVirtualDC;
	}

	public String[] getSpecialServiceNumList() {
		return specialServiceNumList;
	}

	public void setSpecialServiceNumList(String[] specialServiceNumList) {
		this.specialServiceNumList = specialServiceNumList;
	}

	// added by Elton for Chuangshimandao
	protected String SMSOptionHttpSpecialServiceNum = "";

	public String getSMSOptionHttpSpecialServiceNum() {
		return SMSOptionHttpSpecialServiceNum;
	}

	public void setSMSOptionHttpSpecialServiceNum(
			String optionHttpSpecialServiceNum) {
		SMSOptionHttpSpecialServiceNum = optionHttpSpecialServiceNum;
	}

	protected String passwdEncryptMethod = null;
	protected boolean isUrlEncodingTwice = true;

	public String getHttpMethod() {
		return httpMethod;
	}

	public void setHttpMethod(String httpMethod) {
		this.httpMethod = httpMethod;
	}

	public String getHttpQueryMessageFlag() {
		return httpQueryMessageFlag;
	}

	public void setHttpQueryMessageFlag(String queryDR) {
		this.httpQueryMessageFlag = queryDR;
	}

	public int getMsgQueryInterval() {
		return msgQueryInterval;
	}

	public void setMsgQueryInterval(int drQueryInterval) {
		this.msgQueryInterval = drQueryInterval;
	}

	/**
	 * use to get rid of suffix when assemble concatenated message
	 */
	protected String lastHopSplitSuffixFormat = null;

	public String getLastHopSplitSuffixFormat() {
		return lastHopSplitSuffixFormat;
	}

	public void setLastHopSplitSuffixFormat(String lastHopSplitSuffixFormat) {
		this.lastHopSplitSuffixFormat = lastHopSplitSuffixFormat;
	}

	public A2PCustomerInfo() {
		super();

	}

	public String getProtocol() {
		return protocol;
	}

	public void setProtocol(String protocol) {
		this.protocol = protocol;
	}

	public ArrayList<String[]> getAlSenderMapping() {
		return alSenderMapping;
	}

	public void setAlSenderMapping(ArrayList<String[]> alSenderMapping) {
		this.alSenderMapping = alSenderMapping;
	}

	public int getPendingAliveCount() {
		return pendingAliveCount;
	}

	public void setPendingAliveCount(int pendingAliveCount) {
		this.pendingAliveCount = pendingAliveCount;
	}

	public void setSmppMapErrCod4DR(boolean value) {
		isSmppMapErrCod4DR = value;
	}

	public boolean isSmppMapErrCod4DR() {
		return isSmppMapErrCod4DR;
	}

	public String getExtProperty(String attributeName) {
		return extProperties.getProperty(attributeName);
	}

	public Properties getExtProperty() {
		return extProperties;
	}

	public void addExtProperties(String attributeName, String value) {
		this.extProperties.setProperty(attributeName, value);
	}

	public String resolveShortCode(String recipientPhoneNum) {
		ArrayList keyList = new ArrayList(shortCodes.keySet());
		// sort the key (prefixes) first
		Collections.sort(keyList);
		// then loop them through in reverse order
		for (int i = keyList.size() - 1; i >= 0; i--) {
			if (recipientPhoneNum.startsWith((String) keyList.get(i))) {
				return (String) shortCodes.get(keyList.get(i));
			}
		}
		return null;
	}

	public ArrayList<String> getNoticePrefix() {
		return noticePrefix;
	}

	public String getNoticeCustomer() {
		return noticeCustomer;
	}

	public void setNoticePrefix(ArrayList noticePrefix) {
		this.noticePrefix = noticePrefix;
	}

	public void setNoticeCustomer(String noticeCustomer) {
		this.noticeCustomer = noticeCustomer;
	}

	public String getShortCodeIfExpand() {
		return shortCodeIfExpand;
	}

	public void setShortCodeIfExpand(String shortCodeIfExpand) {
		this.shortCodeIfExpand = shortCodeIfExpand;
	}

	public boolean isNeedAntiSpam() {
		return needAntiSpam;
	}

	public boolean isCheckAlphaWhenReplaceOAddr() {
		return checkAlphaWhenReplaceOAddr;
	}

	public void setCheckAlphaWhenReplaceOAddr(boolean checkAlphaWhenReplaceOAddr) {
		this.checkAlphaWhenReplaceOAddr = checkAlphaWhenReplaceOAddr;
	}

	public boolean isMatchFullWhenRouteReplaceOAddr() {
		return matchFullWhenRouteReplaceOAddr;
	}

	public void setMatchFullWhenRouteReplaceOAddr(
			boolean matchFullWhenRouteReplaceOAddr) {
		this.matchFullWhenRouteReplaceOAddr = matchFullWhenRouteReplaceOAddr;
	}

	public void setNeedAntiSpam(boolean needAntiSpam) {
		this.needAntiSpam = needAntiSpam;
	}

	public boolean isSupportOutgoingBinary() {
		return supportOutgoingBinary;
	}

	public void setSupportOutgoingBinary(boolean supportOutgoingBinary) {
		this.supportOutgoingBinary = supportOutgoingBinary;
	}

	public boolean isSupportOutgoingA2P() {
		return supportOutgoingA2P;
	}

	public void setSupportOutgoingA2P(boolean supportOutgoingA2P) {
		this.supportOutgoingA2P = supportOutgoingA2P;
	}

	public void setAllowOriPrefixList(Pattern pattern) {
		this.pAllowOriPrefixList = pattern;
	}

	public Pattern getAllowOriPrefixList() {
		return this.pAllowOriPrefixList;
	}

	public boolean isServerEnquireLink() {
		return isServerEnquireLink;
	}

	public void setIsServerEnquireLink(boolean isServerEnquireLink) {
		this.isServerEnquireLink = isServerEnquireLink;
	}

	public String getChlQueue() {
		return chlQueue;
	}

	public void setChlQueue(String chlQueue) {
		this.chlQueue = chlQueue;
	}

	public boolean isHandlePrefix4DRMO() {
		return bIsHandlePrefix4DRMO;
	}

	public boolean isHandlePrefix4NewMO() {
		return bIsHandlePrefix4NewMO;
	}

	public boolean isHandlePrefix4NewMT() {
		return bIsHandlePrefix4NewMT;
	}

	public ArrayList getLDRMoPrefixList() {
		return lDRMoPrefixList;
	}

	public ArrayList getLNewMoPrefixList() {
		return lNewMoPrefixList;
	}

	public ArrayList getLNewMTPrefixList() {
		return lNewMTPrefixList;
	}

	public void setBIsHandlePrefix4DRMO(boolean bIsHandlePrefix4DRMO) {
		this.bIsHandlePrefix4DRMO = bIsHandlePrefix4DRMO;
	}

	public void setBIsHandlePrefix4NewMO(boolean bIsHandlePrefix4NewMO) {
		this.bIsHandlePrefix4NewMO = bIsHandlePrefix4NewMO;
	}

	public void setBIsHandlePrefix4NewMT(boolean bIsHandlePrefix4NewMT) {
		this.bIsHandlePrefix4NewMT = bIsHandlePrefix4NewMT;
	}

	public void addPrefix4NewMoList(Prefix prefix) {
		this.lNewMoPrefixList.add(prefix);
		if (this.bIsHandlePrefix4NewMO == false) {
			this.bIsHandlePrefix4NewMO = true;
		}
	}

	public void addPrefix4DrMoList(Prefix prefix) {
		this.lDRMoPrefixList.add(prefix);
		if (this.bIsHandlePrefix4DRMO == false) {
			this.bIsHandlePrefix4DRMO = true;
		}

	}

	public void addPrefix4NewMtList(Prefix prefix) {
		this.lNewMTPrefixList.add(prefix);
		if (this.bIsHandlePrefix4NewMT == false) {
			this.bIsHandlePrefix4NewMT = true;
		}
	}

	public void handlePrefix4NewMo(String[] address) {
		for (int i = 0; i < this.lNewMoPrefixList.size(); i++) {
			Prefix pre = this.lNewMoPrefixList.get(i);
			// log.info("pre:"+pre.toString());
			pre.handle(address);
		}
	}

	public void handlePrefix4NewMt(String[] address) {
		for (int i = 0; i < this.lNewMTPrefixList.size(); i++) {
			Prefix pre = this.lNewMTPrefixList.get(i);
			// log.info("pre:"+pre.toString());
			pre.handle(address);
		}
	}

	public void handlePrefix4DrMo(String[] address) {

		for (int i = 0; i < this.lDRMoPrefixList.size(); i++) {
			Prefix pre = this.lDRMoPrefixList.get(i);
			pre.handle(address);
		}
	}

	protected Pattern getPattern(String stringToken) {
		if (stringToken != null && !stringToken.equals("")) {
			StringBuffer buffer = new StringBuffer("(");
			String[] token = stringToken.split(",");
			for (int i = 0; i < token.length; i++) {
				if (!token[i].trim().equals("")) {
					buffer.append(token[i].trim());
					if (i < token.length - 1) {
						buffer.append("|");
					}
				}
			}
			if (buffer.length() > 1) {
				buffer.append(")\\S*");
				return Pattern.compile(buffer.toString());
			}
		}
		return null;
	}

	public ArrayList getRecNumberLens() {
		return recNumberLens;
	}

	public int getExpireTime() {
		return expireTime;
	}

	public int getFinalExpireTime() {
		return finalExpireTime;
	}

//	public String getInClientPull() {
//		return inClientPull;
//	}

	public MessageMode getMessageMode() {
		return messageMode;
	}

	public ArrayList getNumberLens() {
		return oriNumberLens;
	}

	public ArrayList getOriNumberLens() {
		return oriNumberLens;
	}

	public ArrayList getRecUserPhoneLens() {
		return recUserPhoneLens;
	}

	public void setUdhConcatenated(boolean udhConcatenated) {
		this.udhConcatenated = udhConcatenated;
	}

	public int getSSID() {
		return ssid;
	}

	public boolean isParseOoperator() {
		return parseOoperator;
	}

	public String getReceiverQueue() {
		return receiverQueue;
	}

	public String getServerID() {
		return serverID;
	}

	public Hashtable getShortCodes() {
		return shortCodes;
	}

	public int getOperatorPriority() {
		return operatorPriority;
	}

	public boolean isSupportDeliverReport() {
		return supportDeliverReport;
	}

	public boolean getSupportIncomingA2P() {
		return supportIncomingA2P;
	}

	public boolean isSupportIncomingBinary() {
		return supportIncomingBinary;
	}

	public boolean isSupportIncomingAntiSpam() {
		return this.supportIncomingAntiSpam;
	}

	public boolean isSupportOutgoingAntiSpam() {
		return this.supportOutgoingAntiSpam;
	}

	public int getConfigedIncomingThrottlingNum() {
		if (incomingThrottlingNum > 0) {
			return incomingThrottlingNum;
		} else {
			return GmmsUtility.getInstance().getDefaultCustIncomingThreshold();
		}
	}
	
	public int getIncomingThrottlingNum() {
		return incomingThrottlingNum;
	}

	public int getOutgoingThrottlingNum() {
		return outgoingThrottlingNum;
	}	

	public int getCoreProcessorThrottlingNum() {
		return coreProcessorThrottlingNum;
	}

	public void setCoreProcessorThrottlingNum(int coreProcessorThrottlingNum) {
		this.coreProcessorThrottlingNum = coreProcessorThrottlingNum;
	}

	public String getOutClientPull() {
		return outClientPull;
	}

	public int getConnectionSilentTime() {
		return connectionSilentTime;
	}

	public String getA2PPlayerType() {
		return a2pPlayerType;
	}

	public int getMinReceiverNumber() {
		return minReceiverNumber;
	}

	public int getMinSenderNumber() {
		return minSenderNumber;
	}

	public int getMaxSenderNumber() {
		return maxSenderNumber;
	}

	public int getSenderThroughput() {
		return this.senderThroughput;
	}

	public int getSenderAliveTime() {
		return this.senderAliveTime;
	}

	public double getSenderRiseRatio() {
		return this.senderRiseRatio;
	}

	public List getSupportedCharsets() {
		if (supportedCharsets == null || supportedCharsets.size() == 0)
			return null;

		return supportedCharsets;
	}

	public List getConvertToBinaryCharsets() {
		if (convertToBinaryCharsets == null
				|| convertToBinaryCharsets.size() == 0)
			return null;

		return convertToBinaryCharsets;
	}

	public int getWindowSize() {
		return windowSize;
	}

	public String getForeword() {
		return foreword;
	}

	public int getOriMinNumberLen() {
		return oriMinNumberLen;
	}

	public int getRecMinNumberLen() {
		return recMinNumberLen;
	}

	public int getConnectionType() {
		return connectionType;
	}

	public String getService() {
		return service;
	}

	public void setDesNumberLens(ArrayList desNumberLens) {
		this.recNumberLens = desNumberLens;
	}

	// public void setDomainNames(ArrayList domainNames) {
	// this.domainNames = domainNames;
	// }

	public void setExpireTime(int expireTime) {
		this.expireTime = expireTime;
	}

	public void setFinalExpireTime(int finalExpireTime) {
		this.finalExpireTime = finalExpireTime;
	}

//	public void setInClientPull(String inClientPull) {
//		this.inClientPull = inClientPull;
//	}

	public void setMessageMode(MessageMode messageMode) {
		this.messageMode = messageMode;
	}

	public void setOriNumberLens(ArrayList oriNumberLens) {
		this.oriNumberLens = oriNumberLens;
	}

	public void setParseOoperator(boolean parseOoperator) {
		this.parseOoperator = parseOoperator;
	}

	public void setOperatorPriority(int operatorPriority) {
		this.operatorPriority = operatorPriority;
	}

	public void setReceiverQueue(String receiverQueue) {
		this.receiverQueue = receiverQueue;
	}

	public void setServerID(String serverID) {
		this.serverID = serverID;
	}

	public void setShortCodes(Hashtable shortCodes) {
		this.shortCodes = shortCodes;
	}

	public void setSsid(int ssid) {
		this.ssid = ssid;
	}

	public void setSupportDeliverReport(boolean supportDeliverReport) {
		this.supportDeliverReport = supportDeliverReport;
	}

	public void setSupportIncomingA2P(boolean supportIncomingA2P) {
		this.supportIncomingA2P = supportIncomingA2P;
	}

	public void setSupportIncomingBinary(boolean supportIncomingBinary) {
		this.supportIncomingBinary = supportIncomingBinary;
	}

	public void setOutClientPull(String outClientPull) {
		this.outClientPull = outClientPull;
	}

	public void setConnectionSilentTime(int connectionSilentTime) {
		this.connectionSilentTime = connectionSilentTime;
	}

	public void setA2PPlayerType(String playerType) {
		this.a2pPlayerType = playerType;
	}

	public void setMinReceiverNumber(int receiverNumber) {
		this.minReceiverNumber = receiverNumber;
	}

	public void setMinSenderNumber(int senderNumber) {
		this.minSenderNumber = senderNumber;
	}

	public void setMaxSenderNumber(int senderNumber) {
		this.maxSenderNumber = senderNumber;
	}

	public void setWindowSize(int windowSize) {
		this.windowSize = windowSize;
	}

	public void setForeword(String foreword) {
		this.foreword = foreword;
	}

	public int getEnquireLinkTime() {
		return enquireLinkTime;
	}

	public int getBufferTimeout() {
		return bufferTimeout;
	}

	public boolean isUdhConcatenated() {
		return udhConcatenated;
	}

	public boolean isSupportConcatenatedMsg() {
		return supportConcatenatedMsg;
	}

	public int getTransparencyMode() {
		return transparencyMode;
	}

	public void setOriMinNumberLen(int oriMinNumberLen) {
		this.oriMinNumberLen = oriMinNumberLen;
	}

	public void setRecMinNumberLen(int recMinNumberLen) {
		this.recMinNumberLen = recMinNumberLen;
	}

	public void setConnectionType(int connectionType) {
		this.connectionType = connectionType;
	}

	public void setService(String service) {
		this.service = service;
	}

	public void setEnquireLinkTime(int enquireLinkTime) {
		this.enquireLinkTime = enquireLinkTime;
	}

	public void setBufferTimeout(int bufferTimeout) {
		this.bufferTimeout = bufferTimeout;
	}

	public void setIsEnquireLink(boolean isEnquireLink) {
		this.isEnquireLink = isEnquireLink;
	}

	public void setSupportConcatenatedMsg(boolean supportConcatenatedMsg) {
		this.supportConcatenatedMsg = supportConcatenatedMsg;
	}

	public void setTransparencyMode(int transparencyMode) {
		this.transparencyMode = transparencyMode;
	}

	public void setReconnectInterval(int reconnectInterval) {
		this.reconnectInterval = reconnectInterval;
	}
	
	
	public boolean isSupportTemplateParemeterDelivery() {
		return supportTemplateParemeterDelivery;
	}

	public void setSupportTemplateParemeterDelivery(boolean supportTemplateParemeterDelivery) {
		this.supportTemplateParemeterDelivery = supportTemplateParemeterDelivery;
	}

	public void setIsEnquireLink(String enquireLink) {
		if ("yes".equalsIgnoreCase(enquireLink))
			this.isEnquireLink = true;
		else
			this.isEnquireLink = false;

	}

	public String getShortName() {
		return shortName;
	}

	public int getReconnectInterval() {
		return reconnectInterval;
	}

	public void setNameShort(String shortName) {
		this.shortName = shortName;
	}

	public boolean isEnquireLink() {
		return isEnquireLink;
	}

	//
	// public ArrayList getDomainNamesList() {
	// return domainNames;
	// }

	public boolean isChlSMPPMsgIDParse() {
		return chlSMPPMsgIDParse;
	}

	public void setChlSMPPMsgIDParse(boolean chlSMPPMsgIDParse) {
		this.chlSMPPMsgIDParse = chlSMPPMsgIDParse;
	}

	public void setSMPPIsPadZero4SR(boolean isPadZero4SR) {
		this.bSmppIsPadZero4SR = isPadZero4SR;
	}

	public boolean getSMPPIsPadZero4SR() {
		return this.bSmppIsPadZero4SR;
	}

	public void setSMPPIsGenHexMsgId(boolean isGenDec) {
		this.bSmppIsGenHexMsgId = isGenDec;
	}

	public boolean getSMPPIsGenHexMsgId() {
		return this.bSmppIsGenHexMsgId;

	}

	public void setSMPPIsPutMsgId4DSR(boolean isPutMsgId) {
		this.bSmppIsPutMsgId4DSR = isPutMsgId;
	}

	public boolean getSMPPIsPutMsgId4DSR() {
		return this.bSmppIsPutMsgId4DSR;
	}

	public boolean isDealExceptionDR() {
		return dealExceptionDR;
	}

	public int getDeliveryReportMode() {
		return deliveryReportMode;
	}

	public boolean isDrSwapAddr() {
		return drSwapAddr;
	}

	public void setDealExceptionDR(boolean dealExceptionDR) {
		this.dealExceptionDR = dealExceptionDR;
	}

	public void setDeliveryReportMode(int deliveryReportMode) {
		this.deliveryReportMode = deliveryReportMode;
	}

	public void setDrSwapAddr(boolean drSwapAddr) {
		this.drSwapAddr = drSwapAddr;
	}

	public byte getAddrNpi() {
		return addrNpi;
	}

	public byte getAddrTon() {
		return addrTon;
	}

	public String getChlAcctNamer() {
		return chlAcctNamer;
	}

	public String getChlPasswordr() {
		return chlPasswordr;
	}

	public int getClientSessionNumber() {
		return clientSessionNumber;
	}

	public byte getDestAddrNpi() {
		return destAddrNpi;
	}

	public byte getDestAddrTon() {
		return destAddrTon;
	}

	public String getPort2() {
		return port2;
	}

	public byte getPriorityFlag() {
		return priorityFlag;
	}

	public SMPPVersion getSMPPVersion() {
		return smppVersion;
	}

	public byte getReplaceIfFlag() {
		return replaceIfFlag;
	}

	public byte getProtocolId() {
		return protocolId;
	}

	public int getSmppReceiveTimeout() {
		return this.smppReceiveTimeout;
	}

	public String getServiceType() {
		return serviceType;
	}

	public byte getSourceAddrNpi() {
		return sourceAddrNpi;
	}

	public byte getSourceAddrTon() {
		return sourceAddrTon;
	}

	public String getSystemType() {
		return systemType;
	}

	public BindMode getBindMode() {
		return bindMode;
	}

	public void setAddrNpi(byte addrNpi) {
		this.addrNpi = addrNpi;
	}

	public void setAddrTon(byte addrTon) {
		this.addrTon = addrTon;
	}

	public void setChlAcctNamer(String chlAcctNamer) {
		this.chlAcctNamer = chlAcctNamer;
	}

	public void setChlPasswordr(String chlPasswordr) {
		this.chlPasswordr = chlPasswordr;
	}

	public void setClientSessionNumber(int clientSessionNumber) {
		this.clientSessionNumber = clientSessionNumber;
	}

	public void setDestAddrNpi(byte destAddrNpi) {
		this.destAddrNpi = destAddrNpi;
	}

	public void setDestAddrTon(byte destAddrTon) {
		this.destAddrTon = destAddrTon;
	}

	public void setPort2(String port2) {
		this.port2 = port2;
	}

	public void setPriorityFlag(byte priorityFlag) {
		this.priorityFlag = priorityFlag;
	}

	public void setSMPPVersion(SMPPVersion smppVersion) {
		this.smppVersion = smppVersion;
	}

	public void setReplaceIfFlag(byte replaceIfFlag) {
		this.replaceIfFlag = replaceIfFlag;
	}

	public void setProtocolId(byte protocolId) {
		this.protocolId = protocolId;
	}

	public void setSmppReceiveTimeout(int time) {
		this.smppReceiveTimeout = time;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}

	public void setSourceAddrNpi(byte sourceAddrNpi) {
		this.sourceAddrNpi = sourceAddrNpi;
	}

	public void setSourceAddrTon(byte sourceAddrTon) {
		this.sourceAddrTon = sourceAddrTon;
	}

	public void setSystemType(String systemType) {
		this.systemType = systemType;
	}

	public void setBindMode(BindMode bindMode) {
		this.bindMode = bindMode;
	}

	public void assignValue(A2PCustomerConfig cfg, Map<String, Integer> shorNameSsidMap)
			throws CustomerConfigurationException {
		ssid = cfg.getMandatoryInt("CCBSSID");
		iosmsSsid = cfg.getInt("IOSMSSSID", -1);
		customerId = cfg.getInt("CustomerID", -1);
		shortName = cfg.getShortNameWithoutService();
		String temp = cfg.getString("ConnectionType");
		if (temp != null && !temp.equalsIgnoreCase("")) {
			setConnectionType(temp);
		}
		service = cfg.getString("Service");
		if (service == null) {
			service = "A2P-SMS";
		}
		passwdEncryptMethod = cfg.getString("SMSOptionHttpPasswdEncryptMethod");
		isUrlEncodingTwice = cfg.getBool("SMSOptionHttpUrlEncodingTwice", true);
		// added for Spring by Will--begin
		senderId = cfg.getString("SenderID");
		// added for Spring by Will--end
		serverID = cfg.getString("ServerID");
		a2pPlayerType = cfg.getString("A2PPlayerType");
		receiverQueue = cfg.getString("ReceiverQueue");
		chlQueue = cfg.getString("ChlQueue");
		messageMode = MessageMode.parse(cfg.getString("MessageMode"));

		// added for WeMedia by king -begin
		this.msgQueryInterval = cfg.getInt("SMSOptionQueryMsgIntervalTime", 60) * 1000;
		this.httpQueryMessageFlag = cfg.getString(
				"SMSOptinIsSupportHttpQueryMessage", "");
		
//		inClientPull = cfg.getString("InClientPull");
		
		String inClientPullStr = cfg.getString("InClientPull");
		if (inClientPullStr != null && !"".equals(inClientPullStr.trim())) {
			try {
				inClientPull = Integer.parseInt(inClientPullStr);
			} catch (NumberFormatException e) {
				inClientPull = 0;
			}
		} else {
			inClientPull = 0;
		}
		
		outClientPull = cfg.getString("OutClientPull", "0");
		// added for WeMedia by king -end
		// added for http method by king
		this.httpMethod = cfg.getString("SMSOptionHttpMethod", "post");
		this.httpQueryDRMethod = cfg.getString("SMSOptionQueryDRHttpMethod", "post");

		// added for CICN by king
		this.SMSOptionHttpCustomParameter = cfg.getString(
				"SMSOptionHttpCustomParameter", "");
		supportDeliverReport = cfg.getBool("ChlIsSupportDeliveryReport", false);
		supportTemplateParemeterDelivery = cfg.getBool("ChlIsSupportTemplateParemeterDelivery", false);
		supportTemplateParemeterSignature = cfg.getBool("ChlIsSupportTemplateParemeterSignature", false);

		// add SMSOptionHttpType item for Emay
		this.SMSOptionHttpType = cfg.getString("SMSOptionHttpType", "MO");

		// Virtual DC
		this.SmsOptionIsVirtualDC = cfg.getBool("SMSOptionPiggyBackVirtualDC",false);
		
		this.parseValidityPeriod = cfg.getBool("SMSOptionParseValidityPeriod", false);
		this.transferValidityPeriod = cfg.getBool("SMSOptionTransferValidityPeriod", false);
		this.isRelateValidityPeriod = cfg.getBool("SMSOptionIsRelateValidityPeriod", false);
		this.needSupportBlackList = cfg.getBool("SMSOptionSupportBlackList", false);
		this.isSupportMaxInMsgID = cfg.getBool("SMSOptionSupportMaxInMsgID", false);
		
		//for SSL
		this.smsOptionIsSupportHttps = cfg.getBool("SMSOptionIsSupportHttps", false);	
		
		this.MCCMNCLength = cfg.getInt("MCCMNCLength",5);
		this.customerSupportLength = cfg.getInt("CustomerSupportLength",510);
		this.customerServiceType = cfg.getString("CustomerServiceType","all");
		//recipNumberLenCheck
		//isRecipNumberLenCheck
		this.isRecipNumberLenCheck = cfg.getBool("SMSOptionIsRecipitLenCheck", false);
		isEnableGuavaBuffer = cfg.getBool("EnableGuavaBuffer", false);
		//drMapping
		String drUnknownMappingStatus = cfg.getString("SMSOptionDRStatusMapping");
		if(drUnknownMappingStatus!=null && !"".equalsIgnoreCase(drUnknownMappingStatus)) {
			try {
				String [] tempdrUnknownMappingStatusList = drUnknownMappingStatus.split("\\|");
				for (int i = 0; i < tempdrUnknownMappingStatusList.length; i++) {
					String [] tempSmsOptionSubmitRetryCodeArray = tempdrUnknownMappingStatusList[i].split(":");					
					drStatusMapping.put(tempSmsOptionSubmitRetryCodeArray[0], tempSmsOptionSubmitRetryCodeArray[1]);
				}
			} catch (Exception e) {
				log.error("load SMSOptionDRStatusMapping parameter failed, SMSOptionDRStatusMapping={}", drUnknownMappingStatus);
			}
		}else {
			drStatusMapping.clear();
		}
		
		//submitNotPaidStatusMapping
		String submitNotPaidMappingStatus = cfg.getString("SMSOptionSubmitNotPaidMapping");
		if(submitNotPaidMappingStatus!=null && !"".equalsIgnoreCase(submitNotPaidMappingStatus)) {
			try {
				String [] tempdrUnknownMappingStatusList = submitNotPaidMappingStatus.split("\\|");
				for (int i = 0; i < tempdrUnknownMappingStatusList.length; i++) {
					String [] tempSmsOptionSubmitRetryCodeArray = tempdrUnknownMappingStatusList[i].split(":");					
					submitNotPaidStatusMapping.put(Integer.parseInt(tempSmsOptionSubmitRetryCodeArray[0].trim()), Integer.parseInt(tempSmsOptionSubmitRetryCodeArray[1].trim()));
				}
			} catch (Exception e) {
				log.error("load SMSOptionSubmitNotPaidMapping parameter failed, SMSOptionSubmitNotPaidMapping={}", submitNotPaidMappingStatus);
			}
		}else {
			submitNotPaidStatusMapping.clear();
		}
		
		this.isNotSupportUnknowDRStatus = cfg.getBool("SMSOptionIsNotSupportUnknowDRStatus", false);
		
		String tempSMSOptionRecipitLenCheck = cfg.getString("SMSOptionRecipitLenCheck");
		if(tempSMSOptionRecipitLenCheck == null && isRecipNumberLenCheck) {
			tempSMSOptionRecipitLenCheck = GmmsUtility.getInstance().getCommonProperty("SMSOptionRecipitLenCheck");
		}
		if (tempSMSOptionRecipitLenCheck != null
				&& !"".equals(tempSMSOptionRecipitLenCheck)) {
			try {
				String [] tempSmsOptionSubmitRetryCodeList = tempSMSOptionRecipitLenCheck.split("\\|");
				for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
					String [] tempSmsOptionSubmitRetryCodeArray = tempSmsOptionSubmitRetryCodeList[i].split(":");
					String [] lens = tempSmsOptionSubmitRetryCodeArray[1].split(",");
					List<Integer> lenList = new ArrayList();
					for(int t=0; t<lens.length; t++) {
						lenList.add(Integer.parseInt(lens[t].trim()));
					}
					recipNumberLenCheck.put(tempSmsOptionSubmitRetryCodeArray[0], lenList);
				}
			} catch (Exception e) {
				log.error("load SMSOptionRecipitLenCheck parameter failed, SMSOptionRecipitLenCheck={}", tempSMSOptionRecipitLenCheck);
			}
		}else {
			recipNumberLenCheck.clear();
		}
		
		String tempSmsOptionSubmitRetryCode = cfg.getString("SMSOptionSubmitRetryCode");
		if (tempSmsOptionSubmitRetryCode != null
				&& !"".equals(tempSmsOptionSubmitRetryCode)) {
			try {
				String [] tempSmsOptionSubmitRetryCodeList = tempSmsOptionSubmitRetryCode.split(",");
				for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
					smsOptionSubmitRetryCode.add(Integer.parseInt(tempSmsOptionSubmitRetryCodeList[i]));
				}
			} catch (Exception e) {
				log.error("load SMSOptionSubmitRetryCode parameter failed, SMSOptionSubmitRetryCode={}", tempSmsOptionSubmitRetryCode);
			}
		}
		
        String tempSmsOptionDRNoCreditCode = cfg.getString("SMSOptionDRNotPaidCode");
        if (tempSmsOptionDRNoCreditCode != null
                && !"".equals(tempSmsOptionDRNoCreditCode)) {
            try {
                String [] tempSmsOptionSubmitRetryCodeList = tempSmsOptionDRNoCreditCode.split(",");
                for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
                    smsOptionDRNoCreditCode.add(tempSmsOptionSubmitRetryCodeList[i]);
                }
            } catch (Exception e) {
                log.error("load SMSOptionDRNotPaidCode parameter failed, SMSOptionDRNotPaidCode={}", tempSmsOptionDRNoCreditCode);
            }
        }
        
        String tempSmsOptionDRRerouteCode = cfg.getString("SMSOptionDRRerouteCode");
        if (tempSmsOptionDRRerouteCode != null
                && !"".equals(tempSmsOptionDRRerouteCode)) {
            try {
                String [] tempSmsOptionSubmitRetryCodeList = tempSmsOptionDRRerouteCode.split(",");
                for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
                    smsOptionDRRerouteCode.add(Integer.parseInt(tempSmsOptionSubmitRetryCodeList[i]));
                }
            } catch (Exception e) {
                log.error("load SMSOptionDRRerouteCode parameter failed, SMSOptionDRRerouteCode={}", tempSmsOptionDRRerouteCode);
            }
        }
		
		String tempSmsOptionSubmitErrorCodesForFakeDR = cfg.getString("SMSOptionSubmitErrorCodesForFakeDR");
		if (tempSmsOptionSubmitErrorCodesForFakeDR != null
				&& !"".equals(tempSmsOptionSubmitErrorCodesForFakeDR)) {
			try {
				String [] tempSmsOptionSubmitRetryCodeList = tempSmsOptionSubmitErrorCodesForFakeDR.split(",");
				for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
					smsOptionSubmitErrorCodesForFakeDR.add(Integer.parseInt(tempSmsOptionSubmitRetryCodeList[i]));
				}
			} catch (Exception e) {
				log.error("load SMSOptionSubmitErrorCodesForFakeDR parameter failed, SMSOptionSubmitErrorCodesForFakeDR={}", tempSmsOptionSubmitErrorCodesForFakeDR);
			}
		}
		
		String tempSmsOptionRejectRegions = cfg.getString("SMSOptionRejectRegions");
		if (tempSmsOptionRejectRegions != null
				&& !"".equals(tempSmsOptionRejectRegions)) {
			try {
				String [] tempSmsOptionRejectRegionsList = tempSmsOptionRejectRegions.split(",");
				for (int i = 0; i < tempSmsOptionRejectRegionsList.length; i++) {
					smsOptionRejectRegions.add(tempSmsOptionRejectRegionsList[i]);
				}
			} catch (Exception e) {
				log.error("load SMSOptionRejectRegions parameter failed, SMSOptionRejectRegions={}", tempSmsOptionRejectRegions);
			}
		}
		this.SMSOptionHttpSpecialServiceNum = cfg
				.getString("SMSOptionHttpSpecialServiceNum");
		if (SMSOptionHttpSpecialServiceNum != null
				&& !"".equals(SMSOptionHttpSpecialServiceNum)) {
			specialServiceNumList = SMSOptionHttpSpecialServiceNum.split(",");
		}

		A2PCustomerMultiValue supportCharsets = cfg
				.parseMultiValue("SupportedCharsets");
		if (supportCharsets != null) {
			ArrayList<AttrPair> attrs = supportCharsets.getAllAttrs();
			for (AttrPair attr : attrs) {
				this.supportedCharsets.add(attr.getStringValue());
			}
		}

		A2PCustomerMultiValue convertToBinaryCharsets = cfg
				.parseMultiValue("ConvertToBinaryCharsets");
		if (convertToBinaryCharsets != null) {
			ArrayList<AttrPair> attrs = convertToBinaryCharsets.getAllAttrs();
			for (AttrPair attr : attrs) {
				this.convertToBinaryCharsets.add(attr.getStringValue());
			}
		}

		minSenderNumber = cfg.getInt("MinSenderNumber", -1);
		prioritryHanderNumber = cfg.getInt("PrioritryProcesserNumber", 1);
		maxSenderNumber = cfg.getInt("MaxSenderNumber", -1);
		senderThroughput = cfg.getInt("SenderThroughput", 4);
		senderRiseRatio = cfg.getInt("SenderRiseRatio", 20);
		senderAliveTime = cfg.getInt("SenderAliveTime", 5);
		//add for add dr success ratio in 20190201
		drSucRatio = cfg.getInt("SMSOptionDRSuccessRatio", 0);
		blackholeDRSucRatio = cfg.getInt("SMSOptionBlackholeDRSuccessRatio", 0);
		drBiasRatio = cfg.getInt("SMSOptionDRSuccessBiasValue", 0);
		sendDelayDRImmediately = cfg.getInt("SMSOptionSendDelayDRImmediately", 0);
		drDelayTimeInSec = cfg.getString("SMSOptionDRDelayTimeMinMaxInSec", "0");
		smsOptionSendFakeDR = cfg.getBool("SMSOptionSendDRByOutSubmit", false);
		if (cfg.getBool("SMSOptionSendFakeDR", false)) {
			smsOptionSendFakeDR = cfg.getBool("SMSOptionSendFakeDR", false);
		}
		
		String tempSmsRecPrefixForFakeDR = cfg.getString("SMSOptionRecPrefixForFakeDR");
		if (tempSmsRecPrefixForFakeDR != null
				&& !"".equals(tempSmsRecPrefixForFakeDR)) {
			try {
				String [] tempSmsOptionSubmitRetryCodeList = tempSmsRecPrefixForFakeDR.split(",");
				for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
					smsRecPrefixForFakeDR.add(tempSmsOptionSubmitRetryCodeList[i]);
				}
			} catch (Exception e) {
				log.error("load SMSOptionRecPrefixForFakeDR parameter failed, SMSOptionRecPrefixForFakeDR={}", tempSmsRecPrefixForFakeDR);
			}
		}else {
			smsRecPrefixForFakeDR.clear();
		}
		
		
		
		String tempsmsSendFakeDRByInSubmitRecPrefixInCU = cfg.getString("SMSOptionSendFakeDRByInSubmitRecPrefixInCU");
		if (tempsmsSendFakeDRByInSubmitRecPrefixInCU != null
				&& !"".equals(tempsmsSendFakeDRByInSubmitRecPrefixInCU)) {
			try {
				String [] tempSmsOptionSubmitRetryCodeList = tempsmsSendFakeDRByInSubmitRecPrefixInCU.split(",");
				for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
					smsSendFakeDRByInSubmitRecPrefixInCU.add(tempSmsOptionSubmitRetryCodeList[i]);
				}
			} catch (Exception e) {
				log.error("load smsSendFakeDRByInSubmitRecPrefixInCU parameter failed, smsSendFakeDRByInSubmitRecPrefixInCU={}", tempsmsSendFakeDRByInSubmitRecPrefixInCU);
			}
		}else {
			smsSendFakeDRByInSubmitRecPrefixInCU.clear();
		}
		
		String tempsmsSecondRoutingRecPrefix = cfg.getString("SMSOptionSecondRoutingRecPrefix");
		if (tempsmsSecondRoutingRecPrefix != null
				&& !"".equals(tempsmsSecondRoutingRecPrefix)) {
			try {
				String [] tempSmsOptionSubmitRetryCodeList = tempsmsSecondRoutingRecPrefix.split(",");
				for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
					smsSecondRoutingRecPrefix.add(tempSmsOptionSubmitRetryCodeList[i]);
				}
			} catch (Exception e) {
				log.error("load SMSOptionSecondRoutingRecPrefix parameter failed, SMSOptionSecondRoutingRecPrefix={}", tempsmsSecondRoutingRecPrefix);
			}
		}else {
			smsSecondRoutingRecPrefix.clear();
		}
		
		String tempsmsEnumRoutingRecPrefix = cfg.getString("SMSOptionEnumQueryRecPrefix");
		if (tempsmsEnumRoutingRecPrefix != null
				&& !"".equals(tempsmsEnumRoutingRecPrefix)) {
			try {
				String [] tempSmsOptionSubmitRetryCodeList = tempsmsEnumRoutingRecPrefix.split(",");
				for (int i = 0; i < tempSmsOptionSubmitRetryCodeList.length; i++) {
					enumRoutingRecPrefix.add(tempSmsOptionSubmitRetryCodeList[i]);
				}
			} catch (Exception e) {
				log.error("load SMSOptionEnumQueryRecPrefix parameter failed, SMSOptionEnumQueryRecPrefix={}", tempsmsEnumRoutingRecPrefix);
			}
		}else {
			enumRoutingRecPrefix.clear();
		}
		
		String tempsmsIR21EnumRoutingRecPrefix = cfg.getString("SMSOptionIR21EnumQueryRecPrefix");
		if (tempsmsIR21EnumRoutingRecPrefix != null
				&& !"".equals(tempsmsIR21EnumRoutingRecPrefix)) {
			try {
				String [] tempSmsOptionIR21SubmitRetryCodeList = tempsmsIR21EnumRoutingRecPrefix.split(",");
				for (int i = 0; i < tempSmsOptionIR21SubmitRetryCodeList.length; i++) {
					enumIR21RoutingRecPrefix.add(tempSmsOptionIR21SubmitRetryCodeList[i]);
				}
			} catch (Exception e) {
				log.error("load SMSOptionIR21EnumQueryRecPrefix parameter failed, SMSOptionIR21EnumQueryRecPrefix={}", tempsmsIR21EnumRoutingRecPrefix);
			}
		}else {
			enumIR21RoutingRecPrefix.clear();
		}
		
		this.enumIR21RoutingURL = cfg.getString("SMSOptionEnumIR21RoutingURL", "");
		this.enumRoutingURL = cfg.getString("SMSOptionEnumRoutingURL", "");
		this.templatecoderegex = cfg.getString("SMSOptionTemplateCodeRegex", "");
		
		smsOptionSendDRBeforeOutSubmitInDC = cfg.getBool("SMSOptionSendDRBeforeOutSubmitInDC", false);
        smsOptionSendDRByInSubmitInCU = cfg.getBool("SMSOptionSendDRByInSubmitInCU", false);
		smsOptionChargeByGateWay = cfg.getBool("SMSOptionChargeByGateWay", false);
		smsOptionChargeCountryByGateWay = cfg.getBool("SMSOptionChargeCountryByGateWay", false);
		smsOptionOnlySupportDRRetry = cfg.getBool("SMSOptionOnlySupportDRRetry", false);
		smsOptionRemoveUdh = cfg.getBool("SMSOptionRemoveUdh", false);
		drSuccForTemplateReplaceFailed = cfg.getBool("DrSuccForTemplateReplaceFailed", false);
		smsOptionAdvancedSenderReplacement = cfg.getBool("SMSOptionAdvancedSenderReplacement", false);
		needDrReroute = cfg.getBool("NeedDRReroute", false);
		needCheckSession = cfg.getBool("NeedCheckSession", false);
		needCheckMsgSize = cfg.getBool("NeedCheckMsgSize", true);
		needStatisticMsgCount = cfg.getBool("NeedStatisticMsgCount", false);
		needStatisticMsgCountByCountry = cfg.getBool("NeedStatisticMsgCountByCountry", false);
		role = cfg.getString("Role", "");
		smsOptionDisableAutoAdjustThrottlingNumFlag = cfg.getBool("SMSOptionDisableAutoAdjustThrottlingNumFlag", false);

		minReceiverNumber = cfg.getInt("MinReceiverNumber", -1);
		maxReceiverNumber = cfg.getInt("MaxReceiverNumber", -1);

		connectionSilentTime = cfg.getInt("ConnectionSilentTime", 300) * 1000;
		windowSize = cfg.getInt("WindowSize", 1000);

		enquireLinkTime = cfg.getInt("EnquireLinkTime", -1) * 1000;
		bufferTimeout = cfg.getInt("BufferTimeout", 60) * 1000;
		isServerEnquireLink = cfg.getBool("ServerEnquireLink", false);
		isEnquireLink = cfg.getBool("EnquireLink", true);
		isKeepEnquireLink = cfg.getBool("KeepEnquireLink", false);

		String allowOriPrefixList = cfg
				.getString("SMSOptionAllowOriPrefixList");
		pAllowOriPrefixList = getPattern(allowOriPrefixList);

		// new item. Instead of numberLens with two configuration item.
		// private ArrayList<Integer> numberLens = new ArrayList<Integer> ();
		A2PCustomerMultiValue oriNumLenValue = cfg
				.parseMultiValue("SMSOptionOriNumberLenList");
		if (oriNumLenValue != null) {
			ArrayList<AttrPair> attrs = oriNumLenValue.getAllAttrs();
			for (AttrPair attr : attrs) {
				this.oriNumberLens.add(attr.getIntValue());
			}
		}
		
		A2PCustomerMultiValue oriNumberLenControl = cfg.parseMultiValue("SMSOptionOriNumberLenControl");
		if (oriNumberLenControl != null) {
			for(Group g : oriNumberLenControl.getAllGroups()){
				if (g.getAttr("Min") != null) {
					String min = g.getAttr("Min").getStringValue();
					if (min != null && !min.isEmpty()) {
						oriNumberLenController.put("Min", min);
					}					
				}
				if (g.getAttr("Max") != null) {
					String max = g.getAttr("Max").getStringValue();
					if (max != null && !max.isEmpty()) {
						oriNumberLenController.put("Max", max);
					}
				}
				if (g.getAttr("Suffix") != null) {
					String Suffix = g.getAttr("Suffix").getStringValue();
					if (Suffix != null && !Suffix.isEmpty()) {
						oriNumberLenController.put("Suffix", Suffix);
					}
				}								
			}
		}
		for (Map.Entry<String, String> entry: oriNumberLenController.entrySet()) {
			log.info("SMSOptionOriNumberLenControl key:{}, value:{}",entry.getKey(),entry.getValue());
		}

		A2PCustomerMultiValue recUserPhoneLenValue = cfg
				.parseMultiValue("SMSOptionRecUserPhoneLenList");
		if (recUserPhoneLenValue != null) {
			ArrayList<AttrPair> attrs = recUserPhoneLenValue.getAllAttrs();
			for (AttrPair attr : attrs) {
				if (recUserPhoneLens == null) {
					recUserPhoneLens = new ArrayList<Integer>();
				}
				this.recUserPhoneLens.add(attr.getIntValue());
			}
		}

		A2PCustomerMultiValue recNumLenValue = cfg
				.parseMultiValue("SMSOptionRecNumberLenList");
		if (recNumLenValue != null) {
			ArrayList<AttrPair> attrs = recNumLenValue.getAllAttrs();
			for (AttrPair attr : attrs) {
				this.recNumberLens.add(attr.getIntValue());
			}
		}

		oriMinNumberLen = cfg.getInt("SMSOptionOriMinNumberLen", -1);
		recMinNumberLen = cfg.getInt("SMSOptionRecMinNumberLen", -1);

		parseOoperator = cfg.getBool("SMSOptionParseOoperator", false);
		udhConcatenated = cfg.getBool("ChlUdhConcatenated", false);
		supportConcatenatedMsg = cfg.getBool("SMSOptionSupportConcatenatedMsg",
				false);
		transparencyMode = cfg.getInt("SMSOptionTransparencyMode", 0);
		foreword = cfg.getString("Foreword");
		ottContentAddOaddr = cfg.getString("SMSOptionOTTContentAddOaddr");
		operatorPriority = cfg.getInt("OperatorPriority", 5);

		supportIncomingBinary = cfg.getBool("SMSOptionSupportIncomingBinary",
				false);
		supportIncomingA2P = cfg.getBool("SMSOptionSupportIncomingA2P", false);
		supportOutgoingBinary = cfg.getBool("SMSOptionSupportOutgoingBinary",
				false);

		supportOutgoingA2P = cfg.getBool("SMSOptionSupportOutgoingA2P", false);

		supportIncomingAntiSpam = cfg.getBool(
				"SMSOptionSupportIncomingAntiSpam", false);
		supportOutgoingAntiSpam = cfg.getBool(
				"SMSOptionSupportOutgoingAntiSpam", false);

		isSmppMapErrCod4DR = cfg.getBool("SMSOptionSmppMapErrCod4DR", false);

		incomingThrottlingNum = cfg.getInt("SMSOptionIncomingThrottlingNum", 0);

		outgoingThrottlingNum = cfg.getInt("SMSOptionOutgoingThrottlingNum", 0);
		coreProcessorThrottlingNum = cfg.getInt("SMSOptionCoreProcessorThrottlingNum", 0);

		needAntiSpam = cfg.getBool("needAntiSpam", false);

		checkAlphaWhenReplaceOAddr = cfg.getBool(
				"SMSOptionCheckAlphaWhenReplaceOAddr", false);
		matchFullWhenRouteReplaceOAddr = cfg.getBool(
				"SMSOptionMatchFullWhenRouteReplaceOAddr", false);

		expireTime = cfg.getInt("ExpireTime", 0) *60;
		finalExpireTime = cfg.getInt("FinalExpireTime", 0) *60;

		shortCodeIfExpand = cfg.getString("ShortCodeIfExpand");

		noticeCustomer = cfg.getString("NoticeCustomer");

		isDRStatusInOptionPara = cfg.getBool("SMSOptionDRStatusIsOptionPara",
				false);
		this.smsOptionRecipientNotContinueFilter = cfg.getBool("SMSOptionCheckRecipientNotContinueFilter", false);
		if(smsOptionRecipientNotContinueFilter){
			int recipientExpireTime = cfg.getInt("SMSOptionRecipientContinueFilterExpiredTime", 300);
			reicipientCache = CacheBuilder.newBuilder().expireAfterWrite(recipientExpireTime,TimeUnit.SECONDS).build();
		}
		
		this.smsOptionNeedSupportSecondRouting = cfg.getBool("smsOptionNeedSupportSecondRouting", false);
		if(smsOptionNeedSupportSecondRouting){
			int recipientExpireTime = cfg.getInt("smsOptionNeedSupportSecondRoutingExpiredTime", 300);
			reicipienSecondRoutingtCache = CacheBuilder.newBuilder().expireAfterWrite(recipientExpireTime,TimeUnit.SECONDS).build();
		}
		
		
		this.smsOptionWrMonitorCDR = cfg.getBool("SMSOptionWrMonitorCDR", false);
		
		//handle for gsm7bit 
		this.smsOptionConvert2GSM7bit = cfg.getBool("SMSOptionConvert2GSM7bit", false);
		
		this.smsOptionForceTrans2ASCII = cfg.getBool("SMSOptionForceTrans2ASCII", false);
		this.smsOptionIncomingGSM7bit = cfg.getInt("SMSOptionSupportIncomingGSM7bit", 0);
		this.smsOptionOutgoingGSM7bit = cfg.getInt("SMSOptionSupportOutgoingGSM7bit", 0);
		//add template content for wemedia
		this.smsOptionTemplateSignature = cfg.getString("SMSOptionTemplateSignature");
						
		//handle for message split length
		this.smsOptionIsMaxASCIILenTo7BitFlag = cfg.getBool("SMSOptionIsMaxASCIILenTo7BitFlag", false);				
		
		// SMPP
		chlSMPPMsgIDParse = cfg.getBool("ChlSMPPMsgIDParse", false);
		// For eagle
		removeRecPrefix = cfg.getString("SMSOptionRemoveRecPrefix");
		/**
		 * identifier that whether put msg id to DELIVER_SM_RESP
		 */
		bSmppIsPutMsgId4DSR = cfg.getBool("SMSOptionSMPPIsPutMsgId4DSR", false);
		bSmppIsGenHexMsgId = cfg.getBool("SMSOptionSMPPIsGenHexMsgId", false);
		/**
		 * the bSmppIsPadZero4SR's default value same as chlSMPPMsgIDParse both
		 * of them must configure for V34
		 */
		bSmppIsPadZero4SR = cfg.getBool("SMSOptionSMPPIsPadZero4SR",
				chlSMPPMsgIDParse);

		drSwapAddr = cfg.getBool("DrSwapAddr", true);
		deliveryReportMode = cfg.getInt("DeliveryReportMode", -1);
		dSmppParaTonFlag = cfg.getInt("drSmppParaTonFlag", 0);
		dealExceptionDR = cfg.getBool("DealExceptionDR", true);
		chlAcctNamer = cfg.getString("ChlAcctr");
		chlPasswordr = cfg.getString("chlPwdr");
		port2 = cfg.getString("ChlPort2");
		clientSessionNumber = cfg.getInt("ClientSessionNumber", 1);
		bindMode = BindMode.getBindMode(cfg.getString("BindMode"));
		serviceType = cfg.getString("ServiceType");
		systemType = cfg.getString("SystemType");
		sourceAddrTon = (byte) cfg.getInt("SMSOptionSourceAddrTon", 1);
		sourceAddrNpi = (byte) cfg.getInt("SMSOptionSourceAddrNpi", 1);
		addrTon = (byte) cfg.getInt("SMSOptionAddrTon", 0);
		addrNpi = (byte) cfg.getInt("SMSOptionAddrNpi", 0);
		destAddrTon = (byte) cfg.getInt("SMSOptionDestAddrTon", 1);
		destAddrNpi = (byte) cfg.getInt("SMSOptionDestAddrNpi", 1);
		priorityFlag = (byte) cfg.getInt("SMSOptionPriorityFlag", 1);
		replaceIfFlag = (byte) cfg.getInt("SMSOptionReplaceIfFlag", 0);
		protocolId = cfg.getByteWithHexFormat("SMSOptionProtocolId", (byte) 0);// modified
																				// by
																				// Jianming
		smppReceiveTimeout = cfg.getInt("SMSOptionSmppReceiveTimeout", 40) * 1000;
		if ("V34"
				.equalsIgnoreCase(cfg.getString("SMSOptionSMPPVersion", "V34"))) {
			smppVersion = new com.king.gmms.protocol.smpp.version.SMPPVersion34();
		} else {
			smppVersion = new com.king.gmms.protocol.smpp.version.SMPPVersion33();
		}

		String timeZone = cfg.getString("SMSOptionTimeZone");
		if (timeZone != null && timeZone.trim().length()>0) {
			smsOptionTimeZone = TimeZone.getTimeZone("GMT" + timeZone.trim());
		} 

		Prefix prefix = null;
		String prefixString = cfg.getString("SMSOptionAddOriPrefix4NewMO");
		if (prefixString != null) {
			prefix = new AddPrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionAddRecPrefix4NewMO");
		if (prefixString != null) {
			prefix = new AddPrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionAddOriPrefix4DRMO");
		if (prefixString != null) {
			prefix = new AddPrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4DrMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionAddRecPrefix4DRMO");
		if (prefixString != null) {
			prefix = new AddPrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4DrMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionAddOriPrefix4NewMT");
		if (prefixString != null) {
			prefix = new AddPrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMtList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionAddRecPrefix4NewMT");
		if (prefixString != null) {
			prefix = new AddPrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMtList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionRemoveOriPrefix4NewMO");
		if (prefixString != null) {
			prefix = new RemovePrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionRemoveRecPrefix4NewMO");
		if (prefixString != null) {
			prefix = new RemovePrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionRemoveOriPrefix4DRMO");
		if (prefixString != null) {
			prefix = new RemovePrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4DrMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionRemoveRecPrefix4DRMO");
		if (prefixString != null) {
			prefix = new RemovePrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4DrMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionRemoveOriPrefix4NewMT");
		if (prefixString != null) {
			prefix = new RemovePrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMtList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionRemoveRecPrefix4NewMT");
		if (prefixString != null) {
			prefix = new RemovePrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMtList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionReplaceOriPrefix4NewMO");
		if (prefixString != null) {
			prefix = new ReplacePrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionReplaceRecPrefix4NewMO");
		if (prefixString != null) {
			prefix = new ReplacePrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionReplaceOriPrefix4DRMO");
		if (prefixString != null) {
			prefix = new ReplacePrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4DrMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionReplaceRecPrefix4DRMO");
		if (prefixString != null) {
			prefix = new ReplacePrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4DrMoList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionReplaceOriPrefix4NewMT");
		if (prefixString != null) {
			prefix = new ReplacePrefix(1);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMtList(prefix);
			}
		}

		prefixString = cfg.getString("SMSOptionReplaceRecPrefix4NewMT");
		if (prefixString != null) {
			prefix = new ReplacePrefix(2);
			if (prefix.parseValue(prefixString)) {
				addPrefix4NewMtList(prefix);
			}
		}
		

		/** -------------- Start to load retry policy configure items ------------------**/
		RetryPolicyManager policyManager = RetryPolicyManager.getInstance();
    	RetryPolicyInfo policyInfo = null;
    	Map<Integer, RetryPolicyInfo> map = policyManager.getSsid_policy_map();
    	if(map.containsKey(ssid)){
    		policyInfo = map.get(ssid);
    	}else{
    		policyInfo = new RetryPolicyInfo();
    	}
		A2PCustomerMultiValue serviceTypeIDPolicy = cfg.parseMultiValue("SMSOptionServiceTypeIDRetryPolicy");
        if(serviceTypeIDPolicy != null){
            for(Group g : serviceTypeIDPolicy.getAllGroups()){
                String id = null;
                String policy = null;  
                
                AttrPair attrID = g.getAttr("serviceTypeID");
            	if(attrID != null) {
            		id = attrID.getStringValue();
            	}
            	AttrPair attrPolicy = g.getAttr("policy");
            	if(attrPolicy != null) {
            		policy = attrPolicy.getStringValue();
            	}
                
				if (policy != null && id != null) {
					List<Integer> policyList = policyManager.parse(policy);
					policyInfo.getServiceTypeIDPolicyMap().put(Integer.valueOf(id), policyList);
				}
            }               
        }
        
        A2PCustomerMultiValue contentReplacementPolicy = cfg.parseMultiValue("SMSOptionContentReplacementPolicy");
        smsOptionReplaceContent.clear();
        if (contentReplacementPolicy != null) {
        	for(Group g : contentReplacementPolicy.getAllGroups()){
        		String recipitPrefix = null;
        		String replaceContentString = null;
        		AttrPair attrPrefix = g.getAttr("prefix");
            	if(attrPrefix != null) {
            		recipitPrefix = attrPrefix.getStringValue();
            	}
            	AttrPair attrContent = g.getAttr("value");
            	if(attrContent != null) {
            		replaceContentString = attrContent.getStringValue();
            	}
            	log.debug("replacecontent prefix={},value={}",recipitPrefix, replaceContentString);
            	if (replaceContentString != null) {
            		Map<String, String> contentMap = new HashMap<String, String>();
            		//value=rep:content,rep1:content2
					String[] replaceContentArray = replaceContentString.split("\\|");
					if(replaceContentArray!=null && replaceContentArray.length>0){
						for (int i = 0; i < replaceContentArray.length; i++) {
							//rep:content
							String[] replaceContentEntity = replaceContentArray[i].split(":");
							if (replaceContentEntity !=null && replaceContentEntity.length>0) {
								if (replaceContentEntity.length==1) {
									contentMap.put(replaceContentEntity[0], " ");
								}else {
									if (replaceContentEntity[0].contains("^")) {
										replaceContentEntity[0] = replaceContentEntity[0].replaceAll("\\^", " ");
									}
									contentMap.put(replaceContentEntity[0], replaceContentEntity[1]);
								    log.debug("replacecontent key={},value={}", replaceContentEntity[0], replaceContentEntity[1]);
								}
							}
						}
					}
					if (recipitPrefix!=null) {
						smsOptionReplaceContent.put(recipitPrefix, contentMap);
					}
				}            	
        	}
		}
        
        A2PCustomerMultiValue smsContentLengthMultiValue = cfg.parseMultiValue("SMSSpecialSupportContentLengthByCountry");
        smsRSupportLenghtCheck.clear();
        if (smsContentLengthMultiValue != null) {
        	for(Group g : smsContentLengthMultiValue.getAllGroups()){
        		String recipitPrefix = null;
        		String checkContentString = null;
        		AttrPair attrPrefix = g.getAttr("prefix");
            	if(attrPrefix != null) {
            		recipitPrefix = attrPrefix.getStringValue();
            	}
            	AttrPair attrContent = g.getAttr("value");
            	if(attrContent != null) {
            		checkContentString = attrContent.getStringValue();
            	}
            	log.debug("SMSSpecialSupportContentLengthByCountry prefix={},value={}",recipitPrefix, checkContentString);
            	if (checkContentString != null) {
            		TreeRangeMap<Integer, String> smsRSupportLenghtCheckTree = TreeRangeMap.create();                    
            		//value=minlen1:maxlen2:deny|minlen1:minlen2:allow
					String[] checkContentArray = checkContentString.split("\\|");
					if(checkContentArray!=null && checkContentArray.length>0){
						for (int i = 0; i < checkContentArray.length; i++) {
							//0:140:deny
							String[] checkContentEntity = checkContentArray[i].split(":");
							if (checkContentEntity !=null && checkContentEntity.length>0) {
								if (checkContentEntity.length!=3) {
									log.debug("checkcontent value is invalid {}", checkContentEntity);
								}else {
									Range rg = null;
									int minLen = 0;
									try {
										minLen = Integer.parseInt(checkContentEntity[0]);
									} catch (Exception e) {
										minLen=0;
									}
									if(StringUtils.isEmpty(checkContentEntity[1])) {
										rg = Range.atLeast(minLen);
									}else {
										int maxLen = 0;
										try {
											maxLen = Integer.parseInt(checkContentEntity[1]);
										} catch (Exception e) {
											maxLen=0;
										}
										rg = Range.closed(minLen, maxLen);
									}									
									smsRSupportLenghtCheckTree.put(rg, checkContentEntity[2]);
								}
							}
						}
					}
					if (recipitPrefix!=null) {
						smsRSupportLenghtCheck.put(recipitPrefix, smsRSupportLenghtCheckTree);
					}
				}            	
        	}
		}
		
        A2PCustomerMultiValue senderPrefixPolicy = cfg.parseMultiValue("SMSOptionSenderRetryPolicy");
        if(senderPrefixPolicy != null){        	
            for(Group g : senderPrefixPolicy.getAllGroups()){
            	String length = null;
            	String senderPrefix = null;
            	String policy = null;
            	AttrPair attrPrefix = g.getAttr("prefix");
            	if(attrPrefix != null) {
            		senderPrefix = attrPrefix.getStringValue();
            	}
            	AttrPair attrLength = g.getAttr("length");
                if(attrLength != null) {
                	length = attrLength.getStringValue();
                }
                AttrPair attrPolicy = g.getAttr("policy");
                if(attrPolicy != null){
                	policy = attrPolicy.getStringValue();
                }
                
                String regular = null;
                Pattern p = null;
                if(senderPrefix != null){
                	StringBuffer buffer = new StringBuffer(senderPrefix);
                	if(length != null){
                		int confLength = Integer.valueOf(length);
                		int regLength = confLength - senderPrefix.length();
                		buffer.append("[A-Za-z0-9]{"+regLength+"}");
                	}else{
                		buffer.append("[A-Za-z0-9]+");
                	}
                	regular = buffer.toString();
					p = Pattern.compile(regular); 
					
					if (policy != null) {
						List<Integer> policyList = policyManager.parse(policy);
						policyInfo.getSenderPrefixPolicyMap().put(p, policyList);
					}       
                }				         
            }//end of for            
        }
        
        String mtPolicy = cfg.getString("SMSOptionMTRetryPolicy");        
        if(mtPolicy != null){
        	 List<Integer> mtList = policyManager.parse(mtPolicy); 
        	 policyInfo.setMtPolicyList(mtList);        	 
        }
        
        String moPolicy = cfg.getString("SMSOptionMORetryPolicy");        
        if(moPolicy != null){
        	 List<Integer> moList = policyManager.parse(moPolicy);
        	 policyInfo.setMoPolicyList(moList);        	 
        }
        
        map.put(ssid, policyInfo);
        
		/** -------------- end of loading retry policy items ------------------**/

		A2PCustomerMultiValue shortCodeValue = cfg.parseMultiValue("ShortCode");
		if (shortCodeValue != null) {
			if (shortCodeValue.getAttr("code") != null) {
				String code = shortCodeValue.getAttr("code").getStringValue();
				if (code != null) {
					ArrayList<AttrPair> attrs = shortCodeValue.getAllAttrs();
					for (AttrPair attr : attrs) {
						if ("Prefix".equalsIgnoreCase(attr.getStringValue())) {
							this.shortCodes.put(attr.getStringValue(), code);
						}
					}
				}
			}
		}

		// A2PCustomerMultiValue domainNameValue = cfg
		// .parseMultiValue("GMMSDomain");
		// if (domainNameValue != null) {
		// ArrayList<AttrPair> attrs = domainNameValue.getAllAttrs();
		// for (AttrPair attr : attrs) {
		// String newDomain = attr.getStringValue();
		// if (newDomain.endsWith(".")) {
		// newDomain = newDomain.substring(0, newDomain.length() - 1);
		// }
		//
		// this.domainNames.add(newDomain);
		// }
		// }

		// phonePrefix and phoneSubPrefix
		A2PCustomerMultiValue prefixes = cfg.parseMultiValue("PhoneSubPrefix");
		if (prefixes != null) {
			ArrayList<AttrPair> attrs = prefixes.getAllAttrs();
			for (AttrPair attr : attrs) {
				this.phonePrefixs.add(attr.getStringValue());
				this.phonePrefixsMap.put(attr.getStringValue(), ssid);
			}
		}
		if (phonePrefixs.size() == 0) {
			prefixes = cfg.parseMultiValue("PhonePrefix");
			if (prefixes != null) {
				ArrayList<AttrPair> attrs = prefixes.getAllAttrs();
				for (AttrPair attr : attrs) {
					this.phonePrefixs.add(attr.getStringValue());
					this.phonePrefixsMap.put(attr.getStringValue(), ssid);
				}
			}
		}

		String noticePrefixString = cfg.getString("NoticePrefix");
		if (noticePrefixString != null) {
			String[] lens = noticePrefixString.split(",");
			ArrayList<String> al = new ArrayList<String>();
			for (int i = 0; i < lens.length; i++) {
				al.add(lens[i].trim());
			}
			setNoticePrefix(al);
		}

		pendingAliveCount = cfg.getInt("PendingAliveCount", 3);

		String senderMapping = cfg.getString("SenderMapping");
		if (senderMapping != null) {
			String[] sm = senderMapping.split(",");
			if (sm == null || sm.length % 2 != 0) {
				log
						.error("SenderMapping configuration error for SSID: "
								+ ssid);
			} else {
				for (int i = 0; i < sm.length / 2; i++) {
					this.alSenderMapping.add(new String[] { sm[2 * i].trim(),
							sm[2 * i + 1].trim() });
				}
			}
		}

		reconnectInterval = cfg.getInt("ReconnectInterval", 60) * 1000;

		protocol = cfg.getString("Protocol");
		supportDCS = cfg.getBool("SMSOptionSMPPIsSupportDCS", false);// added by
																		// Jianming
																		// in
																		// V1.0.1
		needReceiptedMsgId = cfg.getBool("SMSOptionSMPPNeedReceiptedMessageId",
				false);// added by Jianming in V1.0.1
		SMSOptionChoiceSecurityHttpSession = cfg
				.getString("SMSOptionChoiceSecurityHttpSession");
		
		lastHopSplitSuffixFormat = cfg.getString("LastHopSplitSuffixFormat",
				null);

		// rentAddrPrefixList
		A2PCustomerMultiValue rentAddrPrefix = cfg
				.parseMultiValue("SMSOptionRentAddrPrefix");
		if (rentAddrPrefix != null) {
			ArrayList<AttrPair> attrs = rentAddrPrefix.getAllAttrs();
			for (AttrPair attr : attrs) {
				this.rentAddrPrefixList.add(attr.getStringValue());
			}
		}
		Collections.sort(this.rentAddrPrefixList);
		Collections.reverse(this.rentAddrPrefixList);

		// rentAddrConditionMap
		A2PCustomerMultiValue rentAddrCondition = cfg
				.parseMultiValue("SMSOptionConditionToRentAddr");
		if (rentAddrCondition != null) {
			for (Group g : rentAddrCondition.getAllGroups()) {
				RentAddrCondition condition = assignRentAddrCondition(g);
				if (condition != null) {
					this.rentAddrConditionList.add(condition);
				}
			}
		}
		Collections.sort(this.rentAddrConditionList);
        
        // blackhole percent
        A2PCustomerMultiValue blackholePercent = cfg.parseMultiValue("BlackholePercent");
        if (blackholePercent != null) {
        	for(Group g : blackholePercent.getAllGroups()){
        		int rop = -1;
    			int percent = 0;
        		for (AttrPair attr : g.getAllAttrs()) {
        			String curItem = attr.getItem();
                    if ("R_SSID".equalsIgnoreCase(curItem)) {
                    	rop = attr.getIntValue();
                    }
                    else if ("Percent".equalsIgnoreCase(curItem)) {
                    	percent = attr.getIntValue();
                    }
        		}
        		
        		if (percent >= 0 && percent <= 100) {
        			if (rop > 0) {
        				// key: ossid_rssid
        				String key = this.ssid + "_" + rop;
        				blackholePercentMap.put(key, percent);
        			} else {
        				blackholePercentMap.put(this.ssid+"", percent);
        			}
        		}
            }
        }
        
        A2PCustomerMultiValue priorityPercent = cfg.parseMultiValue("PriorityPercent");
        if (priorityPercent != null) {
        	for(Group g : priorityPercent.getAllGroups()){
        		int priority = -1;
    			int percent = 0;
        		for (AttrPair attr : g.getAllAttrs()) {
        			String curItem = attr.getItem();
                    if ("Priority".equalsIgnoreCase(curItem)) {
                    	String priorityValue = attr.getStringValue("0");
                    	priority = convertPriority(priorityValue);                    	                    	
                    }
                    else if ("Percent".equalsIgnoreCase(curItem)) {
                    	percent = attr.getIntValue();
                    }
        		}
        		
        		if (percent >= 0 && percent <= 100) {
        			if (priority >= 0) {
        				// key: ossid_rssid        				
        				priorityPercentMap.put(priority, percent);
        			}
        		}
            }
        }
        String priorityValue = cfg.getString("Priority", "0");
        priority = convertPriority(priorityValue);
        blackholeDrStatusCode = cfg.getInt("BlackholeDrStatusCode", 10000);
        blackholebatchMinNumberForPortalCust = cfg.getInt("BlackholebatchMinNumberForPortalCust", 0);
        blackholeMinNumberByTimer = cfg.getInt("BlackholeMinNumberByTimer", 0);
        blackholeTimerInSec = cfg.getInt("BlackholeTimerInSec", 60);
        maxRerouteDRDelayTimeInSec = cfg.getInt("RerouteDRDelayTimeInSec", 1800);
        smsOptionRecipientMaxSendCountIn24H = cfg.getInt("SMSOptionRecipientMaxSendCountIn24H", 0);
        customerTypeBySender = cfg.getString("CustomerTypeBySender", "NonPA");
        
		A2PCustomerMultiValue duplicateMsgServiceTypeIDList = cfg.parseMultiValue("DuplicateMessageServiceTypeIDList");
		if (duplicateMsgServiceTypeIDList != null) {
			ArrayList<AttrPair> attrs = duplicateMsgServiceTypeIDList.getAllAttrs();
			for (AttrPair attr : attrs) {
				this.duplicateMsgServiceTypeIDList.add(attr.getIntValue());
			}
		}
		
		duplicateMsgPeriod = cfg.getInt("DuplicateMessagePeriod", 0) * 1000;
		checkDuplicateMsgContent = cfg.getBool("CheckDuplicateMessageContent", false);
		
		parseServiceTypeID = cfg.getBool("SMSOptionParseServiceTypeID", false);
		
		parseScheduleDeliveryTime = cfg.getBool("SMSOptionParseScheduleDeliveryTime", false);
		String deliveryStartTime = cfg.getString("SMSOptionDeliveryStartTime");
		String deliveryEndTime = cfg.getString("SMSOptionDeliveryEndTime");
		// SMSOptionDeliveryStartTime and SMSOptionDeliveryEndTime must be used as a pair. 
		// If only specify one item, assign the other item a default value (SMSOptionDeliveryStartTime as 00:01, SMSOptionDeliveryEndTime as 23:59)
		if ((deliveryStartTime != null && deliveryStartTime.trim().length()>0)
				|| (deliveryEndTime != null && deliveryEndTime.trim().length()>0)) {
			try {
				if (deliveryStartTime != null) {
					String[] startArray = deliveryStartTime.split(":");
					deliveryStartHH = Integer.valueOf(startArray[0].trim());
					deliveryStartmm = Integer.valueOf(startArray[1].trim());
				} else {
					deliveryStartHH = 0;
					deliveryStartmm = 1;
				}
				
				if (deliveryEndTime != null) {
					String[] endArray = deliveryEndTime.split(":");
					deliveryEndHH = Integer.valueOf(endArray[0].trim());
					deliveryEndmm = Integer.valueOf(endArray[1].trim());
				} else {
					deliveryEndHH = 23;
					deliveryEndmm = 59;
				}
				
			} catch (Exception e) {
				log.info("Invalid SMSOptionDeliveryStartTime={} or SMSOptionDeliveryEndTime={}.", e, deliveryStartTime, deliveryEndTime);
			}
		}
		//init sslconfiguration
		String protocol = cfg.getString("Protocol");
		String keyStorePath = cfg.getString("Ssl.KeyStorePath");
		if((protocol != null && "SSLSMPP".equalsIgnoreCase(protocol.trim())) 
				&& (keyStorePath != null && keyStorePath.trim().length()>0)){
			initSSLConfiguration(cfg);
		}
	}

	private int convertPriority(String priorityValue) {
		int priority;
		switch (priorityValue) {
		case "a":
			priority = 1;
			break;
		case "b":
			priority = 2;
			break;
		case "c":
			priority = 3;
			break;
		case "d":
			priority = 4;
			break;
		case "e":
			priority = 5;
			break;
		case "1":
			priority = 1;
			break;
		case "2":
			priority = 2;
			break;
		case "3":
			priority = 3;
			break;
		case "4":
			priority = 4;
			break;
		case "5":
			priority = 5;
			break;
		case "6":
			priority = 6;
			break;
		case "7":
			priority = 7;
			break;
		case "8":
			priority = 8;
			break;
		case "9":
			priority = 9;
			break;
		case "10":
			priority = 10;
			break;
		default:
			priority = 0;
			break;
		}
		return priority;
	}
	
	private String getShortNameWithoutService(String value) {
		if (value == null)
			return null;

		int c = value.lastIndexOf("_");
		if (c > 0)
			return value.substring(0, c);
		else
			return value;
	}
	
	protected RentAddrCondition assignRentAddrCondition(Group group) {
		if (group == null) {
			return null;
		}

		RentAddrCondition condition = new RentAddrCondition();

		for (AttrPair attr : group.getAllAttrs()) {
			String curItem = attr.getItem();
			if ("RAddrPrefix".equalsIgnoreCase(curItem)) {
				condition.setrAddrPrefix(attr.getStringValue());
			} else if ("Partner".equalsIgnoreCase(curItem)) {
				condition.setPartner(attr.getStringValue());
			}
		}

		if (condition.getrAddrPrefix() != null
				&& !condition.getrAddrPrefix().equals("")) {
			return condition;
		}
		return null;
	}

	public ArrayList getPhonePrefixs() {
		return this.phonePrefixs;
	}

	public void setConnectionType(String type) {
		if (type.equalsIgnoreCase("SC")) {
			connectionType = 1;
		} else if (type.equalsIgnoreCase("MC")) {
			connectionType = 2;
		} else if (type.equalsIgnoreCase("MN")) {
			connectionType = 3;
		}
	}

	public void setDRStatusIsOptionPara(boolean isOptionPara) {
		this.isDRStatusInOptionPara = isOptionPara;
	}

	public boolean getDRStatusIsOptionPara() {
		return isDRStatusInOptionPara;
	}

	public String getRemoveRecPrefix() {
		return removeRecPrefix;
	}

	public void setRemoveRecPrefix(String removeRecPrefix) {
		this.removeRecPrefix = removeRecPrefix;
	}

	public boolean isSupportDCS() {
		return supportDCS;
	}

	public void setSupportDCS(boolean supportDCS) {
		this.supportDCS = supportDCS;
	}

	public boolean isNeedReceiptedMsgId() {
		return needReceiptedMsgId;
	}

	public void setNeedReceiptedMsgId(boolean needReceiptedMsgId) {
		this.needReceiptedMsgId = needReceiptedMsgId;
	}

	public String getSenderId() {
		return senderId;
	}

	public void setSenderId(String senderId) {
		this.senderId = senderId;
	}

	public TimeZone getSmsOptionTimeZone() {
		return smsOptionTimeZone;
	}

	public void setSmsOptionTimeZone(TimeZone smsOptionTimeZone) {
		this.smsOptionTimeZone = smsOptionTimeZone;
	}

	// Add by Jianming for http common module on 2012-03-15
	/**
	 * invoke setter method
	 */
	public boolean setProperty(String propertyName, Object value) {
		boolean bret = false;
		Method[] methods = A2PCustomerInfo.class.getMethods();
		try {

			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("set" + propertyName)) {
					f.invoke(this, value);
					bret = true;
				}
			}
		} catch (Exception e) {
			System.out.println("set properyt " + propertyName + " error!");
		}
		return bret;
	}

	/**
	 * invoke getter method
	 */
	public Object getProperty(String propertyName) {
		Method[] methods = A2PCustomerInfo.class.getMethods();
		Object obj = null;
		try {
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("get" + propertyName)) {
					obj = f.invoke(this);
				}
			}
		} catch (Exception e) {
			System.out.println("get properyt " + propertyName + " error!");
		}
		return obj;
	}

	public int getIosmsSsid() {
		return iosmsSsid;
	}

	public void setIosmsSsid(int iosmsSsid) {
		this.iosmsSsid = iosmsSsid;
	}
	
	

	public boolean isSmsOptionDisableAutoAdjustThrottlingNumFlag() {
		return smsOptionDisableAutoAdjustThrottlingNumFlag;
	}

	public void setSmsOptionDisableAutoAdjustThrottlingNumFlag(
			boolean smsOptionDisableAutoAdjustThrottlingNumFlag) {
		this.smsOptionDisableAutoAdjustThrottlingNumFlag = smsOptionDisableAutoAdjustThrottlingNumFlag;
	}

	public boolean isUrlEncodingTwice() {
		return isUrlEncodingTwice;
	}

	public void setUrlEncodingTwice(boolean isUrlEncodingTwice) {
		this.isUrlEncodingTwice = isUrlEncodingTwice;
	}

	public String getPasswdEncryptMethod() {
		return passwdEncryptMethod;
	}

	public void setPasswdEncryptMethod(String passwdEncryptMethod) {
		this.passwdEncryptMethod = passwdEncryptMethod;
	}

	public int getCustomerId() {
		return customerId;
	}

	public void setCustomerId(int customerId) {
		this.customerId = customerId;
	}

	public List<String> getRentAddrPrefixList() {
		return rentAddrPrefixList;
	}

	public void setRentAddrPrefixList(List<String> rentAddrPrefixList) {
		this.rentAddrPrefixList = rentAddrPrefixList;
	}

	public List<RentAddrCondition> getRentAddrConditionList() {
		return rentAddrConditionList;
	}

	public void setRentAddrConditionList(
			List<RentAddrCondition> rentAddrConditionList) {
		this.rentAddrConditionList = rentAddrConditionList;
	}

	public String getOttContentAddOaddr() {
		return ottContentAddOaddr;
	}

	public void setOttContentAddOaddr(String ottContentAddOaddr) {
		this.ottContentAddOaddr = ottContentAddOaddr;
	}

	public String getSMSOptionChoiceSecurityHttpSession() {
		return SMSOptionChoiceSecurityHttpSession;
	}

	public void setSMSOptionChoiceSecurityHttpSession(
			String sMSOptionChoiceSecurityHttpSession) {
		SMSOptionChoiceSecurityHttpSession = sMSOptionChoiceSecurityHttpSession;
	}

	public String getSMSOptionHttpCustomParameter() {
		return SMSOptionHttpCustomParameter;
	}

	public void setSMSOptionHttpCustomParameter(
			String sMSOptionHttpCustomParameter) {
		SMSOptionHttpCustomParameter = sMSOptionHttpCustomParameter;
	}

	public String getSMSOptionHttpType() {
		return SMSOptionHttpType;
	}

	public void setSMSOptionHttpType(String optionHttpType) {
		SMSOptionHttpType = optionHttpType;
	}	

	public boolean isSmsOptionSendFakeDR() {
		return smsOptionSendFakeDR;
	}

	public void setSmsOptionSendFakeDR(boolean smsOptionSendFakeDR) {
		this.smsOptionSendFakeDR = smsOptionSendFakeDR;
	}

	public boolean isSmsOptionChargeByGateWay() {
		return smsOptionChargeByGateWay;
	}

	public void setSmsOptionChargeByGateWay(boolean smsOptionChargeByGateWay) {
		this.smsOptionChargeByGateWay = smsOptionChargeByGateWay;
	}

	public boolean isSupportTemplateParemeterSignature() {
		return supportTemplateParemeterSignature;
	}

	public void setSupportTemplateParemeterSignature(boolean supportTemplateParemeterSignature) {
		this.supportTemplateParemeterSignature = supportTemplateParemeterSignature;
	}
	
	

	public int getPrioritryHanderNumber() {
		return prioritryHanderNumber;
	}

	public void setPrioritryHanderNumber(int prioritryHanderNumber) {
		this.prioritryHanderNumber = prioritryHanderNumber;
	}
	
	

	public Map<String, Map<String, String>> getSmsOptionReplaceContent() {
		return smsOptionReplaceContent;
	}

	public void setSmsOptionReplaceContent(
			Map<String, Map<String, String>> smsOptionReplaceContent) {
		this.smsOptionReplaceContent = smsOptionReplaceContent;
	}	

	public Map<String, String> getOriNumberLenController() {
		return oriNumberLenController;
	}

	public void setOriNumberLenController(Map<String, String> oriNumberLenController) {
		this.oriNumberLenController = oriNumberLenController;
	}

	public String toString() {
		String prefix = "\r\n";
		return new StringBuffer(5210).append("A2PPlayerType:").append(this.a2pPlayerType)
		.append(prefix).append("SMSOptionAddOriPrefix4DRMO:").append(this.bIsHandlePrefix4DRMO)
		.append(prefix).append("SMSOptionAddOriPrefix4NewMO:").append(this.bIsHandlePrefix4NewMO)
		.append(prefix).append("SMSOptionAddOriPrefix4NewMT:").append(this.bIsHandlePrefix4NewMT)
		.append(prefix).append("SMSOptionSMPPIsGenHexMsgId:").append(this.bSmppIsGenHexMsgId)
		.append(prefix).append("SMSOptionSMPPIsPadZero4SR:").append(this.bSmppIsPadZero4SR)
		.append(prefix).append("SMSOptionSMPPIsPutMsgId4DSR:").append(this.bSmppIsPutMsgId4DSR)
		.append(prefix).append("bufferTimeout:").append(this.bufferTimeout)
		.append(prefix).append("BindMode:").append(this.bindMode)
		.append(prefix).append("SMSOptionCheckAlphaWhenReplaceOAddr:").append(this.checkAlphaWhenReplaceOAddr)
		.append(prefix).append("ChlAcctr:").append(this.chlAcctNamer)
		.append(prefix).append("chlPwdr:").append(this.chlPasswordr)
		.append(prefix).append("chlQueue:").append(this.chlQueue)
		.append(prefix).append("ChlSMPPMsgIDParse:").append(this.chlSMPPMsgIDParse)
		.append(prefix).append("ClientSessionNumber:").append(this.clientSessionNumber)
		.append(prefix).append("ConnectionSilentTime:").append(this.connectionSilentTime)
		.append(prefix).append("ConnectionType:").append(this.connectionType)
		.append(prefix).append("CustomerID:").append(this.customerId)
		.append(prefix).append("ConvertToBinaryCharsets:").append(this.convertToBinaryCharsets)
		.append(prefix).append("DealExceptionDR:").append(this.dealExceptionDR)
		.append(prefix).append("DeliveryReportMode:").append(this.deliveryReportMode)
		.append(prefix).append("DrSwapAddr:").append(this.drSwapAddr)
		.append(prefix).append("SMSOptionDestAddrNpi:").append(this.destAddrNpi)
		.append(prefix).append("SMSOptionDestAddrTon:").append(this.destAddrTon)
		.append(prefix).append("EnquireLinkTime:").append(this.enquireLinkTime)
		.append(prefix).append("ExpireTime:").append(this.expireTime)
		.append(prefix).append("FinalExpireTime:").append(this.finalExpireTime)
		.append(prefix).append("Foreword:").append(this.foreword)
		.append(prefix).append("SMSOptionHttpMethod:").append(this.httpMethod)
		.append(prefix).append("SMSOptinIsSupportHttpQueryMessage:").append(this.httpQueryMessageFlag)
		.append(prefix).append("InClientPull:").append(this.inClientPull)
		.append(prefix).append("SMSOptionIncomingThrottlingNum:").append(this.incomingThrottlingNum)
		.append(prefix).append("IOSMSSSID:").append(this.iosmsSsid)
		.append(prefix).append("SMSOptionDRStatusIsOptionPara:").append(this.isDRStatusInOptionPara)
		.append(prefix).append("EnquireLink:").append(this.isEnquireLink)
		.append(prefix).append("ServerEnquireLink:").append(this.isServerEnquireLink)
		.append(prefix).append("SMSOptionSmppMapErrCod4DR:").append(this.isSmppMapErrCod4DR)
		.append(prefix).append("SMSOptionHttpUrlEncodingTwice:").append(this.isUrlEncodingTwice)
		.append(prefix).append("LastHopSplitSuffixFormat:").append(this.lastHopSplitSuffixFormat)
		.append(prefix).append("SMSOptionMatchFullWhenRouteReplaceOAddr:").append(this.matchFullWhenRouteReplaceOAddr)
		.append(prefix).append("SMSOptionQueryMsgIntervalTime:").append(this.msgQueryInterval)
		.append(prefix).append("needAntiSpam:").append(this.needAntiSpam)
		.append(prefix).append("SMSOptionSMPPNeedReceiptedMessageId:").append(this.needReceiptedMsgId)
		.append(prefix).append("NoticeCustomer:").append(this.noticeCustomer)
		.append(prefix).append("NoticePrefix:").append(this.noticePrefix)
		.append(prefix).append("OperatorPriority:").append(this.operatorPriority)
		.append(prefix).append("SMSOptionOriMinNumberLen:").append(this.oriMinNumberLen)
		.append(prefix).append("SMSOptionOTTContentAddOaddr:").append(this.ottContentAddOaddr)
		.append(prefix).append("OutClientPull:").append(this.outClientPull)
		.append(prefix).append("SMSOptionOutgoingThrottlingNum:").append(this.outgoingThrottlingNum)
		.append(prefix).append("SMSOptionOriNumberLenList:").append(this.oriNumberLens)
		.append(prefix).append("SMSOptionParseOoperator:").append(this.parseOoperator)
		.append(prefix).append("SMSOptionHttpPasswdEncryptMethod:").append(this.passwdEncryptMethod)
		.append(prefix).append("PendingAliveCount:").append(this.pendingAliveCount)
		.append(prefix).append("ChlPort2:").append(this.port2)
		.append(prefix).append("Protocol:").append(this.protocol)
		.append(prefix).append("SMSOptionAllowOriPrefixList:").append(this.pAllowOriPrefixList)
		.append(prefix).append("PhoneSubPrefix or PhonePrefix:").append(this.phonePrefixs)
		.append(prefix).append("SMSOptionPriorityFlag:").append(this.priorityFlag)
		.append(prefix).append("SMSOptionProtocolId:").append(this.protocolId)
		.append(prefix).append("MinReceiverNumber:").append(this.minReceiverNumber)
		.append(prefix).append("MaxReceiverNumber:").append(this.maxReceiverNumber)
		.append(prefix).append("ReceiverQueue:").append(this.receiverQueue)
		.append(prefix).append("SMSOptionRecMinNumberLen:").append(this.recMinNumberLen)
		.append(prefix).append("ReconnectInterval:").append(this.reconnectInterval)
		.append(prefix).append("SMSOptionRemoveRecPrefix:").append(this.removeRecPrefix)
		.append(prefix).append("SMSOptionRecNumberLenList:").append(this.recNumberLens)
		.append(prefix).append("SMSOptionRecUserPhoneLenList:").append(this.recUserPhoneLens)
		.append(prefix).append("SMSOptionConditionToRentAddr:").append(this.rentAddrConditionList)
		.append(prefix).append("SMSOptionReplaceIfFlag:").append(this.replaceIfFlag)
		.append(prefix).append("SenderAliveTime:").append(this.senderAliveTime)
		.append(prefix).append("SenderID:").append(this.senderId)
		.append(prefix).append("MinSenderNumber:").append(this.minSenderNumber)
		.append(prefix).append("MaxSenderNumber:").append(this.maxSenderNumber)
		.append(prefix).append("SenderRiseRatio:").append(this.senderRiseRatio)
		.append(prefix).append("SenderThroughput:").append(this.senderThroughput)
		.append(prefix).append("ServerID:").append(this.serverID)
		.append(prefix).append("Service:").append(this.service)
		.append(prefix).append("ServiceType:").append(this.serviceType)
		.append(prefix).append("ShortCodeIfExpand:").append(this.shortCodeIfExpand)
		.append(prefix).append("shortName:").append(this.shortName)
		.append(prefix).append("SMSOptionSmppReceiveTimeout:").append(this.smppReceiveTimeout)
		.append(prefix).append("SMSOptionChoiceSecurityHttpSession:").append(this.SMSOptionChoiceSecurityHttpSession)
		.append(prefix).append("SMSOptionHttpCustomParameter:").append(this.SMSOptionHttpCustomParameter)
		.append(prefix).append("SMSOptionHttpSpecialServiceNum:").append(this.SMSOptionHttpSpecialServiceNum)
		.append(prefix).append("SMSOptionHttpType:").append(this.SMSOptionHttpType)
//		.append(prefix).append("SMSOptionRetryPolicy:").append(this.smsOptionRetryPolicy)
		.append(prefix).append("CCBSSID:").append(this.ssid)
		.append(prefix).append("SMSOptionSupportConcatenatedMsg:").append(this.supportConcatenatedMsg)
		.append(prefix).append("SMSOptionSMPPIsSupportDCS:").append(this.supportDCS)
		.append(prefix).append("ChlIsSupportDeliveryReport:").append(this.supportDeliverReport)
		.append(prefix).append("SMSOptionSupportIncomingA2P:").append(this.supportIncomingA2P)
		.append(prefix).append("SMSOptionSupportIncomingAntiSpam:").append(this.supportIncomingAntiSpam)
		.append(prefix).append("SMSOptionSupportIncomingBinary:").append(this.supportIncomingBinary)
		.append(prefix).append("SMSOptionSupportOutgoingA2P:").append(this.supportOutgoingA2P)
		.append(prefix).append("SMSOptionSupportOutgoingAntiSpam:").append(this.supportOutgoingAntiSpam)
		.append(prefix).append("SMSOptionSupportOutgoingBinary:").append(this.supportOutgoingBinary)
		.append(prefix).append("SystemType:").append(this.systemType)
		.append(prefix).append("customerServiceType:").append(this.customerServiceType)
		.append(prefix).append("SMSOptionTransparencyMode:").append(this.transparencyMode)
		.append(prefix).append("ChlUdhConcatenated:").append(this.udhConcatenated)
		.append(prefix).append("WindowSize:").append(this.windowSize)
		.append(prefix).append("BlackholePercent:").append(this.blackholePercentMap)
		.append(prefix).append("BlackholeDrStatusCode:").append(this.blackholeDrStatusCode)
		.append(prefix).append("SupportedCharsets:").append(this.supportedCharsets)
		.append(prefix).append("SMSOptionRentAddrPrefix:").append(this.rentAddrPrefixList)
		.append(prefix).append("ShortCode:").append(this.shortCodes)
		.append(prefix).append("SMSOptionHttpSpecialServiceNum:").append(this.specialServiceNumList)
		.append(prefix).append("SMSOptionXXXPrefix4NewMO:").append(this.lNewMoPrefixList)
		.append(prefix).append("SMSOptionXXXPrefix4DRMO:").append(this.lDRMoPrefixList)
		.append(prefix).append("SMSOptionXXXPrefix4NewMT:").append(this.lNewMTPrefixList)
//		.append(prefix).append("ALLOW:").append(this.allowRecPrefixAndLens)
		.append(prefix).append("SMSOptionPiggyBackVirtualDC:").append(this.SmsOptionIsVirtualDC)
		.append(prefix).append("SMSOptionSupportIncomingGSM7bit:").append(this.smsOptionIncomingGSM7bit)
		.append(prefix).append("SMSOptionSupportOutgoingGSM7bit:").append(this.smsOptionOutgoingGSM7bit)
		.append(prefix).append("SMSOptionConvert2GSM7bit:").append(this.smsOptionConvert2GSM7bit)
		.append(prefix).append("SMSOptionForceTrans2ASCII:").append(this.smsOptionForceTrans2ASCII)
		.append(prefix).append("SMSOptionParseValidityPeriod:").append(this.parseValidityPeriod)
		.append(prefix).append("SMSOptionTransferValidityPeriod:").append(this.transferValidityPeriod)
		.append(prefix).append("SMSOptionIsMaxASCIILenTo7BitFlag:").append(this.smsOptionIsMaxASCIILenTo7BitFlag)
		.append(prefix).append("SMSOptionQueryDRHttpMethod:").append(this.httpQueryDRMethod)
		.append(prefix).append("SMSOptionIsSupportHttps:").append(this.smsOptionIsSupportHttps)
		.append(prefix).append("MCCMNCLength:").append(this.MCCMNCLength)
		.append(prefix).append("CustomerSupportLength:").append(this.customerSupportLength)
		.append(prefix).append("SMSOptionTemplateSignature:").append(this.smsOptionTemplateSignature)
		.append(prefix).append("SMSOptionParseServiceTypeID:").append(this.parseServiceTypeID)
		.append(prefix).append("DuplicateMsgServiceTypeIDList:").append(this.duplicateMsgServiceTypeIDList)
		.append(prefix).append("DuplicateMsgPeriod:").append(this.duplicateMsgPeriod)
		.append(prefix).append("CheckDuplicateMsgContent:").append(this.checkDuplicateMsgContent)
		.append(prefix).append("SMSOptionParseScheduleDeliveryTime:").append(this.parseScheduleDeliveryTime)
		.append(prefix).append("SMSOptionDRSuccessRatio:").append(this.drSucRatio)
		.append(prefix).append("SMSOptionSubmitRetryCode:").append(this.smsOptionSubmitRetryCode)
		.append(prefix).append("SMSOptionSendFakeDR:").append(this.smsOptionSendFakeDR)
		.append(prefix).append("SMSOptionTimeZone:").append(this.getSmsOptionTimeZone().getDisplayName())
		.append(prefix).append("SMSOptionDeliveryStartTime:").append(this.deliveryStartHH).append(":").append(this.deliveryStartmm)
		.append(prefix).append("SMSOptionDeliveryEndTime:").append(this.deliveryEndHH).append(":").append(this.deliveryEndmm)
		.toString();
	}

	public Map<String, Integer> getBlackholePercentMap() {
		return blackholePercentMap;
	}

	public int getBlackholeDrStatusCode() {
		return blackholeDrStatusCode;
	}
			
	public int getSmsOptionIncomingGSM7bit() {
		return smsOptionIncomingGSM7bit;
	}

	public void setSmsOptionIncomingGSM7bit(int smsOptionIncomingGSM7bit) {
		this.smsOptionIncomingGSM7bit = smsOptionIncomingGSM7bit;
	}	
	
	
	public List<Integer> getSmsOptionSubmitRetryCode() {
		return smsOptionSubmitRetryCode;
	}

	public void setSmsOptionSubmitRetryCode(List<Integer> smsOptionSubmitRetryCode) {
		this.smsOptionSubmitRetryCode = smsOptionSubmitRetryCode;
	}

	public int getSmsOptionOutgoingGSM7bit() {
		return smsOptionOutgoingGSM7bit;
	}

	public void setSmsOptionOutgoingGSM7bit(int smsOptionOutgoingGSM7bit) {
		this.smsOptionOutgoingGSM7bit = smsOptionOutgoingGSM7bit;
	}

	public boolean getSmsOptionForceTrans2ASCII() {
		return smsOptionForceTrans2ASCII;
	}

	public void setSmsOptionForceTrans2ASCII(boolean smsOptionForceTrans2ASCII) {
		this.smsOptionForceTrans2ASCII = smsOptionForceTrans2ASCII;
	}

	public boolean getSmsOptionConvert2GSM7bit() {
		return smsOptionConvert2GSM7bit;
	}

	public void setSmsOptionConvert2GSM7bit(boolean smsOptionConvert2GSM7bit) {
		this.smsOptionConvert2GSM7bit = smsOptionConvert2GSM7bit;
	}	

	public int getDrSucRatio() {
		return drSucRatio;
	}

	public void setDrSucRatio(int drSucRatio) {
		this.drSucRatio = drSucRatio;
	}
			
	public int getDrBiasRatio() {
		return drBiasRatio;
	}

	public void setDrBiasRatio(int drBiasRatio) {
		this.drBiasRatio = drBiasRatio;
	}

	private int getSSidbyCustomerNameshort(Map<String, Integer> shortName_ssid_map, String customerNameshort) {
		try {
			if (shortName_ssid_map.containsKey(customerNameshort)) {
				return shortName_ssid_map.get(customerNameshort).intValue();
			} else {
				return -1;
			}
		} catch (Exception e) {
			log.info("Failed to getSSidbyCustomerNameshort by {}",
					customerNameshort);
			return -1;
		}
	}

	public boolean isApplyInThrottleFlag() {
		return applyInThrottleFlag;
	}

	public void setApplyInThrottleFlag(boolean applyInTrottleFlag) {
		this.applyInThrottleFlag = applyInTrottleFlag;
	}

	public int getQueueNumber() {
		return queueNumber;
	}

	public void setQueueNumber(int queueNumber) {
		this.queueNumber = queueNumber;
	}

	public int getMaxReceiverNumber() {
		return maxReceiverNumber;
	}

	public void setMaxReceiverNumber(int maxReceiverNumber) {
		this.maxReceiverNumber = maxReceiverNumber;
	}

	public boolean isParseValidityPeriod() {
		return parseValidityPeriod;
	}

	public void setParseValidityPeriod(boolean parseValidityPeriod) {
		this.parseValidityPeriod = parseValidityPeriod;
	}

	public boolean isTransferValidityPeriod() {
		return transferValidityPeriod;
	}

	public void setTransferValidityPeriod(boolean transferValidityPeriod) {
		this.transferValidityPeriod = transferValidityPeriod;
	}		

	public boolean isRelateValidityPeriod() {
		return isRelateValidityPeriod;
	}

	public void setRelateValidityPeriod(boolean isRelateValidityPeriod) {
		this.isRelateValidityPeriod = isRelateValidityPeriod;
	}

	public String getHttpQueryDRMethod() {
		return httpQueryDRMethod;
	}

	public void setHttpQueryDRMethod(String httpQueryDRMethod) {
		this.httpQueryDRMethod = httpQueryDRMethod;
	}

	public int getInClientPull() {
		return inClientPull;
	}

	public void setInClientPull(int inClientPull) {
		this.inClientPull = inClientPull;
	}

	public String getSmsOptionTemplateSignature() {
		return smsOptionTemplateSignature;
	}

	public void setSmsOptionTemplateSignature(String smsOptionTemplateSignature) {
		this.smsOptionTemplateSignature = smsOptionTemplateSignature;
	}


	public List<Integer> getDuplicateMsgServiceTypeIDList() {
		
		if (duplicateMsgServiceTypeIDList == null || duplicateMsgServiceTypeIDList.size() == 0)
			return null;

		return duplicateMsgServiceTypeIDList;
	}

	public int getDuplicateMsgPeriod() {
		return duplicateMsgPeriod;
	}

	public boolean isCheckDuplicateMsgContent() {
		return checkDuplicateMsgContent;
	}

	public boolean isParseScheduleDeliveryTime() {
		return parseScheduleDeliveryTime;
	}

	public int getDeliveryStartHH() {
		return deliveryStartHH;
	}

	public int getDeliveryStartmm() {
		return deliveryStartmm;
	}

	public int getDeliveryEndHH() {
		return deliveryEndHH;
	}

	public int getDeliveryEndmm() {
		return deliveryEndmm;
	}

	public boolean isParseServiceTypeID() {
		return parseServiceTypeID;
	}

	public String getCustomerServiceType() {
		return customerServiceType;
	}

	public void setCustomerServiceType(String customerServiceType) {
		this.customerServiceType = customerServiceType;
	}

	public int getCustomerSupportLength() {
		return customerSupportLength;
	}

	public void setCustomerSupportLength(int customerSupportLength) {
		this.customerSupportLength = customerSupportLength;
	}

	public int getdSmppParaTonFlag() {
		return dSmppParaTonFlag;
	}

	public void setdSmppParaTonFlag(int dSmppParaTonFlag) {
		this.dSmppParaTonFlag = dSmppParaTonFlag;
	}

	public List<String> getSmsOptionRejectRegions() {
		return smsOptionRejectRegions;
	}

	public void setSmsOptionRejectRegions(List<String> smsOptionRejectRegions) {
		this.smsOptionRejectRegions = smsOptionRejectRegions;
	}

	public int getBlackholebatchMinNumberForPortalCust() {
		return blackholebatchMinNumberForPortalCust;
	}

	public void setBlackholebatchMinNumberForPortalCust(
			int blackholebatchMinNumberForPortalCust) {
		this.blackholebatchMinNumberForPortalCust = blackholebatchMinNumberForPortalCust;
	}

	public int getBlackholeMinNumberByTimer() {
		return blackholeMinNumberByTimer;
	}

	public void setBlackholeMinNumberByTimer(int blackholeMinNumberByTimer) {
		this.blackholeMinNumberByTimer = blackholeMinNumberByTimer;
	}

	public int getBlackholeTimerInSec() {
		return blackholeTimerInSec;
	}

	public void setBlackholeTimerInSec(int blackholeTimerInSec) {
		this.blackholeTimerInSec = blackholeTimerInSec;
	}

	public Map<Integer, Integer> getPriorityPercentMap() {
		return priorityPercentMap;
	}

	public void setPriorityPercentMap(Map<Integer, Integer> priorityPercentMap) {
		this.priorityPercentMap = priorityPercentMap;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public boolean isSmsOptionOnlySupportDRRetry() {
		return smsOptionOnlySupportDRRetry;
	}

	public void setSmsOptionOnlySupportDRRetry(boolean smsOptionOnlySupportDRRetry) {
		this.smsOptionOnlySupportDRRetry = smsOptionOnlySupportDRRetry;
	}

	public boolean isSmsOptionRemoveUdh() {
		return smsOptionRemoveUdh;
	}

	public void setSmsOptionRemoveUdh(boolean smsOptionRemoveUdh) {
		this.smsOptionRemoveUdh = smsOptionRemoveUdh;
	}

	public boolean isDrSuccForTemplateReplaceFailed() {
		return drSuccForTemplateReplaceFailed;
	}

	public void setDrSuccForTemplateReplaceFailed(
			boolean drSuccForTemplateReplaceFailed) {
		this.drSuccForTemplateReplaceFailed = drSuccForTemplateReplaceFailed;
	}

	public boolean isSmsOptionAdvancedSenderReplacement() {
		return smsOptionAdvancedSenderReplacement;
	}

	public void setSmsOptionAdvancedSenderReplacement(
			boolean smsOptionAdvancedSenderReplacement) {
		this.smsOptionAdvancedSenderReplacement = smsOptionAdvancedSenderReplacement;
	}

	public boolean isNeedDrReroute() {
		return needDrReroute;
	}

	public void setNeedDrReroute(boolean needDrReroute) {
		this.needDrReroute = needDrReroute;
	}

	
    public String getRole() { 
    	return role; 
    	
    }
	 

	public void setRole(String role) {
		this.role = role;
	}

	public String getDrDelayTimeInSec() {
		return drDelayTimeInSec;
	}

	public void setDrDelayTimeInSec(String drDelayTimeInSec) {
		this.drDelayTimeInSec = drDelayTimeInSec;
	}

	public boolean isSmsOptionSendDRBeforeOutSubmitInDC() {
		return smsOptionSendDRBeforeOutSubmitInDC;
	}

	public void setSmsOptionSendDRBeforeOutSubmitInDC(
			boolean smsOptionSendDRBeforeOutSubmitInDC) {
		this.smsOptionSendDRBeforeOutSubmitInDC = smsOptionSendDRBeforeOutSubmitInDC;
	}

	public int getSendDelayDRImmediately() {
		return sendDelayDRImmediately;
	}

	public void setSendDelayDRImmediately(int sendDelayDRImmediately) {
		this.sendDelayDRImmediately = sendDelayDRImmediately;
	}

	public ArrayList<String> getSmsRecPrefixForFakeDR() {
		return smsRecPrefixForFakeDR;
	}

	public void setSmsRecPrefixForFakeDR(ArrayList<String> smsRecPrefixForFakeDR) {
		this.smsRecPrefixForFakeDR = smsRecPrefixForFakeDR;
	}

	public boolean isNeedFakeDRByInSubmitRecPrefixInCU(String recipient) {
		boolean result = true;
		if (smsSendFakeDRByInSubmitRecPrefixInCU != null && !smsSendFakeDRByInSubmitRecPrefixInCU.isEmpty()) {
			for (String prefix : smsSendFakeDRByInSubmitRecPrefixInCU) {
				if(recipient.startsWith(prefix)){
					return result;
				}
			}
			return false;
		}
		return false;
	}
	
	public boolean isNullFakeDRByInSubmitRecPrefixInCU() {
		boolean result = true;
		if (smsSendFakeDRByInSubmitRecPrefixInCU != null && !smsSendFakeDRByInSubmitRecPrefixInCU.isEmpty()) {
			return false;
		}
		return result;
	}
	
	public boolean isNeedFakeDRForRecPrefix(String recipient) {
		boolean result = true;
		if (smsRecPrefixForFakeDR != null && !smsRecPrefixForFakeDR.isEmpty()) {
			for (String prefix : smsRecPrefixForFakeDR) {
				if(recipient.startsWith(prefix)){
					return result;
				}
			}
			return false;
		}
		return true;
	}

	public List<Integer> getSmsOptionSubmitErrorCodesForFakeDR() {
		return smsOptionSubmitErrorCodesForFakeDR;
	}

	public void setSmsOptionSubmitErrorCodesForFakeDR(
			List<Integer> smsOptionSubmitErrorCodesForFakeDR) {
		this.smsOptionSubmitErrorCodesForFakeDR = smsOptionSubmitErrorCodesForFakeDR;
	}

	public boolean isNeedCheckSession() {
		return needCheckSession;
	}

	public void setNeedCheckSession(boolean needCheckSession) {
		this.needCheckSession = needCheckSession;
	}

	public boolean isNeedStatisticMsgCount() {
		return needStatisticMsgCount;
	}

	public void setNeedStatisticMsgCount(boolean needStatisticMsgCount) {
		this.needStatisticMsgCount = needStatisticMsgCount;
	}

	public boolean isNeedCheckMsgSize() {
		return needCheckMsgSize;
	}

	public void setNeedCheckMsgSize(boolean needCheckMsgSize) {
		this.needCheckMsgSize = needCheckMsgSize;
	}
	
	public void initSSLConfiguration(A2PCustomerConfig cfg){
		sslConfiguration = new SslConfiguration();
		String keyStorePath = cfg.getString("Ssl.KeyStorePath", "/usr/local/a2p/conf/ssl/keystore").trim();
		if(keyStorePath != null && keyStorePath.length()>0){
			sslConfiguration.setKeyStorePath(keyStorePath);
		}
		String keyStrorePwd = cfg.getString("Ssl.KeyStorePassword", "tomcata2p").trim();
		if(keyStrorePwd != null && keyStrorePwd.length()>0){
			sslConfiguration.setKeyStorePassword(keyStrorePwd);
		}
		String KeyManagerPwd = cfg.getString("Ssl.KeyManagerPassword", "tomcata2p").trim();
		if(KeyManagerPwd != null && KeyManagerPwd.length()>0){
			sslConfiguration.setKeyManagerPassword(KeyManagerPwd);
		}
		String keyStoreType = cfg.getString("Ssl.KeyStoreType","JKS").trim();
		if(keyStoreType != null && !"".equalsIgnoreCase(keyStoreType)){
			sslConfiguration.setKeyStoreType(keyStoreType);
		}
		String trustStorePath = cfg.getString("Ssl.TrustStorePath", "/usr/local/a2p/conf/ssl/truststore").trim();
		if(trustStorePath != null && trustStorePath.length()>0){
			sslConfiguration.setTrustStorePath(trustStorePath);
		}
		String trustStrorePwd = cfg.getString("Ssl.TrustStorePassword", "tomcata2p").trim();
		if(trustStrorePwd != null && trustStrorePwd.length()>0){
			sslConfiguration.setTrustStorePassword(trustStrorePwd);
		}
		String trustStoreType = cfg.getString("Ssl.TrustStoreType","JKS").trim();
		if(trustStoreType != null && !"".equalsIgnoreCase(trustStoreType)){
			sslConfiguration.setTrustStoreType(trustStoreType);
		}
		
		//When isClientSendCA is false, client module should not send its CA to remote TLS server
		//so renew SSL configuration to screen the configuration of KeyStore and TrustedStore
		String  isClientTrustAll = cfg.getString("Ssl.IsClientTrustAll","false").trim();
		if(isClientTrustAll != null && "true".equalsIgnoreCase(isClientTrustAll)){
			sslConfiguration = new SslConfiguration();
		}
		
		String sslProtocol = cfg.getString("Ssl.SSLProtocol","TLS").trim();
		if(sslProtocol != null && !"".equalsIgnoreCase(sslProtocol)){
			sslConfiguration.setSslProtocol(sslProtocol);
		}
		
		String isWantClientAuth = cfg.getString("Ssl.WantClientAuth","false").trim();
		if(isWantClientAuth != null && "true".equalsIgnoreCase(isWantClientAuth)){
			sslConfiguration.setWantClientAuth(true);
		}
		
		String isNeedClientAuth = cfg.getString("Ssl.NeedClientAuth","false").trim();
		if(isNeedClientAuth != null && "true".equalsIgnoreCase(isNeedClientAuth)){
			sslConfiguration.setNeedClientAuth(true);
		}
		String includeProtocolString = cfg.getString("Ssl.IncludeProtocols");
		if(includeProtocolString!=null && includeProtocolString.length()>0) {
			String[]includeProtocols = includeProtocolString.split(","); 
			sslConfiguration.setIncludeProtocols(includeProtocols);
		}
		
		// use the CertAlias in keystore as A2P certificate
		String certAlias = cfg.getString("Ssl.CertAlias","").trim();
		if(certAlias != null && certAlias.length()>0){
			sslConfiguration.setCertAlias(certAlias);
		}
	}

	public SslConfiguration getSslConfiguration() {
		return sslConfiguration;
	}

	public void setSslConfiguration(SslConfiguration sslConfiguration) {
		this.sslConfiguration = sslConfiguration;
	}
	

    public boolean isSmsOptionSendDRByInSubmitInCU() {
        return smsOptionSendDRByInSubmitInCU;
    }

    public void setSmsOptionSendDRByInSubmitInCU(
            boolean smsOptionSendDRByInSubmitInCU) {
        this.smsOptionSendDRByInSubmitInCU = smsOptionSendDRByInSubmitInCU;
    }

	public int getBlackholeDRSucRatio() {
		return blackholeDRSucRatio;
	}

	public void setBlackholeDRSucRatio(int blackholeDRSucRatio) {
		this.blackholeDRSucRatio = blackholeDRSucRatio;
	}

	public List<String> getSmsOptionDRNoCreditCode() {
		return smsOptionDRNoCreditCode;
	}

	public void setSmsOptionDRNoCreditCode(List<String> smsOptionDRNoCreditCode) {
		this.smsOptionDRNoCreditCode = smsOptionDRNoCreditCode;
	}

	public List<Integer> getSmsOptionDRRerouteCode() {
		return smsOptionDRRerouteCode;
	}

	public void setSmsOptionDRRerouteCode(List<Integer> smsOptionDRRerouteCode) {
		this.smsOptionDRRerouteCode = smsOptionDRRerouteCode;
	}

	public int getMaxRerouteDRDelayTimeInSec() {
		return maxRerouteDRDelayTimeInSec;
	}

	public void setMaxRerouteDRDelayTimeInSec(int maxRerouteDRDelayTimeInSec) {
		this.maxRerouteDRDelayTimeInSec = maxRerouteDRDelayTimeInSec;
	}

	public boolean isNeedSupportBlackList() {
		return needSupportBlackList;
	}

	public void setNeedSupportBlackList(boolean needSupportBlackList) {
		this.needSupportBlackList = needSupportBlackList;
	}

	public String getCustomerTypeBySender() {
		return customerTypeBySender;
	}

	public void setCustomerTypeBySender(String customerTypeBySender) {
		this.customerTypeBySender = customerTypeBySender;
	}

	public int getSmsOptionRecipientMaxSendCountIn24H() {
		return smsOptionRecipientMaxSendCountIn24H;
	}

	public void setSmsOptionRecipientMaxSendCountIn24H(
			int smsOptionRecipientMaxSendCountIn24H) {
		this.smsOptionRecipientMaxSendCountIn24H = smsOptionRecipientMaxSendCountIn24H;
	}

	public Cache<Object, Object> getReicipientCache() {
		return reicipientCache;
	}

	public void setReicipientCache(Cache<Object, Object> reicipientCache) {
		this.reicipientCache = reicipientCache;
	}

	public boolean isSmsOptionRecipientNotContinueFilter() {
		return smsOptionRecipientNotContinueFilter;
	}

	public void setSmsOptionRecipientNotContinueFilter(
			boolean smsOptionRecipientNotContinueFilter) {
		this.smsOptionRecipientNotContinueFilter = smsOptionRecipientNotContinueFilter;
	}

	public boolean isNeedStatisticMsgCountByCountry() {
		return needStatisticMsgCountByCountry;
	}

	public void setNeedStatisticMsgCountByCountry(
			boolean needStatisticMsgCountByCountry) {
		this.needStatisticMsgCountByCountry = needStatisticMsgCountByCountry;
	}

	public boolean isSmsOptionChargeCountryByGateWay() {
		return smsOptionChargeCountryByGateWay;
	}

	public void setSmsOptionChargeCountryByGateWay(
			boolean smsOptionChargeCountryByGateWay) {
		this.smsOptionChargeCountryByGateWay = smsOptionChargeCountryByGateWay;
	}

	public boolean isSupportMaxInMsgID() {
		return isSupportMaxInMsgID;
	}

	public void setSupportMaxInMsgID(boolean isSupportMaxInMsgID) {
		this.isSupportMaxInMsgID = isSupportMaxInMsgID;
	}

	public ArrayList<String> getSmsSendFakeDRByInSubmitRecPrefixInCU() {
		return smsSendFakeDRByInSubmitRecPrefixInCU;
	}

	public void setSmsSendFakeDRByInSubmitRecPrefixInCU(ArrayList<String> smsSendFakeDRByInSubmitRecPrefixInCU) {
		this.smsSendFakeDRByInSubmitRecPrefixInCU = smsSendFakeDRByInSubmitRecPrefixInCU;
	}

	public boolean isSmsOptionWrMonitorCDR() {
		return smsOptionWrMonitorCDR;
	}

	public void setSmsOptionWrMonitorCDR(boolean smsOptionWrMonitorCDR) {
		this.smsOptionWrMonitorCDR = smsOptionWrMonitorCDR;
	}

	public boolean isRSupportContentLenghtAllow(GmmsMessage msg) {
		try {
			if(smsRSupportLenghtCheck==null||smsRSupportLenghtCheck.isEmpty()) {
		    	return true;
		    }
		    for(Map.Entry<String,TreeRangeMap<Integer,String>> entry: smsRSupportLenghtCheck.entrySet()) {
		    	String prefix = entry.getKey();
		    	TreeRangeMap<Integer,String> treeMap = entry.getValue();
		    	if(msg.getRecipientAddress().startsWith(prefix)) {
		    		int len = msg.getTextContent().getBytes(msg.getContentType()).length;
					/*
					 * if("ASCII".equalsIgnoreCase(msg.getContentType())) { String txt =
					 * msg.getTextContent().replace("[", ""); txt = txt.replace("]", ""); txt =
					 * txt.replace("]", ""); if() {
					 * 
					 * } }
					 */		    		
		    		String action = treeMap.get(len);
		    		if("Deny".equalsIgnoreCase(action)) {
		    			return false;
		    		}else {
		    			return true;
		    		} 
		    	}
		    }
		} catch (Exception e) {
			log.error("check the special content len failed", e);
		}
	    
		return true;
	}

	public void setSmsRSupportLenghtCheck(Map<String, TreeRangeMap<Integer, String>> smsRSupportLenghtCheck) {
		this.smsRSupportLenghtCheck = smsRSupportLenghtCheck;
	}

	public boolean isSmsOptionNeedSupportSecondRouting() {
		return smsOptionNeedSupportSecondRouting;
	}

	public void setSmsOptionNeedSupportSecondRouting(boolean smsOptionNeedSupportSecondRouting) {
		this.smsOptionNeedSupportSecondRouting = smsOptionNeedSupportSecondRouting;
	}

	public Cache<Object, Object> getReicipienSecondRoutingtCache() {
		return reicipienSecondRoutingtCache;
	}

	public void setReicipienSecondRoutingtCache(Cache<Object, Object> reicipienSecondRoutingtCache) {
		this.reicipienSecondRoutingtCache = reicipienSecondRoutingtCache;
	}

	public ArrayList<String> getSmsSecondRoutingRecPrefix() {
		return smsSecondRoutingRecPrefix;
	}

	public void setSmsSecondRoutingRecPrefix(ArrayList<String> smsSecondRoutingRecPrefix) {
		this.smsSecondRoutingRecPrefix = smsSecondRoutingRecPrefix;
	}	
	
	public boolean isNeedSecondRoutingRecPrefix(String recipient) {
		boolean result = true;
		if (smsSecondRoutingRecPrefix != null && !smsSecondRoutingRecPrefix.isEmpty()) {
			for (String prefix : smsSecondRoutingRecPrefix) {
				if("all".equalsIgnoreCase(recipient)) {
					return result;
				}
				if(recipient.startsWith(prefix)){
					return result;
				}
			}
			return false;
		}
		return false;
	}

	public ArrayList<String> getEnumRoutingRecPrefix() {
		return enumRoutingRecPrefix;
	}

	public void setEnumRoutingRecPrefix(ArrayList<String> enumRoutingRecPrefix) {
		this.enumRoutingRecPrefix = enumRoutingRecPrefix;
	}
	
	
	
	public ArrayList<String> getEnumIR21RoutingRecPrefix() {
		return enumIR21RoutingRecPrefix;
	}

	public void setEnumIR21RoutingRecPrefix(ArrayList<String> enumIR21RoutingRecPrefix) {
		this.enumIR21RoutingRecPrefix = enumIR21RoutingRecPrefix;
	}

	public String getEnumRoutingURL() {
		return enumRoutingURL;
	}

	public void setEnumRoutingURL(String enumRoutingURL) {
		this.enumRoutingURL = enumRoutingURL;
	}

	public String getEnumIR21RoutingURL() {
		return enumIR21RoutingURL;
	}

	public void setEnumIR21RoutingURL(String enumIR21RoutingURL) {
		this.enumIR21RoutingURL = enumIR21RoutingURL;
	}

	public boolean isNeedEnumQueryRecPrefix(String recipient) {
		boolean result = true;
		if (enumRoutingRecPrefix != null && !enumRoutingRecPrefix.isEmpty()) {
			for (String prefix : enumRoutingRecPrefix) {
				if("all".equalsIgnoreCase(prefix)) {
					return result;
				}
				if(recipient.startsWith(prefix)){
					return result;
				}
			}
			return false;
		}
		return false;
	}
	
	public boolean isNeedIR21EnumQueryRecPrefix(String recipient) {
		boolean result = true;
		if (enumIR21RoutingRecPrefix != null && !enumIR21RoutingRecPrefix.isEmpty()) {
			for (String prefix : enumIR21RoutingRecPrefix) {
				if("all".equalsIgnoreCase(prefix)) {
					return result;
				}
				if(recipient.startsWith(prefix)){
					return result;
				}
			}
			return false;
		}
		return false;
	}
	
	public boolean isCheckRecipitLen(String recipient) {
		boolean result = true;
		if (recipNumberLenCheck != null && !recipNumberLenCheck.isEmpty()) {
			for (Map.Entry<String, List<Integer>> entry: recipNumberLenCheck.entrySet()) {
				String key = entry.getKey();
				List<Integer> value = entry.getValue();
				if(recipient.startsWith(key)){
					if(!value.contains(recipient.length())) {
						return false;
					}
				}
			}
			return true;
		}
		return true;
	}
	
	public String changeDRStatus(String drStatus) {
		if (drStatusMapping != null && !drStatusMapping.isEmpty()) {
			return drStatusMapping.get(drStatus);			
		}
		return null;
	}

	public boolean isNotSupportUnknowDRStatus() {
		return isNotSupportUnknowDRStatus;
	}

	public void setNotSupportUnknowDRStatus(boolean isNotSupportUnknowDRStatus) {
		this.isNotSupportUnknowDRStatus = isNotSupportUnknowDRStatus;
	} 
	
	public boolean isEnableGuavaBuffer() {
		return isEnableGuavaBuffer;
	}

	public void setEnableGuavaBuffer(boolean isEnableGuavaBuffer) {
		this.isEnableGuavaBuffer = isEnableGuavaBuffer;
	}

	public void setIsEnableGuavaBuffer(String buffer) {
		if ("yes".equalsIgnoreCase(buffer))
			this.isEnableGuavaBuffer = true;
		else
			this.isEnableGuavaBuffer = false;

	}

	public boolean isKeepEnquireLink() {
		return isKeepEnquireLink;
	}

	public void setKeepEnquireLink(boolean isKeepEnquireLink) {
		this.isKeepEnquireLink = isKeepEnquireLink;
	}

	public String getTemplatecoderegex() {
		return templatecoderegex;
	}

	public void setTemplatecoderegex(String templatecoderegex) {
		this.templatecoderegex = templatecoderegex;
	}

	public Map<Integer, Integer> getSubmitNotPaidStatusMapping() {
		return submitNotPaidStatusMapping;
	}

	public void setSubmitNotPaidStatusMapping(Map<Integer, Integer> submitNotPaidStatusMapping) {
		this.submitNotPaidStatusMapping = submitNotPaidStatusMapping;
	}
	
	
	
	
}
