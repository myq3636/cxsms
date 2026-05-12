package com.king.gmms.domain;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
import java.util.regex.*;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.BindMode;

import sun.util.logging.resources.logging;

public class A2PMultiConnectionInfo extends A2PCustomerInfo{

	private static SystemLogger log = SystemLogger
			.getSystemLogger(A2PMultiConnectionInfo.class);
    protected int enquireLinkFailureNum = 3;
    protected int failEnquireLinkTime = 10 * 1000;
    protected int maxRetryNum = 3;

    protected boolean autoMode = true;

    //EMIUCP, new configured names like "SMSOptionUCPPid" format to replace old name "SMSOptionPid" format
    protected String oton = "6";
    protected String onpi = "5";
    protected String vers = "0100";
    protected String styp = "1";
    protected String pid = "0539";

    //Peering TCP
    protected int peeringTcpVersion = 0;
    protected Pattern pCdmaPrefixList = null;

    protected String submitConnectionPolicy = "random";
    protected String drConnectionPolicy = "random";
    protected String responseConnectionPolicy = "SameSession";
    protected String primarySubmitConnection = null;
    protected String primaryDRConnection = null;

    protected int iPHSMsgLen = -1;


    protected int bufferLimit = 1000;
    protected String manualNodeName;
    protected boolean init = false;
    
    public A2PMultiConnectionInfo(){
    	super();
    }
    
    public A2PMultiConnectionInfo(A2PSingleConnectionInfo sc){
        if(sc != null){
            this.ssid = sc.getSSID();
            this.service = sc.getService();
            this.shortName = sc.getShortName();

            this.serverID = sc.getServerID();
            this.a2pPlayerType = sc.getA2PPlayerType();
            this.receiverQueue = sc.getReceiverQueue();
            this.chlQueue = sc.getChlQueue();
            this.messageMode = sc.getMessageMode();
            this.inClientPull = sc.getInClientPull();
            this.outClientPull = sc.getOutClientPull();
            this.supportDeliverReport = sc.isSupportDeliverReport();
            this.supportedCharsets = sc.getSupportedCharsets();
            this.convertToBinaryCharsets = sc.getConvertToBinaryCharsets();

            this.minSenderNumber = sc.getMinSenderNumber();
            this.maxSenderNumber = sc.getMaxSenderNumber();

            this.minReceiverNumber = sc.getMinReceiverNumber();
            this.maxReceiverNumber = sc.getMaxReceiverNumber();
            this.connectionSilentTime = sc.getConnectionSilentTime();
            this.windowSize = sc.getWindowSize();

            int etime = sc.getEnquireLinkTime();
            this.enquireLinkTime = (etime > 0 ? etime : 30000);

            this.bufferTimeout = sc.getBufferTimeout();
            this.connectionType = sc.getConnectionType();
            this.isServerEnquireLink = sc.isServerEnquireLink();
            this.isEnquireLink = sc.isEnquireLink();
            this.pAllowOriPrefixList = sc.getAllowOriPrefixList();

            this.oriNumberLens = sc.getOriNumberLens();
            this.recNumberLens = sc.getRecNumberLens();
            this.oriMinNumberLen = sc.getOriMinNumberLen();
            this.recMinNumberLen = sc.getRecMinNumberLen();

            this.parseOoperator = sc.isParseOoperator();
            this.udhConcatenated = sc.isUdhConcatenated();
            this.supportConcatenatedMsg = sc.isSupportConcatenatedMsg();
            this.operatorPriority = sc.getOperatorPriority();

            this.transparencyMode = sc.getTransparencyMode(); //SMSOptionNeedTransparency

            this.foreword = sc.getForeword();
            this.ottContentAddOaddr = sc.getOttContentAddOaddr();

            this.supportIncomingBinary = sc.isSupportIncomingBinary();
            this.supportOutgoingBinary = sc.isSupportOutgoingBinary();
            this.supportIncomingA2P = sc.getSupportIncomingA2P();
            this.supportOutgoingA2P = sc.isSupportOutgoingA2P();

            this.incomingThrottlingNum = sc.getIncomingThrottlingNum();
            this.applyInThrottleFlag = sc.isApplyInThrottleFlag();
            this.outgoingThrottlingNum = sc.getOutgoingThrottlingNum();

            this.needAntiSpam = sc.isNeedAntiSpam(); // default
            this.checkAlphaWhenReplaceOAddr = sc.isCheckAlphaWhenReplaceOAddr();
            this.matchFullWhenRouteReplaceOAddr = sc.isMatchFullWhenRouteReplaceOAddr(); 

            this.expireTime = sc.getExpireTime();
            this.finalExpireTime = sc.getFinalExpireTime();
//            this.smsOptionRetryPolicy = sc.getSmsOptionRetryPolicy();
            this.smsOptionTimeZone = sc.getSmsOptionTimeZone();

            this.bIsHandlePrefix4NewMO = sc.isHandlePrefix4NewMO();
            this.bIsHandlePrefix4DRMO = sc.isHandlePrefix4DRMO();
            this.bIsHandlePrefix4NewMT = sc.isHandlePrefix4NewMT();

            this.lNewMoPrefixList = sc.getLNewMoPrefixList();
            this.lDRMoPrefixList = sc.getLDRMoPrefixList();
            this.lNewMTPrefixList = sc.getLNewMTPrefixList();

            this.shortCodes = sc.getShortCodes(); // Key: prefix, Value: short code

            this.shortCodeIfExpand = sc.getShortCodeIfExpand();
//            this.domainNames = sc.getDomainNamesList(); //domain names

            this.noticePrefix = sc.getNoticePrefix();
            this.noticeCustomer = sc.getNoticeCustomer();

            //phonePrefix and phoneSubPrefix
            this.phonePrefixs = sc.getPhonePrefixs();

            this.extProperties = sc.getExtProperty();
            this.pendingAliveCount = sc.getPendingAliveCount();
            this.alSenderMapping = sc.getAlSenderMapping();

            this.reconnectInterval = sc.getReconnectInterval();
            this.isDRStatusInOptionPara = sc.getDRStatusIsOptionPara();

            this.removeRecPrefix = sc.getRemoveRecPrefix();
            this.bSmppIsPutMsgId4DSR = sc.getSMPPIsPutMsgId4DSR();
            this.isSmppMapErrCod4DR = sc.isSmppMapErrCod4DR();
            this.bSmppIsGenHexMsgId = sc.getSMPPIsGenHexMsgId();
            this.chlSMPPMsgIDParse = sc.isChlSMPPMsgIDParse();
            this.bSmppIsPadZero4SR = sc.getSMPPIsPadZero4SR();
            this.needReceiptedMsgId=sc.isNeedReceiptedMsgId();
            this.supportDCS=sc.isSupportDCS();
            this.drSwapAddr = sc.isDrSwapAddr();
            this.deliveryReportMode = sc.getDeliveryReportMode();
            this.dealExceptionDR = sc.isDealExceptionDR();
            this.chlAcctNamer = sc.getChlAcctNamer();
            this.chlPasswordr = sc.getChlPasswordr();
            this.port2 = sc.getPort2(); //recevier port,
            this.clientSessionNumber = sc.getClientSessionNumber();
            this.bindMode = sc.getBindMode();

            this.serviceType = sc.getServiceType();
            this.systemType = sc.getSystemType();
            this.sourceAddrTon = sc.getSourceAddrTon();
            this.sourceAddrNpi = sc.getSourceAddrNpi();
            this.addrTon = sc.getAddrTon();
            this.addrNpi = sc.getAddrNpi();
            this.destAddrTon = sc.getDestAddrTon();
            this.destAddrNpi = sc.getDestAddrNpi();
            this.priorityFlag = sc.getPriorityFlag();
            this.replaceIfFlag = sc.getReplaceIfFlag();
            this.protocolId = sc.getProtocolId();
            this.smppVersion = sc.getSMPPVersion();
            this.smppReceiveTimeout = sc.getSmppReceiveTimeout();
            this.supportDCS=sc.isSupportDCS();//added by Jianming in v1.01
            supportIncomingAntiSpam = sc.isSupportIncomingAntiSpam();
            supportOutgoingAntiSpam = sc.isSupportOutgoingAntiSpam();
            recUserPhoneLens = sc.getRecUserPhoneLens();
//            allowRecPrefixAndLens = sc.getAllowRecPrefixAndLens();
            this.protocol = sc.getProtocol();
            this.iosmsSsid = sc.getIosmsSsid();
            this.customerId = sc.getCustomerId();
            this.rentAddrPrefixList = sc.getRentAddrPrefixList();
            this.rentAddrConditionList = sc.getRentAddrConditionList();
            this.blackholePercentMap = sc.getBlackholePercentMap();
            this.blackholeDrStatusCode = sc.getBlackholeDrStatusCode();
            this.SmsOptionIsVirtualDC = sc.isSmsOptionIsVirtualDC();
            
            this.smsOptionIncomingGSM7bit = sc.getSmsOptionIncomingGSM7bit();
            this.smsOptionOutgoingGSM7bit = sc.getSmsOptionOutgoingGSM7bit();
            this.parseValidityPeriod = sc.isParseValidityPeriod();
            this.transferValidityPeriod = sc.isTransferValidityPeriod();
            this.smsOptionIsSupportHttps = sc.isSMSOptionIsSupportHttps();
            this.MCCMNCLength = sc.getMCCMNCLength();

            this.duplicateMsgServiceTypeIDList = sc.getDuplicateMsgServiceTypeIDList();
            this.duplicateMsgPeriod = sc.getDuplicateMsgPeriod();
            this.checkDuplicateMsgContent = sc.isCheckDuplicateMsgContent();
            
            this.parseServiceTypeID = sc.isParseServiceTypeID();
            
            this.parseScheduleDeliveryTime = sc.isParseScheduleDeliveryTime();
            this.deliveryStartHH = sc.getDeliveryEndHH();
            this.deliveryStartmm = sc.getDeliveryEndmm();
            this.deliveryEndHH = sc.getDeliveryEndHH();
            this.deliveryEndmm = sc.getDeliveryEndmm();
            this.oriNumberLenController = sc.getOriNumberLenController();
        }
    }
    
    public String getOnpi() {
        return onpi;
    }

    public String getOton() {
        return oton;
    }

    public void setOnpi(String onpi) {
        this.onpi = onpi;
    }

    public void setOton(String oton) {
        this.oton = oton;
    }

    public String getVers() {
        return vers;
    }

    public void setVers(String vers){
        this.vers = vers;
    }

    public String getStyp() {
        return styp;
    }

    public void setStyp(String styp) {
        this.styp = styp;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setPCdmaPrefixList(Pattern pattern){
        this.pCdmaPrefixList = pattern;
    }

    public Pattern getCdmaPrefixList(){
        return this.pCdmaPrefixList;
    }


    public boolean isAutoMode() {
        return autoMode;
    }

    public int getBufferLimit() {
        return bufferLimit;
    }

    public String getDrConnectionPolicy() {
        return drConnectionPolicy;
    }

    public int getEnquireLinkFailureNum() {
        return enquireLinkFailureNum;
    }


    public int getFailEnquireLinkTime() {
        return failEnquireLinkTime;
    }

//    public String getFeeCode() {
//        return feeCode;
//    }

    public boolean isInit() {
        return init;
    }




//
//    public String getLocalGatewayID() {
//        return localGatewayID;
//    }

    public String getManualNodeName() {
        return manualNodeName;
    }

    public int getMaxRetryNum() {
        return maxRetryNum;
    }

//
//
//    public String getMsgSrc() {
//        return msgSrc;
//    }

    public String getPrimaryDRConnection() {
        return primaryDRConnection;
    }

    public String getPrimarySubmitConnection() {
        return primarySubmitConnection;
    }


    public String getResponseConnectionPolicy() {
        return responseConnectionPolicy;
    }

//    public String getRole() {
//        return role;
//    }

    public String getSubmitConnectionPolicy() {
        return submitConnectionPolicy;
    }

//    public String getTpPid() {
//        return tpPid;
//    }
//
//    public String getTpUdhi() {
//        return tpUdhi;
//    }

    public int getPeeringTcpVersion() {
        return peeringTcpVersion;
    }

    public void setAutoMode(boolean autoMode) {
        this.autoMode = autoMode;
    }

    public void setBufferLimit(int bufferLimit) {
        this.bufferLimit = bufferLimit;
    }

    public void setDrConnectionPolicy(String drConnectionPolicy) {
        this.drConnectionPolicy = drConnectionPolicy;
    }

    public void setEnquireLinkFailureNum(int enquireLinkFailureNum) {
        this.enquireLinkFailureNum = enquireLinkFailureNum;
    }

    public void setFailEnquireLinkTime(int failEnquireLinkTime) {
        this.failEnquireLinkTime = failEnquireLinkTime;
    }

//    public void setFeeCode(String feeCode) {
//        this.feeCode = feeCode;
//    }

    public void setInit(boolean init) {
        this.init = init;
    }



//
//    public void setLocalGatewayID(String localGatewayID) {
//        this.localGatewayID = localGatewayID;
//    }

    public void setManualNodeName(String manualNodeName) {
        this.manualNodeName = manualNodeName;
    }

    public void setMaxRetryNum(int maxRetryNum) {
        this.maxRetryNum = maxRetryNum;
    }


//
//    public void setMsgSrc(String msgSrc) {
//        this.msgSrc = msgSrc;
//    }

    public void setPrimaryDRConnection(String primaryDRConnection) {
        this.primaryDRConnection = primaryDRConnection;
    }

    public void setPrimarySubmitConnection(String primarySubmitConnection) {
        this.primarySubmitConnection = primarySubmitConnection;
    }




    public void setResponseConnectionPolicy(String responseConnectionPolicy) {
        this.responseConnectionPolicy = responseConnectionPolicy;
    }
//
//    public void setRole(String role) {
//        this.role = role;
//    }

    public void setSubmitConnectionPolicy(String submitConnectionPolicy) {
        this.submitConnectionPolicy = submitConnectionPolicy;
    }

//    public void setTpPid(String tpPid) {
//        this.tpPid = tpPid;
//    }
//
//    public void setTpUdhi(String tpUdhi) {
//        this.tpUdhi = tpUdhi;
//    }

    public void setPeeringTcpVersion(int peeringTcpVersion) {
        this.peeringTcpVersion = peeringTcpVersion;
    }

    public void setPHSMsgLen(int len){
        this.iPHSMsgLen = len;
    }

    public int getPHSMsgLen(){
        return iPHSMsgLen;
    }

    protected ConnectionInfo assignConnectionInfo(Group group){
        if(group == null) return null;

        ConnectionInfo connInfo = new ConnectionInfo();

        for (AttrPair attr : group.getAllAttrs()) {
            String curItem = attr.getItem();

            if ("URL".equalsIgnoreCase(curItem)) {
                connInfo.setURL(attr.getStringValue());
            }
            else if ("userName".equalsIgnoreCase(curItem)) {
                connInfo.setUserName(attr.getStringValue());
            }
            else if ("password".equalsIgnoreCase(curItem)) {
                try {
					connInfo.setPassword(URLDecoder.decode(attr.getStringValue(), "utf-8"));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
	                log.error("urlDecode passwords error! password value is:{}", attr.getStringValue(), e);
				}
            }
            else if ("Port".equalsIgnoreCase(curItem)) {
                connInfo.setPort(attr.getIntValue());
            }
            else if ("SessionNumber".equalsIgnoreCase(curItem)) {
                connInfo.setSessionNum(attr.getIntValue());
            }
            else if ("MinReceiverNumber".equalsIgnoreCase(curItem)) {
                connInfo.setMinReceiverNum(attr.getIntValue());
            }
            else if ("MaxReceiverNumber".equalsIgnoreCase(curItem)) {
                connInfo.setMaxReceiverNum(attr.getIntValue());
            }
            // the sender config is customer level, init it on assignValue
            
            else if ("IsPersistent".equalsIgnoreCase(curItem)) {
                connInfo.setIsPersistent(attr.getStringValue());
            }
            else if ("Version".equalsIgnoreCase(curItem)) {
                String ver = attr.getStringValue("V34");
                ver = ver.replaceFirst("V","");
                connInfo.setVersion(Integer.parseInt(ver));
            }
            else if ("IsInit".equalsIgnoreCase(curItem)) {
                connInfo.setIsInit(attr.getStringValue());
            }
            else if ("BindMode".equalsIgnoreCase(curItem)) {
                connInfo.setBindMode(BindMode.getBindMode(attr.getStringValue()));
            }
            else if("IsCreateReceiver".equalsIgnoreCase(curItem)){
                connInfo.setIsCreateReceiver(attr.getBoolValue());
            }
            else {
                connInfo.addExtProperties(curItem, attr.getStringValue());
            }
        }

        return connInfo;
    }


    public void assignValue(A2PCustomerConfig cfg, Map<String, Integer> shorNameSsidMap) throws
        CustomerConfigurationException {
        super.assignValue(cfg, shorNameSsidMap);

        enquireLinkFailureNum = cfg.getInt("EnquireLinkFailureNum", 3);
        failEnquireLinkTime = cfg.getInt("FailEnquireLinkTime", 10)*1000;
        maxRetryNum = cfg.getInt("MaxRetryTimes", 3);
        autoMode = cfg.getBool("AutoMode", true);

//        feeCode = cfg.getString("FeeCode", "100");
//        localGatewayID = cfg.getString("LocalGatewayID", "002006");
//        msgSrc = cfg.getString("MsgSrc", "900999");
//        role = cfg.getString("Role", "ISMG");
//        tpPid = cfg.getString("TpPid", "0");
//        tpUdhi = cfg.getString("TpUdhi", "0");

        oton = cfg.getString("SMSOptionUCPOton", "6");
        onpi = cfg.getString("SMSOptionUCPOnpi", "5");
        vers = cfg.getString("SMSOptionUCPVers", "0100");
        styp = cfg.getString("SMSOptionUCPStyp", "1");
        pid = cfg.getString("SMSOptionUCPPid", "0539");

        String PeeringTcpVersionStr = cfg.getString("PeeringTcpVersion");
        if (PeeringTcpVersionStr != null) {
            peeringTcpVersion = getPeeringTcpVersionInt(
                PeeringTcpVersionStr);
            setPeeringTcpVersion(peeringTcpVersion);
        }

        String cdmaPrefixList = cfg.getString("SMSOptionCdmaPrefixList");
        pCdmaPrefixList = getPattern(cdmaPrefixList);

        A2PCustomerMultiValue conn_policy = cfg.parseMultiValue("SubmitPolicy_Conn");
        if (conn_policy != null) {
            this.submitConnectionPolicy = conn_policy.getAttr("Policy").getStringValue("random");
            if("primary".equalsIgnoreCase(submitConnectionPolicy)){
                this.primarySubmitConnection =
                    conn_policy.getAttr("Primary").getStringValue();
            }
        }

        conn_policy = cfg.parseMultiValue("DRPolicy_Conn");
        if (conn_policy != null) {
            this.drConnectionPolicy = conn_policy.getAttr("Policy").getStringValue("random");
            if("primary".equalsIgnoreCase(drConnectionPolicy)){
                this.primaryDRConnection =
                    conn_policy.getAttr("Primary").getStringValue();
            }
        }

        //This item maybe raise some problems. Currently no such configuration in CCB.
        conn_policy = cfg.parseMultiValue("ResponsePolicy_Conn");
        if (conn_policy != null) {
            this.responseConnectionPolicy = conn_policy.getAttr("Policy").getStringValue("SameSession");
        }

        iPHSMsgLen = cfg.getInt("SMSOptionPHSMsgLen", -1);

        bufferLimit = cfg.getInt("BufferLimit", 1000);
        manualNodeName = cfg.getString("ManualNodeName");
        init = cfg.getBool("IsInit", false);

        //Different default value in SingleConnectionInfo and MultiConnectionInfo
        reconnectInterval = cfg.getInt("ReconnectInterval", 10)*1000;
        enquireLinkTime = cfg.getInt("EnquireLinkTime", 30)*1000;
        connectionSilentTime = cfg.getInt("ConnectionSilentTime", 300)*1000;
        minSenderNumber = cfg.getInt("MinSenderNumber", -1);
        maxSenderNumber = cfg.getInt("MaxSenderNumber", -1);
        windowSize = cfg.getInt("WindowSize", 100);
    }

    private int getPeeringTcpVersionInt(String peeringTcpVersionStr) {
        int result = 0;
        if (peeringTcpVersionStr == null) {
            return result;
        }
        else {
            if (!peeringTcpVersionStr.startsWith("V") &&
                !peeringTcpVersionStr.startsWith("v")) {
                return result;
            }
            else {
                try {
                    result = Integer.parseInt(peeringTcpVersionStr.substring(1,
                        peeringTcpVersionStr.length()));
                    return result;
                }
                catch (Exception e) {
                    return 0;
                }
            }
        }
    }

    public String toString(){
		String prefix = "\r\n";
		return new StringBuffer().append(super.toString())
		.append(prefix).append("AutoMode:").append(this.autoMode)
		.append(prefix).append("BufferLimit:").append(this.bufferLimit)
		.append(prefix).append("DRPolicy_Conn:").append(this.drConnectionPolicy)
		.append(prefix).append("EnquireLinkFailureNum:").append(this.enquireLinkFailureNum)
		.append(prefix).append("FailEnquireLinkTime:").append(this.failEnquireLinkTime)
		.append(prefix).append("IsInit:").append(this.init)
		.append(prefix).append("SMSOptionPHSMsgLen:").append(this.iPHSMsgLen)
		.append(prefix).append("ManualNodeName:").append(this.manualNodeName)
		.append(prefix).append("MaxRetryTimes:").append(this.maxRetryNum)
		.append(prefix).append("SMSOptionUCPOnpi:").append(this.onpi)
		.append(prefix).append("SMSOptionUCPOton:").append(this.oton)
		.append(prefix).append("PeeringTcpVersion:").append(this.peeringTcpVersion)
		.append(prefix).append("SMSOptionUCPPid:").append(this.pid)
		.append(prefix).append("DRPolicy_Conn:").append(this.primaryDRConnection)
		.append(prefix).append("SubmitPolicy_Conn:").append(this.primarySubmitConnection)
		.append(prefix).append("ResponsePolicy_Conn:").append(this.responseConnectionPolicy)
		.append(prefix).append("SMSOptionUCPStyp:").append(this.styp)
		.append(prefix).append("SubmitPolicy_Conn:").append(this.submitConnectionPolicy)
		.append(prefix).append("SMSOptionUCPVers:").append(this.vers)
		.append(prefix).append("SMSOptionCdmaPrefixList:").append(this.pCdmaPrefixList)			
		.toString();
	}
}
