package com.king.gmms.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.king.gmms.ContentScan;

/**
 * All customer indexes built from cm.cfg.
 * <p>
 * A snapshot is filled by the reload thread only, then {@link #freeze()} wraps
 * every collection read-only and it is published as a whole by
 * A2PCustomerManager. Readers never see a half built snapshot, and a reload
 * that fails is simply never published.
 * </p>
 * Field names are the ones A2PCustomerManager used before the snapshot existed.
 */
final class CustomerSnapshot {
	/** cm.cfg blocks in effect, key: shortName (with service) */
	Map<String, A2PCustomerConfig> confBlockMap = new HashMap<String, A2PCustomerConfig>();
	/** md5 of the cm.cfg this snapshot was built from */
	String confFileVersion = null;
	/** false when some customers of that cm.cfg kept their old config: the next reload retries them */
	boolean complete = true;
	long generation = 0;

	Map<String, Integer> shortName_ssid_map = new HashMap<String, Integer>();
	Map<String, Integer> shortName_custId_map = new HashMap<String, Integer>();
	Map<Integer, Integer> custId_ssid_map = new HashMap<Integer, Integer>();
	Map<Integer, Integer> iosmsSsid_ssid_map = new HashMap<Integer, Integer>();
	Map<Integer, A2PCustomerInfo> ssid_cust_map = new HashMap<Integer, A2PCustomerInfo>();
	Map<String, A2PCustomerInfo> spid_cust_map = new HashMap<String, A2PCustomerInfo>();
	Map<String, A2PCustomerInfo> serverid_cust_map = new HashMap<String, A2PCustomerInfo>();
	Map<Integer, String> ssid_serviceType = new HashMap<Integer, String>();
	// InitServers: ServerID!=null and ChlInit=true
	List<A2PCustomerInfo> initServers = new ArrayList<A2PCustomerInfo>();
	Map<Integer, Integer> smsConnectRelayinfo = new HashMap<Integer, Integer>();

	Set<Integer> a2pInfo = new HashSet<Integer>();
	Set<Integer> channelInfo = new HashSet<Integer>();
	Map<Integer, Integer> hubInfo = new HashMap<Integer, Integer>();
	Map<Integer, Integer> operatorInfo = new HashMap<Integer, Integer>();
	Set<Integer> virtualOperatorInfo = new HashSet<Integer>();
	Set<Integer> partitionInfo = new HashSet<Integer>();

	Set<String> shortcodeInfo = new HashSet<String>();
	Map<String, Integer> shortcode_ssid_map = new HashMap<String, Integer>();
	Map<Integer, List<Pattern>> ssidToPhonePrefix = new HashMap<Integer, List<Pattern>>();
	Map<String, Integer> smsMncMcc = new HashMap<String, Integer>();
	Map<Integer, String[]> ssidOfMncMcc = new HashMap<Integer, String[]>();
	Map<String, List<String>> opRequiredCharsets = new HashMap<String, List<String>>();
	Map<String, Integer> vfkkInfo = new HashMap<String, Integer>();

	Set<Integer> oOpSupportSplitMsg = new HashSet<Integer>();
	Set<Integer> chlSupportSplitMsg = new HashSet<Integer>();
	Set<Integer> rOpSplitSuffix = new HashSet<Integer>();
	Map<Integer, String> rOpSupportLength = new HashMap<Integer, String>();
	Map<Integer, String> rOpSupportAsciiLength = new HashMap<Integer, String>();
	Set<Integer> rSsidsNotSupportDR = new HashSet<Integer>();
	Set<Integer> rSsidNotSupportUDH = new HashSet<Integer>();
	Set<Integer> rSsidSupport7Bit = new HashSet<Integer>();
	Set<Integer> isSupportExpiredDR = new HashSet<Integer>();
	Map<Integer, Integer> deliveryreportMode = new HashMap<Integer, Integer>();
	Map<Integer, String[]> customerLocalDNSPrefix = new HashMap<Integer, String[]>();
	Map<Integer, ArrayList<ContentScan>> contentScanInfo = new HashMap<Integer, ArrayList<ContentScan>>();
	List<String> noticeInfo = new ArrayList<String>();
	Map<Integer, List<Integer>> recipientLengthMap = new HashMap<Integer, List<Integer>>();

	int currentA2P = 0;
	int peeringTcpVersion = 0;
	Set<Integer> currentA2Ps = new HashSet<Integer>();

	Map<String, List<Integer>> protocol_ssid_map = new HashMap<String, List<Integer>>();
	// used by queryDR for commonHttp
	List<Integer> queryMsg_ssid_list = new ArrayList<Integer>();
	List<Integer> corePriority_ssid_list = new ArrayList<Integer>();

	private boolean frozen = false;

	boolean isFrozen() {
		return frozen;
	}

	/**
	 * Wrap every collection read-only. Called once, right before the snapshot is
	 * published; a write after that throws instead of silently changing live data.
	 */
	CustomerSnapshot freeze() {
		confBlockMap = Collections.unmodifiableMap(confBlockMap);
		shortName_ssid_map = Collections.unmodifiableMap(shortName_ssid_map);
		shortName_custId_map = Collections.unmodifiableMap(shortName_custId_map);
		custId_ssid_map = Collections.unmodifiableMap(custId_ssid_map);
		iosmsSsid_ssid_map = Collections.unmodifiableMap(iosmsSsid_ssid_map);
		ssid_cust_map = Collections.unmodifiableMap(ssid_cust_map);
		spid_cust_map = Collections.unmodifiableMap(spid_cust_map);
		serverid_cust_map = Collections.unmodifiableMap(serverid_cust_map);
		ssid_serviceType = Collections.unmodifiableMap(ssid_serviceType);
		initServers = Collections.unmodifiableList(initServers);
		smsConnectRelayinfo = Collections.unmodifiableMap(smsConnectRelayinfo);
		a2pInfo = Collections.unmodifiableSet(a2pInfo);
		channelInfo = Collections.unmodifiableSet(channelInfo);
		hubInfo = Collections.unmodifiableMap(hubInfo);
		operatorInfo = Collections.unmodifiableMap(operatorInfo);
		virtualOperatorInfo = Collections.unmodifiableSet(virtualOperatorInfo);
		partitionInfo = Collections.unmodifiableSet(partitionInfo);
		shortcodeInfo = Collections.unmodifiableSet(shortcodeInfo);
		shortcode_ssid_map = Collections.unmodifiableMap(shortcode_ssid_map);
		ssidToPhonePrefix = Collections.unmodifiableMap(ssidToPhonePrefix);
		smsMncMcc = Collections.unmodifiableMap(smsMncMcc);
		ssidOfMncMcc = Collections.unmodifiableMap(ssidOfMncMcc);
		opRequiredCharsets = Collections.unmodifiableMap(opRequiredCharsets);
		vfkkInfo = Collections.unmodifiableMap(vfkkInfo);
		oOpSupportSplitMsg = Collections.unmodifiableSet(oOpSupportSplitMsg);
		chlSupportSplitMsg = Collections.unmodifiableSet(chlSupportSplitMsg);
		rOpSplitSuffix = Collections.unmodifiableSet(rOpSplitSuffix);
		rOpSupportLength = Collections.unmodifiableMap(rOpSupportLength);
		rOpSupportAsciiLength = Collections.unmodifiableMap(rOpSupportAsciiLength);
		rSsidsNotSupportDR = Collections.unmodifiableSet(rSsidsNotSupportDR);
		rSsidNotSupportUDH = Collections.unmodifiableSet(rSsidNotSupportUDH);
		rSsidSupport7Bit = Collections.unmodifiableSet(rSsidSupport7Bit);
		isSupportExpiredDR = Collections.unmodifiableSet(isSupportExpiredDR);
		deliveryreportMode = Collections.unmodifiableMap(deliveryreportMode);
		customerLocalDNSPrefix = Collections.unmodifiableMap(customerLocalDNSPrefix);
		contentScanInfo = Collections.unmodifiableMap(contentScanInfo);
		noticeInfo = Collections.unmodifiableList(noticeInfo);
		recipientLengthMap = Collections.unmodifiableMap(recipientLengthMap);
		currentA2Ps = Collections.unmodifiableSet(currentA2Ps);
		protocol_ssid_map = Collections.unmodifiableMap(protocol_ssid_map);
		queryMsg_ssid_list = Collections.unmodifiableList(queryMsg_ssid_list);
		corePriority_ssid_list = Collections.unmodifiableList(corePriority_ssid_list);
		frozen = true;
		return this;
	}
}
