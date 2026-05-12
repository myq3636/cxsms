package com.king.gmms;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class RetryPolicyInfo {		
	
	public RetryPolicyInfo(){
	}
	
	/**  
	    Priority level: The related retry policy configures items priority like this:
		SMSOptionMTRetryPolicy > SMSOptionServiceTypeIDRetryPolicy > 
		SMSOptionSenderRetryPolicy > SMSOptionMORetryPolicy > default Retry Policy
	 **/
	
	/*
	 *   senderPrefixPolicyMap: <Pattern,List<Integer>>
	 *   Key:Prefix regular expression or Prefix + length regular expression
	 *   value:List<Integer> which is used to store the policy info.
	 * */
	private Map<Pattern,List<Integer>> senderPrefixPolicyMap  = new ConcurrentHashMap<Pattern,List<Integer>>();
	
	/*
	 *   senderServiceTypeIDPolicyMap: <Integer,List<Integer>> 
	 *   Key:service type id 
	 *   value:List<Integer> which is used to store the policy info.
	 * */
	private	Map<Integer,List<Integer>> serviceTypeIDPolicyMap = new ConcurrentHashMap<Integer,List<Integer>>();

	/*
	 * mtPolicyList and moPolicyList: List<Integer>
	 * which is used to store the retry policy info for MT and MO direction message for each customer
	 * */
	private List<Integer> mtPolicyList = null;
	
	private List<Integer> moPolicyList = null;	
	
	public Map<Pattern, List<Integer>> getSenderPrefixPolicyMap() {
		return senderPrefixPolicyMap;
	}
	public void setSenderPrefixPolicyMap(
			Map<Pattern, List<Integer>> senderPrefixPolicyMap) {
		this.senderPrefixPolicyMap = senderPrefixPolicyMap;
	}
		
	public List<Integer> getMtPolicyList() {
		return mtPolicyList;
	}
	public void setMtPolicyList(List<Integer> mtPolicyList) {
		this.mtPolicyList = mtPolicyList;
	}
	
	public List<Integer> getMoPolicyList() {
		return moPolicyList;
	}
	public void setMoPolicyList(List<Integer> moPolicyList) {
		this.moPolicyList = moPolicyList;
	}
	
	public Map<Integer, List<Integer>> getServiceTypeIDPolicyMap() {
		return serviceTypeIDPolicyMap;
	}
	public void setServiceTypeIDPolicyMap(
			Map<Integer, List<Integer>> serviceTypeIDPolicyMap) {
		this.serviceTypeIDPolicyMap = serviceTypeIDPolicyMap;
	}
}
