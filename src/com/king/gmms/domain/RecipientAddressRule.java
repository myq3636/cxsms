package com.king.gmms.domain;

import java.util.List;

public class RecipientAddressRule {
	
	private List<Integer> lens;
	private List<String> allowPrefixList;
	private List<String> blackPrefixList;
	private String cc;
	private String formatRegex;
	
	public List<Integer> getLens() {
		return lens;
	}
	public void setLens(List<Integer> lens) {
		this.lens = lens;
	}	
	public List<String> getAllowPrefixList() {
		return allowPrefixList;
	}
	public void setAllowPrefixList(List<String> allowPrefixList) {
		this.allowPrefixList = allowPrefixList;
	}
	
	public List<String> getBlackPrefixList() {
		return blackPrefixList;
	}
	public void setBlackPrefixList(List<String> blackPrefixList) {
		this.blackPrefixList = blackPrefixList;
	}
	public String getCc() {
		return cc;
	}
	public void setCc(String cc) {
		this.cc = cc;
	}
		
	public String getFormatRegex() {
		return formatRegex;
	}
	public void setFormatRegex(String formatRegex) {
		this.formatRegex = formatRegex;
	}
	@Override
	public String toString() {
		return "RecipientAddressRule [lens=" + lens + ", allowPrefixList="
				+ allowPrefixList + ", blackPrefixList=" + blackPrefixList
				+ ", cc=" + cc + ", formatRegex=" + formatRegex + "]";
	}
	
	
	
	
	
	

}
