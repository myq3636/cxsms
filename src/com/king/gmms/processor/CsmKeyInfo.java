package com.king.gmms.processor;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class CsmKeyInfo {
	private int oSsID;
	private String senderAddress;
	private String recipientAddress;
	private String sarMsgRefNum;
	private int sarTotalSegments;
	
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
			
		if (!(obj instanceof CsmKeyInfo)) {
			return false;
		}
		
		boolean bret = false;
		final CsmKeyInfo info = (CsmKeyInfo) obj;
		if (info.getoSsID() == oSsID && info.getSenderAddress().equals(senderAddress) 
				&& info.getRecipientAddress().equals(recipientAddress) 
				&& info.getSarMsgRefNum().equals(sarMsgRefNum)
				&& info.getSarTotalSegments() == sarTotalSegments) {
			bret = true;
		}
		return bret;
	}
	public int hashCode() {
		return this.toString().hashCode();
	}

	public String toString() {
		return oSsID + "-" + senderAddress + "-" + recipientAddress + "-" + sarMsgRefNum + "-" + sarTotalSegments;
	}
	
	public int getoSsID() {
		return oSsID;
	}
	public void setoSsID(int oSsID) {
		this.oSsID = oSsID;
	}
	public String getSenderAddress() {
		return senderAddress;
	}
	public void setSenderAddress(String senderAddress) {
		this.senderAddress = senderAddress;
	}
	public String getRecipientAddress() {
		return recipientAddress;
	}
	public void setRecipientAddress(String recipientAddress) {
		this.recipientAddress = recipientAddress;
	}
	public String getSarMsgRefNum() {
		return sarMsgRefNum;
	}
	public void setSarMsgRefNum(String sarMsgRefNum) {
		this.sarMsgRefNum = sarMsgRefNum;
	}
	public void setSarTotalSegments(int sarTotalSegments) {
		this.sarTotalSegments = sarTotalSegments;
	}
	public int getSarTotalSegments() {
		return sarTotalSegments;
	}

}

