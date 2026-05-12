package com.king.gmms.processor;


import java.util.Date;

/**
 * @author bensonchen
 * @version 1.0.0
 */
public class CsmValueInfo implements Comparable<CsmValueInfo> {
	private int sarSegmentSeqNum;
	private String textContent;
	private byte[] mimeMultiPartData;
	private String inMsgID;
	private String msgID;
	private int messageSize;
	private Date timeStamp;
	private String contentType;
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
			
		if (!(obj instanceof CsmValueInfo)) {
			return false;
		}
		
		boolean bret = false;
		final CsmValueInfo info = (CsmValueInfo) obj;
		if (info.getSarSegmentSeqNum() == sarSegmentSeqNum) {
			bret = true;
		}
		return bret;
	}
	public int hashCode() {
		return ("" + sarSegmentSeqNum).hashCode();
	}

	public String toString() {
		return inMsgID + "-" + msgID + "-" + sarSegmentSeqNum;
	}
	
	public int getMessageSize() {
		return messageSize;
	}
	public void setMessageSize(int messageSize) {
		this.messageSize = messageSize;
	}
	public void setTextContent(String textContent) {
		this.textContent = textContent;
	}
	public String getTextContent() {
		return textContent;
	}
	public void setSarSegmentSeqNum(int sarSegmentSeqNum) {
		this.sarSegmentSeqNum = sarSegmentSeqNum;
	}
	public int getSarSegmentSeqNum() {
		return sarSegmentSeqNum;
	}
	public String getInMsgID() {
		return inMsgID;
	}
	public void setInMsgID(String inMsgID) {
		this.inMsgID = inMsgID;
	}
	public String getMsgID() {
		return msgID;
	}
	public void setMsgID(String msgID) {
		this.msgID = msgID;
	}
	public void setMimeMultiPartData(byte[] mimeMultiPartData) {
		this.mimeMultiPartData = mimeMultiPartData;
	}
	public byte[] getMimeMultiPartData() {
		return mimeMultiPartData;
	}	
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	/** 
	 * @param o
	 * @return
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(CsmValueInfo o) {
		CsmValueInfo other = (CsmValueInfo) o;  
        if (this.sarSegmentSeqNum > other.getSarSegmentSeqNum())  
            return 1;  
        if (this.sarSegmentSeqNum < other.getSarSegmentSeqNum())  
            return -1;  
        return 0;  
	}
	public void setTimeStamp(Date timeStamp) {
		this.timeStamp = timeStamp;
	}
	public Date getTimeStamp() {
		return timeStamp;
	}
}
