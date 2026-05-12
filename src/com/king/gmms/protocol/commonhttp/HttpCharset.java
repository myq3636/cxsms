package com.king.gmms.protocol.commonhttp;

public class HttpCharset {
	private String messageType;  // used by protocol
	private String charset; // used to do charset conversion
	public HttpCharset(String dataCoding, String mtype){
		charset=dataCoding;
		messageType=mtype;
	}
	public String getCharset() {
		return charset;
	}
	public void setCharset(String charset) {
		this.charset = charset;
	}
	public String getMessageType() {
		return messageType;
	}
	public void setMessageType(String messageType) {
		this.messageType = messageType;
	}
	public boolean equals(Object obj) {
		boolean bret = false;
		HttpCharset hc = (HttpCharset) obj;
		if (hc.getMessageType().equalsIgnoreCase(this.messageType)) {
			bret = true;
		}
		return bret;
	}

	public int hashCode() {
		return messageType.hashCode();
	}
	public String toString(){
		return "charset="+this.charset+",messageType="+this.messageType;
	}

}
