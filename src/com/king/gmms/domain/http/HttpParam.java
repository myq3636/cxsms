package com.king.gmms.domain.http;

public class HttpParam {
	private String defaultValue;
	private String type;
	private String format;
	private String param;
	private String oppsiteParam;
	private String encoding;
	
	//add by kevin on 20131123
	private String ccbName;

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getParam() {
		return param;
	}

	public void setParam(String param) {
		this.param = param;
	}

	public String getOppsiteParam() {
		return oppsiteParam;
	}

	public void setOppsiteParam(String oppsiteParam) {
		this.oppsiteParam = oppsiteParam;
	}

	public String getEncoding() {
		return encoding;
	}
	
	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	
	public String getCcbName() {
		return ccbName;
	}

	public void setCcbName(String ccbName) {
		this.ccbName = ccbName;
	}
	
	public String toString() {
		return "(param = " + param + ", oppsiteParam = " + oppsiteParam + ", value = " + defaultValue + ", encoding = " + encoding + ", format = " + format + ", type = " + type
				 + ")";
	}

}
