package com.king.gmms.domain.http;

import java.util.ArrayList;
import java.util.List;

public class HttpPdu {
	private List<HttpParam> paramList = new ArrayList<HttpParam>();
	private String handlerClass = null;
	private String responseDelimiter = "\r\n";
	private String parameterDelimiter = null;

	public boolean add(HttpParam p){
		return this.paramList.add(p);
	}

	public List<HttpParam> getParamList() {
		return paramList;
	}

	public void setParamList(List<HttpParam> paramList) {
		this.paramList = paramList;
	}

	public String getHandlerClass() {
		return handlerClass;
	}

	public void setHandlerClass(String handlerClass) {
		this.handlerClass = handlerClass;
	}
	
	public boolean hasHandlerClass(){
		return (handlerClass != null);
	}

	public String getResponseDelimiter() {
		return responseDelimiter;
	}

	public void setResponseDelimiter(String responseDelimiter) {
		this.responseDelimiter = responseDelimiter;
	}

	public String getParameterDelimiter() {
		return parameterDelimiter;
	}

	public void setParameterDelimiter(String parameterDelimiter) {
		this.parameterDelimiter = parameterDelimiter;
	}
}
