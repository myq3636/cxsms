package com.king.gmms.protocol.commonhttp;


public class HttpStatus {
	protected String code;
	protected String text;
	public static final HttpStatus STATUSMAPERROR = new HttpStatus("-2", "Status Map Error"); 
		
	public HttpStatus(){
		
	}
	public HttpStatus(String code, String text) {
		this.code = code;
		this.text = text;
	}
	

	public String getCode() {
		return code;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public boolean equals(Object obj) {
		boolean bret = false;
		HttpStatus hs = (HttpStatus) obj;
		if(hs.getText()==null&&hs.getCode()!=null){
			if(hs.getCode().equalsIgnoreCase(this.code)){
			   bret =true;
			}
		}else if(hs.getText()!=null&&hs.getCode()==null){
			if(hs.getText().equalsIgnoreCase(this.text)){
				   bret =true;
				}
		}else if(hs.getText()!=null&&hs.getCode()!=null){
			if (hs.getText().equalsIgnoreCase(this.text) && (hs.getCode().equalsIgnoreCase(this.code))) {
				bret = true;
		    }			
		}else {
			bret = false;
		}
		return bret;
	}

	public int hashCode() {
		return this.toString().hashCode();
	}

	public String toString() {
		return this.code + "-" + this.text;
	}		
	
}
