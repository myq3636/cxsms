package com.king.gmms.domain;

import java.util.Properties;

import com.king.gmms.connectionpool.BindMode;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2006</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ConnectionInfo {

    private String connectionName = null;
    private String userName = null;
    private String password = null;
    private int port = -1;
    private int ssid = -1;
    private boolean isServer = false;
    private int sessionNum = 5;
    private int minSenderNum = -1;
    private int maxSenderNum = -1;
	private int version = -1;
    private int minReceiverNum = -1;
    private int maxReceiverNum = -1;
    private boolean supportOutgoingMessage = true;
    private boolean supportDR = true;
    private Properties extProperties = new Properties();
    private String isPersistent;
    private boolean isInit = false;
    private String url;
    private BindMode bindMode = BindMode.Transceiver;

    private boolean isCreateReviver = true;
  
    public ConnectionInfo(){

    }

    public ConnectionInfo(ConnectionInfo one){
        connectionName = one.getConnectionName();
        userName = one.getUserName();
        password = one.getPassword();
        port = one.getPort();
        ssid = one.getSsid();
        isServer = one.isServer();
        sessionNum = one.getSessionNum();
        version = one.getVersion();
        minReceiverNum = one.getMinReceiverNum();
        maxReceiverNum = one.getMaxReceiverNum();
        supportOutgoingMessage = one.isSupportOutgoingMessage();
        supportDR = one.isSupportDR();
        extProperties = one.extProperties;
        isPersistent = one.getIsPersistent();
        isInit = one.IsInit();
        url = one.getURL();
        bindMode = one.getBindMode();
        minSenderNum = one.getMinSenderNum();
        maxSenderNum = one.getMaxSenderNum();
    }
    
    public void setIsCreateReceiver(boolean value){
        this.isCreateReviver = value;
    }

    public boolean isCreateReviver(){
        return this.isCreateReviver;
    }

    public String getURL() {
        return url;
    }

    public boolean isServer() {
        return isServer;
    }

    public String getPassword() {
        return password;
    }

    public int getPort() {
        return port;
    }

    public String getUserName() {
        return userName;
    }

    public int getSsid() {
        return ssid;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public int getSessionNum() {
        return sessionNum;
    }

    public int getVersion() {
        return version;
    }

    public int getMinReceiverNum() {
        return minReceiverNum;
    }

    public String getIsPersistent() {
        return isPersistent;
    }

    public boolean isSupportDR() {
        return supportDR;
    }

    public boolean isSupportOutgoingMessage() {
        return supportOutgoingMessage;
    }

    public BindMode getBindMode() {
        return bindMode;
    }

    public boolean IsInit() {
        return isInit;
    }

    public void setIsInit(String isInit) {
        if (isInit!=null && "yes".equalsIgnoreCase(isInit)){
            this.isInit = true;
        }
    }

    public void setURL(String url) {
        this.url = url;
    }

    public void setIsServer(boolean isServer) {
        this.isServer = isServer;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setSsid(int ssid) {
        this.ssid = ssid;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public void setSessionNum(int sessionNum) {
        this.sessionNum = sessionNum;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void setMinReceiverNum(int minReceiverNum) {
        this.minReceiverNum = minReceiverNum;
    }

    public void addExtProperties(String attributeName, String value) {
        this.extProperties.setProperty(attributeName, value);
    }

    public String getExtProperty(String attributeName) {
        return extProperties.getProperty(attributeName);
    }

    public Properties getProperties(){
        return extProperties;
    }

    public void setIsPersistent(String isPersistent) {
        this.isPersistent = isPersistent;
    }

    public void setSupportDR(boolean supportDR) {
        this.supportDR = supportDR;
    }

    public void setBindMode(BindMode bindMode) {
        this.bindMode = bindMode;
    }

    public void setSupportOutMsg(String SupportNewMsg) {
        if ( (SupportNewMsg != null) && ("no".equalsIgnoreCase(SupportNewMsg))) {
            this.supportOutgoingMessage = false;
        }
    }

    public void setSupportDR(String SupportDR) {
        if ( (SupportDR != null) && ("no".equalsIgnoreCase(SupportDR))) {
            this.supportOutgoingMessage = false;
        }
    }
    
    public int getMinSenderNum() {
		return minSenderNum;
	}

	public void setMinSenderNum(int minSenderNum) {
		this.minSenderNum = minSenderNum;
	}

    public void print(){
        System.out.println("connectionName:" + this.connectionName);
        System.out.println("userName:" + this.userName);
        System.out.println("password:" + this.password);
        System.out.println("port:" + this.port);
        System.out.println("ssid:" + this.ssid);
        System.out.println("isServer:" + this.isServer);
        System.out.println("sessionNum:" + this.sessionNum);
        System.out.println("MinSenderNum:" + this.minSenderNum);
        System.out.println("MaxSenderNum:" + this.maxSenderNum);
        System.out.println("version:" + this.version);
        System.out.println("MinReceiverNum:" + this.minReceiverNum);
        System.out.println("MaxReceiverNum:" + this.maxReceiverNum);
        System.out.println("supportOutgoingMessage:" + this.supportOutgoingMessage);
        System.out.println("supportDR:" + this.supportDR);
        System.out.println("isPersistent:" + this.isPersistent);
        System.out.println("url:" + this.url);
        System.out.println("isInit:" + this.isInit);
    }

	@Override
	public boolean equals(Object obj) {
		ConnectionInfo cinfo = (ConnectionInfo)obj;
		return ((cinfo.getSsid()==this.ssid) && cinfo.getConnectionName().equals(this.connectionName));
	}

	@Override
	public int hashCode() {
		return ssid*31+connectionName.hashCode();
	}
	
	public String toString(){		
        String prefix = ";";
		return new StringBuffer().append("{ connectionName:").append(this.connectionName)
		.append(prefix).append("userName:").append(this.userName)
		.append(prefix).append("password:").append(this.password)
		.append(prefix).append("port:").append(this.port)
		.append(prefix).append("ssid:").append(this.ssid)
		.append(prefix).append("isServer:").append(this.isServer)
		.append(prefix).append("sessionNum:").append(this.sessionNum)
		.append(prefix).append("MinSenderNum:").append(this.minSenderNum)
		.append(prefix).append("MaxSenderNum:").append(this.maxSenderNum)
		.append(prefix).append("version:").append(this.version)
		.append(prefix).append("MinReceiverNum:").append(this.minReceiverNum)
		.append(prefix).append("MaxReceiverNum:").append(this.maxReceiverNum)
		.append(prefix).append("supportOutgoingMessage:").append(this.supportOutgoingMessage)
		.append(prefix).append("supportDR:").append(this.supportDR)
		.append(prefix).append("isPersistent:").append(this.isPersistent)
		.append(prefix).append("url:").append(this.url)
		.append(prefix).append("isInit:").append(this.isInit+" }")		
		.toString();
	}

	public int getMaxSenderNum() {
		return maxSenderNum;
	}

	public void setMaxSenderNum(int maxSenderNum) {
		this.maxSenderNum = maxSenderNum;
	}

	public int getMaxReceiverNum() {
		return maxReceiverNum;
	}

	public void setMaxReceiverNum(int maxReceiverNum) {
		this.maxReceiverNum = maxReceiverNum;
	}
}
