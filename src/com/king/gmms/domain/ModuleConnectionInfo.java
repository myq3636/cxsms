package com.king.gmms.domain;

import com.king.gmms.util.SystemConstants;

public class ModuleConnectionInfo extends ConnectionInfo implements Comparable{
	private String moduleName;
	private String moduleType;
	private String fullModuleType;//module name prefix
	private String classPath;
    private int sysPort = -1; //used by System management
    private int cmdPort = -1; //used by System management
    private int inSessionNum = 0;
    private int outSessionNum = 0;    
    
	public ModuleConnectionInfo(){
		super();
	}
	public ModuleConnectionInfo(ModuleConnectionInfo one){
		super(one);
		moduleName = one.getModuleName();
		moduleType = one.getModuleType();
		fullModuleType = one.getFullModuleType();
		sysPort = one.getSysPort();
		cmdPort = one.getCmdPort();
	}
	public int getSysPort() {
		return sysPort;
	}

	public void setSysPort(int sysPort) {
		this.sysPort = sysPort;
	}

	public int getCmdPort() {
		return cmdPort;
	}

	public void setCmdPort(int cmdPort) {
		this.cmdPort = cmdPort;
	}
	public String getModuleName() {
		return moduleName;
	}
	public void setModuleName(String moduleName) {
		this.moduleName = moduleName;
	}
	public String getModuleType() {
		return moduleType;
	}
	public void setModuleType(String moduleType) {
		this.moduleType = moduleType;
	}
	public String getFullModuleType() {
		return fullModuleType;
	}
	public void setFullModuleType(String FullModuleType) {
		this.fullModuleType = FullModuleType;
	}
	public String getClassPath() {
		return classPath;
	}
	public void setClassPath(String classPath) {
		this.classPath = classPath;
	}
	/**
	 * compare with each other
	 */
	public int compareTo(Object o) {
		ModuleConnectionInfo oo = (ModuleConnectionInfo)o;
		int mine = getValueOfType(this);
		int other = getValueOfType(oo);
		if(mine==other){
			return this.moduleName.compareTo(oo.getModuleName());
		}else{
			return mine - other;
		}
	}
	private int getValueOfType(ModuleConnectionInfo info){
		String ftype = info.getFullModuleType();
		if(SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(ftype)){
			return 1;
		}else if(SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(ftype)){
			return 2;
		}else if(ftype.endsWith(SystemConstants.SERVER_MODULE_TYPE)){
			return 3;
		}else if(ftype.endsWith(SystemConstants.CLIENT_MODULE_TYPE)){
			return 4;
		}else if(SystemConstants.MQM_MODULE_TYPE.equalsIgnoreCase(ftype)){
			return 5;
		}else{
			return 6;
		}
	}
	public int getInSessionNum() {
		return inSessionNum;
	}
	public void setInSessionNum(int inSessionNum) {
		this.inSessionNum = inSessionNum;
	}
	public int getOutSessionNum() {
		return outSessionNum;
	}
	public void setOutSessionNum(int outSessionNum) {
		this.outSessionNum = outSessionNum;
	}
}
