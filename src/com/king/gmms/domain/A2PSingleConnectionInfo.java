package com.king.gmms.domain;

import java.lang.reflect.Method;
import java.util.*;


public class A2PSingleConnectionInfo extends A2PCustomerInfo{
    //Server
    private String spID = null;
    private String authKey = null;
    //Client
    // It's possible that there are multi-sysid. in one Server
    private ArrayList<String> sysId = new ArrayList<String> ();
    private String chlAcctName = null; //account name
    private String chlPassword = null; //password
    private ArrayList<String> chlURL = new ArrayList<String> (); //channel's address
    private String chlDRURL = null; //DR's address
    private String port = null; //port
    private boolean chlInit = false;
    private int sessionNumber = 0;
    private boolean isPersistent = false; //whether persistent

    private String chlCharset = null;
    private String chlRespCharset =null;
    private int sessionRetryTimes = 0;

    //UCP Legacy items, will be used in singleConnection
    private String oton = "6";
    private String onpi = "5";
    private String vers = "0100";
    private String styp = "1";
    private String pid = "0539";
    
   
	public A2PSingleConnectionInfo(){
    	super();
    }
	
	 public String getChlRespCharset() {
			return chlRespCharset;
		}

		public void setChlRespCharset(String chlRespCharset) {
			this.chlRespCharset = chlRespCharset;
		}

    public void addChlURL(String url) {
        if(url == null) {
            return;
        }
        int i = 0;
        for(; i < chlURL.size() ; i++) {
            if(url.equalsIgnoreCase(chlURL.get(i))) {
                break;
            }
        }
        this.chlURL.add(i, url);
    }

    public String[] getChlURL() {
        if(chlURL==null || chlURL.size() <= 0) {
            return null;
        }
        else {
            return chlURL.toArray(new String[chlURL.size()]);
        }
    }

    public String getChlDRURL() {
        if(chlDRURL==null) {
            return null;
        }
        else {
            return chlDRURL;
        }
    }

    public void addSysId(String sysId) {
        if(sysId == null) {
            return;
        }
        int i = 0;
        for(; i < this.sysId.size() ; i++) {
            if(sysId.equalsIgnoreCase(this.sysId.get(i))) {
                break;
            }
        }
        this.sysId.add(i, sysId);

    }

    public ArrayList<String> getSysId() {
        return this.sysId;
    }


    public String getAuthKey() {
        return authKey;
    }

    public String getChlAcctName() {
        return chlAcctName;
    }


    public boolean isChlInit() {
        return chlInit;
    }

    public String getChlPassword() {
        return chlPassword;
    }


    public boolean isPersistent() {
        return isPersistent;
    }

    public String getOnpi() {
        return onpi;
    }


    public String getOton() {
        return oton;
    }

    public String getPid() {
        return pid;
    }

    public String getPort() {
        return port;
    }


    public String getSpID() {
        return spID;
    }

    public String getStyp() {
        return styp;
    }


    public String getVers() {
        return vers;
    }

    public int getSessionNumber() {
        return sessionNumber;
    }


    public String getChlCharset() {
        return chlCharset;
    }

    public int getSessionRetryTimes() {
        return sessionRetryTimes;
    }


    public void setAuthKey(String authKey) {
        this.authKey = authKey;
    }

    public void setChlAcctName(String chlAcctName) {
        this.chlAcctName = chlAcctName;
    }


    public void setChlInit(boolean chlInit) {
        this.chlInit = chlInit;
    }

    public void setChlPassword(String chlPassword) {
        this.chlPassword = chlPassword;
    }

    public void setIsPersistent(boolean isPersistent) {
        this.isPersistent = isPersistent;
    }

    public void setOnpi(String onpi) {
        this.onpi = onpi;
    }

    public void setOton(String oton) {
        this.oton = oton;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setPort(String port) {
        this.port = port;
    }


    public void setSpID(String spID) {
        this.spID = spID;
    }

    public void setStyp(String styp) {
        this.styp = styp;
    }


    public void setVers(String vers) {
        this.vers = vers;
    }

    public void setSessionNumber(int sessionNumber) {
        this.sessionNumber = sessionNumber;
    }


    public void setChlCharset(String chlCharset) {
        this.chlCharset = chlCharset;
    }

    public void setSessionRetryTimes(int sessionRetryTimes) {
        this.sessionRetryTimes = sessionRetryTimes;
    }

    public String getSystemId() {
        if(this.sysId == null || sysId.isEmpty()) {
            return null;
        }
        else {
            return this.sysId.get(0);
        }
    }




    public void assignValue(A2PCustomerConfig cfg, Map<String, Integer> shorNameSsidMap) throws
        CustomerConfigurationException {
        super.assignValue(cfg, shorNameSsidMap);

        spID = cfg.getString("SPID");
        authKey = cfg.getString("AuthKey");

        A2PCustomerMultiValue sysIDs = cfg.parseMultiValue("ChlSysID");
        if (sysIDs != null) {
            ArrayList<AttrPair> attrs = sysIDs.getAllAttrs();
            for (AttrPair attr : attrs) {
                this.sysId.add(attr.getStringValue());
            }
        }
        chlAcctName = cfg.getString("ChlAcct");
        chlPassword = cfg.getString("ChlPwd");

        A2PCustomerMultiValue urls = cfg.parseMultiValue("ChlURL");
        if (urls != null) {
            ArrayList<AttrPair> attrs = urls.getAllAttrs();
            for (AttrPair attr : attrs) {
                this.chlURL.add(attr.getStringValue());
            }
        }
        this.chlDRURL = cfg.getString("ChlDRURL");
        port = cfg.getString("ChlPort");
        chlInit = cfg.getBool("ChlInit", false);
        sessionNumber = cfg.getInt("SessionNumber", 0);
        isPersistent = cfg.getBool("IsPersistent", false);

        chlCharset = cfg.getString("ChlCharset");
        chlRespCharset = cfg.getString("ChlHttpRespCharset");
        sessionRetryTimes = cfg.getInt("SessionRetryTimes", 0); //Quios

        oton = cfg.getString("SMSOptionOton", "6");
        onpi = cfg.getString("SMSOptionOnpi", "5");
        vers = cfg.getString("SMSOptionVers", "0100");
        styp = cfg.getString("SMSOptionStyp", "1");
        pid = cfg.getString("SMSOptionPid", "0539");
    }
    /**
	 * invoke setter method
	 */
	public boolean setProperty(String propertyName,Object value) {
		boolean bret = false;
		Method[] methods = A2PSingleConnectionInfo.class.getMethods();
		try {
			
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("set" + propertyName)) {
					f.invoke(this, value);
					bret = true;
				}
			}
		} catch (Exception e) {
			System.out.println("set properyt " + propertyName + " error!");
		}
		return bret;
	}
	/**
	 * invoke getter method
	 */
	public Object getProperty(String propertyName){
		Method[] methods = A2PSingleConnectionInfo.class.getMethods();
		Object obj = null;
		try {
			for (Method f : methods) {
				if (f.getName().equalsIgnoreCase("get" + propertyName)) {
					obj =f.invoke(this);
				}
			}
		} catch (Exception e) {
			System.out.println("get properyt " + propertyName + " error!");
		}
		return obj;
	}
	public String toString(){
		String prefix = "\r\n";
		return new StringBuffer().append(super.toString())
		.append(prefix).append("AuthKey:").append(this.authKey)
		.append(prefix).append("ChlAcct:").append(this.chlAcctName)
		.append(prefix).append("ChlCharset:").append(this.chlCharset)
		.append(prefix).append("ChlDRURL:").append(this.chlDRURL)
		.append(prefix).append("ChlURL:").append(this.chlURL)
		.append(prefix).append("ChlInit:").append(this.chlInit)
		.append(prefix).append("ChlPwd:").append(this.chlPassword)
		.append(prefix).append("ChlHttpRespCharset:").append(this.chlRespCharset)
		.append(prefix).append("IsPersistent:").append(this.isPersistent)
		.append(prefix).append("SMSOptionOnpi:").append(this.onpi)
		.append(prefix).append("SMSOptionOton:").append(this.oton)
		.append(prefix).append("SMSOptionPid:").append(this.pid)
		.append(prefix).append("ChlPort:").append(this.port)
		.append(prefix).append("SessionRetryTimes:").append(this.sessionRetryTimes)
		.append(prefix).append("SessionNumber:").append(this.sessionNumber)
		.append(prefix).append("SPID:").append(this.spID)
		.append(prefix).append("SMSOptionStyp:").append(this.styp)
		.append(prefix).append("SMSOptionVers:").append(this.vers)
		.toString();
	}
}
