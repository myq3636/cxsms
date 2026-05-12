package com.king.mgt.context;

import java.util.Date;
import java.util.HashMap;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleManager;

/**
 * <p>Title: </p>
 *
 * <p>Description: </p>
 *
 * <p>Copyright: Copyright (c) 2008</p>
 *
 * <p>Company: </p>
 *
 * @author not attributable
 * @version 1.0
 */
public class ContextManager {

    private static SystemLogger log = SystemLogger.getSystemLogger(ContextManager.class);
    private String curUser;
    private String curUserIP;
    private Date curUserLoginTime;

    private Module currentModule=ModuleManager.getInstance().A2P;
    private static HashMap<Integer, Module> mapId2Module=new HashMap<Integer,Module>();
    private static HashMap<String, Module> mapName2Module=new HashMap<String, Module>();
    public ContextManager(String userName, String ip) {
        this.curUser=userName;
        this.curUserIP=ip;
        this.curUserLoginTime=new Date();
    }
    public ContextManager()
    {}
    public boolean change(String name)
    {
        // the module name starts with "_" are reserved.
        if (name==null || name.startsWith("_")) return false;
        Module module=ModuleManager.getInstance().getModuleByName(name);
        if (module==null)
            return false;
        this.currentModule=module;
        return true;
    }
    public Module getCurrentModule()
    {
        return currentModule;
    }

    public String getCurUser() {
        return curUser;
    }

    public String getCurUserIP() {
        return curUserIP;
    }

    public Date getCurUserLoginTime() {
        return curUserLoginTime;
    }

    public String getPrompt()
    {
        return currentModule.getName()+"> ";
    }

    public void setCurUser(String curUser) {
        this.curUser = curUser;
    }

    public void setCurUserIP(String curUserIP) {
        log.info("set ip:{}",curUserIP);
        this.curUserIP = curUserIP;
    }

    public void setCurUserLoginTime(Date curUserLoginTime) {
        this.curUserLoginTime = curUserLoginTime;
    }
}
