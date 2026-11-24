package com.king.mgt.context;

import java.util.Date;

import com.king.framework.SystemLogger;

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
    private static final Module ROOT_MODULE = new Module("A2P", null, -1);
    private String curUser;
    private String curUserIP;
    private Date curUserLoginTime;

    private Module currentModule;
    public ContextManager(String userName, String ip) {
        this.curUser=userName;
        this.curUserIP=ip;
        this.curUserLoginTime=new Date();
        this.currentModule = rootModule();
    }
    public ContextManager()
    {
        this.currentModule = rootModule();
    }
    public Module getCurrentModule()
    {
        if (currentModule == null) {
            currentModule = rootModule();
        }
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
        Module module = getCurrentModule();
        return module.getName()+"> ";
    }

    private Module rootModule() {
        return ROOT_MODULE;
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
