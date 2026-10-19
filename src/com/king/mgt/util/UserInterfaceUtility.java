package com.king.mgt.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import com.king.framework.SystemLogger;
import com.king.gmms.domain.ModuleManager;
import com.king.mgt.cmd.user.UserCommandAntiSpam;
import com.king.mgt.cmd.user.UserCommandBlacklist;
import com.king.mgt.cmd.user.UserCommandContentTpl;
import com.king.mgt.cmd.user.UserCommandCustomer;
import com.king.mgt.cmd.user.UserCommandPhonePrefix;
import com.king.mgt.cmd.user.UserCommandRoutingInfo;
import com.king.mgt.connection.FTP4Client;
import com.king.mgt.connection.SessionFactory;
import com.king.mgt.connection.UserCommandSet;

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
public class UserInterfaceUtility {
    private static SystemLogger log;
    private static UserInterfaceUtility instance=new UserInterfaceUtility();
    private int cmdSeqID=0;
    private int cmdGroupId = 1;
    private Properties prop=null;
    private SessionFactory sessionFactory=null;
    private UserCommandSet commandSet = null;
    private MailSender mailSender=null;
    private InfoTable infoTable;
    private boolean enableNotification=true;
    private boolean isHa=false;
    public UserInterfaceUtility() {
    }
    public SessionFactory getSessionFactory()
    {
        return sessionFactory;
    }
    public UserCommandSet getUserCommandSet()
    {
        return commandSet;
    }
    public int genCommandSeqID(int group)
    {
        if (cmdSeqID>=1000)
            cmdSeqID=0;
        cmdSeqID++;
        return (group*1000+cmdSeqID);

    }

    public int genCommandGroupID(){
        if(cmdGroupId >=100){
            cmdGroupId = 1;
        }

        return cmdGroupId++;
    }
    
    private void initRoutingCfg(){
        FTP4Client ftp=new FTP4Client();
        ftp.downloadConfigFileOrCreate(UserCommandRoutingInfo.keyword);    	
    }
    
    private void initAntiSpamCfg(){
        FTP4Client ftp=new FTP4Client();
        ftp.downloadConfigFileOrCreate(UserCommandAntiSpam.keyword);    	
    }
    
    private void initContentTplCfg(){
        FTP4Client ftp=new FTP4Client();
        ftp.downloadConfigFileOrCreate(UserCommandContentTpl.keyword);    	
    }

    private void initCmCfg()
    {
        FTP4Client ftp=new FTP4Client();
        ftp.downloadConfigFileOrCreate(UserCommandCustomer.keyword);
    }
    
    private void initPhonePrefixCfg()
    {
        FTP4Client ftp=new FTP4Client();
        ftp.downloadConfigFileOrCreate(UserCommandPhonePrefix.keyword);
    }
    
    private void initBlacklistCfg()
    {
        FTP4Client ftp=new FTP4Client();
        ftp.downloadConfigFileOrCreate(UserCommandBlacklist.keyword);
    }
    public void initUtilities()
    {
        initCoreUtilities();
        initConfigFiles();
    }

    public void initCoreUtilities()
    {
        long start = System.currentTimeMillis();
        log.info("SmsUI core utilities init start.");
        String enable=this.getProperty("enableNotification","yes");
        if ("no".equalsIgnoreCase(enable) || "disable".equalsIgnoreCase(enable) ||
            "false".equalsIgnoreCase(enable)) {
            this.enableNotification=false;
        }
        else
        {
            enableNotification=true;
        }
        infoTable=InfoTable.getInstance();
        sessionFactory=new SessionFactory();
        sessionFactory.init();
        commandSet=new UserCommandSet();
        commandSet.init();
        mailSender=MailSender.getInstance();
        log.info("SmsUI core utilities init done. elapsedMs={}", System.currentTimeMillis() - start);
    }

    public void initConfigFiles()
    {
        long start = System.currentTimeMillis();
        log.info("SmsUI config files init start.");
        initConfigFileSafely(UserCommandBlacklist.keyword, new Runnable() {
            public void run() {
                initBlacklistCfg();
            }
        });
        initConfigFileSafely(UserCommandCustomer.keyword, new Runnable() {
            public void run() {
                initCmCfg();
            }
        });
        initConfigFileSafely(UserCommandRoutingInfo.keyword, new Runnable() {
            public void run() {
                initRoutingCfg();
            }
        });
        initConfigFileSafely(UserCommandAntiSpam.keyword, new Runnable() {
            public void run() {
                initAntiSpamCfg();
            }
        });
        initConfigFileSafely(UserCommandContentTpl.keyword, new Runnable() {
            public void run() {
                initContentTplCfg();
            }
        });
        initConfigFileSafely(UserCommandPhonePrefix.keyword, new Runnable() {
            public void run() {
                initPhonePrefixCfg();
            }
        });
        log.info("SmsUI config files init done. elapsedMs={}", System.currentTimeMillis() - start);
    }

    public void initConfigFilesAsync()
    {
        Thread thread = new Thread(new Runnable() {
            public void run() {
                initConfigFiles();
            }
        }, "SmsUiConfigInitThread");
        thread.setDaemon(true);
        thread.start();
    }

    public void initModuleManagerAsync()
    {
        Thread thread = new Thread(new Runnable() {
            public void run() {
                long start = System.currentTimeMillis();
                log.info("SmsUI module manager init start.");
                try {
                    ModuleManager.getInstance().initSmsUI();
                    log.info("SmsUI module manager init done. elapsedMs={}", System.currentTimeMillis() - start);
                } catch (Throwable e) {
                    log.error("Initial smsui module manager load failed.", e);
                }
            }
        }, "SmsUiModuleInitThread");
        thread.setDaemon(true);
        thread.start();
    }

    private void initConfigFileSafely(String keyword, Runnable task)
    {
        try {
            task.run();
        } catch (Throwable e) {
            log.error("Initial config file load failed. keyword={}", keyword, e);
        }
    }
    public void init()
    {
        String home = System.getProperty("home","/usr/local/smsui/");
        if (!home.endsWith("/")) {
            home += "/";
            System.setProperty("home",home);
        }
        
        String a2phome = System.getProperty("a2p_home", "/usr/local/a2p/");
        if(!a2phome.endsWith("/")) {
        	a2phome = a2phome + "/";
            System.setProperty("a2p_home",a2phome);
        }
        
        System.setProperty("log4j.configurationFile",home + "conf/log4j2.xml");
        log=SystemLogger.getSystemLogger(UserInterfaceUtility.class);
        String confFile = home + "conf/userinterface.properties";
        log.trace("Conf File: {}",confFile);
        try {
            prop = new Properties();
            prop.load(new FileInputStream(confFile));
        } catch (FileNotFoundException ex) {
            log.fatal("Configuration File missing. Path: {}", confFile, ex);

        } catch (IOException ex) {
            log.fatal("Error when loading configuration file. ", ex);
        }
    }
    public void setProperties(Properties prop)
    {
        this.prop=prop;
    }
    public String getProperty(String name)
    {
        String result = prop.getProperty(name);
        if(result != null) {
            return result.trim();
        }
        else {
            return null;
        }
    }
    public String getProperty(String name, String defaultValue)
    {
        String result = prop.getProperty(name, defaultValue);
        if(result != null) {
            return result.trim();
        }
        else {
            return null;
        }
    }
    public Properties getProperties()
    {
        return prop;
    }
    public static UserInterfaceUtility getInstance()
    {
        return instance;
    }

    public MailSender getMailSender() {
        return mailSender;
    }

    public InfoTable getInfoTable() {
        return infoTable;
    }

    public boolean isEnableNotification() {
        return enableNotification;
    }

    public void setMailSender(MailSender mailSender) {
        this.mailSender = mailSender;
    }

    public void setInfoTable(InfoTable infoTable) {
        this.infoTable = infoTable;
    }

    public void setEnableNotification(boolean enableNotification) {
        this.enableNotification = enableNotification;
    }

    public boolean isHa() {
        return isHa;
    }

    public void setHa() {
        isHa = true;
        log.info("HA mode!");
    }
}
