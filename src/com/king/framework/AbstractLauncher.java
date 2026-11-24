/**
 * Author: frank.xue@King.com
 * Date: 2006-5-29
 * Time: 16:57:32
 * Document Version: 0.1
 */

package com.king.framework;


import java.io.*;
import java.net.*;

import com.king.gmms.domain.ModuleManager;
import com.king.gmms.GmmsUtility;


public abstract class AbstractLauncher implements Runnable {
    protected static final String STOP = "stop";
    protected static final String SHUTDOWN = "shutdown";
    protected static final String RESTART = "restart";
    protected static final String START = "start";
    protected A2PService service;
    protected String moduleName = null;
    protected ModuleManager moduleManager = null;
    public AbstractLauncher() {
        moduleName =  System.getProperty("module");
    }

    protected abstract void beforeStart() throws Exception;

    protected abstract void beforeStop() throws Exception;

    public void run() {
        

    }

    public boolean startService(String className) {
        try {
            System.out.println("Starting service:" + moduleName + "...");
            beforeStart();
            moduleManager = ModuleManager.getInstance();
            if (!com.king.gmms.ha.MessageIdSetup.validate()) {
                System.out.println("Start service fail!");
                return false;
            }
            service = (A2PService) Class.forName(className).newInstance();
            new Thread(A2PThreadGroup.getInstance(),this).start();
            GracefulStop.setService(service);
            if(service.startService()) {
                Runtime.getRuntime().addShutdownHook(new Thread("AicShutdownHook") {
                    public void run() {
                        System.out.println(System.getProperty("module") + " is successfully stopped!");
                    }
                });
                System.out.println("Start service successfully!");
                return true;
            }
            else {
                System.out.println("Start service fail!");
                return false;
            }
        }
        catch(Exception e) {
            System.out.println("Start service fail!");
            e.printStackTrace();
            return false;
        }
    }

    public boolean stopService() {
        // no module listens on a command port any more (the commands by TCP are gone,
        // docs/流模式下系统管理和模块间连接代码清理分析.md 3.4): a module stops on SIGTERM (a2pShut.sh, GracefulStop)
        // or by the smsui shutdown command
        System.out.println("Stop service fail! No command port: stop " + moduleName + " with a2pShut.sh or the smsui shutdown command.");
        return false;
    }

    public boolean restartService() {
        // no module listens on a command port any more (the commands by TCP are gone,
        // docs/流模式下系统管理和模块间连接代码清理分析.md 3.4): a module stops on SIGTERM (a2pShut.sh, GracefulStop)
        // or by the smsui shutdown command
        System.out.println("Restart service fail! No command port: stop " + moduleName + " with a2pShut.sh or the smsui shutdown command.");
        return false;
    }


}
