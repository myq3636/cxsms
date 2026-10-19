/**
 * Author: frank.xue@King.com
 * Date: 2006-5-29
 * Time: 16:57:32
 * Document Version: 0.1
 */

package com.king.framework;


import java.io.*;
import java.net.*;

import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.GmmsUtility;
import com.king.mgt.cmd.system.SystemCommandStop;
import com.king.mgt.cmd.user.UserCommandStop;


public abstract class AbstractLauncher implements Runnable {
    protected static final String STOP = "stop";
    protected static final String SHUTDOWN = "shutdown";
    protected static final String RESTART = "restart";
    protected static final String START = "start";
    protected int port;
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
            ModuleConnectionInfo moduleInfo = moduleManager.getConnectionInfo(moduleName);
            port = resolveCmdPort(moduleInfo);
            service = (A2PService) Class.forName(className).newInstance();
            new Thread(A2PThreadGroup.getInstance(),this).start();
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
        try {
        	beforeStop();
        	moduleManager = ModuleManager.getInstance();
            ModuleConnectionInfo moduleInfo = moduleManager.getConnectionInfo(moduleName);
            port = resolveCmdPort(moduleInfo);
            if (port <= 0) {
                System.out.println("Stop service fail! cmd port is not configured for module " + moduleName);
                return false;
            }
            String ip = resolveModuleUrl(moduleInfo);
            System.out.println("Stopping service...");
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(ip, port),15000);
            BufferedOutputStream bWriter = new BufferedOutputStream(socket.getOutputStream());
            SystemCommandStop stopcmd = new SystemCommandStop(new UserCommandStop());
            bWriter.write(stopcmd.getByteArray());
            bWriter.flush();
            bWriter.close();
            socket.close();
            System.out.println("Stop service successfully!");
            return true;
        }
        catch(Exception e) {
            System.out.println("Stop service fail!");
            e.printStackTrace();
            return false;
        }
    }

    public boolean restartService() {
        try {
        	beforeStop();
        	moduleManager = ModuleManager.getInstance();
            ModuleConnectionInfo moduleInfo = moduleManager.getConnectionInfo(moduleName);
            port = resolveCmdPort(moduleInfo);
            if (port <= 0) {
                System.out.println("Restart service fail! cmd port is not configured for module " + moduleName);
                return false;
            }
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("127.0.0.1", port),15000);
            BufferedOutputStream bWriter = new BufferedOutputStream(socket.getOutputStream());
            SystemCommandStop stopcmd = new SystemCommandStop(new UserCommandStop());
            bWriter.write(stopcmd.getByteArray());
            bWriter.flush();
            bWriter.close();
            socket.close();
            startup();
            return true;
        }
        catch(Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    private boolean startup() {
        try {
        	String a2phome = System.getProperty("a2p_home");
            String cmmod = a2phome+"/bin/"+moduleName+".sh";
            String[] cmd = {cmmod, "start"};
            Runtime.getRuntime().exec(cmd);
        }
        catch (Exception ex) {
            System.out.println("Runtime error!" + ex.getMessage());
            return false;
        }
        return true;

    }

    private int resolveCmdPort(ModuleConnectionInfo moduleInfo) {
        if (moduleInfo != null && moduleInfo.getCmdPort() > 0) {
            return moduleInfo.getCmdPort();
        }
        String value = System.getProperty(moduleName + ".CmdPort");
        if (value == null || value.trim().length() == 0) {
            value = System.getProperty("Module.CmdPort");
        }
        if (value == null || value.trim().length() == 0) {
            value = getCommonProperty(moduleName + ".CmdPort", getCommonProperty("Module.CmdPort", "-1"));
        }
        try {
            return value == null ? -1 : Integer.parseInt(value.trim());
        } catch (Exception e) {
            return -1;
        }
    }

    private String resolveModuleUrl(ModuleConnectionInfo moduleInfo) {
        if (moduleInfo != null && moduleInfo.getURL() != null && moduleInfo.getURL().trim().length() > 0) {
            return moduleInfo.getURL().trim();
        }
        String value = System.getProperty(moduleName + ".ModuleUrl");
        if (value == null || value.trim().length() == 0) {
            value = getCommonProperty(moduleName + ".ModuleUrl", getCommonProperty("Module.ModuleUrl", "127.0.0.1"));
        }
        return value.trim();
    }

    private String getCommonProperty(String key, String defaultValue) {
        try {
            return GmmsUtility.getInstance().getCommonProperty(key, defaultValue);
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
