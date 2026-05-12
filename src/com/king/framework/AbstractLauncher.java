/**
 * Author: frank.xue@King.com
 * Date: 2006-5-29
 * Time: 16:57:32
 * Document Version: 0.1
 */

package com.king.framework;


import java.io.*;
import java.net.*;

import com.king.framework.lifecycle.SystemCommandListener;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.mgt.cmd.system.SystemCommandStop;
import com.king.mgt.cmd.user.UserCommandStop;


public abstract class AbstractLauncher implements Runnable {
    protected static final String STOP = "stop";
    protected static final String SHUTDOWN = "shutdown";
    protected static final String RESTART = "restart";
    protected static final String START = "start";
    protected int port;
    protected A2PService service;
    protected SystemCommandListener cmdListener;
    protected String moduleName = null;
    protected ModuleManager moduleManager = null;
    public AbstractLauncher() {
        moduleName =  System.getProperty("module");
    }

    protected abstract void beforeStart() throws Exception;

    protected abstract void beforeStop() throws Exception;

    public void run() {
        cmdListener=new SystemCommandListener(port);
        cmdListener.service();

    }

    public boolean startService(String className) {
        try {
            System.out.println("Starting service:" + moduleName + "...");
            beforeStart();
            moduleManager = ModuleManager.getInstance();
            ModuleConnectionInfo moduleInfo = moduleManager.getConnectionInfo(moduleName);
            port = moduleInfo.getCmdPort();
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
            port = moduleInfo.getCmdPort();
            String ip = moduleInfo.getURL();
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
            port = moduleInfo.getCmdPort();
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
}
