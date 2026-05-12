package com.king.gmms.ha;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.king.framework.A2PService;
import com.king.framework.SystemLogger;
import com.king.framework.lifecycle.SystemCommandListener;
import com.king.gmms.GmmsUtility;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;

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
public class HALauncher {
    private static SystemLogger log = null;
    protected static final String STOP = "stop";
    protected static final String RESTART = "restart";
    protected static final String START = "start";
    protected A2PService service;
    protected SystemCommandListener cmdListener;
    protected int port;
    protected String moduleName = null;
    protected ModuleManager moduleManager = null;

    public HALauncher() {
        moduleName =  System.getProperty("module");
    }

    private static final String SYS_INFO = "Wrong parameters!\nThe right format should be: HALauncher  <'start' class_name> | <'stop'>";
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    protected void beforeStart() throws Exception {
        String a2phome = System.getProperty("a2p_home","/usr/local/a2p");
        if (a2phome == null) {
            System.out.println("No a2p_home definited!");
            System.exit( -1);
        }
        if (!a2phome.endsWith("/")) {
        	a2phome = a2phome + "/";
            System.setProperty("a2p_home", a2phome);
        }
        //Init log system
        //DOMConfigurator.configure(a2phome + "conf/log4j-config.xml");
        System.setProperty("log4j.configurationFile",a2phome + "conf/log4j2.xml");
        log = SystemLogger.getSystemLogger(HALauncher.class);

        GmmsUtility gmmsUtility = GmmsUtility.getInstance();
        gmmsUtility.initUtility(a2phome + "Gmms/GmmsConfig.properties");
    }

    protected void beforeStop() {
    	cmdListener.stopService();
    	GmmsUtility.getInstance().close();
    }

    public static void main(String[] args) {
        try {
            if (! ( (args.length == 2 && args[0].equalsIgnoreCase(START))
                   || (args.length == 2 && args[0].equalsIgnoreCase(STOP))
                   || (args.length == 2 && args[0].equalsIgnoreCase(RESTART)))) {
                System.out.println(SYS_INFO);
                System.exit( -1);
            }
            if (args[0].equalsIgnoreCase(START)) {
                if (!new HALauncher().startService(args[1])) {
                    System.exit( -1);
                }
            }
            else if (args[0].equalsIgnoreCase(STOP)) {
                new HALauncher().stopService();
            }
            else if (args[0].equalsIgnoreCase(RESTART)) {
                new HALauncher().restartService();
            }
            else {
                System.out.println(SYS_INFO);
                System.exit( -1);
            }
        }
        catch (Exception e) {
            System.err.println("Uncaught exception thrown at" +
                               SDF.format(new Date()));
            e.printStackTrace();
        }
    }

    public boolean startService(String className) {
        try {
            System.out.println("Starting service:" +
                               System.getProperty("module") + "...");
            beforeStart();
            moduleManager = ModuleManager.getInstance();
            ModuleConnectionInfo moduleInfo = moduleManager.getConnectionInfo(moduleName);
            port = moduleInfo.getCmdPort();
            service = (A2PService) Class.forName(className).newInstance();
            if (service.startService()) {
                Runtime.getRuntime().addShutdownHook(new Thread(
                    "AicShutdownHook") {
                    public void run() {
                        System.out.println(System.getProperty("module") +
                                           " is successfully stopped!");
                    }
                });
                cmdListener=new SystemCommandListener(port);
                cmdListener.service();
                System.out.println("Start service successfully!");
                return true;
            }
            else {
                System.out.println("Start service fail!");
                return false;
            }
        }
        catch (Exception e) {
            System.out.println("Start service fail!");
            e.printStackTrace();
            return false;
        }
    }

    public boolean stopService() {
        return true;
    }

    public boolean restartService() {
        return true;
    }
}
