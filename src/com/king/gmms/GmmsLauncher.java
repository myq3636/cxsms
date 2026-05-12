/**
 */

package com.king.gmms;

import com.king.framework.AbstractLauncher;

import java.text.SimpleDateFormat;
import java.util.Date;


public class GmmsLauncher extends AbstractLauncher {
    private static final String SYS_INFO = "Wrong parameters!\nThe right format should be: GmmsLauncher <'start' class_name> | <'stop'>";
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");


    public GmmsLauncher() {
        super();
    }

    protected void beforeStart() throws Exception {
        String a2phome = System.getProperty("a2p_home");
        if(a2phome == null) {
            System.out.println("No a2p_home definited!");
            System.exit(-1);
        }
        if(!a2phome.endsWith("/")) {
        	a2phome = a2phome + "/";
            System.setProperty("a2p_home",a2phome);
        }
        System.setProperty("log4j.configurationFile",a2phome + "conf/log4j2.xml");
        GmmsUtility gmmsUtility = GmmsUtility.getInstance();
        gmmsUtility.initUtility(a2phome + "Gmms/GmmsConfig.properties");
        gmmsUtility.startControlSubscriber();
        
        // V4.1 响应式外发流控消费者启动 (仅限 Client 模块)
        String moduleName = System.getProperty("module");
        if (com.king.gmms.domain.ModuleManager.getInstance().getClientModules().contains(moduleName)) {
            System.out.println("Starting OutboundStreamConsumer for Client module: " + moduleName);
            com.king.gmms.messagequeue.OutboundStreamConsumer.getInstance().start();
        }
    }

    protected void beforeStop() {
    	String a2phome = System.getProperty("a2p_home");
        if(a2phome == null) {
            System.out.println("No a2p_home definited!");
            System.exit(-1);
        }
        if(!a2phome.endsWith("/")) {
        	a2phome = a2phome + "/";
            System.setProperty("a2p_home",a2phome);
        }
        System.setProperty("log4j.configurationFile",a2phome + "conf/log4j2.xml");
        GmmsUtility.getInstance().close();
    }

    public static void main(String[] args) {
        try {
            if(!((args.length == 2 && args[0].equalsIgnoreCase(START))
                 || (args.length == 2 && args[0].equalsIgnoreCase(STOP))
                 || (args.length == 2 && args[0].equalsIgnoreCase(RESTART)))) {
                System.out.println(SYS_INFO);
                System.exit(-1);
            }
            String action = args[0];
            if(action.equalsIgnoreCase(START)) {
                String className = args[1];
                if(!new GmmsLauncher().startService(className)) {
                    System.exit(-1);
                }
            }
            else if(action.equalsIgnoreCase(STOP)) {
                //TODO: judge if not start.
                new GmmsLauncher().stopService();
            }
            else if(action.equalsIgnoreCase(RESTART)) {
                //TODO: judge if not start.
                new GmmsLauncher().restartService();
            }
            else {
                System.out.println(SYS_INFO);
                System.exit(-1);
            }
        }
        catch(Exception e) {
            System.err.println("Uncaught exception thrown at" + SDF.format(new Date()));
            e.printStackTrace();
        }
    }
}
