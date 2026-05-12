/**
 * Copyright 2000-2012 King Inc. All rights reserved.
 */
package com.king.mgt.util;

import java.io.BufferedOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.util.SystemConstants;
import com.king.mgt.cmd.system.SystemCommandStop;
import com.king.mgt.cmd.user.UserCommandStop;

/**
 * called by a2pStartup.sh
 * @author bensonchen
 * @version 1.0.0
 */
public class StartupModuleManager {
	final static String POLARIS_SH = "/bin/polaris.sh";
	final static String POLARIS_HA_SH = "/bin/polaris_ha.sh";
	private static String a2phome = "/usr/local/a2p/";
	public static void main(String[] args) {
		a2phome = System.getProperty("a2p_home", "/usr/local/a2p/");
        if(!a2phome.endsWith("/")) {
        	a2phome = a2phome + "/";
            System.setProperty("a2p_home",a2phome);
        }
        String action = "start";
        List<String> moduleNames = new ArrayList<String>();
        if(args.length<1){
        	action = "start";
        }else if(args[0].equalsIgnoreCase("start")){
        	action = "start";
        }else if(args[0].equalsIgnoreCase("stop")){
        	action = "stop";
        }else if(args[0].equalsIgnoreCase("kill")){
        	action = "kill";
        }
        if(args.length>1){
        	for(int i=1;i<args.length;i++){
        		moduleNames.add(args[i]);
        	}
        }

        System.out.println(action + " modules...");
        try {
            List<ModuleConnectionInfo> moduleList = getModules(moduleNames);
            if(moduleList==null||moduleList.isEmpty()){
        		System.out.println("No modules need to "+action);
        		return;
            }
            if(!"start".equalsIgnoreCase(action)){//stop system manager in last
            	Collections.reverse(moduleList);
            }
            for (ModuleConnectionInfo moduleInfo : moduleList) {
            	launch(moduleInfo,action);
            	Thread.sleep(10);
            }
		} catch(InterruptedException ex){
			ex.printStackTrace();
		} 
	}
	/**
	 * start module
	 * @return
	 */
	public static boolean start(ModuleConnectionInfo module,String action) {
        try {
        	String moduleType = module.getFullModuleType();
        	String moduleName = module.getModuleName();
        	String className = module.getClassPath();
        	if(moduleType.equalsIgnoreCase(SystemConstants.COMMONHTTPSERVER_MODULE_TYPE)){
       		 String cmmod = a2phome+"/bin/"+moduleName+".sh";
                String[] cmd = {cmmod, action};
                Runtime.getRuntime().exec(cmd);
                Thread.sleep(2000);
        	}else if(a2phome==null||moduleType==null||moduleName==null||className==null){
            	System.out.println(action + " module failed due to invalid module configration.");
            	System.out.println("moduleType="+moduleType);
            	System.out.println("moduleName="+moduleName);
            	System.out.println("className="+className);
            	return false;
        	}else if(moduleType.equalsIgnoreCase(SystemConstants.MGT_MODULE_TYPE)){
                String cmmod = a2phome+POLARIS_HA_SH;
                String[] cmd = {cmmod, action,moduleName,className};
                Runtime.getRuntime().exec(cmd);
                Thread.sleep(10000);
        	}else{
        		 String cmmod = a2phome+POLARIS_SH;
                 String[] cmd = {cmmod, action,moduleName,className};
                 Runtime.getRuntime().exec(cmd);
                 if(moduleType.equalsIgnoreCase(SystemConstants.ROUTER_MODULE_TYPE)){
                	 Thread.sleep(10000);
                 }else if(moduleType.equalsIgnoreCase(SystemConstants.MQM_MODULE_TYPE)){
                	 Thread.sleep(5000);
                 }
        	}
        	System.out.println(action + " module " + moduleName);
        }catch (Exception ex) {
            System.out.println("Runtime error!" + ex.getMessage());
            return false;
        }
        return true;
    }
	/**
	 * kill module
	 * @return
	 */
	public static boolean kill(ModuleConnectionInfo module,String action) {
        try {
        	String moduleType = module.getFullModuleType();
        	String moduleName = module.getModuleName();
        	
        	if(moduleType.equalsIgnoreCase(SystemConstants.MGT_MODULE_TYPE)){
                String cmmod = a2phome+POLARIS_HA_SH;
                String[] cmd = {cmmod, action, moduleName};
                Runtime.getRuntime().exec(cmd);
        	}else if(moduleType.equalsIgnoreCase(SystemConstants.COMMONHTTPSERVER_MODULE_TYPE)){
        		 String cmmod = a2phome+"/bin/"+moduleName+".sh";
                 String[] cmd = {cmmod, action};
                 Runtime.getRuntime().exec(cmd);
        	}else{
        		 String cmmod = a2phome+POLARIS_SH;
                 String[] cmd = {cmmod, action, moduleName};
                 Runtime.getRuntime().exec(cmd);
        	}
        	System.out.println(action + " module " + moduleName);
        }catch (Exception ex) {
            System.out.println("Runtime error!" + ex.getMessage());
            return false;
        }
        return true;
    }
	/**
	 * stop module
	 * @param moduleInfo
	 * @return
	 */
	public static boolean stop(ModuleConnectionInfo moduleInfo) {
		String moduleName = moduleInfo.getModuleName();
    	String moduleType = moduleInfo.getFullModuleType();
        try {
        	if(moduleType.equalsIgnoreCase(SystemConstants.COMMONHTTPSERVER_MODULE_TYPE)){
       		 String cmmod = a2phome+"/bin/"+moduleName+".sh";
                String[] cmd = {cmmod, "stop"};
                Runtime.getRuntime().exec(cmd);
                System.out.println("Stop "+moduleName+" successfully!");
                return true;
        	}else if(moduleType.equalsIgnoreCase(SystemConstants.MGT_MODULE_TYPE)){
                String cmmod = a2phome+POLARIS_HA_SH;
                String[] cmd = {cmmod, "stop", moduleName};
                Runtime.getRuntime().exec(cmd);
                System.out.println("Stop "+moduleName+" successfully!");
                return true;
        	}
            int port = moduleInfo.getCmdPort();
            String ip = moduleInfo.getURL();
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(ip, port),15000);
            BufferedOutputStream bWriter = new BufferedOutputStream(socket.getOutputStream());
            SystemCommandStop stopcmd = new SystemCommandStop(new UserCommandStop());
            bWriter.write(stopcmd.getByteArray());
            bWriter.flush();
            bWriter.close();
            socket.close();
            System.out.println("Stop "+moduleName+" successfully!");
            return true;
        }
        catch(Exception e) {
            System.out.println("Stop "+moduleName+" failed due to "+e.getMessage());
            return false;
        }
    }
	
	/**
	 * start module
	 * @return
	 */
	private static boolean launch(ModuleConnectionInfo module,String action) {
		if("start".equalsIgnoreCase(action)){
			return start(module,action);
		}else if("stop".equalsIgnoreCase(action)){
			return stop(module);
		}else if("kill".equalsIgnoreCase(action)){
			return kill(module,action);
		}
		System.out.println("Unsupport action:"+action);
		return false;
    }
	
	private static List<ModuleConnectionInfo> getModules(List<String> moduleNames) {
        List<ModuleConnectionInfo> modulelist = new ArrayList<ModuleConnectionInfo>();
    	ModuleManager moduleManager = ModuleManager.getInstance();
    	if(moduleNames!=null && !moduleNames.isEmpty()){
    		for(String moduleName:moduleNames){
    			moduleName = moduleName.trim();
    			ModuleConnectionInfo moduleInfo = moduleManager.getConnectionInfo(moduleName);
        		if (moduleInfo!=null && isLocalModule(moduleInfo)){
        			modulelist.add(moduleInfo);
        		}else{
        			System.out.println(moduleName+" is not a local module!");
        		}
    		}
    	}else{
    		List<ModuleConnectionInfo> sysMgtModules = moduleManager.getConnectionInfo4MGT();
            addLocalModues(modulelist, sysMgtModules);
            List<ModuleConnectionInfo> routerModules = moduleManager.getConnectionInfo4Router();
            addLocalModues(modulelist, routerModules);
        	List<ModuleConnectionInfo> serverModules = moduleManager.getConnectionInfo4Server();
            addLocalModues(modulelist, serverModules);
            List<ModuleConnectionInfo> clientModules = moduleManager.getConnectionInfo4Client();
            addLocalModues(modulelist, clientModules);
            List<ModuleConnectionInfo> mqmModules = moduleManager.getConnectionInfo4MQM();
            addLocalModues(modulelist, mqmModules);
    	}
        return modulelist;
	}
	/**
	 * getLocalAddress 
	 * @return
	 */
	private static Set<InetAddress> getLocalAddress(){
		Enumeration<NetworkInterface> netInterfaces = null;
		Set<InetAddress> inetAddressSet = new HashSet<InetAddress>();
		try {  
		    netInterfaces = NetworkInterface.getNetworkInterfaces();  
		    while (netInterfaces.hasMoreElements()) {  
		        NetworkInterface ni = netInterfaces.nextElement();  
//		        System.out.println("DisplayName:" + ni.getDisplayName());  
//		        System.out.println("Name:" + ni.getName());  
		        Enumeration<InetAddress> ips = ni.getInetAddresses();  
		        while (ips.hasMoreElements()) {
		        	InetAddress address = ips.nextElement();
		        	inetAddressSet.add(address);
//		            System.out.println("IP:"  + address.getHostAddress());  
		        }  
		    }  
		} catch (Exception e) {  
		    e.printStackTrace();  
		}  
		return inetAddressSet;
	}
	/**
	 * is local address
	 * @param conInfo
	 * @return
	 */
	private static boolean isLocalModule(ModuleConnectionInfo conInfo){
		Set<InetAddress> inetAddressSet = getLocalAddress();
		String url = conInfo.getURL();
    	InetAddress urlAddr = null;
    	try {
    		urlAddr = InetAddress.getByName(url);
    	} catch(UnknownHostException e) {
    		e.printStackTrace();
    	}
    	
    	if (urlAddr.isLoopbackAddress() || inetAddressSet.contains(urlAddr)) {
    		return true;
    	}
    	return false;
	}
	/**
	 * add start modules, exclude non local url in ModuleManagement.xml
	 * @param moduleList
	 * @param localAddr
	 * @param confModules
	 */
	private static void addLocalModues(List<ModuleConnectionInfo> moduleList, List<ModuleConnectionInfo> confModules) {
		for (ModuleConnectionInfo conInfo : confModules) {
        	if (isLocalModule(conInfo)) {
        		moduleList.add(conInfo);
        	}

        }
	}

}
