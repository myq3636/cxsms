package com.king.gmms.domain;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.GmmsMessage;
import com.king.redis.RedisClient;

/**
 * The modules: conf/a2p_modules.conf and the module registry in Redis (applyRegistry). ModuleManagement.xml is
 * not read any more (docs/流模式下系统管理和模块间连接代码清理分析.md 3.4).
 * 
 * @author jianmingyang
 * 
 */
public class ModuleManager {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(ModuleManager.class);
	private static ModuleManager instance = null;
	private Map<String, ModuleConnectionInfo> functionConnectionMap = null;
	private Map<String, ModuleConnectionInfo> routerConnectionMap = null;
	private Map<String, ModuleConnectionInfo> serverConnectionMap = null;
	private Map<String, ModuleConnectionInfo> clientConnectionMap = null;
	private Map<String, ModuleConnectionInfo> mqmConnectionMap = null;
	private Map<String, ModuleConnectionInfo> systemMgtConnectionMap = null;
	private Map<String, ArrayList<String>> moduleTypeMap = null; // module prefix,module name list
	/** the lists follow the registry: a position in them says nothing any more */
	private volatile boolean registryApplied = false;
	private volatile List<String> routerList = null;
	private volatile List<String> serverList = null;
	private volatile List<String> clientList = null;
	private volatile List<String> mqmList = null;
	private final String moduleListConfigFile = "a2p_modules.conf";
	//random for loadbalance routing;
	private int loadbalanceRandom = 0;

	private ModuleManager() {
		functionConnectionMap = new ConcurrentHashMap<String, ModuleConnectionInfo>();
		routerConnectionMap = new ConcurrentHashMap<String, ModuleConnectionInfo>();
		serverConnectionMap = new ConcurrentHashMap<String, ModuleConnectionInfo>();
		clientConnectionMap = new ConcurrentHashMap<String, ModuleConnectionInfo>();
		mqmConnectionMap = new ConcurrentHashMap<String, ModuleConnectionInfo>();
		systemMgtConnectionMap = new ConcurrentHashMap<String, ModuleConnectionInfo>();
		moduleTypeMap = new ConcurrentHashMap<String, ArrayList<String>>();
		routerList = new ArrayList<String>();
		serverList = new ArrayList<String>();
		clientList = new ArrayList<String>();
		mqmList = new ArrayList<String>();
	}

	/**
	 * singleton model
	 * 
	 * @return
	 */
	public static synchronized ModuleManager getInstance() {
		if (null == instance) {
			instance = new ModuleManager();
			instance.initConnectionMap();
		}
		return instance;
	}

	/**
	 * init ConnectionMap
	 */
	private void initConnectionMap() {
		initModuleListConfig();
	}

	private void initModuleListConfig() {
		String a2pHome = System.getProperty("a2p_home", "/usr/local/a2p/");
		if (!a2pHome.endsWith("/") && !a2pHome.endsWith("\\")) {
			a2pHome = a2pHome + "/";
		}
		File configFile = new File(a2pHome + "conf/" + moduleListConfigFile);
		if (!configFile.exists()) {
			log.info("{} not found, skip module list supplement.", configFile.getAbsolutePath());
			return;
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(configFile));
			String line;
			int lineNo = 0;
			while ((line = reader.readLine()) != null) {
				lineNo++;
				parseModuleListLine(line, lineNo);
			}
		} catch (Exception e) {
			log.error("init module list configFile " + moduleListConfigFile + " error!!!", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					log.warn("close module list config reader failed", e);
				}
			}
		}
	}

	private void parseModuleListLine(String line, int lineNo) {
		if (line == null) {
			return;
		}
		String trim = line.trim();
		if (trim.length() == 0 || trim.startsWith("#")) {
			return;
		}
		String[] parts = trim.split("\\|", -1);
		if (parts.length < 4) {
			log.warn("Ignore invalid {} line {}: {}", moduleListConfigFile, lineNo, line);
			return;
		}
		if (!"1".equals(parts[0].trim())) {
			return;
		}
		String moduleName = parts[1].trim();
		String role = parts[2].trim();
		if (moduleName.length() == 0) {
			return;
		}
		if (getConnectionInfo(moduleName) != null) {
			return;
		}
		registerModuleConnection(moduleName, role, newConnectionInfo(moduleName));
	}

	private ModuleConnectionInfo newConnectionInfo(String moduleName) {
		ModuleConnectionInfo connectionInfo = new ModuleConnectionInfo();
		connectionInfo.setModuleName(moduleName);
		connectionInfo.setUserName(moduleName);
		connectionInfo.setConnectionName(moduleName);
		connectionInfo.setURL(resolveModuleUrl(moduleName));
		connectionInfo.setCmdPort(resolveModuleInt(moduleName, "CmdPort", -1));
		connectionInfo.setSysPort(resolveModuleInt(moduleName, "SysPort", -1));
		connectionInfo.setInSessionNum(resolveModuleInt(moduleName, "InBindSessionNumber", 0));
		connectionInfo.setOutSessionNum(resolveModuleInt(moduleName, "OutBindSessionNumber", 0));
		return connectionInfo;
	}

	/**
	 * The modules of the cluster as the registry sees them, each {name, role}: the lists of the module types
	 * it manages are replaced as a whole, readers have the old or the new one. Modules on other machines need
	 * not be in the local module list.
	 */
	public synchronized void applyRegistry(List<String[]> visible) {
		Map<String, ArrayList<String>> byType = new HashMap<String, ArrayList<String>>();
		List<String> routers = new ArrayList<String>();
		List<String> servers = new ArrayList<String>();
		List<String> clients = new ArrayList<String>();
		List<String> mqms = new ArrayList<String>();
		List<String[]> sorted = new ArrayList<String[]>(visible);
		sorted.sort(new java.util.Comparator<String[]>() {
			public int compare(String[] a, String[] b) {
				return a[0].compareTo(b[0]);
			}
		});
		for (String[] module : sorted) {
			ModuleConnectionInfo info = getConnectionInfo(module[0]);
			if (info == null) {
				info = newConnectionInfo(module[0]);
				registerModuleConnection(module[0], module[1], info, false);
			}
			String fullType = info.getFullModuleType();
			ArrayList<String> ofType = byType.get(fullType);
			if (ofType == null) {
				ofType = new ArrayList<String>();
				byType.put(fullType, ofType);
			}
			ofType.add(module[0]);
			if (routerConnectionMap.containsKey(module[0])) {
				routers.add(module[0]);
			} else if (serverConnectionMap.containsKey(module[0])) {
				servers.add(module[0]);
			} else if (clientConnectionMap.containsKey(module[0])) {
				clients.add(module[0]);
			} else if (mqmConnectionMap.containsKey(module[0])) {
				mqms.add(module[0]);
			}
		}
		for (String type : new ArrayList<String>(moduleTypeMap.keySet())) {
			if (!byType.containsKey(type) && !SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(type)) {
				moduleTypeMap.put(type, new ArrayList<String>());
			}
		}
		moduleTypeMap.putAll(byType);
		routerList = routers;
		serverList = servers;
		clientList = clients;
		mqmList = mqms;
		registryApplied = true;
	}

	private void registerModuleConnection(String moduleName, String role, ModuleConnectionInfo connectionInfo) {
		registerModuleConnection(moduleName, role, connectionInfo, true);
	}

	/** addToLists false: the registry sets the lists itself, as a whole. */
	private void registerModuleConnection(String moduleName, String role, ModuleConnectionInfo connectionInfo, boolean addToLists){
		String fullModuleType = resolveFullModuleType(moduleName, role);
		connectionInfo.setFullModuleType(fullModuleType);
		if (SystemConstants.ROUTER_MODULE_TYPE.equalsIgnoreCase(fullModuleType)) {
			connectionInfo.setModuleType(SystemConstants.ROUTER_MODULE_TYPE);
			routerConnectionMap.put(moduleName, connectionInfo);
			if (addToLists) {
				routerList.add(moduleName);
			}
		} else if (SystemConstants.MQM_MODULE_TYPE.equalsIgnoreCase(fullModuleType)) {
			connectionInfo.setModuleType(SystemConstants.MQM_MODULE_TYPE);
			mqmConnectionMap.put(moduleName, connectionInfo);
			if (addToLists) {
				mqmList.add(moduleName);
			}
		} else if (SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(fullModuleType)) {
			connectionInfo.setModuleType(SystemConstants.MGT_MODULE_TYPE);
			systemMgtConnectionMap.put(moduleName, connectionInfo);
		} else if (fullModuleType.endsWith(SystemConstants.SERVER_MODULE_TYPE)) {
			connectionInfo.setModuleType(SystemConstants.SERVER_MODULE_TYPE);
			serverConnectionMap.put(moduleName, connectionInfo);
			if (addToLists) {
				serverList.add(moduleName);
			}
		} else if (fullModuleType.endsWith(SystemConstants.CLIENT_MODULE_TYPE)) {
			connectionInfo.setModuleType(SystemConstants.CLIENT_MODULE_TYPE);
			clientConnectionMap.put(moduleName, connectionInfo);
			if (addToLists) {
				clientList.add(moduleName);
			}
		} else {
			functionConnectionMap.put(moduleName, connectionInfo);
		}
		if (addToLists) {
			addModuleType(moduleName, fullModuleType);
		}
		log.info("Module added from {}. module={}, role={}, fullType={}, cmdPort={}",
				moduleListConfigFile, moduleName, role, fullModuleType, connectionInfo.getCmdPort());
	}

	private String resolveFullModuleType(String moduleName, String role) {
		String lowerRole = role == null ? "" : role.toLowerCase();
		if ("core".equals(lowerRole)) {
			return SystemConstants.ROUTER_MODULE_TYPE;
		}
		if ("mqm".equals(lowerRole)) {
			return SystemConstants.MQM_MODULE_TYPE;
		}
		if ("mgt".equals(lowerRole) || "system-mgt".equals(lowerRole)) {
			return SystemConstants.MGT_MODULE_TYPE;
		}
		if (moduleName != null) {
			if (moduleName.startsWith(SystemConstants.MULTISMPPSERVER_MODULE_TYPE)) {
				return SystemConstants.MULTISMPPSERVER_MODULE_TYPE;
			}
			if (moduleName.startsWith(SystemConstants.COMMONHTTPSERVER_MODULE_TYPE)) {
				return SystemConstants.COMMONHTTPSERVER_MODULE_TYPE;
			}
			if (moduleName.startsWith(SystemConstants.SSLSMPPSERVER_MODULE_TYPE)) {
				return SystemConstants.SSLSMPPSERVER_MODULE_TYPE;
			}
			if (moduleName.startsWith(SystemConstants.MULTISMPPCLIENT_MODULE_TYPE)) {
				return SystemConstants.MULTISMPPCLIENT_MODULE_TYPE;
			}
			if (moduleName.startsWith(SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE)) {
				return SystemConstants.COMMONHTTPCLIENT_MODULE_TYPE;
			}
			if (moduleName.startsWith(SystemConstants.SSLSMPPCLIENT_MODULE_TYPE)) {
				return SystemConstants.SSLSMPPCLIENT_MODULE_TYPE;
			}
		}
		if (lowerRole.endsWith("server")) {
			return SystemConstants.MULTISMPPSERVER_MODULE_TYPE;
		}
		if (lowerRole.endsWith("client")) {
			return SystemConstants.MULTISMPPCLIENT_MODULE_TYPE;
		}
		return role == null || role.length() == 0 ? "Function" : role;
	}

	private String resolveModuleUrl(String moduleName) {
		String value = getUtilityProperty(moduleName + ".ModuleUrl", null);
		if (value == null || value.length() == 0) {
			value = getUtilityProperty("Module.ModuleUrl", null);
		}
		if (value == null || value.length() == 0) {
			value = getUtilityProperty("ServerIP", "127.0.0.1");
		}
		return value;
	}

	private int resolveModuleInt(String moduleName, String suffix, int defaultValue) {
		String value = getUtilityProperty(moduleName + "." + suffix, null);
		if (value == null || value.length() == 0) {
			value = getUtilityProperty("Module." + suffix, null);
		}
		if (value == null || value.length() == 0) {
			return defaultValue;
		}
		try {
			return Integer.parseInt(value.trim());
		} catch (Exception e) {
			log.warn("Invalid module integer config. module={}, suffix={}, value={}", moduleName, suffix, value);
			return defaultValue;
		}
	}

	private String getUtilityProperty(String key, String defaultValue) {
		try {
			return GmmsUtility.getInstance().getCommonProperty(key, defaultValue);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private void addModuleType(String moduleName, String fullModuleType) {
		ArrayList<String> moduleList = moduleTypeMap.get(fullModuleType);
		if (moduleList == null) {
			moduleList = new ArrayList<String>();
			moduleTypeMap.put(fullModuleType, moduleList);
		}
		if (!moduleList.contains(moduleName)) {
			moduleList.add(moduleName);
		}
	}

	public int getModuleIndex(String module) {
		if (module == null) {
			return 0;
		}

		ModuleConnectionInfo moduleConnection = getConnectionInfo(module);

		if (moduleConnection == null) {
			return 0;
		}

		String moduleType = moduleConnection.getFullModuleType();

		ArrayList<String> moduleList = moduleTypeMap.get(moduleType);
		if (moduleList != null) {
			int index = moduleList.indexOf(module);
			int fromName = trailingNumber(module);
			if (registryApplied && fromName > 0) {
				index = fromName - 1;
			}
			if (SystemConstants.CLIENT_MODULE_TYPE
					.equalsIgnoreCase(moduleConnection.getModuleType())) {
				return index + 3;
			} else {
				return index;
			}
		}
		return 0;
	}

	/**
	 * The number at the end of the module name, so it stays the same when the list changes and on every
	 * machine; without one, the position in the list of its module type, from 1. 0 when the module is not listed.
	 */
	public int getModuleNo(String module) {
		ModuleConnectionInfo moduleConnection = module == null ? null : getConnectionInfo(module);
		if (moduleConnection == null) {
			return 0;
		}
		int fromName = trailingNumber(module);
		if (fromName >= 0) {
			return fromName;
		}
		ArrayList<String> moduleList = moduleTypeMap.get(moduleConnection.getFullModuleType());
		return moduleList == null ? 0 : moduleList.indexOf(module) + 1;
	}

	/** The digits at the end of the name, -1 without any, Integer.MAX_VALUE when too long to be a module number. */
	public static int trailingNumber(String name) {
		int start = name.length();
		while (start > 0 && Character.isDigit(name.charAt(start - 1))) {
			start--;
		}
		if (start == name.length()) {
			return -1;
		}
		try {
			return Integer.parseInt(name.substring(start));
		} catch (NumberFormatException e) {
			return Integer.MAX_VALUE;
		}
	}

	public boolean isClientType(String module) {
		ModuleConnectionInfo moduleConnection = module == null ? null : getConnectionInfo(module);
		return moduleConnection != null
				&& SystemConstants.CLIENT_MODULE_TYPE.equalsIgnoreCase(moduleConnection.getModuleType());
	}

	/**
	 * get router module names
	 * 
	 * @param moduleName
	 * @return
	 */
	public List<String> getRouterModules() {
		return routerList;
	}

	/**
	 * get server module names
	 * 
	 * @param moduleName
	 * @return
	 */
	public List<String> getServerModules() {
		return serverList;
	}

	/**
	 * get client module names
	 * 
	 * @param moduleName
	 * @return
	 */
	public List<String> getClientModules() {
		return clientList;
	}

	
	

	/**
	 * select Channel for load balance
	 * 
	 * @return moduleName
	 */
	public String selectChannel(String queue) {
		if (queue == null) {
			return null;
		}
		int size = 0;
		ArrayList<String> channelList = null;
		if (moduleTypeMap.containsKey(queue)) {
			channelList = moduleTypeMap.get(queue);
		}
		if (channelList == null) {
			return null;
		} else {
			size = channelList.size();
			if (size <= 0) {
				return null;
			} else if (size == 1) {
				return channelList.get(0);
			}
		}
		int index = this.getRandomIndex(size);
		return channelList.get(index);
	}

	/**
	 * 
	 */
	/**
	 * getRandomIndex
	 * 
	 * @param size
	 * @return
	 */
	private int getRandomIndex(int size) {
		if (size <= 0) {
			return -1;
		}
		double d = Math.random() * (double) size + 0.5D;
		int index = (int) Math.round(d) - 1;
		return index;
	}

	/**
	 * get ConnectionInfo
	 * 
	 * @param moduleName
	 * @return
	 */
	public ModuleConnectionInfo getConnectionInfo(String moduleName) {
		ModuleConnectionInfo connectionInfo = null;
		if (moduleName == null) {
			return connectionInfo;
		}
		if (routerConnectionMap.containsKey(moduleName)) {
			connectionInfo = routerConnectionMap.get(moduleName);
		} else if (serverConnectionMap.containsKey(moduleName)) {
			connectionInfo = serverConnectionMap.get(moduleName);
		} else if (clientConnectionMap.containsKey(moduleName)) {
			connectionInfo = clientConnectionMap.get(moduleName);
		} else if (mqmConnectionMap.containsKey(moduleName)) {
			connectionInfo = mqmConnectionMap.get(moduleName);
		} else if (functionConnectionMap.containsKey(moduleName)) {
			connectionInfo = functionConnectionMap.get(moduleName);
		} else if (systemMgtConnectionMap.containsKey(moduleName)) {
			connectionInfo = systemMgtConnectionMap.get(moduleName);
		}
		return connectionInfo;
	}

	/**
	 * getModuleType
	 * 
	 * @param moduleName
	 * @return
	 */
	public String getModuleType(String moduleName) {
		ModuleConnectionInfo connectionInfo = (ModuleConnectionInfo) this
				.getConnectionInfo(moduleName);
		if (connectionInfo != null) {
			return connectionInfo.getModuleType();
		}
		return null;
	}

	/**
	 * getModuleType
	 * 
	 * @param moduleName
	 * @return
	 */
	public String getFullModuleType(String moduleName) {
		ModuleConnectionInfo connectionInfo = (ModuleConnectionInfo) this
				.getConnectionInfo(moduleName);
		if (connectionInfo != null) {
			return connectionInfo.getFullModuleType();
		}
		return null;
	}

	/**
	 * assign sessionOfModules
	 * 
	 * @param cinfo
	 * @param modType
	 * @return
	 */
	public int getSessionNumberOfModule(int sessionLimit, String moduleName) {
		ArrayList<String> modules = null;
		Set<String> keySet = moduleTypeMap.keySet();
		for (String key : keySet) {
			if (moduleName.startsWith(key)) {
				modules = moduleTypeMap.get(key);
				break;
			}
		}
		if (modules == null) {
			return sessionLimit;
		} else {
			int moduleNumber = modules.size();
			if (moduleNumber <= 1) {
				return sessionLimit;
			} else {
				int average = sessionLimit / moduleNumber;
				if (sessionLimit % moduleNumber == 0) {
					return average;
				} else {
					return average + 1;
				}
			}
		}
	}

	/**
	 * assign sessionOfModules
	 * 
	 * @param cinfo
	 * @param modType
	 * @return
	 */
	public int getSessionNumberOfSelf(int sessionLimit) {
		String moduleName = System.getProperty("module");
		return getSessionNumberOfModule(sessionLimit, moduleName);
	}

	/**
	 * is core engine
	 * 
	 * @param module
	 * @return
	 */
	public boolean isCoreEngine(String module) {
		return routerList.contains(module);
	}

}
