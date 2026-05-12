package com.king.gmms.domain;

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

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.util.SystemConstants;
import com.king.message.gmms.GmmsMessage;
import com.king.mgt.context.Module;
import com.king.mgt.util.ConfigurationException;
import com.king.mgt.util.UserInterfaceUtility;

/**
 * Init ModuleManagement.xml
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
	private ModuleConnectionInfo connectionInfoOfMasterMGT = null;
	private ModuleConnectionInfo connectionInfoOfSlaveMGT = null;
	private Map<String, ArrayList<String>> moduleTypeMap = null; // module prefix,module name list
	private Map<String, ArrayList<String>> moduleStatusMap = null;// module name,module status list
	private List<String> routerList = null;
	private List<String> serverList = null;
	private List<String> clientList = null;
	private List<String> mqmList = null;
	private List<String> functionModuleList = null;
	private List<String> systemMgtList = null;
	private Set<String> moduleUrls = null;
	private final String moduleConfigFile = "ModuleManagement.xml";
	// below for smsui
	private HashMap<String, Module> mapName2Module = new HashMap<String, Module>();
	private ArrayList<String> directStopModules = new ArrayList<String>();
	private ArrayList<String> activeModules = new ArrayList<String>();
	private UserInterfaceUtility uiUtil = null;
	public Module A2P;
	private Object mutex = new Object();
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
		moduleStatusMap = new ConcurrentHashMap<String, ArrayList<String>>();
		routerList = new ArrayList<String>();
		serverList = new ArrayList<String>();
		clientList = new ArrayList<String>();
		mqmList = new ArrayList<String>();
		systemMgtList = new ArrayList<String>();
		moduleUrls = new HashSet<String>();
		functionModuleList = new ArrayList<String>();
		uiUtil = UserInterfaceUtility.getInstance();
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
		try {
			String configFilePath = System.getProperty("a2p_home",
					"/usr/local/a2p/")
					+ "conf/" + moduleConfigFile;
			SAXReader saxReader = new SAXReader();
			Document doc = saxReader.read(configFilePath);
			Element rootElement = doc.getRootElement();
			initModuleElement(rootElement);
		} catch (DocumentException e) {
			log.error("init module configFile " + moduleConfigFile
					+ " conf file error!!!", e);
			System.out.println("init module configFile " + moduleConfigFile
					+ " conf file error:" + e.getMessage());
		}
	}

	/**
	 * init module element
	 * 
	 * @param root
	 */
	private void initModuleElement(Element root) {
		Iterator itInner = root.elementIterator();
		while (itInner.hasNext()) {
			Element sub = (Element) itInner.next();
			String moduleName = sub.getName();
			String fullModuleType = getStringAttrValue(sub, "type");
			ModuleConnectionInfo connectionInfo = this.initConnectionInfo(sub);
			connectionInfo.setFullModuleType(fullModuleType);
			if (fullModuleType.endsWith(SystemConstants.ROUTER_MODULE_TYPE)) {
				connectionInfo
						.setModuleType(SystemConstants.ROUTER_MODULE_TYPE);
				routerConnectionMap.put(moduleName, connectionInfo);
				routerList.add(moduleName);
			} else if (fullModuleType
					.endsWith(SystemConstants.SERVER_MODULE_TYPE)) {
				connectionInfo
						.setModuleType(SystemConstants.SERVER_MODULE_TYPE);
				serverConnectionMap.put(moduleName, connectionInfo);
				serverList.add(moduleName);
			} else if (fullModuleType
					.endsWith(SystemConstants.CLIENT_MODULE_TYPE)) {
				connectionInfo
						.setModuleType(SystemConstants.CLIENT_MODULE_TYPE);
				clientConnectionMap.put(moduleName, connectionInfo);
				clientList.add(moduleName);
			} else if (SystemConstants.MQM_MODULE_TYPE
					.equalsIgnoreCase(fullModuleType)) {
				connectionInfo.setModuleType(SystemConstants.MQM_MODULE_TYPE);
				mqmConnectionMap.put(moduleName, connectionInfo);
				mqmList.add(moduleName);
			} else if (SystemConstants.MGT_MODULE_TYPE
					.equalsIgnoreCase(fullModuleType)) {
				connectionInfo.setModuleType(SystemConstants.MGT_MODULE_TYPE);
				String value = getStringAttrValue(sub, "value");
				if (SystemConstants.MASTER_MODULE_TYPE.equalsIgnoreCase(value)) {
					connectionInfoOfMasterMGT = connectionInfo;
				} else if (SystemConstants.SLAVE_MODULE_TYPE
						.equalsIgnoreCase(value)) {
					connectionInfoOfSlaveMGT = connectionInfo;
				}
				systemMgtConnectionMap.put(moduleName, connectionInfo);
				systemMgtList.add(moduleName);
			} else {
				functionConnectionMap.put(moduleName, connectionInfo);
				functionModuleList.add(moduleName);
			}
			if (moduleTypeMap.containsKey(fullModuleType)) {
				moduleTypeMap.get(fullModuleType).add(moduleName);
			} else {
				ArrayList<String> moduleList = new ArrayList<String>();
				moduleList.add(moduleName);
				moduleTypeMap.put(fullModuleType, moduleList);
			}
			ArrayList<String> statusList = new ArrayList<String>();
			statusList.add(SystemConstants.INITIAL_MODULE_STATUS);
			statusList.add(SystemConstants.INITIAL_MODULE_STATUS);
			moduleStatusMap.put(moduleName, statusList);
		}
	}

	/**
	 * get ConnectionInfo
	 * 
	 * @param root
	 * @return
	 */
	private ModuleConnectionInfo initConnectionInfo(Element root) {
		Iterator itInner = root.elementIterator();
		String moduleName = root.getName();
		ModuleConnectionInfo connectionInfo = new ModuleConnectionInfo();
		connectionInfo.setModuleName(moduleName);
		connectionInfo.setUserName(moduleName);
		connectionInfo.setConnectionName(moduleName);
		while (itInner.hasNext()) {
			Element sub = (Element) itInner.next();
			String elemName = sub.getName();
			String elemValue = sub.getTextTrim();
			if ("url".equalsIgnoreCase(elemName)) {
				connectionInfo.setURL(elemValue);
				moduleUrls.add(elemValue);
			} else if ("classpath".equalsIgnoreCase(elemName)) {
				connectionInfo.setClassPath(elemValue);
			} else if ("msg_port".equalsIgnoreCase(elemName)) {
				try {
					int port = Integer.parseInt(elemValue);
					connectionInfo.setPort(port);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("sys_port".equalsIgnoreCase(elemName)) {
				try {
					int port = Integer.parseInt(elemValue);
					connectionInfo.setSysPort(port);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("cmd_port".equalsIgnoreCase(elemName)) {
				try {
					int port = Integer.parseInt(elemValue);
					connectionInfo.setCmdPort(port);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("MinSenderNumber".equalsIgnoreCase(elemName)) {
				try {
					int senderNum = Integer.parseInt(elemValue);
					connectionInfo.setMinSenderNum(senderNum);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("MaxSenderNumber".equalsIgnoreCase(elemName)) {
				try {
					int senderNum = Integer.parseInt(elemValue);
					connectionInfo.setMaxSenderNum(senderNum);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("MinReceiverNumber".equalsIgnoreCase(elemName)) {
				try {
					int receiverNum = Integer.parseInt(elemValue);
					connectionInfo.setMinReceiverNum(receiverNum);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("MaxReceiverNumber".equalsIgnoreCase(elemName)) {
				try {
					int receiverNum = Integer.parseInt(elemValue);
					connectionInfo.setMaxReceiverNum(receiverNum);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("InBindSessionNumber".equalsIgnoreCase(elemName)) {
				try {
					int sessionNum = Integer.parseInt(elemValue);
					connectionInfo.setInSessionNum(sessionNum);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			} else if ("OutBindSessionNumber".equalsIgnoreCase(elemName)) {
				try {
					int sessionNum = Integer.parseInt(elemValue);
					connectionInfo.setOutSessionNum(sessionNum);
				} catch (NumberFormatException e) {
					log.error(e,e);
				}
			}
		}
		return connectionInfo;
	}

	/**
	 * init for smsui
	 */
	public void initSmsUI() {
		String dStopModules = uiUtil.getProperty("DirectStopModules");
		if (dStopModules != null && !dStopModules.equalsIgnoreCase("")) {
			String[] list = dStopModules.trim().split(",");
			for (int i = 0; i < list.length; i++) {
				directStopModules.add(list[i].trim());
				log.info("Add directStopModule:{}", list[i]);
			}
		}
		// local address
		InetAddress localAddr = null;
		try {
			localAddr = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			log.error(e,e);
		}
		System.out.println("InetAddress localAddr=" + localAddr);

		List<String> confModules = new ArrayList<String>();
		List<String> modules = new ArrayList<String>();
		confModules.addAll(this.getSystemMgtList());
		confModules.addAll(this.getRouterModules());
		confModules.addAll(this.getServerModules());
		confModules.addAll(this.getClientModules());
		confModules.addAll(this.getMqmModules());
		addLocalModues(modules, localAddr, confModules);

		log.trace("modules: {}", modules.toString());

		if (modules.size() == 0) {
			uiUtil.setEnableNotification(false);
			return;
		}
		String[] moduleList = modules.toArray(new String[0]);
		for (int i = 0; i < moduleList.length; i++) {
			String name = moduleList[i];
			if (name == null) {
				throw new ConfigurationException(
						"Module configuraiton error. Item " + i);
			}
			name = moduleList[i].trim();
			ModuleConnectionInfo conInfo = this.getConnectionInfo(name);
			String ip = conInfo.getURL();
			int port = conInfo.getCmdPort();
			if (port == -1) {
				throw new ConfigurationException(
						"Wrong port configuration for module " + name);
			}

			Module module = new Module(name, ip, port);
			addModule(module);
			addActiveModule(name);
		}
		A2P = new Module("A2P", null, -1);
	}

	/**
	 * add local modules, exclude non local url in ModuleManagement.xml
	 * 
	 * @param moduleList
	 * @param localAddr
	 * @param confModules
	 */
	private void addLocalModues(List<String> moduleList, InetAddress localAddr,
			List<String> confModules) {
		ModuleManager domainModuleManager = ModuleManager.getInstance();
		for (String item : confModules) {
			ConnectionInfo conInfo = domainModuleManager
					.getConnectionInfo(item);
			String url = conInfo.getURL();
			System.out.println(item + "url=" + url);

			InetAddress urlAddr = null;
			try {
				urlAddr = InetAddress.getByName(url);
			} catch (UnknownHostException e) {
				log.error(e,e);
			}

			if (urlAddr.isLoopbackAddress() || urlAddr.equals(localAddr)) {
				moduleList.add(item);
			}
		}
	}

	/**
	 * getActiveModules for smsui
	 */
	public String[] getActiveModules() {
		String[] result = null;
		if (activeModules.size() == 0) {
			return null;
		}
		synchronized (mutex) {
			result = new String[activeModules.size()];
			int i = 0;
			for (String module : activeModules) {
				result[i] = module;
				i++;
			}
		}
		return result;
	}

	/**
	 * getActiveModulesSize for smsui
	 */
	public int getActiveModulesSize() {
		synchronized (mutex) {
			return activeModules.size();
		}
	}

	/**
	 * isActiveModule for smsui
	 */
	public boolean isActiveModule(String moduleName) {
		synchronized (mutex) {
			if (moduleName == null) {
				return false;
			}
			return activeModules.contains(moduleName.trim());
		}
	}

	/**
	 * isContainModule for smsui
	 */
	public boolean isContainModule(String name) {
		if (name == null) {
			return false;
		}
		return mapName2Module.containsKey(name.trim());
	}

	/**
	 * getModuleByName for smsui
	 */
	public Module getModuleByName(String name) {
		if (name == null) {
			return null;
		}
		return mapName2Module.get(name.trim());
	}

	/**
	 * isDirectStopModule for smsui
	 */
	public boolean isDirectStopModule(String name) {
		if (name == null) {
			return false;
		}
		return directStopModules.contains(name);
	}

	/**
	 * addModule for smsui
	 */
	private void addModule(Module module) {
		if (module == null) {
			return;
		}
		String name = module.getName();
		if (name == null) {
			return;
		}
		mapName2Module.put(name.trim(), module);
	}

	/**
	 * addActiveModule for smsui
	 */
	private void addActiveModule(String module) {
		if (module == null) {
			return;
		}
		activeModules.add(module);
		log.info("Add active module: {}", module);
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
	 * get attribute of xml tag
	 * 
	 * @param el
	 * @param name
	 * @return
	 */
	private String getStringAttrValue(Element el, String name) {
		try {
			return el.attribute(name).getValue().trim();
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * getConnectionInfo list for function Modules
	 * 
	 * @return
	 */
	public List<ModuleConnectionInfo> getConnectionInfo4ServiceModule() {
		List<ModuleConnectionInfo> serverConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				serverConnectionMap.values());
		List<ModuleConnectionInfo> clientConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				clientConnectionMap.values());
		List<ModuleConnectionInfo> mqmConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				mqmConnectionMap.values());
		serverConnectionInfoList.addAll(clientConnectionInfoList);
		serverConnectionInfoList.addAll(mqmConnectionInfoList);
		return serverConnectionInfoList;
	}

	/**
	 * get connectionInfo list for router modules
	 * 
	 * @return
	 */
	public List<ModuleConnectionInfo> getConnectionInfo4Router() {
		List<ModuleConnectionInfo> routerConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				routerConnectionMap.values());
		return routerConnectionInfoList;
	}

	/**
	 * get connectionInfo list for router modules
	 * 
	 * @return
	 */
	public List<ModuleConnectionInfo> getConnectionInfo4Server() {
		List<ModuleConnectionInfo> serverConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				serverConnectionMap.values());
		return serverConnectionInfoList;
	}

	/**
	 * get connectionInfo list for router modules
	 * 
	 * @return
	 */
	public List<ModuleConnectionInfo> getConnectionInfo4Client() {
		List<ModuleConnectionInfo> clientConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				clientConnectionMap.values());
		return clientConnectionInfoList;
	}

	/**
	 * getConnectionInfo list of System management
	 * 
	 * @return
	 */
	public List<ModuleConnectionInfo> getConnectionInfo4MGT() {
		List<ModuleConnectionInfo> mgtConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				systemMgtConnectionMap.values());
		return mgtConnectionInfoList;
	}

	/**
	 * getConnectionInfo list of MQM
	 * 
	 * @return
	 */
	public List<ModuleConnectionInfo> getConnectionInfo4MQM() {
		List<ModuleConnectionInfo> mqmConnectionInfoList = new ArrayList<ModuleConnectionInfo>(
				mqmConnectionMap.values());
		return mqmConnectionInfoList;
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
	 * select router for load balance
	 * 
	 * @param msg
	 * @param moduleName
	 * @return
	 */
	public String selectRouter(GmmsMessage msg) {
		int size = routerList.size();
		if (size <= 0) {
			return null;
		} else if (size == 1) {
			return routerList.get(0);
		}
		String destAddress = msg.getRecipientAddress();
		if (destAddress == null) {
			int index = this.getRandomIndex(size);
			return routerList.get(index);
		}
		try {
			//int lastInt = destAddress.charAt(destAddress.length() - 1) - '0';
			//return routerList.get(lastInt % size);
			int index = (destAddress.hashCode() & Integer.MAX_VALUE)%size;
			if(index<0){
				index = Math.abs(index);
			}
			return routerList.get(index);
		} catch (Exception e) {
			return routerList.get(0);
		}

		/*int index = loadbalanceRandom%size;
		loadbalanceRandom++;
		if (loadbalanceRandom>100000000) {
			loadbalanceRandom = 0;
		}
		return routerList.get(index);*/
	}

	/**
	 * select router for load balance
	 * 
	 * @param msg
	 * @param moduleName
	 * @return
	 */
	public String selectAliveRouter(String failedRouter, GmmsMessage msg) {
		ArrayList<String> aliveRouters = new ArrayList<String>(routerList);
		aliveRouters.remove(failedRouter);
		int size = aliveRouters.size();
		if (size <= 0) {
			return null;
		} else if (size == 1) {
			return aliveRouters.get(0);
		}
		String destAddress = msg.getRecipientAddress();
		if (destAddress == null) {
			int index = this.getRandomIndex(size);
			return aliveRouters.get(index);
		}
		try {
			//int lastInt = destAddress.charAt(destAddress.length() - 1) - '0';
			//return aliveRouters.get(lastInt % size);
			int index = (destAddress.hashCode() & Integer.MAX_VALUE)%size;
			if(index<0){
				index = Math.abs(index);
			}
			return aliveRouters.get(index);
		} catch (Exception e) {
			return aliveRouters.get(0);
		}
		/*int index = loadbalanceRandom%size;
		loadbalanceRandom++;
		if (loadbalanceRandom>100000000) {
			loadbalanceRandom = 0;
		}
		return aliveRouters.get(index);*/
	}
	
	/**
	 * select router for load balance
	 * 
	 * @param msg
	 * @param moduleName
	 * @return
	 */
	public String selectAliveRouter(ArrayList<String> failedRouters, GmmsMessage msg) {
		ArrayList<String> aliveRouters = new ArrayList<String>(routerList);
		if(failedRouters != null){
			for(String failedRouter: failedRouters){
				aliveRouters.remove(failedRouter);
			}
		}
		int size = aliveRouters.size();
		if (size <= 0) {
			return null;
		} else if (size == 1) {
			return aliveRouters.get(0);
		}
		String destAddress = msg.getRecipientAddress();
		if (destAddress == null) {
			int index = this.getRandomIndex(size);
			return aliveRouters.get(index);
		}
		try {
			//int lastInt = destAddress.charAt(destAddress.length() - 1) - '0';
			//return aliveRouters.get(lastInt % size);
			int index = (destAddress.hashCode() & Integer.MAX_VALUE)%size;
			if(index<0){
				index = Math.abs(index);
			}
			return aliveRouters.get(index);
		} catch (Exception e) {
			return aliveRouters.get(0);
		}
		/*int index = loadbalanceRandom%size;
		loadbalanceRandom++;
		if (loadbalanceRandom>100000000) {
			loadbalanceRandom = 0;
		}
		return aliveRouters.get(index);*/
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
	 * select Channel for load balance
	 * 
	 * @return moduleName
	 */
	public String selectPeeringChannel() {
		String peeringChannel = getClusterProperty("DefaultPeeringChannel",
				SystemConstants.PEERING2CLIENT_MODULE_TYPE);
		return this.selectChannel(peeringChannel);
	}

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
	 * get server/client module's ConnectionInfo
	 * 
	 * @param moduleName
	 * @return
	 */
	public ModuleConnectionInfo getServiceConnectionInfo(String moduleName) {
		ModuleConnectionInfo connectionInfo = null;
		if (moduleName == null) {
			return connectionInfo;
		}
		if (serverConnectionMap.containsKey(moduleName)) {
			connectionInfo = serverConnectionMap.get(moduleName);
		} else if (clientConnectionMap.containsKey(moduleName)) {
			connectionInfo = clientConnectionMap.get(moduleName);
		} else if (mqmConnectionMap.containsKey(moduleName)) {
			connectionInfo = mqmConnectionMap.get(moduleName);
		} else if (functionConnectionMap.containsKey(moduleName)) {
			connectionInfo = functionConnectionMap.get(moduleName);
		}
		return connectionInfo;
	}

	/**
	 * get Router's ConnectionInfo
	 * 
	 * @param moduleName
	 * @return
	 */
	public ModuleConnectionInfo getRouterConnectionInfo(String moduleName) {
		ModuleConnectionInfo connectionInfo = null;
		if (moduleName == null) {
			return connectionInfo;
		}
		if (routerConnectionMap.containsKey(moduleName)) {
			connectionInfo = routerConnectionMap.get(moduleName);
		}
		return connectionInfo;
	}

	/**
	 * get Server's ConnectionInfo
	 * 
	 * @param moduleName
	 * @return
	 */
	public ModuleConnectionInfo getServerConnectionInfo(String moduleName) {
		ModuleConnectionInfo connectionInfo = null;
		if (moduleName == null) {
			return connectionInfo;
		}
		if (serverConnectionMap.containsKey(moduleName)) {
			connectionInfo = serverConnectionMap.get(moduleName);
		}
		return connectionInfo;
	}

	/**
	 * get Client's ConnectionInfo
	 * 
	 * @param moduleName
	 * @return
	 */
	public ModuleConnectionInfo getClientConnectionInfo(String moduleName) {
		ModuleConnectionInfo connectionInfo = null;
		if (moduleName == null) {
			return connectionInfo;
		}
		if (clientConnectionMap.containsKey(moduleName)) {
			connectionInfo = clientConnectionMap.get(moduleName);
		}
		return connectionInfo;
	}

	/**
	 * get MQM's ConnectionInfo
	 * 
	 * @param moduleName
	 * @return
	 */
	public ModuleConnectionInfo getMQMConnectionInfo(String moduleName) {
		ModuleConnectionInfo connectionInfo = null;
		if (moduleName == null) {
			return connectionInfo;
		}
		if (mqmConnectionMap.containsKey(moduleName)) {
			connectionInfo = mqmConnectionMap.get(moduleName);
		}
		return connectionInfo;
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
	 * getConnectionInfo of system management module
	 */
	public ModuleConnectionInfo getConnectionInfoOfMasterMGT() {
		return connectionInfoOfMasterMGT;
	}

	/**
	 * getConnectionInfo of system management module
	 */
	public ModuleConnectionInfo getConnectionInfoOfSlaveMGT() {
		return connectionInfoOfSlaveMGT;
	}

	/**
	 * getClusterProperty
	 * 
	 * @param properpty
	 * @param defaultValue
	 * @return
	 */
	public String getClusterProperty(String properpty, String defaultValue) {
		GmmsUtility gmmsUtility = GmmsUtility.getInstance();
		return gmmsUtility.getCommonProperty(properpty, defaultValue);
	}

	/**
	 * getter
	 * 
	 * @return
	 */
	public Map<String, ModuleConnectionInfo> getSystemMgtConnectionMap() {
		return systemMgtConnectionMap;
	}

	/**
	 * when module stop
	 * 
	 * @param moduleName
	 */
	public void updateModuleStatus2Down(String moduleName) {
		log.trace("updateModuleStatus2Down:{}", moduleName);
		ArrayList<String> statusList = moduleStatusMap.get(moduleName);// current
																		// status,
																		// last
																		// status
		String currentStatus = SystemConstants.DOWN_MODULE_STATUS;
		String lastStatus = statusList.get(0);
		statusList.clear();
		statusList.add(currentStatus);
		statusList.add(lastStatus);
		moduleStatusMap.put(moduleName, statusList);
	}

	/**
	 * when module start
	 * 
	 * @param moduleName
	 */
	public void updateModuleStatus2Up(String moduleName) {
		log.trace("updateModuleStatus2Up:{}", moduleName);
		ArrayList<String> statusList = moduleStatusMap.get(moduleName);// current status,last status
		String currentStatus = SystemConstants.UP_MODULE_STATUS;
		String lastStatus = statusList.get(0);
		statusList.clear();
		statusList.add(currentStatus);
		statusList.add(lastStatus);
		moduleStatusMap.put(moduleName, statusList);
	}

	public Map<String, ArrayList<String>> getModuleStatusMap() {
		return moduleStatusMap;
	}
	/**
	 * getModules By Type
	 * 
	 * @param fullModuleType
	 * @return
	 */
	public List<String> getModulesWithFullType(String fullModuleType) {
		List<String> modules = this.moduleTypeMap.get(fullModuleType);
		return modules;
	}
	/**
	 * getAliveModules By Type
	 * 
	 * @param fullModuleType
	 * @return
	 */
	public List<String> getAliveModulesWithSameType(String moduleName) {
		ModuleConnectionInfo modInfo = this.getConnectionInfo(moduleName);
		String fullModuleType = modInfo.getFullModuleType();
		List<String> modules = this.moduleTypeMap.get(fullModuleType);
		List<String> aliveModules = new ArrayList<String>();
		for (String module : modules) {
			String moduleStatus = this.moduleStatusMap.get(module).get(0);
			if (SystemConstants.UP_MODULE_STATUS.equalsIgnoreCase(moduleStatus)) {
				aliveModules.add(module);
			}
		}
		return aliveModules;
	}

	/**
	 * isAliveModule
	 * 
	 * @param module
	 * @return
	 */
	public boolean isAliveModule(String module) {
		List<String> modList = this.moduleStatusMap.get(module);
		if(modList==null || modList.isEmpty()){
			return false;
		}
		String status = modList.get(0);
		return SystemConstants.UP_MODULE_STATUS.equals(status);
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

	public List<String> getFunctionModuleList() {
		return functionModuleList;
	}

	public List<String> getSystemMgtList() {
		return systemMgtList;
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

	/**
	 * is system manager
	 * 
	 * @param module
	 * @return
	 */
	public boolean isSystemManager(String module) {
		return systemMgtList.contains(module);
	}

	public List<String> getMqmModules() {
		return mqmList;
	}

	public Set<String> getModuleUrls() {
		return moduleUrls;
	}
}
