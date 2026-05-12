package com.king.gmms.ha.systemmanagement;

import java.io.IOException;

import com.king.framework.A2PThreadGroup;
import com.king.framework.SystemLogger;
import com.king.gmms.GmmsUtility;
import com.king.gmms.connectionpool.ConnectionStatus;
import com.king.gmms.connectionpool.sessionthread.SessionThread;
import com.king.gmms.domain.ConnectionInfo;
import com.king.gmms.domain.ModuleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.ha.systemmanagement.SystemSession;
import com.king.gmms.util.SystemConstants;

import java.util.List;

/**
 * <p>
 * Title:
 * </p>
 * 
 * <p>
 * Description:
 * </p>
 * 
 * <p>
 * Copyright: Copyright (c) 2006
 * </p>
 * 
 * <p>
 * Company:
 * </p>
 * 
 * @author not attributable
 * @version 1.0
 */
public class SystemClientSessionThread implements Runnable, SessionThread {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SystemClientSessionThread.class);
	private SystemSession systemSession = null;
	private static int maxRetryNumber = 3;
	private volatile int retryNum = 0;
	private static int maxEnquireLinkNum = 3;
	private static long connectionInterval = 1000L;
	private static long enquireLinkTime = 30000L;
	private static long maxConnectionSilentTime = 300 * 1000;
	private static long recoveryTime = 60000L;
	private String moduleName = "";
	private Object mutex = new Object();
	private String serverAddress = null;
	private int serverPort = 0;
	private int addressIndex = 0;
	private int failedAddressNum = 0;
	private long lastRecoverTime = -1;
	private ModuleManager moduleManager = null;
	private String module = null;
	private static int exceptionCount = 0;

	public SystemClientSessionThread(String name, SystemSession systemSession) {
		if (name != null && name.length() > 0) {
			this.moduleName = name;
		}
		if (systemSession != null) {
			this.systemSession = systemSession;
		}
		module = System.getProperty("module");
		moduleManager = ModuleManager.getInstance();
		ConnectionInfo conInfo = moduleManager.getConnectionInfoOfMasterMGT();
		serverAddress = conInfo.getURL();
		init();
	}

	private void init() {
		maxRetryNumber = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MaxRetryTimes", "2"));
		maxRetryNumber = maxRetryNumber < 0 ? 1 : maxRetryNumber;
		maxEnquireLinkNum = Integer.parseInt(moduleManager.getClusterProperty(
				"SystemManager.MaxEnquireLinkNumber", "3"));
		maxEnquireLinkNum = maxEnquireLinkNum < 3 ? 3 : maxEnquireLinkNum;
		connectionInterval = Long.parseLong(moduleManager.getClusterProperty(
				"SystemManager.ReconnectInterval", "1")) * 1000;
		connectionInterval = connectionInterval < 10 * 1000 ? 10 * 1000
				: connectionInterval;
		enquireLinkTime = Long.parseLong(moduleManager.getClusterProperty(
				"SystemManager.EnquireLinkTime", "30")) * 1000;
		enquireLinkTime = enquireLinkTime < 10 * 1000 ? 10 * 1000
				: enquireLinkTime;
		maxConnectionSilentTime = Long
				.parseLong(moduleManager.getClusterProperty(
						"SystemManager.ConnectionSilentTime", "100")) * 1000;
		maxConnectionSilentTime = maxConnectionSilentTime < 100 * 1000 ? 100 * 1000
				: maxConnectionSilentTime;
		recoveryTime = Long.parseLong(moduleManager.getClusterProperty(
				"SystemManager.RecoveryTime", "60")) * 1000;
		recoveryTime = recoveryTime < 60 * 1000 ? 60 * 1000 : recoveryTime;
	}

	public void start() {
		Thread thread = new Thread(A2PThreadGroup.getInstance(), this,
				moduleName + "_CliSessionTd");
		thread.start();
	}

	public void run() {
		while (systemSession.isKeepRunning()) {
			try {
				switch (systemSession.getStatus()) {
				case INITIAL:
					if (systemSession.create() && systemSession.open()) {
						log
								.warn(
										"The session({}) status is set: CONNECT and the original status is INITIAL",
										moduleName);
						systemSession.setStatus(ConnectionStatus.CONNECT);
						retryNum = 0;
						failedAddressNum = 0;
						exceptionCount = 0;
						break;
					} else {
						systemSession.stop();
						continue;
					}
				case RETRY:
					Thread.sleep(connectionInterval);
					retryNum++;
					if (systemSession.create() && systemSession.open()) {
						log
								.warn(
										"The session({}) status is set: CONNECT and the original status is RETRY",
										moduleName);
						systemSession.setStatus(ConnectionStatus.CONNECT);
						retryNum = 0;
						failedAddressNum = 0;
						exceptionCount = 0;
						break;
					} else {
						systemSession.stop();
						continue;
					}
				case DISCONNECT:
					Thread.sleep(connectionInterval);
					retryNum++;
					if (systemSession.create() && systemSession.open()) {
						log
								.warn(
										"The session({}) status is set: RECOVER and the original status is DISCONNECT",
										moduleName);
						systemSession.setStatus(ConnectionStatus.RECOVER);
						lastRecoverTime = System.currentTimeMillis();
						retryNum = 0;
						failedAddressNum = 0;
						exceptionCount = 0;
						break;
					} else {
						systemSession.stop();
						continue;
					}
				}

				if (System.currentTimeMillis()
						- systemSession.getLastActivity() > maxConnectionSilentTime) {
					systemSession.stop();
				} else if (lastRecoverTime > 0
						&& ConnectionStatus.RECOVER.equals(systemSession
								.getStatus())
						&& System.currentTimeMillis() - lastRecoverTime > recoveryTime) {
					systemSession.setStatus(ConnectionStatus.CONNECT);
					lastRecoverTime = -1;
				} else if (System.currentTimeMillis()
						- systemSession.getLastActivity() >= enquireLinkTime) {
					int count = systemSession.activeTest();
					if (count >= maxEnquireLinkNum) {
						log
								.warn(
										" {}alive requests didn't receive response, so close the connection.",
										count);
						systemSession.exceptionHandler(count);
					} else {
						Thread.sleep(enquireLinkTime
								+ GmmsUtility.getInstance()
										.getEnquireLinkResponseTiem());
					}
				} else {
					Thread.sleep(enquireLinkTime
							+ GmmsUtility.getInstance()
									.getEnquireLinkResponseTiem());
				}
			} catch (IOException e) {
				log.error(e.getMessage());
				exceptionCount++;
				systemSession.exceptionHandler(exceptionCount);
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
	}

	public void stopThread() {
		switch (systemSession.getStatus()) {
		case INITIAL:
			log
					.warn(
							"The session({}) status is set:  RETRY and the original status is INITIAL",
							moduleName);
			systemSession.setStatus(ConnectionStatus.RETRY);
			break;
		case CONNECT:
			log
					.warn(
							"The session({}) status is set:  RETRY and the original status is CONNECT",
							moduleName);
			systemSession.setStatus(ConnectionStatus.RETRY);
			break;
		case RECOVER:
			log
					.warn(
							"The session({}) status is set:  DISCONNECT and the original status is RECOVER",
							moduleName);
			systemSession.setStatus(ConnectionStatus.DISCONNECT);
			lastRecoverTime = -1;
			break;
		case RETRY:
			if (retryNum >= maxRetryNumber) {
				retryNextAddress();
				if (failedAddressNum >= 2) {
					log
							.warn(
									"The session({}) status is set:  DISCONNECT and the original status is RETRY",
									moduleName);
					systemSession.setStatus(ConnectionStatus.DISCONNECT);
				}
			}
			break;
		case DISCONNECT:
			if (retryNum >= maxRetryNumber) {
				retryNextAddress();
			}
			break;
		default:
			break;
		}
	}

	public void retryNextAddress() {
		String moduleType = this.moduleManager.getModuleType(module);
		if (SystemConstants.MGT_MODULE_TYPE.equalsIgnoreCase(moduleType)) {
			failedAddressNum++;
			return;
		}
		synchronized (mutex) {
			List<ModuleConnectionInfo> mgtModules = moduleManager
					.getConnectionInfo4MGT();
			int mgtSize = mgtModules.size();
			if (mgtSize < 2) {
				log.warn("The {} is down with Non-HA mode.", systemSession.getName());
				return;
			}
			log.warn("The {} is down", systemSession.getName());

			if (addressIndex >= mgtSize - 1) {
				addressIndex = 0;
			} else {
				addressIndex++;
			}
			retryNum = 0;
			failedAddressNum++;
			serverAddress = mgtModules.get(addressIndex).getURL();
			serverPort = mgtModules.get(addressIndex).getSysPort();
			String moduleName = mgtModules.get(addressIndex).getModuleName();
			systemSession.setServerAddress(serverAddress);
			systemSession.setPort(serverPort);
			systemSession.setName(moduleName);
//			systemSession.restartSenders();
			log.warn("Try {}", moduleName);
		}
	}
	
	public void interrupt(){
		
	}
}
