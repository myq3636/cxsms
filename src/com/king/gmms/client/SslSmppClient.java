package com.king.gmms.client;

import java.util.ArrayList;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.connectionpool.systemmanagement.ConnectionManagementForFunction;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.customerconnectionfactory.SslSmppClientFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ModuleManager;
import com.king.gmms.routing.ADSServerMonitor;
import com.king.gmms.throttle.ReportInMsgCountTimer;
import com.king.gmms.throttle.ResetDynamicCustInThresholdTimer;
import com.king.message.gmms.GmmsMessage;

public class SslSmppClient extends AbstractClient {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(SslSmppClient.class);
	private final String PROTOCOL_NAME = "SSLSMPP";
	private InternalAgentConnectionFactory agentFactory = null;
	private SslSmppClientFactory customerFactory = null;
	private ReportInMsgCountTimer reportInMsgCountTimer = null;
	private ResetDynamicCustInThresholdTimer resetDynamicCustInThresholdTimer = null;

	public SslSmppClient() {
		customerFactory = SslSmppClientFactory.getInstance();
	}

	private void initConnectionFactory() {
		try {
			ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
					.getSsidByProtocol(PROTOCOL_NAME);
			if (alSsid != null && alSsid.size() > 0) {
				A2PCustomerInfo ci;
				for (int i = 0; i < alSsid.size(); i++) {
					int ssid = alSsid.get(i);
					log.trace("{} get ssid: {}", PROTOCOL_NAME, ssid);
					ci = gmmsUtility.getCustomerManager().getCustomerBySSID(
							ssid);
					/**
					 * single connection type
					 */
					if (ci.getConnectionType() == 1) {
						if ((((A2PSingleConnectionInfo) ci).isChlInit())
								&& ctm.inCurrentA2P(ctm.getConnectedRelay(ssid,
										GmmsMessage.AIC_MSG_TYPE_TEXT))) {
							customerFactory.initConnectionFactory(ssid, ci
									.getConnectionType());
						}
					} else {
						if (((A2PMultiConnectionInfo) ci).isInit()
								&& ctm.inCurrentA2P(ctm.getConnectedRelay(ssid,
										GmmsMessage.AIC_MSG_TYPE_TEXT))) {
							if(log.isInfoEnabled()){
								log.info("Start the client {} directly.", ssid);
							}
							customerFactory.initConnectionFactory(ssid, ci
									.getConnectionType());
						}
					}
				}
			} // end of size > 0
			else {
				log.info("No client is started directly.");
			}
		} catch (Exception ex) {
			log.debug(ex, ex);
		}
	}

	/**
	 * start agent message queue and listener
	 */
	private void startAgentConnection() {
		// start MessageQueue of InternalAgent
		agentFactory = InternalAgentConnectionFactory.getInstance();
		agentFactory.setCustomerFactory(customerFactory);
		ModuleManager moduleManager = ModuleManager.getInstance();
		List<String> moduleNameList = moduleManager.getRouterModules();
		if (moduleNameList != null) {
			for (String routerModuleName : moduleNameList) {
				agentFactory.initInternalConnectionFactory(routerModuleName);
			}
		}
	}

	public boolean startService() {
		initConnectionFactory();
		//ADSServerMonitor.getInstance().start();// start thread to monitor the DNS server connection
		if (!initSystemManagement()) {
			log.warn("module register failed!");
		}
		customerFactory.initializeSession();
		
		if((canHandover || isEnableSysMgt) && systemSession != null){
			reportInMsgCountTimer = new ReportInMsgCountTimer(systemSession, 
					gmmsUtility.getReportModuleIncomingMsgCountInterval());
			reportInMsgCountTimer.startTimer("reportInMsgCountTimer");
			resetDynamicCustInThresholdTimer = 
				new ResetDynamicCustInThresholdTimer(gmmsUtility.getDynamicCustInThresholdExipreTime()/3);
			resetDynamicCustInThresholdTimer.startTimer("resetDynamicCustInThresholdTimer");
		}
		
		startAgentConnection();
		agentListener.start();
		return true;
	}

	public boolean stopService() {
		if (canHandover || isEnableSysMgt) {
			beforeStop();
			systemListener.stop();
			if (systemSession != null) {
				systemSession.shutdown();
			}
		}
		agentListener.stop();
		return false;
	}

	/**
	 * send stop request
	 */
	public void beforeStop() {
		if (canHandover || isEnableSysMgt) {
			reportInMsgCountTimer.stopTimer();
			resetDynamicCustInThresholdTimer.stopTimer();
			ConnectionManagementForFunction systemManager = customerFactory.getSystemManager();
			boolean flag = systemManager.moduleStop(module);
			if (flag) {
				systemSession.moduleStop();
			}
		}
	}
}
