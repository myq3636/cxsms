package com.king.gmms.client;

import java.util.ArrayList;
import java.util.List;

import com.king.framework.SystemLogger;
import com.king.gmms.customerconnectionfactory.InternalAgentConnectionFactory;
import com.king.gmms.customerconnectionfactory.PeeringTcp2ClientFactory;
import com.king.gmms.domain.A2PCustomerInfo;
import com.king.gmms.domain.A2PMultiConnectionInfo;
import com.king.gmms.domain.A2PSingleConnectionInfo;
import com.king.gmms.domain.ModuleManager;

public class PeeringTcp2Client extends AbstractClient {
	private static SystemLogger log = SystemLogger
			.getSystemLogger(PeeringTcp2Client.class);
	private final String PROTOCOL_NAME = "Peering2";
	private InternalAgentConnectionFactory agentFactory = null;
	private PeeringTcp2ClientFactory customerFactory = null;

	public PeeringTcp2Client() {
		customerFactory = PeeringTcp2ClientFactory.getInstance();
	}

	private void initConnectionFactory() {
		try {
			ArrayList<Integer> alSsid = gmmsUtility.getCustomerManager()
					.getSsidByProtocol(PROTOCOL_NAME);
			if (alSsid != null && alSsid.size() > 0) {
				A2PCustomerInfo ci;
				for (int i = 0; i < alSsid.size(); i++) {
					int ssid = alSsid.get(i);
					log.trace("{} get ssid:{}", PROTOCOL_NAME, ssid);
					ci = gmmsUtility.getCustomerManager().getCustomerBySSID(
							ssid);
					/**
					 * single connection type
					 */
					if (ci.getConnectionType() == 1) {
						if ((((A2PSingleConnectionInfo) ci).isChlInit())) {
							customerFactory.initConnectionFactory(ssid, ci
									.getConnectionType());
						}
					} else {
						if (((A2PMultiConnectionInfo) ci).isInit()) {
							if(log.isInfoEnabled()){
								log.info("Start the client {} directly.", ssid);
							}
							customerFactory.initConnectionFactory(ssid, ci.getConnectionType());
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
		if (!initSystemManagement()) {
			log.warn("module register failed!");
		}
		startAgentConnection();
		agentListener.start();
		initConnectionFactory();
		return true;
	}

	public boolean stopService() {
		if (canHandover || isEnableSysMgt) {
			systemSession.moduleStop();
			systemListener.stop();
			if (systemSession != null) {
				systemSession.shutdown();
			}
		}
		agentListener.stop();
		return false;
	}
}
